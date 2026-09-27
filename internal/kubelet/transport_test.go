package kubelet

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func testOptions(t *testing.T, srv *httptest.Server) (Options, string) {
	t.Helper()
	dir := t.TempDir()
	ca, token := filepath.Join(dir, "ca.pem"), filepath.Join(dir, "token")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(token, []byte("first"), 0600); err != nil {
		t.Fatal(err)
	}
	host, portText, err := net.SplitHostPort(strings.TrimPrefix(srv.URL, "https://"))
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatal(err)
	}
	return Options{CAFile: ca, TokenFile: token, Port: port}, host
}

func TestVerifiedTLSAndTokenRotation(t *testing.T) {
	var tokens []string
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tokens = append(tokens, r.Header.Get("Authorization"))
		_, _ = io.WriteString(w, `{"items":["/var/lib/kubelet/checkpoints/snapshot.tar"]}`)
	}))
	defer srv.Close()
	opts, host := testOptions(t, srv)
	c, err := NewClientWithOptions(opts)
	if err != nil {
		t.Fatal(err)
	}
	if c.httpClient.Transport.(*http.Transport).TLSClientConfig.InsecureSkipVerify {
		t.Fatal("verification disabled by default")
	}
	for _, token := range []string{"first", "rotated"} {
		if err := os.WriteFile(opts.TokenFile, []byte(token), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Checkpoint(context.Background(), host, "ns", "pod", "container", time.Second); err != nil {
			t.Fatal(err)
		}
	}
	if len(tokens) != 2 || tokens[0] != "Bearer first" || tokens[1] != "Bearer rotated" {
		t.Fatalf("tokens=%v", tokens)
	}
	if err := os.WriteFile(opts.TokenFile, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Checkpoint(context.Background(), host, "ns", "pod", "container", time.Second); err == nil {
		t.Fatal("empty token accepted")
	}
}

func TestTLSRejectsUnknownCAAndRedirects(t *testing.T) {
	hits := 0
	destination := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits++; w.WriteHeader(200) }))
	defer destination.Close()
	source := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, destination.URL, http.StatusTemporaryRedirect)
	}))
	defer source.Close()
	opts, host := testOptions(t, source)
	c, err := NewClientWithOptions(opts)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Checkpoint(context.Background(), host, "ns", "pod", "container", time.Second); err == nil {
		t.Fatal("redirect accepted")
	}
	if hits != 0 {
		t.Fatal("redirect leaked request")
	}
	c.httpClient.Transport.(*http.Transport).TLSClientConfig.RootCAs = nil
	c.httpClient.Transport.(*http.Transport).CloseIdleConnections()
	if _, err := c.Checkpoint(context.Background(), host, "ns", "pod", "container", time.Second); err == nil {
		t.Fatal("untrusted certificate accepted")
	}
	opts.InsecureSkipVerify = true
	opts.CAFile = "missing"
	if _, err := NewClientWithOptions(opts); err != nil {
		t.Fatalf("explicit lab override: %v", err)
	}
	opts.InsecureSkipVerify = false
	if _, err := NewClientWithOptions(opts); err == nil {
		t.Fatal("missing CA accepted")
	}
}

func TestCheckpointRejectsBadPaths(t *testing.T) {
	for _, body := range []string{`{"items":[""]}`, `{"items":["/tmp/file"]}`, `{"items":["/var/lib/kubelet/checkpoints/../bad"]}`, `{"error":"boom","items":["/var/lib/kubelet/checkpoints/a.tar"]}`} {
		if _, err := parseCheckpointPath([]byte(body)); err == nil {
			t.Fatalf("accepted %s", body)
		}
	}
}

func TestMountedCARotationFailsClosedWithoutRestart(t *testing.T) {
	hits := 0
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		_, _ = io.WriteString(w, `{"items":["/var/lib/kubelet/checkpoints/snapshot.tar"]}`)
	}))
	defer srv.Close()
	opts, host := testOptions(t, srv)
	c, err := NewClientWithOptions(opts)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint := func() error {
		_, err := c.Checkpoint(context.Background(), host, "ns", "pod", "container", time.Second)
		return err
	}
	if err := checkpoint(); err != nil {
		t.Fatal(err)
	}
	original, err := os.ReadFile(opts.CAFile)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(opts.CAFile, []byte("invalid CA"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := checkpoint(); err == nil {
		t.Fatal("invalid rotated CA used old trust")
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cert := &x509.Certificate{SerialNumber: big.NewInt(99), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	otherCA := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if err := os.WriteFile(opts.CAFile, otherCA, 0600); err != nil {
		t.Fatal(err)
	}
	if err := checkpoint(); err == nil {
		t.Fatal("removed CA remained trusted through pooled connection")
	}
	if hits != 1 {
		t.Fatalf("untrusted requests reached endpoint: %d", hits)
	}
	if err := os.WriteFile(opts.CAFile, original, 0600); err != nil {
		t.Fatal(err)
	}
	if err := checkpoint(); err != nil {
		t.Fatalf("recovery after CA rotation: %v", err)
	}
	if hits != 2 {
		t.Fatalf("successful requests: %d", hits)
	}
}
