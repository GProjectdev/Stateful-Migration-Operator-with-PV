/*
Copyright 2026 Leehun.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package kubelet provides a thin client over the kubelet checkpoint API used
// to take CRIU container checkpoints. The kubelet writes the resulting archive
// to CheckpointBasePath on the pod's host node.
package kubelet

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
)

const (
	// CheckpointBasePath is where the kubelet writes CRIU checkpoint archives
	// on the host node.
	CheckpointBasePath = "/var/lib/kubelet/checkpoints"

	// DefaultKubeletPort is the kubelet read/write HTTPS server port.
	DefaultKubeletPort = 10250

	// ServiceAccountTokenPath is the in-cluster service-account token used to
	// authenticate to the kubelet.
	ServiceAccountTokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	ServiceAccountCAPath    = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
)

// Client calls the kubelet checkpoint API.
type Client struct {
	httpClient *http.Client
	tokenFile  string
	port       int
}

// checkpointResponse models the kubelet checkpoint API response, which carries
// the absolute paths of the produced archives.
type checkpointResponse struct {
	Items []string `json:"items"`
	Error string   `json:"error"`
}

// Options configures member-local kubelet transport. InsecureSkipVerify is
// an explicit lab-only escape hatch; CAFile must trust kubelet serving certs.
type Options struct {
	CAFile             string
	InsecureSkipVerify bool
	TokenFile          string
	Port               int
}

// NewClient verifies TLS using the service-account CA and reloads its token
// for every request. Use Options.CAFile for a separate kubelet serving CA.
func NewClient() (*Client, error) {
	return NewClientWithOptions(Options{})
}

func NewClientWithOptions(opts Options) (*Client, error) {
	if opts.TokenFile == "" {
		opts.TokenFile = ServiceAccountTokenPath
	}
	if opts.Port == 0 {
		opts.Port = DefaultKubeletPort
	}
	if opts.Port < 1 || opts.Port > 65535 {
		return nil, fmt.Errorf("invalid kubelet port")
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: opts.InsecureSkipVerify}
	if !opts.InsecureSkipVerify {
		if opts.CAFile == "" {
			opts.CAFile = ServiceAccountCAPath
		}
		pem, err := os.ReadFile(opts.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read kubelet CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("kubelet CA contains no certificates")
		}
		tlsConfig.RootCAs = pool
	}
	if _, err := readToken(opts.TokenFile); err != nil {
		return nil, err
	}
	return &Client{
		httpClient: &http.Client{
			Timeout:       310 * time.Second,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse },
			Transport: &http.Transport{
				TLSClientConfig:     tlsConfig,
				TLSHandshakeTimeout: 10 * time.Second,
			},
		},
		tokenFile: opts.TokenFile,
		port:      opts.Port,
	}, nil
}

func readToken(file string) (string, error) {
	data, err := os.ReadFile(file)
	if err != nil {
		return "", fmt.Errorf("read service account token: %w", err)
	}
	token := strings.TrimSpace(string(data))
	if token == "" {
		return "", fmt.Errorf("service account token is empty")
	}
	return token, nil
}

// Checkpoint invokes POST /checkpoint/{namespace}/{pod}/{container} on the
// kubelet of the node identified by hostIP and returns the absolute path of the
// checkpoint archive on that node.
func (c *Client) Checkpoint(ctx context.Context, hostIP, namespace, pod, container string, timeout time.Duration) (string, error) {
	if hostIP == "" {
		return "", fmt.Errorf("host IP is empty for pod %s/%s", namespace, pod)
	}
	if net.ParseIP(hostIP) == nil {
		return "", fmt.Errorf("invalid host IP")
	}
	if timeout <= 0 || timeout > 300*time.Second {
		timeout = 300 * time.Second
	}
	secs := int(timeout.Seconds())
	endpoint := fmt.Sprintf("https://%s/checkpoint/%s/%s/%s?timeout=%d", net.JoinHostPort(hostIP, strconv.Itoa(c.port)), url.PathEscape(namespace), url.PathEscape(pod), url.PathEscape(container), secs)

	reqCtx, cancel := context.WithTimeout(ctx, timeout+10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, endpoint, nil)
	if err != nil {
		return "", fmt.Errorf("build kubelet request: %w", err)
	}
	token, err := readToken(c.tokenFile)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("call kubelet checkpoint API: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", fmt.Errorf("read kubelet response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("kubelet checkpoint API status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	return parseCheckpointPath(body)
}

// parseCheckpointPath extracts the first checkpoint archive path from the
// kubelet response body.
func parseCheckpointPath(body []byte) (string, error) {
	var parsed checkpointResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return "", fmt.Errorf("parse kubelet response %q: %w", strings.TrimSpace(string(body)), err)
	}
	if parsed.Error != "" || len(parsed.Items) != 1 || strings.TrimSpace(parsed.Items[0]) == "" {
		return "", fmt.Errorf("kubelet response contained no checkpoint path: %s", strings.TrimSpace(string(body)))
	}
	if !strings.HasPrefix(parsed.Items[0], CheckpointBasePath+"/") || path.Clean(parsed.Items[0]) != parsed.Items[0] {
		return "", fmt.Errorf("invalid checkpoint archive path")
	}
	return parsed.Items[0], nil
}
