package ctrlapi

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestCheckpointRejectsIncompleteWorkerResults(t *testing.T) {
	for _, body := range []string{`{}`, `{"results":{}}`, `{"results":{"1":"permission-denied"}}`, `{"results":{"1":"checkpoint-ready","2":"timeout-waiting-lock"}}`, `{"results":{"1":"checkpoint-signalled"}}`} {
		t.Run(body, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, body) }))
			defer srv.Close()
			host, port := listenerHostPort(t, srv.URL)
			if _, err := NewClient().Checkpoint(context.Background(), host, port, time.Second, "mig-round-001"); err == nil {
				t.Fatal("incomplete checkpoint accepted")
			}
		})
	}
}

func TestControlRedirectAndCancellation(t *testing.T) {
	destinationCalls := 0
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { destinationCalls++ }))
	defer destination.Close()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, destination.URL, http.StatusTemporaryRedirect)
	}))
	defer srv.Close()
	host, port := listenerHostPort(t, srv.URL)
	if _, err := NewClient().Checkpoint(context.Background(), host, port, time.Second, "mig-round-001"); err == nil || destinationCalls != 0 {
		t.Fatal("redirect followed")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := NewClient().Checkpoint(ctx, host, port, time.Second, "mig-round-001"); err == nil {
		t.Fatal("cancellation ignored")
	}
}
