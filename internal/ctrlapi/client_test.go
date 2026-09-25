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

package ctrlapi

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

// listenerHostPort returns the host and port of an httptest server URL.
func listenerHostPort(t *testing.T, rawURL string) (string, int) {
	t.Helper()
	host, portStr, err := net.SplitHostPort(strings.TrimPrefix(rawURL, "http://"))
	if err != nil {
		t.Fatalf("split host port: %v", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("parse port: %v", err)
	}
	return host, port
}

func TestCheckpointAndResume(t *testing.T) {
	var gotPaths []string
	var gotBodies []map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var parsed map[string]any
		_ = json.Unmarshal(body, &parsed)
		gotPaths = append(gotPaths, r.URL.Path)
		gotBodies = append(gotBodies, parsed)
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/resume" {
			_, _ = w.Write([]byte(`{"results":{"1234":"lock-removed"}}`))
			return
		}
		_, _ = w.Write([]byte(`{"results":{"1234":"checkpoint-ready"}}`))
	}))
	defer srv.Close()

	host, port := listenerHostPort(t, srv.URL)
	c := NewClient()
	ctx := context.Background()

	results, err := c.Checkpoint(ctx, host, port, 2*time.Second)
	if err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if results["1234"] != "checkpoint-ready" {
		t.Errorf("results = %v, want 1234=checkpoint-ready", results)
	}
	if _, err := c.Resume(ctx, host, port, 2*time.Second); err != nil {
		t.Fatalf("resume: %v", err)
	}

	if len(gotPaths) != 2 || gotPaths[0] != "/checkpoint" || gotPaths[1] != "/resume" {
		t.Fatalf("paths = %v, want [/checkpoint /resume]", gotPaths)
	}
	if gotBodies[0]["wait"] != true || gotBodies[0]["timeoutSeconds"] != float64(2) {
		t.Fatalf("checkpoint must request bounded lock confirmation: %v", gotBodies[0])
	}
	if _, exists := gotBodies[1]["wait"]; exists {
		t.Fatal("resume unexpectedly waits for checkpoint locks")
	}
	for _, b := range gotBodies {
		if all, ok := b["all"].(bool); !ok || !all {
			t.Errorf("request body = %v, want {\"all\":true}", b)
		}
	}
}

func TestCheckpointServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"boom"}`))
	}))
	defer srv.Close()

	host, port := listenerHostPort(t, srv.URL)
	if _, err := NewClient().Checkpoint(context.Background(), host, port, time.Second); err == nil {
		t.Fatal("expected error on 500 response")
	}
}

func TestSummarizeResults(t *testing.T) {
	if got := SummarizeResults(nil); got != "no workers" {
		t.Errorf("empty: got %q, want 'no workers'", got)
	}
	// Statuses are summarized in alphabetical order.
	got := SummarizeResults(map[string]string{"1": "checkpoint-ready", "2": "checkpoint-ready", "3": "no-such-process"})
	want := "2 checkpoint-ready, 1 no-such-process"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}
