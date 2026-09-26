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

// Package ctrlapi provides a client for the in-pod FluidCR control API exposed
// by the launcher (see fluidcr/ctrl.py). It triggers application-level
// checkpoints and releases checkpoint locks (resume).
package ctrlapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// DefaultCtrlPort is the default in-pod control-API port (FLUIDCR_CTRL_PORT).
const DefaultCtrlPort = 8298

// Client talks to the in-pod FluidCR control API over the pod IP.
type Client struct {
	httpClient *http.Client
}

// NewClient returns a control-API client.
func NewClient() *Client {
	return &Client{httpClient: &http.Client{
		Timeout:       310 * time.Second,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse },
	}}
}

// allRequest is the request body that targets every GPU-using worker in the pod.
type allRequest struct {
	All            bool    `json:"all"`
	Wait           bool    `json:"wait,omitempty"`
	TimeoutSeconds float64 `json:"timeoutSeconds,omitempty"`
	CheckpointID   string  `json:"checkpointID,omitempty"`
}

// response models the control-API JSON response.
type response struct {
	Results map[string]string `json:"results"`
	Error   string            `json:"error"`
}

// Checkpoint triggers an application checkpoint of all GPU-using workers in the
// pod and blocks (server-side) until the checkpoint locks are ready or the
// in-pod timeout elapses. It returns the per-worker result map.
func (c *Client) Checkpoint(ctx context.Context, podIP string, port int, timeout time.Duration, checkpointID string) (map[string]string, error) {
	return c.post(ctx, podIP, port, "/checkpoint", timeout, checkpointID)
}

// Resume releases all pending checkpoint locks in the pod so the workers
// continue. It is idempotent: pods without a pending lock report no-op.
func (c *Client) Resume(ctx context.Context, podIP string, port int, timeout time.Duration) (map[string]string, error) {
	return c.post(ctx, podIP, port, "/resume", timeout, "")
}

func (c *Client) post(ctx context.Context, podIP string, port int, path string, timeout time.Duration, checkpointID string) (map[string]string, error) {
	if net.ParseIP(podIP) == nil {
		return nil, fmt.Errorf("pod IP is empty")
	}
	if port <= 0 {
		port = DefaultCtrlPort
	}
	if port > 65535 {
		return nil, fmt.Errorf("invalid control port")
	}
	if timeout <= 0 || timeout > 300*time.Second {
		timeout = 300 * time.Second
	}
	url := fmt.Sprintf("http://%s%s", net.JoinHostPort(podIP, strconv.Itoa(port)), path)

	payloadData := allRequest{All: true}
	if path == "/checkpoint" {
		payloadData.Wait = true
		payloadData.TimeoutSeconds = timeout.Seconds()
		payloadData.CheckpointID = checkpointID
	}
	payload, err := json.Marshal(payloadData)
	if err != nil {
		return nil, fmt.Errorf("marshal control-API request: %w", err)
	}

	reqCtx, cancel := context.WithTimeout(ctx, timeout+10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("build control-API request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("call control API %s: %w", path, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("read control-API response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("control API %s status %d: %s", path, resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var parsed response
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, fmt.Errorf("parse control-API response %q: %w", strings.TrimSpace(string(body)), err)
	}
	if parsed.Error != "" {
		return nil, fmt.Errorf("control API %s error: %s", path, parsed.Error)
	}
	if path == "/checkpoint" && len(parsed.Results) == 0 {
		return nil, fmt.Errorf("checkpoint returned no workers")
	}
	for worker, result := range parsed.Results {
		ok := result == "checkpoint-ready"
		if path == "/resume" {
			ok = result == "removed" || result == "already-gone" || result == "lock-removed" || result == "no-lock"
		}
		if !ok {
			return nil, fmt.Errorf("control API %s worker %s: %s", path, worker, result)
		}
	}
	return parsed.Results, nil
}

// SummarizeResults renders a compact, deterministic summary of a per-worker
// result map, e.g. "2 checkpoint-ready, 1 no-such-process".
func SummarizeResults(results map[string]string) string {
	if len(results) == 0 {
		return "no workers"
	}
	counts := map[string]int{}
	for _, status := range results {
		counts[status]++
	}
	statuses := make([]string, 0, len(counts))
	for status := range counts {
		statuses = append(statuses, status)
	}
	sort.Strings(statuses)
	parts := make([]string, 0, len(statuses))
	for _, status := range statuses {
		parts = append(parts, fmt.Sprintf("%d %s", counts[status], status))
	}
	return strings.Join(parts, ", ")
}
