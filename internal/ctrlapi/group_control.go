package ctrlapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// GroupOperation is supplied only after the orchestration caller has verified
// an operation-bound fence for the entire old world. This transport client
// validates identity, not whether infrastructure fencing actually happened.
type GroupOperation struct {
	CheckpointID   string
	OperationUID   string
	SourceWorldUID string
	EvidenceRef    string
	WorldSize      int64
	// Producer generation from the immutable full-group checkpoint metadata,
	// not the Kubernetes RestorePlan or FluidCRMigration generation.
	CheckpointGeneration int64
}

type GroupControlResult struct {
	CheckpointID       string            `json:"checkpointID"`
	OperationUID       string            `json:"operationUID"`
	Prepared           bool              `json:"prepared"`
	State              string            `json:"state"`
	Generation         int64             `json:"generation"`
	CheckpointPointers map[string]string `json:"checkpointPointers"`
}

var groupIdentifier = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)

// PrepareGroup binds immutable per-rank artifacts to a fenced operation.
// The caller must keep restored targets stopped until preparation succeeds.
func (c *Client) PrepareGroup(ctx context.Context, podIP string, port int, timeout time.Duration, operation GroupOperation) (GroupControlResult, error) {
	return c.groupControl(ctx, podIP, port, timeout, "/prepare-group", operation)
}

// ResumeGroup releases only a prepared operation. Old payloads fail closed:
// neither a 404 nor any other failure falls back to the unscoped /resume API.
func (c *Client) ResumeGroup(ctx context.Context, podIP string, port int, timeout time.Duration, operation GroupOperation) (GroupControlResult, error) {
	return c.groupControl(ctx, podIP, port, timeout, "/resume-group", operation)
}

func (c *Client) groupControl(ctx context.Context, podIP string, port int, timeout time.Duration, path string, op GroupOperation) (GroupControlResult, error) {
	var zero GroupControlResult
	if !groupIdentifier.MatchString(op.CheckpointID) || !groupIdentifier.MatchString(op.OperationUID) ||
		strings.TrimSpace(op.SourceWorldUID) == "" || strings.TrimSpace(op.EvidenceRef) == "" {
		return zero, fmt.Errorf("full-group control requires checkpoint, operation, source world and fence evidence identities")
	}
	if op.WorldSize <= 0 || op.CheckpointGeneration <= 0 {
		return zero, fmt.Errorf("full-group control requires expected world size and producer checkpoint generation")
	}
	if net.ParseIP(podIP) == nil {
		return zero, fmt.Errorf("invalid pod IP")
	}
	if port <= 0 {
		port = DefaultCtrlPort
	}
	if port > 65535 {
		return zero, fmt.Errorf("invalid control port")
	}
	if timeout <= 0 || timeout > 300*time.Second {
		timeout = 300 * time.Second
	}
	payload, err := json.Marshal(map[string]interface{}{
		"all": true, "checkpointID": op.CheckpointID, "operationUID": op.OperationUID,
		"sourceFenceProof": map[string]interface{}{
			"allRanksFenced": true, "checkpointID": op.CheckpointID, "operationUID": op.OperationUID,
			"sourceWorldUID": op.SourceWorldUID, "evidenceRef": op.EvidenceRef,
		},
	})
	if err != nil {
		return zero, err
	}
	ctx, cancel := context.WithTimeout(ctx, timeout+10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+net.JoinHostPort(podIP, strconv.Itoa(port))+path, bytes.NewReader(payload))
	if err != nil {
		return zero, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return zero, fmt.Errorf("group control API: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
	if err != nil || len(body) > 1<<20 {
		return zero, fmt.Errorf("group control response unreadable or too large: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		return zero, fmt.Errorf("group control API status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var result GroupControlResult
	if err := json.Unmarshal(body, &result); err != nil {
		return zero, fmt.Errorf("invalid group control response: %w", err)
	}
	if result.CheckpointID != op.CheckpointID || result.OperationUID != op.OperationUID ||
		!result.Prepared || result.Generation != op.CheckpointGeneration || int64(len(result.CheckpointPointers)) != op.WorldSize {
		return zero, fmt.Errorf("group control response lacks matching operation evidence")
	}
	if result.State != "prepared" && result.State != "resuming" && result.State != "completed" {
		return zero, fmt.Errorf("unexpected group control state %q", result.State)
	}
	if path == "/resume-group" && result.State != "completed" {
		return zero, fmt.Errorf("group resume is not completed")
	}
	for rank := 0; rank < len(result.CheckpointPointers); rank++ {
		if !strings.HasPrefix(result.CheckpointPointers[strconv.Itoa(rank)], "/") {
			return zero, fmt.Errorf("group response must cover contiguous ranks with absolute artifact paths")
		}
	}
	return result, nil
}
