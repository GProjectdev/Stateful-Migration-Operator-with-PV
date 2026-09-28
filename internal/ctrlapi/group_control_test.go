package ctrlapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func groupOperationFixture() GroupOperation {
	return GroupOperation{CheckpointID: "round-1", OperationUID: "operation-1", SourceWorldUID: "world-1", EvidenceRef: "fence-1"}
}

func groupResponseFixture() GroupControlResult {
	return GroupControlResult{CheckpointID: "round-1", OperationUID: "operation-1", Prepared: true, State: "completed", Generation: 7, CheckpointPointers: map[string]string{"0": "/checkpoint/rank-0/rounds/round-1/latest.pt"}}
}

func TestGroupControlOperationContract(t *testing.T) {
	for _, path := range []string{"/prepare-group", "/resume-group"} {
		t.Run(path, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != path || r.Method != http.MethodPost {
					t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
				}
				var body map[string]interface{}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Fatal(err)
				}
				proof, ok := body["sourceFenceProof"].(map[string]interface{})
				if !ok || len(body) != 4 || len(proof) != 5 || body["all"] != true ||
					body["checkpointID"] != "round-1" || body["operationUID"] != "operation-1" ||
					proof["allRanksFenced"] != true || proof["checkpointID"] != body["checkpointID"] ||
					proof["operationUID"] != body["operationUID"] || proof["sourceWorldUID"] != "world-1" || proof["evidenceRef"] != "fence-1" {
					t.Errorf("unexpected payload: %#v", body)
				}
				_ = json.NewEncoder(w).Encode(groupResponseFixture())
			}))
			defer server.Close()
			host, port := listenerHostPort(t, server.URL)
			call := NewClient().PrepareGroup
			if path == "/resume-group" {
				call = NewClient().ResumeGroup
			}
			if _, err := call(context.Background(), host, port, time.Second, groupOperationFixture()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestGroupControlRejectsIncompleteOrStaleSuccess(t *testing.T) {
	for name, mutate := range map[string]func(*GroupControlResult){
		"wrong operation":   func(r *GroupControlResult) { r.OperationUID = "other" },
		"wrong checkpoint":  func(r *GroupControlResult) { r.CheckpointID = "other" },
		"not prepared":      func(r *GroupControlResult) { r.Prepared = false },
		"zero generation":   func(r *GroupControlResult) { r.Generation = 0 },
		"resume unfinished": func(r *GroupControlResult) { r.State = "prepared" },
		"missing ranks":     func(r *GroupControlResult) { r.CheckpointPointers = nil },
		"rank gap":          func(r *GroupControlResult) { r.CheckpointPointers = map[string]string{"1": "/artifact"} },
	} {
		t.Run(name, func(t *testing.T) {
			result := groupResponseFixture()
			mutate(&result)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _ = json.NewEncoder(w).Encode(result) }))
			defer server.Close()
			host, port := listenerHostPort(t, server.URL)
			if _, err := NewClient().ResumeGroup(context.Background(), host, port, time.Second, groupOperationFixture()); err == nil {
				t.Fatal("invalid success response accepted")
			}
		})
	}
}

func TestGroupControlNoLegacyFallback(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Path != "/resume-group" {
			t.Errorf("unsafe fallback: %s", r.URL.Path)
		}
		http.NotFound(w, r)
	}))
	defer server.Close()
	host, port := listenerHostPort(t, server.URL)
	if _, err := NewClient().ResumeGroup(context.Background(), host, port, time.Second, groupOperationFixture()); err == nil {
		t.Fatal("old payload accepted")
	}
	invalid := groupOperationFixture()
	invalid.EvidenceRef = ""
	if _, err := NewClient().PrepareGroup(context.Background(), host, port, time.Second, invalid); err == nil {
		t.Fatal("missing fence reference accepted")
	}
	if calls != 1 {
		t.Fatalf("unexpected attempts: %d", calls)
	}
}
