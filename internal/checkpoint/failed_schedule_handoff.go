package checkpoint

import (
	"encoding/json"
	"reflect"
	"strconv"
	"time"

	api "github.com/GProjectdev/Stateful-Migration-Operator-with-PV/api/fluidcr/v1alpha1"
)

const verifiedScheduledRetirements = "training.dcnlab.com/verified-scheduled-retirements"

type scheduledRetirement struct {
	PolicyUID             string            `json:"policyUID"`
	SourceWorldUID        string            `json:"sourceWorldUID"`
	ChildUID              string            `json:"childUID"`
	ChildGeneration       int64             `json:"childGeneration"`
	CheckpointID          string            `json:"checkpointID"`
	PodUIDs               map[string]string `json:"podUIDs"`
	RestoreRequestUID     string            `json:"restoreRequestUID"`
	OperationUID          string            `json:"operationUID"`
	SourceFenceEvidenceID string            `json:"sourceFenceEvidenceID"`
	VerifiedAt            string            `json:"verifiedAt"`
}

func terminalFailedFullChild(child *api.FluidCRMigration) bool {
	return child.Spec.PartialCheckpoint == nil && child.Status.Phase == api.PhaseFailed && child.UID != "" && child.Generation > 0 && child.Status.ObservedGeneration == child.Generation && child.Status.CompletionTime != nil && child.DeletionTimestamp.IsZero()
}

// Paused acknowledges that the controller issued no further checkpoint work.
// A finalized Failed child is never a usable archive or a resumed workload.
// Enabled schedules keep blocking on it until management certifies an exact
// full-group Verified restore with source fencing, using this UID-bound receipt.
func activeScheduledChildForParent(parent *api.FluidCRMigration, children []api.FluidCRMigration) *api.FluidCRMigration {
	for i := range children {
		child := &children[i]
		if durableFullCheckpointComplete(child) || cancelledBeforeActuation(child) {
			continue
		}
		if terminalFailedFullChild(child) && ((!parent.Spec.Schedule.Enabled) || verifiedFailedChildRetirement(parent, child)) {
			continue
		}
		return child
	}
	return nil
}

func verifiedFailedChildRetirement(parent, child *api.FluidCRMigration) bool {
	if !terminalFailedFullChild(child) {
		return false
	}
	var records map[string]scheduledRetirement
	if json.Unmarshal([]byte(parent.Annotations[verifiedScheduledRetirements]), &records) != nil {
		return false
	}
	r, ok := records[string(child.UID)]
	if !ok || r.ChildUID != string(child.UID) || r.ChildGeneration != child.Generation || r.CheckpointID == "" || r.CheckpointID != child.Annotations[AnnotationCheckpointID] || r.PolicyUID == "" || r.PolicyUID != parent.Labels["training.dcnlab.com/policy-uid"] || r.SourceWorldUID == "" || r.SourceWorldUID != string(parent.Spec.WorkloadRef.UID) || r.SourceWorldUID != string(child.Spec.WorkloadRef.UID) || r.RestoreRequestUID == "" || r.OperationUID == "" || r.SourceFenceEvidenceID == "" {
		return false
	}
	if _, err := time.Parse(time.RFC3339Nano, r.VerifiedAt); err != nil {
		return false
	}
	pods := map[string]string{}
	seen := map[string]bool{}
	for _, p := range child.Status.Pods {
		rank := strconv.FormatInt(p.Rank, 10)
		if p.Rank < 0 || p.PodUID == "" || seen[p.PodUID] || pods[rank] != "" {
			return false
		}
		pods[rank] = p.PodUID
		seen[p.PodUID] = true
	}
	if len(pods) == 0 || !reflect.DeepEqual(pods, r.PodUIDs) {
		return false
	}
	for rank := 0; rank < len(pods); rank++ {
		if pods[strconv.Itoa(rank)] == "" {
			return false
		}
	}
	return true
}
