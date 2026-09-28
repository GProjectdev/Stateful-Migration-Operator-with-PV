package v1alpha1

import (
	"fmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"path"
	"regexp"
)

var groupIdentifier = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)

type GroupNodeProvisionRef struct {
	Name       string `json:"name"`
	UID        string `json:"uid"`
	InstanceID string `json:"instanceID"`
}
type GroupSourcePod struct {
	Rank             int64                  `json:"rank"`
	PodName          string                 `json:"podName"`
	PodUID           string                 `json:"podUID"`
	NodeName         string                 `json:"nodeName"`
	NodeProvisionRef *GroupNodeProvisionRef `json:"nodeProvisionRef,omitempty"`
}
type GroupRestoreSpec struct {
	OperationUID   string           `json:"operationUID"`
	SourceWorldUID string           `json:"sourceWorldUID"`
	WorldSize      int64            `json:"worldSize"`
	SharedPVC      string           `json:"sharedPVC"`
	CheckpointRoot string           `json:"checkpointRoot"`
	SourcePods     []GroupSourcePod `json:"sourcePods"`
}
type GroupControlStatus struct {
	VolumeServer         string       `json:"volumeServer,omitempty"`
	VolumePath           string       `json:"volumePath,omitempty"`
	VolumePVCUID         string       `json:"volumePVCUID,omitempty"`
	VolumePVUID          string       `json:"volumePVUID,omitempty"`
	OperationUID         string       `json:"operationUID"`
	CheckpointID         string       `json:"checkpointID"`
	CheckpointGeneration int64        `json:"checkpointGeneration,omitempty"`
	PrepareJobUID        string       `json:"prepareJobUID,omitempty"`
	PreparedAt           *metav1.Time `json:"preparedAt,omitempty"`
	ResumeJobUID         string       `json:"resumeJobUID,omitempty"`
	ResumedAt            *metav1.Time `json:"resumedAt,omitempty"`
}

func CopyGroupRestore(in *GroupRestoreSpec) *GroupRestoreSpec {
	if in == nil {
		return nil
	}
	out := *in
	out.SourcePods = append([]GroupSourcePod(nil), in.SourcePods...)
	for i := range out.SourcePods {
		if in.SourcePods[i].NodeProvisionRef != nil {
			ref := *in.SourcePods[i].NodeProvisionRef
			out.SourcePods[i].NodeProvisionRef = &ref
		}
	}
	return &out
}
func copyGroupControl(in *GroupControlStatus) *GroupControlStatus {
	if in == nil {
		return nil
	}
	out := *in
	if in.PreparedAt != nil {
		out.PreparedAt = in.PreparedAt.DeepCopy()
	}
	if in.ResumedAt != nil {
		out.ResumedAt = in.ResumedAt.DeepCopy()
	}
	return &out
}
func ValidateGroup(g *GroupRestoreSpec, partial *PartialRestoreSpec, ref WorkloadReference, pods []RestorePod) error {
	if g == nil {
		return nil
	}
	if partial != nil || ref.Kind != "StatefulSet" || ref.APIVersion != "apps/v1" || g.SourceWorldUID != ref.UID || !groupIdentifier.MatchString(g.OperationUID) || !groupIdentifier.MatchString(g.SourceWorldUID) {
		return fmt.Errorf("groupRestore requires exclusive full StatefulSet world and operation UID")
	}
	if g.WorldSize < 1 || g.WorldSize > 64 || int64(len(pods)) != g.WorldSize || int64(len(g.SourcePods)) != g.WorldSize {
		return fmt.Errorf("groupRestore must cover every source and target rank")
	}
	if g.SharedPVC == "" || len(validation.IsDNS1123Subdomain(g.SharedPVC)) != 0 || g.CheckpointRoot != "/checkpoint" {
		return fmt.Errorf("groupRestore requires sharedPVC mounted at /checkpoint")
	}
	ranks, names, uids := map[int64]bool{}, map[string]bool{}, map[string]bool{}
	for _, p := range pods {
		if p.Rank < 0 || p.Rank >= g.WorldSize || ranks[p.Rank] || p.TargetPod != fmt.Sprintf("%s-%d", ref.Name, p.Rank) {
			return fmt.Errorf("group target ranks must be contiguous stable ordinals")
		}
		ranks[p.Rank] = true
	}
	for _, p := range g.SourcePods {
		if p.Rank < 0 || p.Rank >= g.WorldSize || names[p.PodName] || uids[p.PodUID] || p.PodName != fmt.Sprintf("%s-%d", ref.Name, p.Rank) || p.PodUID == "" || p.NodeName == "" {
			return fmt.Errorf("group current source identities must cover every ordinal exactly once")
		}
		names[p.PodName], uids[p.PodUID] = true, true
		if n := p.NodeProvisionRef; n != nil && (n.Name == "" || n.UID == "" || n.InstanceID == "") {
			return fmt.Errorf("incomplete source NodeProvision identity")
		}
	}
	for _, p := range pods {
		for _, a := range p.Archives {
			if a.DurableRef == "" || a.SHA256 == "" || a.TargetPath != path.Join("/var/lib/kubelet/checkpoints", a.SHA256+".tar") {
				return fmt.Errorf("group restore requires immutable exported archives")
			}
		}
	}
	return nil
}
