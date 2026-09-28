package groupcontract

import (
	"encoding/json"
	"fmt"
	api "github.com/GProjectdev/Stateful-Migration-Operator-with-PV/api/v1alpha1"
)

const FenceAnnotation = "migration.dcnlab.com/group-source-fence"
const TargetVerifiedAnnotation = "migration.dcnlab.com/group-target-verified"

func TargetVerified(plan *api.RestorePlan) bool {
	return plan.Spec.GroupRestore != nil && plan.Spec.RequestUID != "" && plan.Annotations[TargetVerifiedAnnotation] == plan.Spec.RequestUID
}

// Receipts are written only by the management controller after reading source
// status. RBAC must not grant workload users patch access to RestorePlans.
type FenceReceipt struct {
	RequestUID     string                     `json:"requestUID"`
	OperationUID   string                     `json:"operationUID"`
	SourceWorldUID string                     `json:"sourceWorldUID"`
	SourceCluster  string                     `json:"sourceCluster"`
	VolumeServer   string                     `json:"volumeServer"`
	VolumePath     string                     `json:"volumePath"`
	Fences         []api.SourcePodFenceStatus `json:"fences"`
}

func ValidateFences(plan *api.RestorePlan, fences []api.SourcePodFenceStatus) error {
	g := plan.Spec.GroupRestore
	if g == nil || len(fences) != len(g.SourcePods) {
		return fmt.Errorf("incomplete group source fences")
	}
	seen := map[string]bool{}
	for _, source := range g.SourcePods {
		found := false
		for _, f := range fences {
			if f.PodName != source.PodName {
				continue
			}
			if seen[f.PodName] || f.SourcePodUID != source.PodUID || f.Phase != "SourceGone" || f.ObservedGeneration != plan.Generation || f.DeleteRequestedAt == nil || f.GoneObservedAt == nil || f.GoneObservedAt.Before(f.DeleteRequestedAt) {
				return fmt.Errorf("source fence receipt is not current and UID-bound")
			}
			seen[f.PodName], found = true, true
		}
		if !found {
			return fmt.Errorf("source fence missing for %s", source.PodName)
		}
	}
	return nil
}
func Encode(plan *api.RestorePlan, fences []api.SourcePodFenceStatus) (string, error) {
	if err := ValidateFences(plan, fences); err != nil {
		return "", err
	}
	g := plan.Spec.GroupRestore
	if plan.Status.GroupControl == nil || plan.Status.GroupControl.VolumeServer == "" || plan.Status.GroupControl.VolumePath == "" {
		return "", fmt.Errorf("source volume identity missing")
	}
	b, err := json.Marshal(FenceReceipt{RequestUID: plan.Spec.RequestUID, OperationUID: g.OperationUID, SourceWorldUID: g.SourceWorldUID, SourceCluster: plan.Spec.SourceCluster, VolumeServer: plan.Status.GroupControl.VolumeServer, VolumePath: plan.Status.GroupControl.VolumePath, Fences: fences})
	return string(b), err
}
func Decode(plan *api.RestorePlan) ([]api.SourcePodFenceStatus, error) {
	g := plan.Spec.GroupRestore
	if g == nil {
		return nil, fmt.Errorf("groupRestore required")
	}
	var r FenceReceipt
	if err := json.Unmarshal([]byte(plan.Annotations[FenceAnnotation]), &r); err != nil {
		return nil, fmt.Errorf("waiting for management source fence receipt")
	}
	if r.RequestUID != plan.Spec.RequestUID || r.OperationUID != g.OperationUID || r.SourceWorldUID != g.SourceWorldUID || r.SourceCluster != plan.Spec.SourceCluster {
		return nil, fmt.Errorf("source fence receipt identity mismatch")
	}
	if err := ValidateFences(plan, r.Fences); err != nil {
		return nil, err
	}
	return r.Fences, nil
}
