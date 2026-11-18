package checkpoint

import (
	"context"
	api "github.com/GProjectdev/Stateful-Migration-Operator-with-PV/api/fluidcr/v1alpha1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
)

// Retry transport/resume only. Saved per-pod phases prevent another application
// or container checkpoint. A disabled parent must remain quiescent for recovery.
func (r *FluidCRMigrationReconciler) retryFailedResumeAllowed(ctx context.Context, m *api.FluidCRMigration) (bool, error) {
	if !terminalFailedFullChild(m) || m.Generation != 1 || !shouldResume(m) {
		return false, nil
	}
	c := meta.FindStatusCondition(m.Status.Conditions, conditionReady)
	if c == nil || c.Reason != "ResumeFailed" || c.ObservedGeneration != m.Generation {
		return false, nil
	}
	id := m.Annotations[AnnotationCheckpointID]
	if id == "" || len(m.Status.Pods) == 0 {
		return false, nil
	}
	pending := false
	for _, p := range m.Status.Pods {
		if p.PodUID == "" || p.CheckpointID != id || p.AppCheckpointResult == "" || len(p.CheckpointFiles) == 0 {
			return false, nil
		}
		if p.Phase != api.PodPhaseResumed && p.Phase != api.PodPhaseContainerCheckpointed {
			return false, nil
		}
		pending = pending || p.Phase != api.PodPhaseResumed
	}
	if !pending {
		return false, nil
	}
	if parentUID := m.Labels[LabelScheduledParent]; parentUID != "" {
		name := m.Annotations[AnnotationScheduledParentName]
		if name == "" {
			return false, nil
		}
		var parent api.FluidCRMigration
		reader := r.APIReader
		if reader == nil {
			reader = r.Client
		}
		if err := reader.Get(ctx, types.NamespacedName{Namespace: m.Namespace, Name: name}, &parent); err != nil {
			return false, err
		}
		if string(parent.UID) != parentUID || !parent.DeletionTimestamp.IsZero() || parent.Spec.Schedule == nil || !parent.Spec.Schedule.Enabled || verifiedFailedChildRetirement(&parent, m) {
			return false, nil
		}
	}
	return true, nil
}
