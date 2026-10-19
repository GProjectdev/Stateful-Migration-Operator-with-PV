package checkpoint

import (
	"context"
	"fmt"

	fluidcr "github.com/GProjectdev/Stateful-Migration-Operator-with-PV/api/fluidcr/v1alpha1"
	ctrl "sigs.k8s.io/controller-runtime"
)

const groupFallbackCancelAnnotation = "training.dcnlab.com/group-fallback-cancel"

// Reconciliation for a migration is serialized. Acknowledging here means its
// previous workflow returned; keep survivors paused for the group fence.
func (r *FluidCRMigrationReconciler) reconcileGroupFallbackCancel(ctx context.Context, m *fluidcr.FluidCRMigration, operationUID string) (ctrl.Result, error) {
	if m.Labels["training.dcnlab.com/managed-by"] != "hybridspotvm-system" ||
		m.Labels["training.dcnlab.com/role"] != "replacement-checkpoint" ||
		m.Labels["training.dcnlab.com/policy-uid"] == "" ||
		m.Annotations["training.dcnlab.com/recovery-operation"] == "" ||
		operationUID == "" || operationUID != m.Annotations["training.dcnlab.com/recovery-operation-uid"] ||
		m.Spec.PartialCheckpoint == nil || m.Spec.Schedule != nil || m.Spec.Resume == nil || *m.Spec.Resume {
		return ctrl.Result{}, fmt.Errorf("group fallback cancellation requires a UID-bound non-resuming partial checkpoint")
	}
	message := "group fallback checkpoint cancelled: " + operationUID
	if m.Status.Phase == fluidcr.PhaseFailed && m.Status.ObservedGeneration == m.Generation && m.Status.Message == message {
		return ctrl.Result{}, nil
	}
	return r.markFailed(ctx, m, message)
}
