package member

import (
	"context"
	"crypto/sha256"
	"fmt"
	api "github.com/GProjectdev/Stateful-Migration-Operator-with-PV/api/v1alpha1"
	"github.com/GProjectdev/Stateful-Migration-Operator-with-PV/internal/groupcontract"
	appsv1 "k8s.io/api/apps/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const preparedWorkloadAnnotation = "migration.dcnlab.com/group-prepare-reconcile"

// Admission must observe persisted status before the workload wakes.
// The annotation is the durable acknowledgement across controller restarts.
func (r *Reconciler) wakePreparedWorkload(ctx context.Context, key client.ObjectKey) error {
	var plan api.RestorePlan
	if err := r.Reader.Get(ctx, key, &plan); err != nil {
		return client.IgnoreNotFound(err)
	}
	if plan.Spec.GroupRestore == nil || plan.Spec.TargetCluster != r.ClusterName ||
		!plan.DeletionTimestamp.IsZero() || plan.Status.Phase != "Prepared" ||
		groupcontract.TargetVerified(&plan) || !groupPrepared(&plan) ||
		plan.Spec.WorkloadRef.Kind != "StatefulSet" {
		return nil
	}
	if err := groupSourceReceipt(&plan); err != nil {
		return err
	}
	var sts appsv1.StatefulSet
	if err := r.Reader.Get(ctx, client.ObjectKey{Namespace: plan.Namespace, Name: plan.Spec.WorkloadRef.Name}, &sts); err != nil {
		return client.IgnoreNotFound(err)
	}
	if !sts.DeletionTimestamp.IsZero() {
		return nil
	}
	origin := sts.Labels[WorkloadUIDLabel]
	if origin == "" {
		origin = string(sts.UID)
	}
	if plan.Spec.WorkloadRef.UID == "" || origin != plan.Spec.WorkloadRef.UID || sts.Spec.Template.Labels[WorkloadUIDLabel] != origin {
		return fmt.Errorf("prepared workload origin UID mismatch")
	}
	receipt := plan.Status.GroupControl
	token := fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprintf("%s/%d/%s/%s/%s/%d", plan.UID, plan.Generation, receipt.OperationUID, receipt.PrepareJobUID, receipt.CheckpointID, receipt.CheckpointGeneration))))
	if sts.Annotations[preparedWorkloadAnnotation] == token {
		return nil
	}
	before := sts.DeepCopy()
	if sts.Annotations == nil {
		sts.Annotations = map[string]string{}
	}
	sts.Annotations[preparedWorkloadAnnotation] = token
	// Reject a patch if the workload was replaced or concurrently modified.
	return r.Client.Patch(ctx, &sts, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
}
