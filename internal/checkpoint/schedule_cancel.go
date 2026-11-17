package checkpoint

import (
	"context"
	fluidcr "github.com/GProjectdev/Stateful-Migration-Operator-with-PV/api/fluidcr/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// A disabled schedule may cancel only a UID-pinned child that has never
// begun actuation. Started checkpoints still finish through the normal path.
func (r *FluidCRMigrationReconciler) cancelUnstartedScheduledChild(ctx context.Context, child *fluidcr.FluidCRMigration) (bool, error) {
	if child.Spec.Schedule != nil || child.Status.StartTime != nil || len(child.Status.Pods) != 0 || (child.Status.Phase != "" && child.Status.Phase != fluidcr.PhasePending) {
		return false, nil
	}
	name := child.Annotations[AnnotationScheduledParentName]
	if name == "" || child.UID == "" {
		return false, nil
	}
	parent := &fluidcr.FluidCRMigration{}
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	if err := reader.Get(ctx, client.ObjectKey{Namespace: child.Namespace, Name: name}, parent); err != nil {
		return false, err
	}
	ref := parent.Status.CurrentRun
	if parent.Spec.Schedule == nil || parent.Spec.Schedule.Enabled || parent.DeletionTimestamp != nil || !controlledBy(child, parent) || ref == nil || ref.Name != child.Name || ref.UID != string(child.UID) || ref.CheckpointID != child.Annotations[AnnotationCheckpointID] {
		return false, nil
	}
	_, err := r.markFailed(ctx, child, scheduleCancelledBeforeActuation)
	return true, err
}

const scheduleCancelledBeforeActuation = "scheduled checkpoint cancelled before actuation: parent schedule disabled"

func cancelledBeforeActuation(child *fluidcr.FluidCRMigration) bool {
	return child.Status.Phase == fluidcr.PhaseFailed && child.Status.Message == scheduleCancelledBeforeActuation && child.Status.ObservedGeneration == child.Generation && child.Status.CompletionTime != nil && child.Status.StartTime == nil && len(child.Status.Pods) == 0
}
