package checkpoint

import (
	"context"
	fluidcr "github.com/GProjectdev/Stateful-Migration-Operator-with-PV/api/fluidcr/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"testing"
)

func TestDisabledScheduleCancelsOnlyUnstartedPinnedChild(t *testing.T) {
	for _, name := range []string{"unstarted", "started", "pod-status", "wrong-uid", "enabled", "wrong-parent"} {
		t.Run(name, func(t *testing.T) {
			s := newTestScheme(t)
			parent := newTestMigration()
			parent.Name = "paused"
			parent.UID = "parent-uid"
			parent.Annotations = nil
			parent.Spec.Schedule = &fluidcr.MigrationScheduleSpec{Enabled: false, IntervalSeconds: 60}
			child := scheduledChildFor(parent)
			child.UID = "child-uid"
			child.Status.Phase = fluidcr.PhasePending
			parent.Status.CurrentRun = scheduledRunReference(child, false)
			switch name {
			case "started":
				now := metav1.Now()
				child.Status.StartTime = &now
			case "pod-status":
				child.Status.Pods = []fluidcr.PodMigrationStatus{{PodName: "trainer-0"}}
			case "wrong-uid":
				parent.Status.CurrentRun.UID = "other"
			case "enabled":
				parent.Spec.Schedule.Enabled = true
			case "wrong-parent":
				child.Annotations[AnnotationScheduledParentUID] = "other-parent"
			}
			c := fake.NewClientBuilder().WithScheme(s).WithObjects(parent, child).WithStatusSubresource(&fluidcr.FluidCRMigration{}).Build()
			r := &FluidCRMigrationReconciler{Client: c, APIReader: c, Scheme: s}
			cancelled, err := r.cancelUnstartedScheduledChild(context.Background(), child)
			if err != nil {
				t.Fatal(err)
			}
			if cancelled != (name == "unstarted") {
				t.Fatalf("cancelled=%v", cancelled)
			}
			got := &fluidcr.FluidCRMigration{}
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(child), got); err != nil {
				t.Fatal(err)
			}
			if cancelled && (got.Status.Phase != fluidcr.PhaseFailed || got.Status.CompletionTime == nil || got.Status.StartTime != nil || len(got.Status.Pods) != 0) {
				t.Fatalf("unsafe cancellation receipt: %+v", got.Status)
			}
		})
	}
}

func TestCancelledUnstartedChildAllowsSchedulePause(t *testing.T) {
	s := newTestScheme(t)
	parent := newTestMigration()
	parent.Name = "paused-live-regression"
	parent.UID = "parent-uid"
	parent.Annotations = nil
	parent.Finalizers = []string{FinalizerName}
	parent.Spec.Schedule = &fluidcr.MigrationScheduleSpec{Enabled: false, IntervalSeconds: 60}
	child := scheduledChildFor(parent)
	child.UID = "child-uid"
	child.Status.Phase = fluidcr.PhasePending
	parent.Status.CurrentRun = scheduledRunReference(child, false)
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(parent, child).WithStatusSubresource(&fluidcr.FluidCRMigration{}).Build()
	r := &FluidCRMigrationReconciler{Client: c, APIReader: c, Scheme: s}
	if cancelled, err := r.cancelUnstartedScheduledChild(context.Background(), child); err != nil || !cancelled {
		t.Fatalf("cancel=%v err=%v", cancelled, err)
	}
	if _, err := r.reconcileSchedule(context.Background(), parent); err != nil {
		t.Fatal(err)
	}
	got := &fluidcr.FluidCRMigration{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(parent), got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != fluidcr.PhasePaused {
		t.Fatalf("parent did not pause: %+v", got.Status)
	}
	now := metav1.Now()
	child.Status.StartTime = &now
	if cancelledBeforeActuation(child) {
		t.Fatal("started child accepted as unstarted cancellation")
	}
}
