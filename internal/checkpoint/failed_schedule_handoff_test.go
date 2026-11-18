package checkpoint

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	api "github.com/GProjectdev/Stateful-Migration-Operator-with-PV/api/fluidcr/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func failedScheduledFixture() (*api.FluidCRMigration, *api.FluidCRMigration) {
	parent := newTestMigration()
	parent.Name = "schedule-failed"
	parent.UID = "schedule-uid"
	parent.Generation = 1
	parent.Finalizers = []string{FinalizerName}
	parent.Labels = map[string]string{"training.dcnlab.com/policy-uid": "policy-uid"}
	parent.Annotations = nil
	parent.Spec.WorkloadRef.UID = "world-uid"
	parent.Spec.Schedule = &api.MigrationScheduleSpec{Enabled: false, IntervalSeconds: 60}
	child := scheduledChildFor(parent)
	child.UID = "failed-child"
	child.Generation = 1
	completed := metav1.NewTime(time.Now().Add(-time.Hour))
	child.Status = api.FluidCRMigrationStatus{ObservedGeneration: 1, Phase: api.PhaseFailed, CompletionTime: &completed, Pods: []api.PodMigrationStatus{{Rank: 0, PodUID: "old0"}, {Rank: 1, PodUID: "old1"}}}
	parent.Status.CurrentRun = scheduledRunReference(child, false)
	return parent, child
}

func TestFailedFullRoundPausesWithoutInventingSuccessAndNeedsVerifiedRetirementToRestart(t *testing.T) {
	parent, child := failedScheduledFixture()
	s := newTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(parent, child).WithStatusSubresource(&api.FluidCRMigration{}).Build()
	r := &FluidCRMigrationReconciler{Client: c, Scheme: s, CtrlClient: &fakeCtrl{}, KubeletClient: &fakeKubelet{}}
	key := client.ObjectKeyFromObject(parent)
	ctx := context.Background()
	req := reconcile.Request{NamespacedName: types.NamespacedName{Name: key.Name, Namespace: key.Namespace}}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	var got api.FluidCRMigration
	if err := c.Get(ctx, key, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != api.PhasePaused || got.Status.CurrentRun.Phase != api.PhaseFailed || got.Status.CurrentRun.Result == nil || got.Status.LastSuccessfulFullCheckpoint != nil {
		t.Fatalf("failed round must pause with retained failure, never success: %+v", got.Status)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(child), child); err != nil {
		t.Fatal(err)
	}
	if child.Status.Phase != api.PhaseFailed {
		t.Fatal("child failure was rewritten")
	}
	got.Spec.Schedule.Enabled = true
	if err := c.Update(ctx, &got); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, key, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != api.PhasePending || got.Status.CurrentRun.UID != "failed-child" {
		t.Fatalf("unverified failed child must still block new rounds: %+v", got.Status)
	}
	receipt := scheduledRetirement{PolicyUID: "policy-uid", SourceWorldUID: "world-uid", ChildUID: string(child.UID), ChildGeneration: 1, CheckpointID: child.Annotations[AnnotationCheckpointID], PodUIDs: map[string]string{"0": "old0", "1": "old1"}, RestoreRequestUID: "verified-restore", OperationUID: "operation", SourceFenceEvidenceID: "source-fence", VerifiedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	data, _ := json.Marshal(map[string]scheduledRetirement{string(child.UID): receipt})
	got.Annotations = map[string]string{verifiedScheduledRetirements: string(data)}
	if err := c.Update(ctx, &got); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, err := r.Reconcile(ctx, req); err != nil {
			t.Fatal(err)
		}
	}
	var children api.FluidCRMigrationList
	if err := c.List(ctx, &children, client.InNamespace(parent.Namespace), client.MatchingLabels{LabelScheduledParent: string(parent.UID)}); err != nil {
		t.Fatal(err)
	}
	if len(children.Items) != 2 {
		t.Fatalf("verified handoff did not permit a fresh checkpoint round: %d", len(children.Items))
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(child), child); err != nil {
		t.Fatal(err)
	}
	if child.Status.Phase != api.PhaseFailed {
		t.Fatal("retirement must retain the failed attempt")
	}
}

func TestVerifiedRetirementIsBoundToFailedUIDWorldGenerationAndAllRanks(t *testing.T) {
	parent, child := failedScheduledFixture()
	parent.Spec.Schedule.Enabled = true
	good := scheduledRetirement{PolicyUID: "policy-uid", SourceWorldUID: "world-uid", ChildUID: string(child.UID), ChildGeneration: 1, CheckpointID: child.Annotations[AnnotationCheckpointID], PodUIDs: map[string]string{"0": "old0", "1": "old1"}, RestoreRequestUID: "verified-restore", OperationUID: "operation", SourceFenceEvidenceID: "fence-proof", VerifiedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	for _, change := range []string{"valid", "missing-fence", "other-world", "wrong-generation", "missing-rank", "different-rank-uid", "wrong-child"} {
		t.Run(change, func(t *testing.T) {
			record := good
			record.PodUIDs = map[string]string{"0": "old0", "1": "old1"}
			switch change {
			case "missing-fence":
				record.SourceFenceEvidenceID = ""
			case "other-world":
				record.SourceWorldUID = "other"
			case "wrong-generation":
				record.ChildGeneration = 2
			case "missing-rank":
				delete(record.PodUIDs, "1")
			case "different-rank-uid":
				record.PodUIDs["1"] = "other"
			case "wrong-child":
				record.ChildUID = "recreated"
			}
			data, _ := json.Marshal(map[string]scheduledRetirement{string(child.UID): record})
			parent.Annotations = map[string]string{verifiedScheduledRetirements: string(data)}
			retired := activeScheduledChildForParent(parent, []api.FluidCRMigration{*child}) == nil
			if retired != (change == "valid") {
				t.Fatalf("retired=%v for %s", retired, change)
			}
		})
	}
	child.Status.Phase = api.PhaseAppCheckpointing
	parent.Spec.Schedule.Enabled = false
	if activeScheduledChildForParent(parent, []api.FluidCRMigration{*child}) == nil {
		t.Fatal("an actively checkpointing round must prevent pause acknowledgement")
	}
}
