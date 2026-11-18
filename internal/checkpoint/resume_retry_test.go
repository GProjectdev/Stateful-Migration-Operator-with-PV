package checkpoint

import (
	"context"
	api "github.com/GProjectdev/Stateful-Migration-Operator-with-PV/api/fluidcr/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"testing"
)

func TestResumeFailureRetryGuards(t *testing.T) {
	for _, name := range []string{"allowed", "disabled-parent", "wrong-parent-uid", "checkpoint-failed", "partial", "missing-checkpoint", "pod-failed", "stale-generation", "missing-archive", "already-resumed"} {
		t.Run(name, func(t *testing.T) {
			parent, child := failedScheduledFixture()
			parent.Spec.Schedule.Enabled = true
			child.Status.Conditions = []metav1.Condition{{Type: conditionReady, Status: metav1.ConditionFalse, Reason: "ResumeFailed", ObservedGeneration: 1}}
			for i := range child.Status.Pods {
				p := &child.Status.Pods[i]
				p.CheckpointID = child.Annotations[AnnotationCheckpointID]
				p.AppCheckpointResult = "ready"
				p.Phase = api.PodPhaseContainerCheckpointed
				p.CheckpointFiles = []api.CheckpointFile{{CheckpointID: p.CheckpointID, FilePath: "/saved/checkpoint.tar"}}
			}
			want := name == "allowed"
			switch name {
			case "disabled-parent":
				parent.Spec.Schedule.Enabled = false
			case "wrong-parent-uid":
				parent.UID = "other-parent"
			case "checkpoint-failed":
				child.Status.Conditions[0].Reason = "CheckpointFailed"
			case "partial":
				child.Spec.PartialCheckpoint = &api.PartialCheckpointSpec{}
			case "missing-checkpoint":
				child.Status.Pods[0].CheckpointID = ""
			case "pod-failed":
				child.Status.Pods[0].Phase = api.PodPhaseFailed
			case "stale-generation":
				child.Status.ObservedGeneration = 2
			case "missing-archive":
				child.Status.Pods[0].CheckpointFiles = nil
			case "already-resumed":
				for i := range child.Status.Pods {
					child.Status.Pods[i].Phase = api.PodPhaseResumed
				}
			}
			c := fake.NewClientBuilder().WithScheme(newTestScheme(t)).WithObjects(parent).Build()
			r := &FluidCRMigrationReconciler{Client: c, APIReader: c}
			got, err := r.retryFailedResumeAllowed(context.Background(), child)
			if err != nil || got != want {
				t.Fatalf("retry=%v err=%v want=%v", got, err, want)
			}
		})
	}
}
