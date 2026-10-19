package checkpoint

import (
	"context"
	"testing"

	fluidcr "github.com/GProjectdev/Stateful-Migration-Operator-with-PV/api/fluidcr/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func TestGroupFallbackCancelAcknowledgesWithoutRuntimeOrResume(t *testing.T) {
	for _, phase := range []fluidcr.MigrationPhase{fluidcr.PhasePending, fluidcr.PhaseFailed, fluidcr.PhaseCompleted} {
		t.Run(string(phase), func(t *testing.T) {
			m := fallbackCancelFixture()
			m.Status.Phase = phase
			m.Status.ObservedGeneration = m.Generation
			m.Status.Pods = []fluidcr.PodMigrationStatus{{PodName: "rank-1", PodUID: "survivor", AppCheckpointResult: "checkpoint-ready"}}
			c := fake.NewClientBuilder().WithScheme(newTestScheme(t)).WithStatusSubresource(m).WithObjects(m).Build()
			r := &FluidCRMigrationReconciler{Client: c} // nil runtime clients must not be invoked.
			key := client.ObjectKeyFromObject(m)
			for i := 0; i < 2; i++ {
				if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: key}); err != nil {
					t.Fatal(err)
				}
			}
			var got fluidcr.FluidCRMigration
			if err := c.Get(context.Background(), key, &got); err != nil {
				t.Fatal(err)
			}
			if got.Status.Phase != fluidcr.PhaseFailed || got.Status.Message != "group fallback checkpoint cancelled: operation-uid" || got.Status.ObservedGeneration != got.Generation || len(got.Status.Pods) != 1 {
				t.Fatalf("invalid cancellation receipt: %+v", got.Status)
			}
		})
	}
}

func TestGroupFallbackCancelRejectsUnboundOrResumingWork(t *testing.T) {
	for _, scenario := range []string{"uid", "role", "policy", "full", "resume", "implicit-resume"} {
		t.Run(scenario, func(t *testing.T) {
			m := fallbackCancelFixture()
			switch scenario {
			case "uid":
				m.Annotations["training.dcnlab.com/recovery-operation-uid"] = "different"
			case "role":
				m.Labels["training.dcnlab.com/role"] = "periodic"
			case "policy":
				delete(m.Labels, "training.dcnlab.com/policy-uid")
			case "full":
				m.Spec.PartialCheckpoint = nil
			case "resume":
				yes := true
				m.Spec.Resume = &yes
			case "implicit-resume":
				m.Spec.Resume = nil
			}
			r := &FluidCRMigrationReconciler{}
			if _, err := r.reconcileGroupFallbackCancel(context.Background(), m, "operation-uid"); err == nil {
				t.Fatal("unsafe cancellation accepted")
			}
		})
	}
}

func fallbackCancelFixture() *fluidcr.FluidCRMigration {
	m := newTestMigration()
	m.Labels = map[string]string{"training.dcnlab.com/managed-by": "hybridspotvm-system", "training.dcnlab.com/role": "replacement-checkpoint", "training.dcnlab.com/policy-uid": "policy-uid"}
	m.Annotations[groupFallbackCancelAnnotation] = "operation-uid"
	m.Annotations["training.dcnlab.com/recovery-operation"] = "operation"
	m.Annotations["training.dcnlab.com/recovery-operation-uid"] = "operation-uid"
	no := false
	m.Spec.Resume = &no
	m.Spec.PartialCheckpoint = &fluidcr.PartialCheckpointSpec{TargetRanks: []int64{0}}
	return m
}
