package checkpoint

import (
	"context"
	"errors"
	"testing"

	fluidcr "github.com/GProjectdev/Stateful-Migration-Operator-with-PV/api/fluidcr/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestDeletionUsesRecordedPodUIDWithoutWorkload(t *testing.T) {
	for _, scenario := range []string{"gone", "orphan", "recreated", "terminal", "no-checkpoint"} {
		t.Run(scenario, func(t *testing.T) {
			m := newTestMigration()
			m.Finalizers = []string{FinalizerName}
			m.Status.Pods = []fluidcr.PodMigrationStatus{{PodName: "p0", PodUID: "uid-p0", AppCheckpointResult: "checkpoint-ready"}}
			p := newTestPod("p0", "10.0.0.1", "192.168.0.1")
			objects := []client.Object{m}
			if scenario == "recreated" {
				p.UID = "new-uid"
			}
			if scenario == "terminal" {
				p.Status.Phase = corev1.PodSucceeded
			}
			if scenario == "no-checkpoint" {
				m.Status.Pods[0].AppCheckpointResult = ""
			}
			if scenario != "gone" {
				objects = append(objects, p)
			}
			c := fake.NewClientBuilder().WithScheme(newTestScheme(t)).WithObjects(objects...).Build()
			r := &FluidCRMigrationReconciler{Client: c}
			pods, err := r.resolveDeletionPods(context.Background(), m)
			if err != nil {
				t.Fatal(err)
			}
			want := 0
			if scenario == "orphan" {
				want = 1
			}
			if len(pods) != want {
				t.Fatalf("pods=%d want=%d", len(pods), want)
			}
			if want == 0 {
				if _, err := r.reconcileDelete(context.Background(), m); err != nil {
					t.Fatal(err)
				}
				if len(m.Finalizers) != 0 {
					t.Fatal("finalizer retained")
				}
			}
		})
	}
}

type unavailableDeletionClient struct{ client.Client }

func (c unavailableDeletionClient) Get(context.Context, client.ObjectKey, client.Object, ...client.GetOption) error {
	return errors.New("API unavailable")
}

func TestDeletionKeepsFinalizerOnAPIError(t *testing.T) {
	m := newTestMigration()
	m.Finalizers = []string{FinalizerName}
	m.Status.Pods = []fluidcr.PodMigrationStatus{{PodName: "p0", PodUID: "uid-p0", AppCheckpointResult: "ready"}}
	r := &FluidCRMigrationReconciler{Client: unavailableDeletionClient{}}
	if _, err := r.reconcileDelete(context.Background(), m); err == nil {
		t.Fatal("API error ignored")
	}
	if len(m.Finalizers) != 1 {
		t.Fatal("finalizer removed on API failure")
	}
}
