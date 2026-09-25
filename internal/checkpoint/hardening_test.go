package checkpoint

import (
	"context"
	fluidcr "github.com/GProjectdev/Stateful-Migration-Operator-with-PV/api/fluidcr/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"testing"
)

func TestStatefulSetRequiresCompleteOwnedSnapshot(t *testing.T) {
	for _, scenario := range []string{"complete", "missing", "unavailable", "stale", "foreign", "uninjected", "terminating", "rollout", "scaled"} {
		t.Run(scenario, func(t *testing.T) {
			replicas := int32(2)
			sts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{Name: "trainer", Namespace: "default", UID: "sts-uid", Generation: 1},
				Spec:       appsv1.StatefulSetSpec{Replicas: &replicas, Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "trainer"}}},
				Status:     appsv1.StatefulSetStatus{ObservedGeneration: 1, Replicas: 2, CurrentReplicas: 2, ReadyReplicas: 2, AvailableReplicas: 2, UpdatedReplicas: 2, CurrentRevision: "rev", UpdateRevision: "rev"},
			}
			p0, p1 := newTestPod("p0", "10.0.0.1", "192.168.0.1"), newTestPod("p1", "10.0.0.2", "192.168.0.2")
			for _, p := range []*corev1.Pod{p0, p1} {
				p.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(sts, appsv1.SchemeGroupVersion.WithKind("StatefulSet"))}
			}
			m := newTestMigration()
			m.Spec.WorkloadRef.Kind = "StatefulSet"
			objects := []client.Object{sts, p0, p1}
			switch scenario {
			case "missing":
				objects = objects[:2]
			case "unavailable":
				sts.Status.AvailableReplicas = 1
			case "stale":
				sts.Status.ObservedGeneration = 0
			case "foreign":
				p1.OwnerReferences[0].UID = "foreign"
			case "uninjected":
				p1.Annotations = nil
			case "terminating":
				now := metav1.Now()
				p1.DeletionTimestamp = &now
				p1.Finalizers = []string{"hold"}
			case "rollout":
				sts.Status.UpdateRevision = "next"
			case "scaled":
				replicas = 3
			}
			c := fake.NewClientBuilder().WithScheme(newTestScheme(t)).WithObjects(objects...).Build()
			r := &FluidCRMigrationReconciler{Client: c}
			pods, err := r.resolveTargetPods(context.Background(), m)
			if scenario == "complete" {
				if err != nil || len(pods) != 2 {
					t.Fatalf("complete snapshot: %v, %d", err, len(pods))
				}
			} else if err == nil {
				t.Fatalf("accepted partial snapshot: %d", len(pods))
			}
		})
	}
}

func TestRestartRejectsChangedOrMissingPodUID(t *testing.T) {
	for _, uid := range []string{"old-uid", ""} {
		t.Run("uid="+uid, func(t *testing.T) {
			m := newTestMigration()
			m.Spec.WorkloadRef = fluidcr.WorkloadReference{APIVersion: "v1", Kind: "Pod", Name: "p0"}
			m.Status = fluidcr.FluidCRMigrationStatus{Phase: fluidcr.PhaseContainerCheckpointing, ObservedGeneration: 1, Pods: []fluidcr.PodMigrationStatus{{PodName: "p0", PodUID: uid, Phase: fluidcr.PodPhaseAppCheckpointed, AppCheckpointResult: "ready"}}}
			s := newTestScheme(t)
			c := fake.NewClientBuilder().WithScheme(s).WithObjects(m, newTestPod("p0", "10.0.0.1", "192.168.0.1")).WithStatusSubresource(m).Build()
			fc, fk := &fakeCtrl{}, &fakeKubelet{}
			r := &FluidCRMigrationReconciler{Client: c, CtrlClient: fc, KubeletClient: fk}
			got := reconcileToCompletion(t, r, c)
			cp, rs := fc.counts()
			if got.Status.Phase != fluidcr.PhaseFailed || cp != 0 || rs != 0 || fk.count() != 0 {
				t.Fatalf("replacement received work: %+v cp=%d rs=%d", got.Status, cp, rs)
			}
		})
	}
}

func TestSaveStatusRejectsNewGenerationAndSpec(t *testing.T) {
	ctx := context.Background()
	for _, change := range []string{"generation", "spec", "uid"} {
		t.Run(change, func(t *testing.T) {
			m := newTestMigration()
			m.UID = "migration-uid"
			c := fake.NewClientBuilder().WithScheme(newTestScheme(t)).WithObjects(m).WithStatusSubresource(m).Build()
			var stale fluidcr.FluidCRMigration
			if err := c.Get(ctx, client.ObjectKeyFromObject(m), &stale); err != nil {
				t.Fatal(err)
			}
			latest := stale.DeepCopy()
			switch change {
			case "generation":
				latest.Generation++
			case "spec":
				latest.Spec.Container = "changed"
			case "uid":
				latest.UID = "replacement"
			}
			if err := c.Update(ctx, latest); err != nil {
				t.Fatal(err)
			}
			stale.Status.Phase = fluidcr.PhaseCompleted
			r := &FluidCRMigrationReconciler{Client: c}
			if err := r.saveStatus(ctx, &stale); err == nil {
				t.Fatal("stale work crossed object/spec boundary")
			}
			if err := c.Get(ctx, client.ObjectKeyFromObject(m), latest); err != nil {
				t.Fatal(err)
			}
			if latest.Status.Phase == fluidcr.PhaseCompleted {
				t.Fatal("persisted stale completion")
			}
		})
	}
}

func TestGenerationTwoDoesNotRetrigger(t *testing.T) {
	m := newTestMigration()
	m.Generation = 2
	c := fake.NewClientBuilder().WithScheme(newTestScheme(t)).WithObjects(m).WithStatusSubresource(m).Build()
	fc, fk := &fakeCtrl{}, &fakeKubelet{}
	r := &FluidCRMigrationReconciler{Client: c, CtrlClient: fc, KubeletClient: fk}
	got := reconcileToCompletion(t, r, c)
	if got.Status.Phase != fluidcr.PhaseFailed || fk.count() != 0 {
		t.Fatalf("generation two retriggered: %+v", got.Status)
	}
}
