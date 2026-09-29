package checkpoint

import (
	"context"
	api "github.com/GProjectdev/Stateful-Migration-Operator-with-PV/api/fluidcr/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"testing"
	"time"
)

func TestReadinessBlocksAllSignalsAndTimesOut(t *testing.T) {
	for _, expired := range []bool{false, true} {
		s := newTestScheme(t)
		mig := newTestMigration()
		if expired {
			stamp := metav1.NewTime(time.Now().Add(-time.Hour))
			mig.Status.StartTime = &stamp
		}
		c := fake.NewClientBuilder().WithScheme(s).WithObjects(newTestDeployment(), newTestReplicaSet("default"), newTestPod("p0", "10.0.0.1", "192.168.0.1"), mig).WithStatusSubresource(&api.FluidCRMigration{}).Build()
		fc := &fakeCtrl{readinessBlocked: true}
		fk := &fakeKubelet{}
		r := &FluidCRMigrationReconciler{Client: c, Scheme: s, CtrlClient: fc, KubeletClient: fk}
		key := types.NamespacedName{Namespace: "default", Name: "mig"}
		for i := 0; i < 3; i++ {
			if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
				t.Fatal(err)
			}
		}
		if calls, _ := fc.counts(); calls != 0 || fk.count() != 0 {
			t.Fatal("checkpoint signalled before all workers ready")
		}
		var got api.FluidCRMigration
		if err := c.Get(context.Background(), key, &got); err != nil {
			t.Fatal(err)
		}
		if expired && got.Status.Phase != api.PhaseFailed {
			t.Fatalf("no bounded timeout: %+v", got.Status)
		}
		if !expired {
			fc.readinessBlocked = false
			got = reconcileToCompletion(t, r, c)
			if got.Status.Phase != api.PhaseCompleted {
				t.Fatal(got.Status)
			}
		}
	}
}
