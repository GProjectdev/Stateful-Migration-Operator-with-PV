package member

import (
	"context"
	"fmt"
	api "github.com/GProjectdev/Stateful-Migration-Operator-with-PV/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"reflect"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"testing"
)

type wakeClient struct {
	client.Client
	patches int
	fail    bool
}

func (c *wakeClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	c.patches++
	if c.fail {
		return fmt.Errorf("injected patch failure")
	}
	return c.Client.Patch(ctx, obj, patch, opts...)
}

func TestPreparedWorkloadWake(t *testing.T) {
	for _, mode := range []string{"ready", "preparing", "stale", "uid", "deleting", "source", "fence", "retry"} {
		t.Run(mode, func(t *testing.T) {
			p, _, _, _, _ := groupFixture()
			p.UID = "plan"
			p.Generation = 1
			now := metav1.Now()
			p.Status.Phase = "Prepared"
			p.Status.ObservedGeneration = 1
			p.Status.GroupControl = &api.GroupControlStatus{OperationUID: "operation", CheckpointID: "round", CheckpointGeneration: 10, PrepareJobUID: "job", PreparedAt: &now}
			p.Status.SourceFences = []api.SourcePodFenceStatus{{PodName: "trainer-0", SourcePodUID: "old", ObservedGeneration: 1, Phase: "SourceGone", DeleteRequestedAt: &now, GoneObservedAt: &now}}
			sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "trainer", Namespace: p.Namespace, UID: "local-world", Labels: map[string]string{WorkloadUIDLabel: "world"}}}
			sts.Spec.Template.Labels = map[string]string{WorkloadUIDLabel: "world"}
			switch mode {
			case "preparing":
				p.Status.Phase = "Preparing"
			case "stale":
				p.Status.ObservedGeneration = 0
			case "uid":
				sts.Labels[WorkloadUIDLabel] = "different"
			case "deleting":
				sts.DeletionTimestamp = &now
				sts.Finalizers = []string{"test"}
			case "source":
				p.Spec.TargetCluster = "other"
			case "fence":
				p.Status.SourceFences = nil
			}
			scheme := runtime.NewScheme()
			if err := api.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			if err := appsv1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			c := &wakeClient{Client: fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&api.RestorePlan{}).WithObjects(p, sts).Build(), fail: mode == "retry"}
			r := NewReconciler(c, c, "target")
			key := client.ObjectKeyFromObject(p)
			err := r.wakePreparedWorkload(context.Background(), key)
			wantErr := mode == "uid" || mode == "fence" || mode == "retry"
			if (err != nil) != wantErr {
				t.Fatalf("error=%v", err)
			}
			if mode == "retry" {
				c.fail = false
				r = NewReconciler(c, c, "target")
				if err := r.wakePreparedWorkload(context.Background(), key); err != nil {
					t.Fatal(err)
				}
			}
			var got appsv1.StatefulSet
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(sts), &got); err != nil {
				t.Fatal(err)
			}
			wantWake := mode == "ready" || mode == "retry"
			if (got.Annotations[preparedWorkloadAnnotation] != "") != wantWake {
				t.Fatalf("annotations=%v", got.Annotations)
			}
			if !reflect.DeepEqual(sts.Spec, got.Spec) {
				t.Fatal("workload spec changed")
			}
			if wantWake {
				count := c.patches
				if err := NewReconciler(c, c, "target").wakePreparedWorkload(context.Background(), key); err != nil {
					t.Fatal(err)
				}
				if count != c.patches {
					t.Fatal("repeated wake after restart")
				}
			}
		})
	}
}
