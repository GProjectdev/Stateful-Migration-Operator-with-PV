package suspension

import (
	"context"
	api "github.com/GProjectdev/Stateful-Migration-Operator-with-PV/api/v1alpha1"
	"github.com/GProjectdev/Stateful-Migration-Operator-with-PV/internal/groupcontract"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"testing"
	"time"
)

func TestGroupRestoreDoesNotBypassOrdinalVolumes(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*fixture)
		reject bool
	}{
		{name: "all volumes prepared"},
		{name: "missing PV operation", mutate: func(f *fixture) { a := f.rb.GetAnnotations(); delete(a, PVAnnotation); f.rb.SetAnnotations(a) }, reject: true},
		{name: "stale PV status", mutate: func(f *fixture) { f.pv.SetGeneration(2) }, reject: true},
		{name: "incomplete ordinal mapping", mutate: func(f *fixture) { _ = unstructured.SetNestedSlice(f.pv.Object, []interface{}{}, "spec", "volumes") }, reject: true},
		{name: "no ordinal claims", mutate: func(f *fixture) {
			f.sts.Spec.VolumeClaimTemplates = nil
			a := f.rb.GetAnnotations()
			delete(a, PVAnnotation)
			f.rb.SetAnnotations(a)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture()
			now := metav1.NewTime(time.Now())
			g := &api.GroupRestoreSpec{OperationUID: "operation", SourceWorldUID: "workload-uid", WorldSize: 2, SharedPVC: "shared", CheckpointRoot: "/checkpoint"}
			fences := []api.SourcePodFenceStatus{}
			for rank, pod := range f.request.Spec.Pods {
				f.request.Spec.Pods[rank].Rank = int64(rank)
				f.plan.Spec.Pods[rank].Rank = int64(rank)
				f.request.Spec.Pods[rank].TargetNode = pod.SourcePod + "-target"
				f.plan.Spec.Pods[rank].TargetNode = pod.SourcePod + "-target"
				for i := range f.request.Spec.Pods[rank].Archives {
					a := &f.request.Spec.Pods[rank].Archives[i]
					a.DurableRef = "file-store:demo/sha256/" + a.SHA256
					a.TargetPath = "/var/lib/kubelet/checkpoints/" + a.SHA256 + ".tar"
				}
				g.SourcePods = append(g.SourcePods, api.GroupSourcePod{Rank: int64(rank), PodName: pod.SourcePod, PodUID: pod.SourcePod + "-current", NodeName: pod.SourceNode})
				fences = append(fences, api.SourcePodFenceStatus{PodName: pod.SourcePod, SourcePodUID: pod.SourcePod + "-current", ObservedGeneration: 1, Phase: "SourceGone", DeleteRequestedAt: &now, GoneObservedAt: &now})
			}
			f.request.Spec.GroupRestore = g
			cpPods := f.cp.Object["status"].(map[string]interface{})["clusters"].([]interface{})[0].(map[string]interface{})["pods"].([]interface{})
			for _, raw := range cpPods {
				raw.(map[string]interface{})["checkpointFiles"].([]interface{})[0].(map[string]interface{})["exportedAt"] = now.Format(time.RFC3339)
			}
			f.plan.Spec.GroupRestore = api.CopyGroupRestore(g)
			control := &api.GroupControlStatus{OperationUID: "operation", CheckpointID: "round-001", CheckpointGeneration: 1, PrepareJobUID: "prepare", PreparedAt: &now, VolumeServer: "nfs", VolumePath: "/shared"}
			f.plan.Status.GroupControl = control
			f.plan.Status.Clusters[0].GroupControl = control
			receipt, err := groupcontract.Encode(f.plan, fences)
			if err != nil {
				t.Fatal(err)
			}
			f.plan.Annotations = map[string]string{groupcontract.FenceAnnotation: receipt}
			if tc.mutate != nil {
				tc.mutate(f)
			}
			err = check(context.Background(), f.client(t), f.rb)
			if (err != nil) != tc.reject {
				t.Fatalf("reject=%v err=%v", tc.reject, err)
			}
		})
	}
}
