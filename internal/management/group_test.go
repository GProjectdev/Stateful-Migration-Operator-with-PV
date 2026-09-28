package management

import (
	"context"
	api "github.com/GProjectdev/Stateful-Migration-Operator-with-PV/api/v1alpha1"
	"github.com/GProjectdev/Stateful-Migration-Operator-with-PV/internal/groupcontract"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"testing"
	"time"
)

func groupRequestFixture() (*api.RestoreRequest, *unstructured.Unstructured) {
	r, c := fixture()
	r.Spec.TargetCluster = r.Spec.SourceCluster
	r.Spec.SourceFenced = false
	r.Spec.GroupRestore = &api.GroupRestoreSpec{OperationUID: "operation", SourceWorldUID: r.Spec.WorkloadRef.UID, WorldSize: 1, SharedPVC: "shared", CheckpointRoot: "/checkpoint", SourcePods: []api.GroupSourcePod{{Rank: 0, PodName: "db-0", PodUID: "current-source", NodeName: "source-node"}}}
	r.Spec.Pods[0].SourcePodUID = "historical-source"
	r.Spec.Pods[0].Archives[0].DurableRef = "file-store:default/sha256/" + r.Spec.Pods[0].Archives[0].SHA256
	c.Object["spec"].(map[string]interface{})["resume"] = true
	sourcePod(c)["podUID"] = "historical-source"
	sourcePod(c)["phase"] = "Resumed"
	sourcePod(c)["checkpointFiles"].([]interface{})[0].(map[string]interface{})["exportedAt"] = "2026-09-27T10:00:00Z"
	return r, c
}
func TestGroupHistoricalExportedCheckpoint(t *testing.T) {
	r, c := groupRequestFixture()
	if err := validateRequest(r); err != nil {
		t.Fatal(err)
	}
	if ok, err := validateCheckpoint(r, c); err != nil || !ok {
		t.Fatalf("historical completed round rejected: %v %v", ok, err)
	}
	partial := c.DeepCopy()
	_ = unstructured.SetNestedMap(partial.Object, map[string]interface{}{"targetRanks": []interface{}{int64(0)}}, "spec", "partialCheckpoint")
	if ok, err := validateCheckpoint(r, partial); err == nil && ok {
		t.Fatal("partial checkpoint accepted as full world")
	}
	delete(sourcePod(c)["checkpointFiles"].([]interface{})[0].(map[string]interface{}), "exportedAt")
	if ok, err := validateCheckpoint(r, c); err == nil && ok {
		t.Fatal("unexported group archive accepted")
	}
}
func TestGroupVerificationRequiresPostResumeProgressAndFence(t *testing.T) {
	for _, mode := range []string{"valid", "before-resume", "missing-fence"} {
		t.Run(mode, func(t *testing.T) {
			r, c := groupRequestFixture()
			p, err := desiredPlan(r, c)
			if err != nil {
				t.Fatal(err)
			}
			p.UID = "plan"
			p.Generation = 1
			now := metav1.Now()
			resume := metav1.NewTime(now.Add(-3 * time.Second))
			if mode == "before-resume" {
				resume = now
			}
			fences := []api.SourcePodFenceStatus{{PodName: "db-0", SourcePodUID: "current-source", ObservedGeneration: 1, Phase: "SourceGone", DeleteRequestedAt: &resume, GoneObservedAt: &resume}}
			if mode == "missing-fence" {
				fences = nil
			}
			p.Status.Clusters = []api.ClusterStatus{{ClusterName: r.Spec.TargetCluster, ObservedGeneration: 1, Phase: "Running", Pods: []api.PodStatus{{Name: "db-0", UID: "target", Phase: "Running"}}, SourceFences: fences, GroupControl: &api.GroupControlStatus{OperationUID: "operation", CheckpointID: r.Spec.CheckpointRef.CheckpointID, CheckpointGeneration: 7, PrepareJobUID: "prepare", PreparedAt: &resume, ResumeJobUID: "resume", ResumedAt: &resume}}}
			cl := testClient(t, r, c, p, trainingRuntime(r, "target", now))
			got := reconcile(t, cl, r)
			if mode == "valid" {
				if got.Status.Phase != "Verified" || got.Status.Verification == nil || !got.Status.Verification.SourceFenced || got.Status.GroupControl == nil {
					t.Fatalf("verification missing: %+v", got.Status)
				}
				if err := cl.Get(context.Background(), client.ObjectKeyFromObject(p), p); err != nil {
					t.Fatal(err)
				}
				if !groupcontract.TargetVerified(p) {
					t.Fatal("target enrollment not retired")
				}
			} else if got.Status.Phase == "Verified" {
				t.Fatal("invalid proof accepted")
			}
		})
	}
}
func TestGroupSourceReceiptAuthorization(t *testing.T) {
	r, c := groupRequestFixture()
	r.Spec.TargetCluster = "target"
	p, err := desiredPlan(r, c)
	if err != nil {
		t.Fatal(err)
	}
	p.Generation = 1
	p.UID = "plan"
	now := metav1.Now()
	p.Status.Clusters = []api.ClusterStatus{{ClusterName: "source", Phase: "SourceFenced", ObservedGeneration: 1, SourceFences: []api.SourcePodFenceStatus{{PodName: "db-0", SourcePodUID: "current-source", ObservedGeneration: 1, Phase: "SourceGone", DeleteRequestedAt: &now, GoneObservedAt: &now}}, GroupControl: &api.GroupControlStatus{VolumeServer: "nfs", VolumePath: "/shared"}}}
	cl := testClient(t, p)
	reconciler := RestoreReconciler{Client: cl, APIReader: cl}
	if err := reconciler.authorizeGroupFence(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	if _, err := groupcontract.Decode(p); err != nil {
		t.Fatal(err)
	}
	p.Status.Clusters[0].SourceFences[0].SourcePodUID = "archive-source"
	if err := reconciler.authorizeGroupFence(context.Background(), p); err == nil {
		t.Fatal("historical Pod UID authorized as current fence")
	}
}
