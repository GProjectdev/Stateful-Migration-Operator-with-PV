package management

import (
	"context"
	"fmt"
	api "github.com/GProjectdev/Stateful-Migration-Operator-with-PV/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"reflect"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"strings"
	"testing"
	"time"
)

func fixture() (*api.RestoreRequest, *unstructured.Unstructured) {
	req := &api.RestoreRequest{ObjectMeta: metav1.ObjectMeta{Name: "restore", Namespace: "default", UID: "request-uid", Generation: 1}, Spec: api.RestoreRequestSpec{
		CheckpointRef: api.CheckpointReference{Name: "checkpoint", UID: "checkpoint-uid", Generation: 2, CheckpointID: "round-001"}, WorkloadRef: api.WorkloadReference{APIVersion: "apps/v1", Kind: "StatefulSet", Name: "db", UID: "workload-uid"}, TrainingRuntimeRef: api.RuntimeReference{Name: "db-runtime"}, SourceCluster: "source", TargetCluster: "target", SourceFenced: true, VolumesReady: true,
		Pods: []api.RestorePod{{SourcePod: "db-0", SourceNode: "source-node", TargetPod: "db-0", TargetNode: "node-a", Archives: []api.Archive{{ContainerName: "db", SourcePath: "/var/lib/kubelet/checkpoints/db.tar", TargetPath: "/var/lib/kubelet/checkpoints/" + strings.Repeat("a", 64) + ".tar", SHA256: strings.Repeat("a", 64)}}}}}}
	cp := &unstructured.Unstructured{Object: map[string]interface{}{"spec": map[string]interface{}{"resume": false, "workloadRef": map[string]interface{}{"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "db", "namespace": "default", "uid": "workload-uid"}}, "status": map[string]interface{}{"clusters": []interface{}{map[string]interface{}{"clusterName": "source", "observedGeneration": int64(2), "phase": "Completed", "pods": []interface{}{map[string]interface{}{"podName": "db-0", "nodeName": "source-node", "phase": "ContainerCheckpointed", "checkpointFiles": []interface{}{map[string]interface{}{"containerName": "db", "filePath": "/var/lib/kubelet/checkpoints/db.tar", "sha256": strings.Repeat("a", 64), "durableRef": "file-store:default/sha256/" + strings.Repeat("a", 64)}}}}}}}}}
	cp.SetGroupVersionKind(checkpointGVK)
	cp.SetName("checkpoint")
	cp.SetNamespace("default")
	cp.SetUID("checkpoint-uid")
	cp.SetGeneration(2)
	cp.SetAnnotations(map[string]string{"training.dcnlab.com/checkpoint-id": "round-001"})
	return req, cp
}

func TestNamespaceDefaultAndSchemaGuards(t *testing.T) {
	req, cp := fixture()
	delete(cp.Object["spec"].(map[string]interface{})["workloadRef"].(map[string]interface{}), "namespace")
	if got := reconcile(t, testClient(t, req, cp), req); got.Status.Phase != "Preparing" {
		t.Fatal(got.Status)
	}
	for _, change := range []func(*api.RestoreRequest){func(r *api.RestoreRequest) { r.Spec.Pods[0].Archives[0].SHA256 = strings.Repeat("A", 64) }, func(r *api.RestoreRequest) { r.Spec.Pods[0].Archives[0].TargetPath = "/restore/db.tar" }} {
		r, _ := fixture()
		change(r)
		if validateRequest(r) == nil {
			t.Fatal("schema violation accepted")
		}
	}
}

func TestPolicyDefaultsAndRoutingDrift(t *testing.T) {
	req, _ := fixture()
	want := desiredPolicy(req, "plan")
	got := want.DeepCopy()
	spec := got.Object["spec"].(map[string]interface{})
	for k, v := range map[string]interface{}{"priority": int64(0), "preemption": "Never", "conflictResolution": "Abort", "schedulerName": "default-scheduler", "propagateDeps": false, "preserveResourcesOnDeletion": false} {
		spec[k] = v
	}
	spec["placement"].(map[string]interface{})["clusterTolerations"] = []interface{}{map[string]interface{}{"key": "cluster.karmada.io/not-ready", "operator": "Exists", "effect": "NoExecute", "tolerationSeconds": int64(300)}, map[string]interface{}{"key": "cluster.karmada.io/unreachable", "operator": "Exists", "effect": "NoExecute", "tolerationSeconds": int64(300)}}
	if !policyMatches(got, want) {
		t.Fatal("admission defaults rejected")
	}
	for _, change := range []func(map[string]interface{}){
		func(s map[string]interface{}) {
			s["resourceSelectors"] = append(s["resourceSelectors"].([]interface{}), map[string]interface{}{"kind": "Pod"})
		},
		func(s map[string]interface{}) {
			s["placement"].(map[string]interface{})["clusterAffinity"].(map[string]interface{})["clusterNames"] = []interface{}{"target", "other"}
		},
		func(s map[string]interface{}) {
			s["placement"].(map[string]interface{})["clusterAffinities"] = []interface{}{map[string]interface{}{"affinityName": "other"}}
		},
		func(s map[string]interface{}) { s["conflictResolution"] = "Overwrite" },
		func(s map[string]interface{}) { s["failover"] = map[string]interface{}{} },
	} {
		altered := got.DeepCopy()
		change(altered.Object["spec"].(map[string]interface{}))
		if policyMatches(altered, want) {
			t.Fatal("routing drift accepted")
		}
	}
}
func source(cp *unstructured.Unstructured) map[string]interface{} {
	return cp.Object["status"].(map[string]interface{})["clusters"].([]interface{})[0].(map[string]interface{})
}
func sourcePod(cp *unstructured.Unstructured) map[string]interface{} {
	return source(cp)["pods"].([]interface{})[0].(map[string]interface{})
}
func survivorPodEvidence() map[string]interface{} {
	return map[string]interface{}{
		"podName":  "db-1",
		"podUID":   "survivor-uid",
		"nodeName": "source-node-survivor",
		"rank":     int64(0),
		"phase":    "SurvivorPaused",
		"survivorEvidence": map[string]interface{}{
			"generation":    int64(7),
			"pauseLockPath": "/checkpoint/rank0/pause-lock",
			"pauseLockPID":  int64(1234),
			"observedAt":    "2026-09-27T00:00:00Z",
		},
	}
}
func makeSameClusterPartial(req *api.RestoreRequest, cp *unstructured.Unstructured) {
	req.Spec.TargetCluster = req.Spec.SourceCluster
	req.Spec.SourceFenced = false
	req.Spec.Pods[0].Rank = 1
	req.Spec.Pods[0].SourcePodUID = "target-source-uid"
	req.Spec.PartialRestore = &api.PartialRestoreSpec{TargetRanks: []int64{1}, PreventPeriodicResume: true, PreservedSurvivors: []api.SurvivorEvidence{{Rank: 0, PodName: "db-1", PodUID: "survivor-uid", NodeName: "source-node-survivor", Generation: 7, PauseLockPath: "/checkpoint/rank0/pause-lock"}}}
	cp.Object["spec"].(map[string]interface{})["resume"] = false
	sourcePod(cp)["rank"] = int64(1)
	sourcePod(cp)["podUID"] = "target-source-uid"
	source(cp)["pods"] = append(source(cp)["pods"].([]interface{}), survivorPodEvidence())
}
func testClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()
	s := runtime.NewScheme()
	if err := api.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return fake.NewClientBuilder().WithScheme(s).WithStatusSubresource(&api.RestoreRequest{}, &api.RestorePlan{}).WithObjects(objects...).Build()
}

func TestSameClusterPartialRestoreContract(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		req, cp := fixture()
		makeSameClusterPartial(req, cp)
		got := reconcile(t, testClient(t, req, cp), req)
		if got.Status.Phase != "Preparing" {
			t.Fatal(got.Status)
		}
	})
	for _, tt := range []struct {
		name   string
		mutate func(*api.RestoreRequest, *unstructured.Unstructured)
	}{
		{"checkpoint auto-resume", func(_ *api.RestoreRequest, c *unstructured.Unstructured) {
			c.Object["spec"].(map[string]interface{})["resume"] = true
		}},
		{"missing survivor", func(_ *api.RestoreRequest, c *unstructured.Unstructured) {
			source(c)["pods"] = source(c)["pods"].([]interface{})[:1]
		}},
		{"source uid mismatch", func(_ *api.RestoreRequest, c *unstructured.Unstructured) { sourcePod(c)["podUID"] = "other" }},
		{"survivor boolean only", func(_ *api.RestoreRequest, c *unstructured.Unstructured) {
			survivorPodEvidence := source(c)["pods"].([]interface{})[1].(map[string]interface{})
			survivorPodEvidence["survivorEvidence"] = map[string]interface{}{"paused": true}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			req, cp := fixture()
			makeSameClusterPartial(req, cp)
			tt.mutate(req, cp)
			got := reconcile(t, testClient(t, req, cp), req)
			if got.Status.Phase != "Failed" {
				t.Fatalf("phase %s: %s", got.Status.Phase, got.Status.Message)
			}
		})
	}
}
func reconcile(t *testing.T, c client.Client, req *api.RestoreRequest) *api.RestoreRequest {
	t.Helper()
	r := &RestoreReconciler{Client: c, APIReader: c}
	result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(req)})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter <= 0 {
		t.Fatal("expected polling")
	}
	got := &api.RestoreRequest{}
	if err = c.Get(context.Background(), client.ObjectKeyFromObject(req), got); err != nil {
		t.Fatal(err)
	}
	return got
}

func TestCheckpointGates(t *testing.T) {
	tests := []struct {
		name, phase string
		mutate      func(*api.RestoreRequest, *unstructured.Unstructured)
	}{
		{"valid", "Preparing", func(*api.RestoreRequest, *unstructured.Unstructured) {}},
		{"stale source", "AwaitingCheckpoint", func(r *api.RestoreRequest, c *unstructured.Unstructured) { source(c)["observedGeneration"] = int64(1) }},
		{"wrong source", "AwaitingCheckpoint", func(r *api.RestoreRequest, c *unstructured.Unstructured) { source(c)["clusterName"] = "other" }},
		{"root completed is insufficient", "AwaitingCheckpoint", func(r *api.RestoreRequest, c *unstructured.Unstructured) {
			c.Object["status"] = map[string]interface{}{"phase": "Completed"}
		}},
		{"wrong uid", "Failed", func(r *api.RestoreRequest, c *unstructured.Unstructured) { c.SetUID("other") }},
		{"wrong generation", "Failed", func(r *api.RestoreRequest, c *unstructured.Unstructured) { c.SetGeneration(3) }},
		{"resume true", "Preparing", func(r *api.RestoreRequest, c *unstructured.Unstructured) {
			c.Object["spec"].(map[string]interface{})["resume"] = true
		}},
		{"resume absent", "Failed", func(r *api.RestoreRequest, c *unstructured.Unstructured) {
			delete(c.Object["spec"].(map[string]interface{}), "resume")
		}},
		{"namespace mismatch", "Failed", func(r *api.RestoreRequest, c *unstructured.Unstructured) {
			c.Object["spec"].(map[string]interface{})["workloadRef"].(map[string]interface{})["namespace"] = "other"
		}},
		{"unfenced", "Failed", func(r *api.RestoreRequest, c *unstructured.Unstructured) { r.Spec.SourceFenced = false }},
		{"volumes unready", "Failed", func(r *api.RestoreRequest, c *unstructured.Unstructured) { r.Spec.VolumesReady = false }},
		{"same cluster", "Failed", func(r *api.RestoreRequest, c *unstructured.Unstructured) { r.Spec.TargetCluster = r.Spec.SourceCluster }},
		{"incomplete mappings", "Failed", func(r *api.RestoreRequest, c *unstructured.Unstructured) {
			source(c)["pods"] = append(source(c)["pods"].([]interface{}), map[string]interface{}{"podName": "db-1"})
		}},
		{"uncheckpointed", "Failed", func(r *api.RestoreRequest, c *unstructured.Unstructured) { sourcePod(c)["phase"] = "AppCheckpointed" }},
		{"extra archive", "Failed", func(r *api.RestoreRequest, c *unstructured.Unstructured) {
			sourcePod(c)["checkpointFiles"] = append(sourcePod(c)["checkpointFiles"].([]interface{}), map[string]interface{}{"filePath": "/extra.tar"})
		}},
		{"wrong path", "Failed", func(r *api.RestoreRequest, c *unstructured.Unstructured) {
			r.Spec.Pods[0].Archives[0].SourcePath = "/wrong.tar"
		}},
		{"wrong container", "Failed", func(r *api.RestoreRequest, c *unstructured.Unstructured) {
			r.Spec.Pods[0].Archives[0].ContainerName = "other"
		}},
		{"traversal", "Failed", func(r *api.RestoreRequest, c *unstructured.Unstructured) {
			r.Spec.Pods[0].Archives[0].TargetPath = "/restore/../etc/file"
		}},
		{"bad digest", "Failed", func(r *api.RestoreRequest, c *unstructured.Unstructured) {
			r.Spec.Pods[0].Archives[0].SHA256 = "sha256:bad"
		}},
		{"empty node", "Failed", func(r *api.RestoreRequest, c *unstructured.Unstructured) { r.Spec.Pods[0].TargetNode = "" }},
		{"unstable identity", "Failed", func(r *api.RestoreRequest, c *unstructured.Unstructured) { r.Spec.Pods[0].TargetPod = "db-1" }},
		{"wrong ordinal", "Failed", func(r *api.RestoreRequest, c *unstructured.Unstructured) {
			r.Spec.Pods[0].TargetPod = "db-01"
			r.Spec.Pods[0].SourcePod = "db-01"
		}},
		{"duplicate mappings", "Failed", func(r *api.RestoreRequest, c *unstructured.Unstructured) {
			r.Spec.Pods = append(r.Spec.Pods, r.Spec.Pods[0])
		}},
		{"duplicate source", "Failed", func(r *api.RestoreRequest, c *unstructured.Unstructured) {
			c.Object["status"].(map[string]interface{})["clusters"] = []interface{}{source(c), source(c)}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, cp := fixture()
			tt.mutate(req, cp)
			c := testClient(t, req, cp)
			got := reconcile(t, c, req)
			if got.Status.Phase != tt.phase {
				t.Fatalf("phase %s: %s", got.Status.Phase, got.Status.Message)
			}
			plans := &api.RestorePlanList{}
			if err := c.List(context.Background(), plans); err != nil {
				t.Fatal(err)
			}
			if tt.phase != "Preparing" && len(plans.Items) != 0 {
				t.Fatal("invalid request created plan")
			}
		})
	}
}

func TestIdempotencyAndTargetReflection(t *testing.T) {
	req, cp := fixture()
	c := testClient(t, req, cp)
	got := reconcile(t, c, req)
	if got.Status.Phase != "Preparing" {
		t.Fatal(got.Status)
	}
	plan := &api.RestorePlan{}
	key := client.ObjectKey{Namespace: req.Namespace, Name: got.Status.PlanName}
	if err := c.Get(context.Background(), key, plan); err != nil {
		t.Fatal(err)
	}
	policy := desiredPolicy(req, plan.Name)
	if err := c.Get(context.Background(), key, policy); err != nil {
		t.Fatal(err)
	}
	original := plan.DeepCopy()
	originalPolicy := policy.DeepCopy()
	reconcile(t, c, req)
	if err := c.Get(context.Background(), key, plan); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(original, plan) {
		t.Fatal("plan mutated")
	}
	if err := c.Get(context.Background(), key, policy); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(originalPolicy, policy) {
		t.Fatal("policy mutated")
	}
	// Fake clients do not assign generation on create.
	plan.Generation = 1
	if err := c.Update(context.Background(), plan); err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		cluster     string
		gen         int64
		phase, want string
	}{{"target", 0, "Running", "Preparing"}, {"source", 1, "Running", "Preparing"}, {"target", 1, "Prepared", "Prepared"}, {"target", 1, "Running", "Running"}, {"target", 1, "Completed", "Preparing"}, {"target", 1, "Failed", "Failed"}} {
		plan.Status.Clusters = []api.ClusterStatus{{ClusterName: tt.cluster, ObservedGeneration: tt.gen, Phase: tt.phase, Message: "member evidence"}}
		if err := c.Status().Update(context.Background(), plan); err != nil {
			t.Fatal(err)
		}
		got = reconcile(t, c, req)
		if got.Status.Phase != tt.want {
			t.Fatalf("got %s want %s", got.Status.Phase, tt.want)
		}
	}
}

func TestConflictsNeverAdoptOrMutate(t *testing.T) {
	for _, kind := range []string{"foreign plan", "plan drift", "foreign policy", "policy drift"} {
		t.Run(kind, func(t *testing.T) {
			req, cp := fixture()
			p := mustDesiredPlan(req)
			pp := desiredPolicy(req, p.Name)
			switch kind {
			case "foreign plan":
				p.OwnerReferences = nil
			case "plan drift":
				p.Spec.TargetCluster = "other"
			case "foreign policy":
				pp.SetOwnerReferences(nil)
			case "policy drift":
				_ = unstructured.SetNestedSlice(pp.Object, []interface{}{"other"}, "spec", "placement", "clusterAffinity", "clusterNames")
			}
			beforeP, beforePP := p.DeepCopy(), pp.DeepCopy()
			c := testClient(t, req, cp, p, pp)
			got := reconcile(t, c, req)
			if got.Status.Phase != "Failed" {
				t.Fatal(got.Status)
			}
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(p), p); err != nil {
				t.Fatal(err)
			}
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(pp), pp); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(beforeP.Spec, p.Spec) || !reflect.DeepEqual(beforeP.OwnerReferences, p.OwnerReferences) || !reflect.DeepEqual(beforePP.Object["spec"], pp.Object["spec"]) || !reflect.DeepEqual(beforePP.GetOwnerReferences(), pp.GetOwnerReferences()) {
				t.Fatal("conflicting resource changed")
			}
		})
	}
}

func TestStablePod(t *testing.T) {
	req, cp := fixture()
	req.Spec.WorkloadRef = api.WorkloadReference{APIVersion: "v1", Kind: "Pod", Name: "db-0", UID: "workload-uid"}
	cp.Object["spec"].(map[string]interface{})["workloadRef"] = map[string]interface{}{"apiVersion": "v1", "kind": "Pod", "name": "db-0", "namespace": "default", "uid": "workload-uid"}
	got := reconcile(t, testClient(t, req, cp), req)
	if got.Status.Phase != "Preparing" {
		t.Fatal(got.Status)
	}
}

func TestAwaitingArtifactsReflection(t *testing.T) {
	req, cp := fixture()
	plan := mustDesiredPlan(req)
	plan.Generation = 1
	plan.Status.Clusters = []api.ClusterStatus{{ClusterName: "target", ObservedGeneration: 1, Phase: "AwaitingArtifacts", Message: "awaiting node reports"}}
	got := reconcile(t, testClient(t, req, cp, plan), req)
	if got.Status.Phase != "AwaitingArtifacts" || got.Status.Message != "awaiting node reports" {
		t.Fatal(got.Status)
	}
}

func trainingRuntime(req *api.RestoreRequest, podUID string, observed metav1.Time) *unstructured.Unstructured {
	tr := &unstructured.Unstructured{Object: map[string]interface{}{"status": map[string]interface{}{"clusters": []interface{}{map[string]interface{}{"clusterName": req.Spec.TargetCluster, "observedGeneration": int64(1), "status": map[string]interface{}{
		"phase": "Running", "globalStep": int64(11), "checkpointID": req.Spec.CheckpointRef.CheckpointID, "readyRanks": int64(1), "worldSize": int64(1), "workloadUID": req.Spec.WorkloadRef.UID, "memberWorkloadUID": "member-workload", "observedAt": observed.Time.Format(time.RFC3339),
		"pods": []interface{}{map[string]interface{}{"name": "db-0", "uid": podUID, "rank": int64(0), "globalStep": int64(11), "previousGlobalStep": int64(10), "checkpointID": req.Spec.CheckpointRef.CheckpointID, "previousObservedAt": observed.Time.Add(-time.Second).Format(time.RFC3339), "observedAt": observed.Time.Format(time.RFC3339)}},
	}}}}}}
	tr.SetGroupVersionKind(trainingRuntimeGVK)
	tr.SetName(req.Spec.TrainingRuntimeRef.Name)
	tr.SetNamespace(req.Namespace)
	tr.SetUID("training-runtime-uid")
	tr.SetGeneration(1)
	return tr
}

func partialTrainingRuntime(req *api.RestoreRequest, targetUID string, observed metav1.Time) *unstructured.Unstructured {
	previous := observed.Time.Add(-time.Second).Format(time.RFC3339)
	current := observed.Time.Format(time.RFC3339)
	tr := &unstructured.Unstructured{Object: map[string]interface{}{"spec": map[string]interface{}{"workloadRef": map[string]interface{}{"uid": req.Spec.WorkloadRef.UID}}, "status": map[string]interface{}{"clusters": []interface{}{map[string]interface{}{"clusterName": req.Spec.TargetCluster, "observedGeneration": int64(1), "status": map[string]interface{}{
		"phase": "Running", "globalStep": int64(12), "checkpointID": req.Spec.CheckpointRef.CheckpointID, "readyRanks": int64(2), "worldSize": int64(2), "workloadUID": req.Spec.WorkloadRef.UID, "memberWorkloadUID": "member-workload", "observedAt": current,
		"pods": []interface{}{
			map[string]interface{}{"name": "db-0", "uid": targetUID, "rank": int64(1), "globalStep": int64(12), "previousGlobalStep": int64(11), "checkpointID": req.Spec.CheckpointRef.CheckpointID, "previousObservedAt": previous, "observedAt": current},
			map[string]interface{}{"name": "db-1", "uid": "survivor-uid", "rank": int64(0), "globalStep": int64(13), "previousGlobalStep": int64(12), "checkpointID": req.Spec.CheckpointRef.CheckpointID, "previousObservedAt": previous, "observedAt": current},
		},
	}}}}}}
	tr.SetGroupVersionKind(trainingRuntimeGVK)
	tr.SetName(req.Spec.TrainingRuntimeRef.Name)
	tr.SetNamespace(req.Namespace)
	tr.SetUID("training-runtime-uid")
	tr.SetGeneration(1)
	return tr
}

func TestRuntimeTelemetryGatesVerifiedStatus(t *testing.T) {
	req, cp := fixture()
	plan := mustDesiredPlan(req)
	plan.Generation = 1
	plan.Status.Clusters = []api.ClusterStatus{{ClusterName: "target", ObservedGeneration: 1, Phase: "Running", Message: "pods ready", Pods: []api.PodStatus{{Name: "db-0", UID: "target-pod-uid", Phase: "Running"}}}}
	got := reconcile(t, testClient(t, req, cp, plan), req)
	if got.Status.Phase != "Running" {
		t.Fatalf("without runtime evidence: %+v", got.Status)
	}
	got = reconcile(t, testClient(t, req, cp, plan, trainingRuntime(req, "target-pod-uid", metav1.Now())), req)
	if got.Status.Phase != "Verified" {
		t.Fatalf("with runtime evidence: %+v", got.Status)
	}
	stale := metav1.NewTime(time.Now().Add(-time.Minute))
	got = reconcile(t, testClient(t, req, cp, plan, trainingRuntime(req, "target-pod-uid", stale)), req)
	if got.Status.Phase != "Running" {
		t.Fatalf("stale runtime evidence accepted: %+v", got.Status)
	}
	got = reconcile(t, testClient(t, req, cp, plan, trainingRuntime(req, "wrong-pod-uid", metav1.Now())), req)
	if got.Status.Phase != "Running" {
		t.Fatalf("wrong pod runtime evidence accepted: %+v", got.Status)
	}
}

func TestStagedPartialEvidenceRejectsUnsafeTargets(t *testing.T) {
	for _, mode := range []string{"missing", "duplicate", "source-uid", "wrong-name", "pending", "probe-failed", "no-fence", "stale-fence"} {
		t.Run(mode, func(t *testing.T) {
			req, cp := fixture()
			makeSameClusterPartial(req, cp)
			plan, err := desiredPlan(req, cp)
			if err != nil {
				t.Fatal(err)
			}
			plan.Generation = 1
			now := metav1.Now()
			pods := []api.PodStatus{{Name: "db-0", UID: "replacement", Phase: "Staged"}}
			fences := []api.SourcePodFenceStatus{{PodName: "db-0", SourcePodUID: "target-source-uid", ObservedGeneration: 1, Phase: "SourceGone", DeleteRequestedAt: &now, GoneObservedAt: &now}}
			switch mode {
			case "missing":
				pods = nil
			case "duplicate":
				pods = append(pods, pods[0])
			case "source-uid":
				pods[0].UID = "target-source-uid"
			case "wrong-name":
				pods[0].Name = "other"
			case "pending":
				pods[0].Phase = "Pending"
			case "probe-failed":
				pods[0].Message = "wrong checkpoint"
			case "no-fence":
				fences = nil
			case "stale-fence":
				fences[0].ObservedGeneration = 2
			}
			plan.Status.Clusters = []api.ClusterStatus{{ClusterName: req.Spec.TargetCluster, ObservedGeneration: 1, Phase: "StagedReady", Pods: pods, SourceFences: fences}}
			c := testClient(t, req, cp, plan)
			got := reconcile(t, c, req)
			if got.Status.Phase == "RestoreReady" || got.Status.Phase == "Verified" {
				t.Fatal("invalid staging authorized")
			}
			var current unstructured.Unstructured
			current.SetGroupVersionKind(checkpointGVK)
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(cp), &current); err != nil {
				t.Fatal(err)
			}
			if current.GetAnnotations()[annotationRestoreOwnedResume] == "true" {
				t.Fatal("unsafe release annotation written")
			}
		})
	}
}

func TestPartialRestoreAuthorizesReleaseThenVerifiesAfterSurvivorResume(t *testing.T) {
	for _, staged := range []bool{false, true} {
		t.Run(fmt.Sprintf("staged=%v", staged), func(t *testing.T) {
			req, cp := fixture()
			makeSameClusterPartial(req, cp)
			plan, err := desiredPlan(req, cp)
			if err != nil {
				t.Fatal(err)
			}
			plan.Generation = 1
			now := metav1.Now()
			gone := metav1.NewTime(now.Time.Add(-time.Second))
			fences := []api.SourcePodFenceStatus{{PodName: "db-0", SourcePodUID: "target-source-uid", ObservedGeneration: 1, Phase: "SourceGone", DeleteRequestedAt: &gone, GoneObservedAt: &gone}}
			plan.Status.Clusters = []api.ClusterStatus{{ClusterName: req.Spec.TargetCluster, ObservedGeneration: 1, Phase: "Running", Message: "target restored", Pods: []api.PodStatus{{Name: "db-0", UID: "target-pod-uid", Phase: "Running"}}, SourceFences: fences}}
			if staged {
				plan.Status.Clusters[0].Phase = "StagedReady"
				plan.Status.Clusters[0].Pods[0].Phase = "Staged"
			}
			c := testClient(t, req, cp, plan)

			got := reconcile(t, c, req)
			if got.Status.Phase != "RestoreReady" {
				t.Fatalf("phase = %q: %+v", got.Status.Phase, got.Status)
			}
			var annotated unstructured.Unstructured
			annotated.SetGroupVersionKind(checkpointGVK)
			if err := c.Get(context.Background(), types.NamespacedName{Namespace: cp.GetNamespace(), Name: cp.GetName()}, &annotated); err != nil {
				t.Fatal(err)
			}
			if annotated.GetAnnotations()[annotationRestoreOwnedResume] != "true" {
				t.Fatalf("restore-owned release annotation missing: %v", annotated.GetAnnotations())
			}
			source(&annotated)["phase"] = "Completed"
			survivor := source(&annotated)["pods"].([]interface{})[1].(map[string]interface{})
			survivor["phase"] = "Resumed"
			if err := c.Update(context.Background(), &annotated); err != nil {
				t.Fatal(err)
			}
			if err := c.Create(context.Background(), partialTrainingRuntime(req, "target-pod-uid", now)); err != nil {
				t.Fatal(err)
			}
			if staged {
				var current api.RestorePlan
				if err := c.Get(context.Background(), client.ObjectKeyFromObject(plan), &current); err != nil {
					t.Fatal(err)
				}
				current.Status.Clusters[0].Phase = "Running"
				current.Status.Clusters[0].Pods[0].Phase = "Running"
				if err := c.Status().Update(context.Background(), &current); err != nil {
					t.Fatal(err)
				}
			}
			got = reconcile(t, c, req)
			if got.Status.Phase != "Verified" {
				t.Fatalf("phase after release/runtime = %q: %+v", got.Status.Phase, got.Status)
			}
		})
	}
}
