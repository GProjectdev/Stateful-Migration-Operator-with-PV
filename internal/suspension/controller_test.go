package suspension

import (
	"context"
	"strings"
	"testing"

	api "github.com/GProjectdev/Stateful-Migration-Operator-with-PV/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type fixture struct {
	rb, cp, pv, md *unstructured.Unstructured
	request        *api.RestoreRequest
	plan           *api.RestorePlan
	sts            *appsv1.StatefulSet
}

func newFixture() *fixture {
	f := &fixture{}
	meta := func(name, uid string) metav1.ObjectMeta {
		return metav1.ObjectMeta{Name: name, Namespace: "demo", UID: types.UID(uid), Generation: 1}
	}
	f.request = &api.RestoreRequest{ObjectMeta: meta("restore", "request-uid"),
		Spec:   api.RestoreRequestSpec{CheckpointRef: api.CheckpointReference{Name: "checkpoint", UID: "checkpoint-uid", Generation: 1}, WorkloadRef: api.WorkloadReference{APIVersion: "apps/v1", Kind: "StatefulSet", Name: "trainer"}, SourceCluster: "onprem", TargetCluster: "aws", SourceFenced: true, VolumesReady: true},
		Status: api.RestoreStatus{ObservedGeneration: 1, Phase: "Prepared", PlanName: "plan"}}
	pods := []interface{}{}
	volumes := []interface{}{}
	works := []interface{}{}
	for _, name := range []string{"trainer-0", "trainer-1"} {
		archive := api.Archive{ContainerName: "trainer", SourcePath: "/source/" + name + ".tar", TargetPath: "/var/lib/kubelet/checkpoints/" + name + ".tar", SHA256: strings.Repeat("a", 64)}
		f.request.Spec.Pods = append(f.request.Spec.Pods, api.RestorePod{SourcePod: name, TargetPod: name, TargetNode: "node1", Archives: []api.Archive{archive}})
		pods = append(pods, map[string]interface{}{"podName": name, "podUID": name + "-uid", "phase": "ContainerCheckpointed", "checkpointFiles": []interface{}{map[string]interface{}{"containerName": "trainer", "filePath": archive.SourcePath}}})
		volumes = append(volumes, map[string]interface{}{"sourcePVC": "data-" + name, "targetPVC": "data-" + name})
		works = append(works, map[string]interface{}{"name": "pv-" + name, "namespace": "karmada-es-aws", "applied": true, "detached": true})
	}
	yes := true
	s := f.request.Spec
	f.plan = &api.RestorePlan{ObjectMeta: meta("plan", "plan-uid"), Spec: api.RestorePlanSpec{RequestUID: string(f.request.UID), CheckpointRef: s.CheckpointRef, WorkloadRef: s.WorkloadRef, SourceCluster: s.SourceCluster, TargetCluster: s.TargetCluster, SourceFenced: true, VolumesReady: true, Pods: s.Pods}, Status: api.RestoreStatus{Clusters: []api.ClusterStatus{{ClusterName: "aws", ObservedGeneration: 1, Phase: "Prepared"}}}}
	f.plan.OwnerReferences = []metav1.OwnerReference{{APIVersion: api.GroupVersion.String(), Kind: "RestoreRequest", Name: f.request.Name, UID: f.request.UID, Controller: &yes}}
	replicas := int32(2)
	f.sts = &appsv1.StatefulSet{ObjectMeta: meta("trainer", "workload-uid"), Spec: appsv1.StatefulSetSpec{Replicas: &replicas, Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{api.PlanLabel: "plan"}}}, VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{ObjectMeta: metav1.ObjectMeta{Name: "data"}}}}}
	f.rb = object(bindingGVK.GroupVersion().String(), bindingGVK.Kind)
	f.rb.SetName("trainer-binding")
	f.rb.SetNamespace("demo")
	f.rb.SetUID("rb-uid")
	f.rb.SetAnnotations(map[string]string{RequestAnnotation: "restore", RequestUIDAnnotation: "request-uid", PVAnnotation: "pv", PVUIDAnnotation: "pv-uid"})
	f.rb.Object["spec"] = map[string]interface{}{"resource": map[string]interface{}{"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "trainer", "namespace": "demo", "uid": "workload-uid"}, "suspension": map[string]interface{}{"dispatching": true, "scheduling": true}, "clusters": []interface{}{map[string]interface{}{"name": "aws", "replicas": int64(2)}}}
	f.cp = object("fluidcr.dcnlab.com/v1alpha1", "FluidCRMigration")
	f.cp.SetName("checkpoint")
	f.cp.SetNamespace("demo")
	f.cp.SetUID("checkpoint-uid")
	f.cp.SetGeneration(1)
	f.cp.Object["spec"] = map[string]interface{}{"resume": false, "workloadRef": map[string]interface{}{"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "trainer"}}
	f.cp.Object["status"] = map[string]interface{}{"clusters": []interface{}{map[string]interface{}{"clusterName": "onprem", "observedGeneration": int64(1), "phase": "Completed", "pods": pods}}}
	f.pv = object(api.GroupVersion.String(), "PVMigration")
	f.pv.SetName("pv")
	f.pv.SetNamespace("demo")
	f.pv.SetUID("pv-uid")
	f.pv.SetGeneration(1)
	f.pv.Object["spec"] = map[string]interface{}{"sourceCluster": "onprem", "targetCluster": "aws", "resourceBinding": "trainer-binding", "sourceFenced": true, "metadataRef": "metadata", "volumes": volumes}
	f.pv.Object["status"] = map[string]interface{}{"observedGeneration": int64(1), "phase": "Completed", "planHash": "hash", "works": works}
	f.md = object(api.GroupVersion.String(), "PVMetadata")
	f.md.SetName("metadata")
	f.md.SetNamespace("demo")
	f.md.SetUID("metadata-uid")
	f.md.Object["spec"] = map[string]interface{}{"sourceCluster": "onprem", "workloadRef": map[string]interface{}{"name": "trainer", "uid": "workload-uid"}}
	return f
}
func (f *fixture) client(t *testing.T) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := api.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	for _, obj := range []*unstructured.Unstructured{f.rb, f.cp, f.pv, f.md} {
		gvk := obj.GroupVersionKind()
		scheme.AddKnownTypeWithName(gvk, &unstructured.Unstructured{})
		scheme.AddKnownTypeWithName(gvk.GroupVersion().WithKind(gvk.Kind+"List"), &unstructured.UnstructuredList{})
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&api.RestoreRequest{}, &api.RestorePlan{}).
		WithObjects(f.rb, f.cp, f.pv, f.md, f.request, f.plan, f.sts).Build()
}
func suspended(t *testing.T, c client.Client, rb *unstructured.Unstructured) bool {
	t.Helper()
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(rb), rb); err != nil {
		t.Fatal(err)
	}
	return truth(rb.Object, "spec", "suspension", "dispatching")
}
func TestGates(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*fixture)
		allow  bool
	}{
		{"ready", func(*fixture) {}, true},
		{"not-enrolled", func(f *fixture) { f.rb.SetAnnotations(nil) }, false},
		{"missing-request-uid", func(f *fixture) { a := f.rb.GetAnnotations(); delete(a, RequestUIDAnnotation); f.rb.SetAnnotations(a) }, false},
		{"recreated-request", func(f *fixture) { f.request.UID = "new" }, false},
		{"stale-request", func(f *fixture) { f.request.Status.ObservedGeneration = 0 }, false},
		{"failed-request", func(f *fixture) { f.request.Status.Phase = "Failed" }, false},
		{"unfenced", func(f *fixture) { f.request.Spec.SourceFenced = false; f.plan.Spec.SourceFenced = false }, false},
		{"volumes-not-ready", func(f *fixture) { f.request.Spec.VolumesReady = false; f.plan.Spec.VolumesReady = false }, false},
		{"stale-plan", func(f *fixture) { f.plan.Status.Clusters[0].ObservedGeneration = 0 }, false},
		{"failed-plan", func(f *fixture) { f.plan.Status.Clusters[0].Phase = "Failed" }, false},
		{"duplicate-plan", func(f *fixture) { f.plan.Status.Clusters = append(f.plan.Status.Clusters, f.plan.Status.Clusters[0]) }, false},
		{"wrong-owner", func(f *fixture) { f.plan.OwnerReferences[0].UID = "other" }, false},
		{"wrong-workload-uid", func(f *fixture) { f.sts.UID = "other" }, false},
		{"wrong-plan-label", func(f *fixture) { f.sts.Spec.Template.Labels[api.PlanLabel] = "other" }, false},
		{"replica-mismatch", func(f *fixture) { n := int32(3); f.sts.Spec.Replicas = &n }, false},
		{"source-still-selected", func(f *fixture) {
			_ = unstructured.SetNestedSlice(f.rb.Object, []interface{}{map[string]interface{}{"name": "onprem"}}, "spec", "clusters")
		}, false},
		{"legacy-nested", func(f *fixture) {
			_ = unstructured.SetNestedMap(f.rb.Object, map[string]interface{}{"dispatching": true}, "spec", "suspension", "suspension")
		}, false},
		{"per-cluster", func(f *fixture) {
			_ = unstructured.SetNestedMap(f.rb.Object, map[string]interface{}{"clusterNames": []interface{}{"aws"}}, "spec", "suspension", "dispatchingOnClusters")
		}, false},
		{"checkpoint-recreated", func(f *fixture) { f.cp.SetUID("other") }, false},
		{"checkpoint-resumed", func(f *fixture) { _ = unstructured.SetNestedField(f.cp.Object, true, "spec", "resume") }, false},
		{"checkpoint-stale", func(f *fixture) { f.cp.SetGeneration(2) }, false},
		{"pv-recreated", func(f *fixture) { f.pv.SetUID("new") }, false},
		{"pv-stale", func(f *fixture) {
			_ = unstructured.SetNestedField(f.pv.Object, int64(0), "status", "observedGeneration")
		}, false},
		{"pv-wrong-target", func(f *fixture) { _ = unstructured.SetNestedField(f.pv.Object, "other", "spec", "targetCluster") }, false},
		{"pv-incomplete", func(f *fixture) { _ = unstructured.SetNestedField(f.pv.Object, "Ready", "status", "phase") }, false},
		{"pv-work-missing", func(f *fixture) { _ = unstructured.SetNestedSlice(f.pv.Object, []interface{}{}, "status", "works") }, false},
		{"pv-mapping-missing", func(f *fixture) { _ = unstructured.SetNestedSlice(f.pv.Object, []interface{}{}, "spec", "volumes") }, false},
		{"pv-old-workload", func(f *fixture) { _ = unstructured.SetNestedField(f.md.Object, "other", "spec", "workloadRef", "uid") }, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture()
			tc.mutate(f)
			c := f.client(t)
			r := &Reconciler{Client: c, APIReader: c}
			if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.rb)}); err != nil {
				t.Fatal(err)
			}
			if got := suspended(t, c, f.rb); got == tc.allow {
				t.Fatalf("suspended=%v expected allow=%v", got, tc.allow)
			}
			if tc.allow {
				if _, found, _ := unstructured.NestedFieldNoCopy(f.rb.Object, "spec", "suspension", "dispatching"); found {
					t.Fatal("must remove field, not write false")
				}
				if !truth(f.rb.Object, "spec", "suspension", "scheduling") {
					t.Fatal("unrelated scheduling field lost")
				}
			}
		})
	}
}

func TestReleasedOperationDoesNotReopenLaterSuspension(t *testing.T) {
	f := newFixture()
	c := f.client(t)
	r := &Reconciler{Client: c, APIReader: c}
	key := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.rb)}
	if _, err := r.Reconcile(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	if suspended(t, c, f.rb) {
		t.Fatal("initial release failed")
	}
	if f.rb.GetAnnotations()[ReleasedUIDAnnotation] != "request-uid" {
		t.Fatal("release receipt missing")
	}
	before := f.rb.DeepCopy()
	if err := unstructured.SetNestedField(f.rb.Object, true, "spec", "suspension", "dispatching"); err != nil {
		t.Fatal(err)
	}
	if err := c.Patch(context.Background(), f.rb, client.MergeFrom(before)); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	if !suspended(t, c, f.rb) {
		t.Fatal("old operation reopened an operator suspension")
	}
}

type conflictClient struct {
	client.Client
	calls   int
	request client.ObjectKey
}

func (c *conflictClient) Patch(ctx context.Context, obj client.Object, p client.Patch, opts ...client.PatchOption) error {
	c.calls++
	if c.calls == 1 {
		req := &api.RestoreRequest{}
		if err := c.Client.Get(ctx, c.request, req); err != nil {
			return err
		}
		req.Status.Phase = "Failed"
		if err := c.Client.Status().Update(ctx, req); err != nil {
			return err
		}
		return apierrors.NewConflict(schema.GroupResource{Group: "work.karmada.io", Resource: "resourcebindings"}, obj.GetName(), nil)
	}
	return c.Client.Patch(ctx, obj, p, opts...)
}
func TestConflictRechecksEveryGate(t *testing.T) {
	f := newFixture()
	base := f.client(t)
	c := &conflictClient{Client: base, request: client.ObjectKeyFromObject(f.request)}
	r := &Reconciler{Client: c, APIReader: base}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.rb)}); err != nil {
		t.Fatal(err)
	}
	if c.calls != 1 || !suspended(t, base, f.rb) {
		t.Fatal("conflict retry released a failed operation")
	}
}
