package member

import (
	"context"
	"testing"

	api "github.com/GProjectdev/Stateful-Migration-Operator-with-PV/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func localFixture() (*api.RestorePlan, *corev1.Pod, *corev1.Node, *unstructured.Unstructured) {
	p, pod, node := fixture()
	p.Spec.LocalPodRestore = true
	p.Spec.SourceCluster = p.Spec.TargetCluster
	p.Spec.SourceFenced = false
	p.Spec.WorkloadRef.UID = "source-uid"
	p.Spec.RequestUID = p.Spec.CheckpointRef.UID
	p.Spec.CheckpointRef.CheckpointID = "round-one"
	m := &p.Spec.Pods[0]
	m.SourcePodUID, m.SourceNode = "source-uid", node.Name
	a := &m.Archives[0]
	a.SourcePath = a.TargetPath
	a.DurableRef = p.Status.Artifacts[0].DurableRef
	node.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}
	cp := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "fluidcr.dcnlab.com/v1alpha1", "kind": "FluidCRMigration",
		"metadata": map[string]interface{}{"name": "checkpoint", "namespace": p.Namespace, "uid": "checkpoint-uid", "generation": int64(1), "annotations": map[string]interface{}{"training.dcnlab.com/checkpoint-id": "round-one"}},
		"spec":     map[string]interface{}{"resume": false, "workloadRef": map[string]interface{}{"apiVersion": "v1", "kind": "Pod", "name": pod.Name, "uid": "source-uid"}},
		"status": map[string]interface{}{"phase": "Completed", "observedGeneration": int64(1), "pods": []interface{}{map[string]interface{}{
			"podName": pod.Name, "podUID": "source-uid", "nodeName": node.Name, "phase": "ContainerCheckpointed",
			"checkpointFiles": []interface{}{map[string]interface{}{"containerName": a.ContainerName, "filePath": a.SourcePath, "sha256": a.SHA256, "durableRef": a.DurableRef}},
		}}},
	}}
	return p, pod, node, cp
}

func TestLocalPodRestoreValidation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*api.RestorePlan, *unstructured.Unstructured)
	}{
		{"missing opt in", func(p *api.RestorePlan, _ *unstructured.Unstructured) { p.Spec.LocalPodRestore = false }},
		{"different cluster", func(p *api.RestorePlan, _ *unstructured.Unstructured) { p.Spec.SourceCluster = "other" }},
		{"statefulset", func(p *api.RestorePlan, _ *unstructured.Unstructured) { p.Spec.WorkloadRef.Kind = "StatefulSet" }},
		{"partial bypass", func(p *api.RestorePlan, _ *unstructured.Unstructured) {
			p.Spec.PartialRestore = &api.PartialRestoreSpec{}
		}},
		{"UID mismatch", func(p *api.RestorePlan, _ *unstructured.Unstructured) { p.Spec.Pods[0].SourcePodUID = "other" }},
		{"asserted fence", func(p *api.RestorePlan, _ *unstructured.Unstructured) { p.Spec.SourceFenced = true }},
		{"wrong checkpoint", func(_ *api.RestorePlan, cp *unstructured.Unstructured) { cp.SetUID("other") }},
		{"resuming checkpoint", func(_ *api.RestorePlan, cp *unstructured.Unstructured) {
			unstructured.SetNestedField(cp.Object, true, "spec", "resume")
		}},
		{"stale checkpoint", func(_ *api.RestorePlan, cp *unstructured.Unstructured) {
			unstructured.SetNestedField(cp.Object, int64(0), "status", "observedGeneration")
		}},
		{"wrong archive", func(p *api.RestorePlan, _ *unstructured.Unstructured) {
			p.Spec.Pods[0].Archives[0].SourcePath += ".other"
		}},
		{"wrong digest", func(p *api.RestorePlan, _ *unstructured.Unstructured) { p.Spec.Pods[0].Archives[0].SHA256 = "bad" }},
		{"missing export", func(p *api.RestorePlan, _ *unstructured.Unstructured) { p.Spec.Pods[0].Archives[0].DurableRef = "" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, _, n, cp := localFixture()
			tc.change(p, cp)
			c := testClient(t, p, n, cp)
			if validatePlan(p, "target") == nil && validateLocalCheckpoint(context.Background(), c, p) == nil {
				t.Fatal("unsafe local restore accepted")
			}
		})
	}
}

func TestLocalPodRestoreFencingAndAdmission(t *testing.T) {
	ctx := context.Background()
	p, restored, node, cp := localFixture()
	source := restored.DeepCopy()
	source.UID = "source-uid"
	source.Labels = nil
	source.Spec.NodeName = node.Name
	c := testClient(t, p, source, node, cp)
	w := NewWebhook(c, "target")
	if err := w.Apply(ctx, restored.DeepCopy()); err == nil {
		t.Fatal("admitted before source fenced")
	}
	r := NewReconciler(c, c, "target")
	key := client.ObjectKeyFromObject(p)
	step := func() {
		t.Helper()
		if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
			t.Fatal(err)
		}
		if err := c.Get(ctx, key, p); err != nil {
			t.Fatal(err)
		}
	}
	step()
	if err := c.Get(ctx, client.ObjectKeyFromObject(source), &corev1.Pod{}); err != nil {
		t.Fatal("source deleted before intent persistence", err)
	}
	if len(p.Status.SourceFences) != 1 || p.Status.SourceFences[0].DeleteRequestedAt == nil {
		t.Fatal("missing durable intent", p.Status)
	}
	step()
	if err := c.Get(ctx, client.ObjectKeyFromObject(source), &corev1.Pod{}); !apierrors.IsNotFound(err) {
		t.Fatal("source not deleted", err)
	}
	if err := w.Apply(ctx, restored.DeepCopy()); err == nil {
		t.Fatal("admitted before gone observation")
	}
	step()
	if p.Status.SourceFences[0].Phase != "SourceGone" {
		t.Fatal(p.Status)
	}
	// Unlabelled recreations must also discover the active plan, not cold start.
	restored.Labels = nil
	if err := w.Apply(ctx, restored); err != nil {
		t.Fatal(err)
	}
	if restored.Annotations[api.RestoreAnnotationPrefix+"main"] == "" {
		t.Fatal("missing native restore annotation")
	}
	inject(restored)
	if err := NewValidator(c, "target").Apply(ctx, restored); err != nil {
		t.Fatal(err)
	}
}

func TestLocalPodRestoreRefusesUnsafeDeletion(t *testing.T) {
	for _, name := range []string{"missing artifact", "missing capability", "wrong source UID", "wrong source node", "checkpoint mismatch"} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			p, source, node, cp := localFixture()
			source.UID = "source-uid"
			source.Labels = nil
			source.Spec.NodeName = node.Name
			switch name {
			case "missing artifact":
				p.Status.Artifacts = nil
			case "missing capability":
				delete(node.Labels, RuntimeCapabilityLabel)
			case "wrong source UID":
				source.UID = "new-unrelated-pod"
			case "wrong source node":
				source.Spec.NodeName = "another-node"
			case "checkpoint mismatch":
				cp.SetUID("another-checkpoint")
			}
			c := testClient(t, p, source, node, cp)
			r := NewReconciler(c, c, "target")
			for i := 0; i < 3; i++ {
				if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(p)}); err != nil {
					t.Fatal(err)
				}
			}
			var remaining corev1.Pod
			if err := c.Get(ctx, client.ObjectKeyFromObject(source), &remaining); err != nil || remaining.UID != source.UID {
				t.Fatalf("unsafe source deletion: %v", err)
			}
		})
	}
}

func TestFailedLocalPlanBlocksColdStart(t *testing.T) {
	p, pod, node, cp := localFixture()
	p.Status.Phase = "Failed"
	pod.Labels = nil
	c := testClient(t, p, node, cp)
	if err := NewWebhook(c, "target").Apply(context.Background(), pod); err == nil {
		t.Fatal("failed local plan permitted an unlabelled cold start")
	}
}
