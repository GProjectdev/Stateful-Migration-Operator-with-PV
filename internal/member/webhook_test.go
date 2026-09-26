package member

import (
	"context"
	"encoding/json"
	api "github.com/GProjectdev/Stateful-Migration-Operator-with-PV/api/v1alpha1"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"reflect"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
	"strings"
	"testing"
	"time"
)

func fixture() (*api.RestorePlan, *corev1.Pod, *corev1.Node) {
	p := &api.RestorePlan{ObjectMeta: metav1.ObjectMeta{Name: "restore", Namespace: "jobs", UID: "plan-uid", Generation: 3}, Spec: api.RestorePlanSpec{RequestUID: "request-uid", CheckpointRef: api.CheckpointReference{Name: "checkpoint", UID: "checkpoint-uid", Generation: 1}, WorkloadRef: api.WorkloadReference{APIVersion: "v1", Kind: "Pod", Name: "job"}, SourceCluster: "source", TargetCluster: "target", SourceFenced: true, VolumesReady: true, Pods: []api.RestorePod{{SourcePod: "old-job", TargetPod: "job", TargetNode: "node-a", Archives: []api.Archive{{ContainerName: "main", TargetPath: "/var/lib/kubelet/checkpoints/job.tar", SHA256: strings.Repeat("a", 64)}}}}}, Status: api.RestoreStatus{Artifacts: []api.ArtifactStatus{{NodeName: "node-a", ObservedGeneration: 3, Verified: true, DurableRef: "file-store:jobs/sha256/" + strings.Repeat("a", 64), CheckedAt: metav1.Now()}}}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "job", Namespace: "jobs", Labels: map[string]string{api.PlanLabel: "restore"}, Annotations: map[string]string{InjectAnnotation: "true"}}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "original:tag"}}}}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a", Labels: map[string]string{RuntimeCapabilityLabel: "true"}}}
	p.Spec.Pods[0].SourcePod = p.Spec.Pods[0].TargetPod
	return p, pod, node
}
func testClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := api.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&api.RestorePlan{}, &corev1.Pod{}).WithObjects(objects...).Build()
}
func inject(pod *corev1.Pod) {
	pod.Annotations[InjectedAnnotation] = "true"
	pod.Spec.Volumes = []corev1.Volume{{Name: "fluidcr-payload", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}, {Name: "checkpoints", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}}
	pod.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{Name: "fluidcr-payload", MountPath: "/opt/fluidcr", ReadOnly: true}, {Name: "checkpoints", MountPath: "/checkpoint"}}
	pod.Spec.InitContainers = []corev1.Container{{Name: "fluidcr-inject", Image: "payload:v1", VolumeMounts: []corev1.VolumeMount{{Name: "fluidcr-payload", MountPath: "/fluidcr"}}}}
}

func TestMutationIdempotentPreservesSchedulingAndImage(t *testing.T) {
	plan, pod, node := fixture()
	c := testClient(t, plan, node)
	w := NewWebhook(c, "target")
	pod.Spec.NodeSelector = map[string]string{"zone": "east"}
	terms := []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: "gpu", Operator: corev1.NodeSelectorOpExists}}}, {MatchExpressions: []corev1.NodeSelectorRequirement{{Key: "zone", Operator: corev1.NodeSelectorOpIn, Values: []string{"west"}}}}}
	pod.Spec.Affinity = &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: terms}}}
	if err := w.Apply(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	if pod.Spec.NodeName != "" || pod.Spec.Containers[0].Image != "original:tag" || pod.Spec.NodeSelector["zone"] != "east" {
		t.Fatal("scheduling bypass or image mutation")
	}
	for i, term := range pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
		if !reflect.DeepEqual(term.MatchExpressions, terms[i].MatchExpressions) || len(term.MatchFields) != 1 {
			t.Fatal("existing affinity lost or not pinned")
		}
	}
	before := pod.DeepCopy()
	if err := w.Apply(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, pod) {
		t.Fatal("mutation is not idempotent")
	}
	if pod.Annotations[PlanUIDAnnotation] != string(plan.UID) || pod.Annotations[api.RestoreAnnotationPrefix+"main"] != plan.Spec.Pods[0].Archives[0].TargetPath {
		t.Fatal("missing restore binding")
	}
}

func TestAdmissionFailsClosed(t *testing.T) {
	cases := []struct {
		name   string
		change func(*api.RestorePlan, *corev1.Pod, *corev1.Node)
	}{
		{"same cluster", func(p *api.RestorePlan, _ *corev1.Pod, _ *corev1.Node) { p.Spec.SourceCluster = p.Spec.TargetCluster }},
		{"changed pod identity", func(p *api.RestorePlan, _ *corev1.Pod, _ *corev1.Node) { p.Spec.Pods[0].SourcePod = "another" }},
		{"wrong cluster", func(p *api.RestorePlan, _ *corev1.Pod, _ *corev1.Node) { p.Spec.TargetCluster = "elsewhere" }},
		{"wrong namespace", func(_ *api.RestorePlan, p *corev1.Pod, _ *corev1.Node) { p.Namespace = "other" }},
		{"missing plan", func(_ *api.RestorePlan, p *corev1.Pod, _ *corev1.Node) { p.Labels[api.PlanLabel] = "absent" }},
		{"empty label", func(_ *api.RestorePlan, p *corev1.Pod, _ *corev1.Node) { p.Labels[api.PlanLabel] = "" }},
		{"unfenced", func(p *api.RestorePlan, _ *corev1.Pod, _ *corev1.Node) { p.Spec.SourceFenced = false }},
		{"volumes unready", func(p *api.RestorePlan, _ *corev1.Pod, _ *corev1.Node) { p.Spec.VolumesReady = false }},
		{"stale report generation", func(p *api.RestorePlan, _ *corev1.Pod, _ *corev1.Node) { p.Status.Artifacts[0].ObservedGeneration-- }},
		{"expired", func(p *api.RestorePlan, _ *corev1.Pod, _ *corev1.Node) {
			p.Status.Artifacts[0].CheckedAt = metav1.NewTime(time.Now().Add(-3 * time.Minute))
		}},
		{"future report", func(p *api.RestorePlan, _ *corev1.Pod, _ *corev1.Node) {
			p.Status.Artifacts[0].CheckedAt = metav1.NewTime(time.Now().Add(time.Minute))
		}},
		{"failed artifacts", func(p *api.RestorePlan, _ *corev1.Pod, _ *corev1.Node) { p.Status.Artifacts[0].Verified = false }},
		{"no artifacts", func(p *api.RestorePlan, _ *corev1.Pod, _ *corev1.Node) { p.Status.Artifacts = nil }},
		{"duplicate reports", func(p *api.RestorePlan, _ *corev1.Pod, _ *corev1.Node) {
			p.Status.Artifacts = append(p.Status.Artifacts, p.Status.Artifacts[0])
		}},
		{"uncertified node", func(_ *api.RestorePlan, _ *corev1.Pod, n *corev1.Node) { n.Labels = nil }},
		{"node missing", func(_ *api.RestorePlan, _ *corev1.Pod, n *corev1.Node) { n.Name = "other" }},
		{"optout", func(_ *api.RestorePlan, p *corev1.Pod, _ *corev1.Node) { p.Annotations[InjectAnnotation] = "false" }},
		{"no explicit optin", func(_ *api.RestorePlan, p *corev1.Pod, _ *corev1.Node) { delete(p.Annotations, InjectAnnotation) }},
		{"wrong container", func(_ *api.RestorePlan, p *corev1.Pod, _ *corev1.Node) {
			p.Annotations[ContainerAnnotation] = "sidecar"
		}},
		{"ambiguous multi container", func(_ *api.RestorePlan, p *corev1.Pod, _ *corev1.Node) {
			p.Spec.Containers = append(p.Spec.Containers, corev1.Container{Name: "sidecar"})
		}},
		{"missing container", func(_ *api.RestorePlan, p *corev1.Pod, _ *corev1.Node) { p.Spec.Containers = nil }},
		{"unplanned pod", func(_ *api.RestorePlan, p *corev1.Pod, _ *corev1.Node) { p.Name = "other" }},
		{"generated name", func(_ *api.RestorePlan, p *corev1.Pod, _ *corev1.Node) { p.Name = ""; p.GenerateName = "job-" }},
		{"nodeName bypass", func(_ *api.RestorePlan, p *corev1.Pod, _ *corev1.Node) { p.Spec.NodeName = "node-a" }},
		{"stale plan uid", func(_ *api.RestorePlan, p *corev1.Pod, _ *corev1.Node) { p.Annotations[PlanUIDAnnotation] = "old" }},
		{"stale generation", func(_ *api.RestorePlan, p *corev1.Pod, _ *corev1.Node) { p.Annotations[PlanGenerationAnnotation] = "2" }},
		{"restore collision", func(_ *api.RestorePlan, p *corev1.Pod, _ *corev1.Node) {
			p.Annotations[api.RestoreAnnotationPrefix+"main"] = "/wrong"
		}},
		{"unplanned restore", func(_ *api.RestorePlan, p *corev1.Pod, _ *corev1.Node) {
			p.Annotations[api.RestoreAnnotationPrefix+"sidecar"] = "/wrong"
		}},
		{"created pod", func(_ *api.RestorePlan, p *corev1.Pod, _ *corev1.Node) { p.UID = "existing" }},
		{"standalone with owner", func(_ *api.RestorePlan, p *corev1.Pod, _ *corev1.Node) {
			p.OwnerReferences = []metav1.OwnerReference{{Kind: "Job", Name: "owner"}}
		}},
		{"traversal", func(p *api.RestorePlan, _ *corev1.Pod, _ *corev1.Node) {
			p.Spec.Pods[0].Archives[0].TargetPath = "/var/lib/kubelet/checkpoints/../secret"
		}},
		{"bad digest", func(p *api.RestorePlan, _ *corev1.Pod, _ *corev1.Node) { p.Spec.Pods[0].Archives[0].SHA256 = "bad" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan, pod, node := fixture()
			tc.change(plan, pod, node)
			w := NewWebhook(testClient(t, plan, node), "target")
			if err := w.Apply(context.Background(), pod); err == nil {
				t.Fatal("unsafe request allowed")
			}
		})
	}
}

func TestStatefulSetOwnerAndExistingPod(t *testing.T) {
	plan, pod, node := fixture()
	plan.Spec.WorkloadRef = api.WorkloadReference{APIVersion: "apps/v1", Kind: "StatefulSet", Name: "worker"}
	plan.Spec.Pods[0].SourcePod, plan.Spec.Pods[0].TargetPod, pod.Name = "worker-0", "worker-0", "worker-0"
	controller := true
	pod.OwnerReferences = []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "StatefulSet", Name: "worker", UID: "sts-uid", Controller: &controller}}
	c := testClient(t, plan, node)
	w := NewWebhook(c, "target")
	if err := w.Apply(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	pod.OwnerReferences[0].Name = "other"
	if err := w.Apply(context.Background(), pod); err == nil {
		t.Fatal("wrong owner accepted")
	}
	pod.OwnerReferences[0].Name = "worker"
	pod.OwnerReferences[0].Kind = "ReplicaSet"
	if err := w.Apply(context.Background(), pod); err == nil {
		t.Fatal("wrong owner kind accepted")
	}
	pod.OwnerReferences[0].Kind = "StatefulSet"
	existing := pod.DeepCopy()
	existing.UID = "old-pod"
	if err := c.Create(context.Background(), existing); err != nil {
		t.Fatal(err)
	}
	if err := w.Apply(context.Background(), pod); err == nil {
		t.Fatal("existing Pod accepted")
	}
}

func TestValidatorRequiresFinalInjectionAndBinding(t *testing.T) {
	cases := []struct {
		name   string
		change func(*corev1.Pod)
	}{
		{"no injected flag", func(p *corev1.Pod) { delete(p.Annotations, InjectedAnnotation) }},
		{"no payload mount", func(p *corev1.Pod) { p.Spec.Containers[0].VolumeMounts = p.Spec.Containers[0].VolumeMounts[1:] }},
		{"no checkpoint mount", func(p *corev1.Pod) { p.Spec.Containers[0].VolumeMounts = p.Spec.Containers[0].VolumeMounts[:1] }},
		{"no payload volume", func(p *corev1.Pod) { p.Spec.Volumes = p.Spec.Volumes[1:] }},
		{"no init", func(p *corev1.Pod) { p.Spec.InitContainers = nil }},
		{"wrong init mount", func(p *corev1.Pod) { p.Spec.InitContainers[0].VolumeMounts[0].MountPath = "/wrong" }},
		{"missing plan uid", func(p *corev1.Pod) { delete(p.Annotations, PlanUIDAnnotation) }},
		{"missing restore annotation", func(p *corev1.Pod) { delete(p.Annotations, api.RestoreAnnotationPrefix+"main") }},
		{"missing affinity", func(p *corev1.Pod) { p.Spec.Affinity = nil }},
		{"unbound affinity branch", func(p *corev1.Pod) {
			p.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms = append(p.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms, corev1.NodeSelectorTerm{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: "any", Operator: corev1.NodeSelectorOpExists}}})
		}},
	}
	plan, pod, node := fixture()
	c := testClient(t, plan, node)
	if err := NewWebhook(c, "target").Apply(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	if err := NewValidator(c, "target").Apply(context.Background(), pod); err == nil {
		t.Fatal("ignored FluidCR injection accepted")
	}
	inject(pod)
	pod.UID = "apiserver-assigned-before-validation"
	pod.CreationTimestamp = metav1.Now()
	before := pod.DeepCopy()
	if err := NewValidator(c, "target").Apply(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, pod) {
		t.Fatal("validator mutated pod")
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			candidate := pod.DeepCopy()
			tc.change(candidate)
			if err := NewValidator(c, "target").Apply(context.Background(), candidate); err == nil {
				t.Fatal("invalid final injection accepted")
			}
		})
	}
}

func TestHandleCreateScopeAndNamespace(t *testing.T) {
	plan, pod, node := fixture()
	w := NewWebhook(testClient(t, plan, node), "target")
	raw, _ := json.Marshal(pod)
	req := admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{Operation: admissionv1.Create, Kind: metav1.GroupVersionKind{Version: "v1", Kind: "Pod"}, Namespace: "jobs", Object: runtime.RawExtension{Raw: raw}}}
	result := w.Handle(context.Background(), req)
	if !result.Allowed || len(result.Patches) == 0 {
		t.Fatalf("expected mutation: %+v", result)
	}
	req.Namespace = "wrong"
	if w.Handle(context.Background(), req).Allowed {
		t.Fatal("namespace mismatch allowed")
	}
	req.Operation = admissionv1.Update
	if !w.Handle(context.Background(), req).Allowed {
		t.Fatal("unexpected UPDATE interception")
	}
	req.Operation = admissionv1.Create
	pod.Labels = nil
	req.Object.Raw, _ = json.Marshal(pod)
	if !w.Handle(context.Background(), req).Allowed {
		t.Fatal("unlabelled Pod blocked")
	}
}

func TestValidatorRejectsPersistedPodDespiteValidInjection(t *testing.T) {
	plan, pod, node := fixture()
	c := testClient(t, plan, node)
	if err := NewWebhook(c, "target").Apply(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	inject(pod)
	pod.UID = "server-assigned-uid"
	pod.CreationTimestamp = metav1.Now()
	v := NewValidator(c, "target")
	if err := v.Apply(context.Background(), pod); err != nil {
		t.Fatalf("new server-assigned metadata rejected: %v", err)
	}
	if err := c.Create(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	if err := v.Apply(context.Background(), pod); err == nil {
		t.Fatal("persisted Pod accepted by validator")
	}
}
