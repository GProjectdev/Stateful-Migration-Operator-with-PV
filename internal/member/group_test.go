package member

import (
	"context"
	"encoding/json"
	api "github.com/GProjectdev/Stateful-Migration-Operator-with-PV/api/v1alpha1"
	"github.com/GProjectdev/Stateful-Migration-Operator-with-PV/internal/groupcontract"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"strings"
	"testing"
)

func groupFixture() (*api.RestorePlan, *corev1.Pod, *corev1.Node, *corev1.PersistentVolumeClaim, *corev1.PersistentVolume) {
	plan, pod, node := fixture()
	plan.Spec.SourceCluster = "target"
	plan.Spec.SourceFenced = false
	plan.Spec.WorkloadRef = api.WorkloadReference{APIVersion: "apps/v1", Kind: "StatefulSet", Name: "trainer", UID: "world"}
	plan.Spec.CheckpointRef.CheckpointID = "round"
	plan.Spec.GroupRestore = &api.GroupRestoreSpec{OperationUID: "operation", SourceWorldUID: "world", WorldSize: 1, SharedPVC: "shared", CheckpointRoot: "/checkpoint", SourcePods: []api.GroupSourcePod{{Rank: 0, PodName: "trainer-0", PodUID: "old", NodeName: "node-a"}}}
	plan.Spec.Pods[0].SourcePod = "trainer-0"
	plan.Spec.Pods[0].TargetPod = "trainer-0"
	plan.Spec.Pods[0].SourceNode = "node-a"
	plan.Spec.Pods[0].SourcePodUID = "archive-older-pod"
	a := &plan.Spec.Pods[0].Archives[0]
	a.TargetPath = "/var/lib/kubelet/checkpoints/" + a.SHA256 + ".tar"
	a.DurableRef = "file-store:jobs/sha256/" + a.SHA256
	yes := true
	pod.Name = "trainer-0"
	pod.UID = "old"
	pod.Labels = map[string]string{WorkloadUIDLabel: "world"}
	pod.OwnerReferences = []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "StatefulSet", Name: "trainer", UID: "local-world", Controller: &yes}}
	pod.Spec.NodeName = "node-a"
	pod.Spec.Volumes = []corev1.Volume{{Name: "checkpoints", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "shared"}}}}
	pod.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{Name: "checkpoints", MountPath: "/checkpoint"}}
	node.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue, LastHeartbeatTime: metav1.Now()}}
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "jobs", UID: "pvc"}, Spec: corev1.PersistentVolumeClaimSpec{VolumeName: "pv"}, Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound}}
	pv := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "pv", UID: "pv"}, Spec: corev1.PersistentVolumeSpec{ClaimRef: &corev1.ObjectReference{Name: "shared", Namespace: "jobs", UID: "pvc"}, PersistentVolumeSource: corev1.PersistentVolumeSource{NFS: &corev1.NFSVolumeSource{Server: "nfs", Path: "/shared"}}}}
	return plan, pod, node, pvc, pv
}

func TestGroupProviderFenceMemberIdentityAndGeneration(t *testing.T) {
	for _, mode := range []string{"valid", "wrong-member-uid", "wrong-operation", "stale-generation"} {
		t.Run(mode, func(t *testing.T) {
			p, _, node, pvc, pv := groupFixture()
			p.Spec.GroupRestore.SourcePods[0].NodeProvisionRef = &api.GroupNodeProvisionRef{Name: "worker", UID: "member-np", InstanceID: "i-source"}
			fence := map[string]interface{}{"operationUID": "operation", "instanceID": "i-source"}
			np := &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "ml.dcn.ssu.ac.kr/v1alpha1", "kind": "NodeProvision", "metadata": map[string]interface{}{"name": "worker", "namespace": p.Namespace, "uid": "member-np", "generation": int64(3)}, "spec": map[string]interface{}{"fence": fence}, "status": map[string]interface{}{"instanceId": "i-source", "nodeName": "node-a", "fence": map[string]interface{}{"operationUID": "operation", "instanceID": "i-source", "phase": "Fenced", "observedGeneration": int64(3), "observedAt": metav1.Now().Format("2006-01-02T15:04:05Z07:00")}}}}
			switch mode {
			case "wrong-member-uid":
				np.SetUID("karmada-np")
			case "wrong-operation":
				_ = unstructured.SetNestedField(np.Object, "other", "status", "fence", "operationUID")
			case "stale-generation":
				np.SetGeneration(4)
			}
			c := groupClient(t, p, node, pvc, pv, np)
			r := NewReconciler(c, c, "target")
			r.GroupControlImage = "control"
			reconcileGroup(t, r, p)
			reconcileGroup(t, r, p)
			if mode == "valid" {
				if p.Status.Phase != "Preparing" {
					t.Fatalf("valid provider fence rejected: %+v", p.Status)
				}
			} else if p.Status.Phase != "AwaitingSourceFence" {
				t.Fatalf("invalid fence accepted: %+v", p.Status)
			}
		})
	}
}

func TestGroupPreservesDeleteIntentOnTransientFailure(t *testing.T) {
	p, old, node, pvc, pv := groupFixture()
	c := groupClient(t, p, old, node, pvc, pv)
	r := NewReconciler(c, c, "target")
	reconcileGroup(t, r, p)
	if err := c.Delete(context.Background(), node); err != nil {
		t.Fatal(err)
	}
	reconcileGroup(t, r, p)
	if p.Status.Phase != "AwaitingSourceFence" || len(p.Status.SourceFences) != 1 || p.Status.SourceFences[0].DeleteRequestedAt == nil {
		t.Fatalf("persisted intent lost: %+v", p.Status)
	}
}

func TestGroupRetirementKeepsCrossSourceBarrier(t *testing.T) {
	p, pod, _, _, _ := groupFixture()
	p.Spec.SourceCluster = "source"
	p.Annotations = map[string]string{groupcontract.TargetVerifiedAnnotation: p.Spec.RequestUID}
	if activePartialPlan(p, "target") || !activePartialPlan(p, "source") {
		t.Fatal("retirement removed source barrier or kept target enrollment")
	}
	c := groupClient(t, p)
	pod.Name = "trainer-2"
	if err := NewWebhook(c, "source").Apply(context.Background(), pod); err == nil {
		t.Fatal("unplanned ordinal bypassed whole-world source barrier")
	}
}

func TestGroupBarrierRejectsExplicitForeignPlan(t *testing.T) {
	p, pod, _, _, _ := groupFixture()
	pod.Labels[api.PlanLabel] = "old-partial-plan"
	c := groupClient(t, p)
	if err := NewWebhook(c, "target").Apply(context.Background(), pod); err == nil || !strings.Contains(err.Error(), "bypass") {
		t.Fatalf("explicit label bypassed active world barrier: %v", err)
	}
}

func TestGroupSourceOnlyFencesWithoutControlJob(t *testing.T) {
	p, old, node, pvc, pv := groupFixture()
	p.Spec.TargetCluster = "other"
	c := groupClient(t, p, old, node, pvc, pv)
	r := NewReconciler(c, c, "target")
	reconcileGroup(t, r, p)
	reconcileGroup(t, r, p)
	reconcileGroup(t, r, p)
	if p.Status.Phase != "SourceFenced" {
		t.Fatalf("source actuator incomplete: %+v", p.Status)
	}
	var jobs batchv1.JobList
	if err := c.List(context.Background(), &jobs); err != nil {
		t.Fatal(err)
	}
	if len(jobs.Items) != 0 {
		t.Fatal("source actuator executed target group control")
	}
}
func groupClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, batchv1.AddToScheme, api.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&api.RestorePlan{}, &corev1.Pod{}, &batchv1.Job{}).WithObjects(objects...).Build()
}
func reconcileGroup(t *testing.T, r *Reconciler, p *api.RestorePlan) {
	t.Helper()
	ctx := context.Background()
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(p)}); err != nil {
		t.Fatal(err)
	}
	if err := r.Reader.Get(ctx, client.ObjectKeyFromObject(p), p); err != nil {
		t.Fatal(err)
	}
}
func completeGroupJob(t *testing.T, c client.Client, p *api.RestorePlan, action string, generation int64) {
	t.Helper()
	ctx := context.Background()
	var job batchv1.Job
	if err := c.Get(ctx, client.ObjectKey{Namespace: p.Namespace, Name: groupJobName(p, action)}, &job); err != nil {
		t.Fatal(err)
	}
	job.UID = types.UID(action + "-job")
	if err := c.Update(ctx, &job); err != nil {
		t.Fatal(err)
	}
	yes := true
	result := groupJobResult{OperationUID: p.Spec.GroupRestore.OperationUID, CheckpointID: p.Spec.CheckpointRef.CheckpointID, Generation: generation, Prepared: true, State: "prepared"}
	if action == "resume" {
		result.State = "completed"
	}
	b, _ := json.Marshal(result)
	worker := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: action + "-worker", Namespace: p.Namespace, UID: types.UID(action + "-worker"), Labels: map[string]string{"batch.kubernetes.io/job-name": job.Name}, OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: job.Name, UID: job.UID, Controller: &yes}}}, Spec: job.Spec.Template.Spec, Status: corev1.PodStatus{Phase: corev1.PodSucceeded, ContainerStatuses: []corev1.ContainerStatus{{Name: "control", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0, Message: string(b)}}}}}}
	if err := c.Create(ctx, worker); err != nil {
		t.Fatal(err)
	}
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	if err := c.Status().Update(ctx, &job); err != nil {
		t.Fatal(err)
	}
}
func TestGroupEndToEndFencingPrepareAdmissionResume(t *testing.T) {
	ctx := context.Background()
	p, old, node, pvc, pv := groupFixture()
	c := groupClient(t, p, old, node, pvc, pv)
	r := NewReconciler(c, c, "target")
	r.GroupControlImage = "control@sha256:" + strings.Repeat("b", 64)
	candidate := old.DeepCopy()
	candidate.UID = ""
	candidate.ResourceVersion = ""
	candidate.Spec.NodeName = ""
	if err := NewWebhook(c, "target").Apply(ctx, candidate); err == nil {
		t.Fatal("admitted before prepare")
	}
	reconcileGroup(t, r, p)
	if p.Status.Phase != "SourceFencing" || len(p.Status.SourceFences) != 1 || p.Status.SourceFences[0].DeleteRequestedAt == nil {
		t.Fatalf("intent missing: %+v", p.Status)
	}
	var still corev1.Pod
	if err := c.Get(ctx, client.ObjectKeyFromObject(old), &still); err != nil {
		t.Fatal("deleted before persisted intent")
	}
	reconcileGroup(t, r, p)
	reconcileGroup(t, r, p)
	if p.Status.Phase != "Preparing" {
		t.Fatalf("want Preparing: %+v", p.Status)
	}
	completeGroupJob(t, c, p, "prepare", 7)
	reconcileGroup(t, r, p)
	if p.Status.Phase != "Prepared" || !groupPrepared(p) {
		t.Fatalf("want Prepared: %+v", p.Status)
	}
	target := candidate.DeepCopy()
	if err := NewWebhook(c, "target").Apply(ctx, target); err != nil {
		t.Fatal(err)
	}
	inject(target)
	target.Spec.Volumes[1].EmptyDir = nil
	target.Spec.Volumes[1].PersistentVolumeClaim = &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "shared"}
	target.UID = "new"
	target.Spec.NodeName = node.Name
	target.Status.Phase = corev1.PodRunning
	target.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	if err := c.Create(ctx, target); err != nil {
		t.Fatal(err)
	}
	reconcileGroup(t, r, p)
	if p.Status.Phase != "Resuming" {
		t.Fatalf("want Resuming not premature Running: %+v", p.Status)
	}
	completeGroupJob(t, c, p, "resume", 7)
	reconcileGroup(t, r, p)
	if p.Status.Phase != "Running" || p.Status.GroupControl.ResumedAt == nil || p.Status.GroupControl.ResumeJobUID == "" {
		t.Fatalf("resume evidence missing: %+v", p.Status)
	}
	reconcileGroup(t, r, p)
	if p.Status.Phase != "Running" {
		t.Fatal("restart/idempotent evaluation failed")
	}
}
func TestGroupFailsClosedWithoutProviderFenceForLostSource(t *testing.T) {
	p, _, node, pvc, pv := groupFixture()
	c := groupClient(t, p, node, pvc, pv)
	r := NewReconciler(c, c, "target")
	reconcileGroup(t, r, p)
	if p.Status.Phase != "AwaitingSourceFence" || !strings.Contains(p.Status.Message, "provider") {
		t.Fatalf("missing Pod treated as fenced: %+v", p.Status)
	}
}
func TestGroupCrossClusterBooleanCannotAuthorizePrepare(t *testing.T) {
	p, _, node, pvc, pv := groupFixture()
	p.Spec.SourceCluster = "source"
	p.Spec.SourceFenced = true
	c := groupClient(t, p, node, pvc, pv)
	r := NewReconciler(c, c, "target")
	r.GroupControlImage = "control"
	reconcileGroup(t, r, p)
	if p.Status.Phase != "AwaitingSourceFence" {
		t.Fatalf("boolean bypassed receipt: %+v", p.Status)
	}
}
func TestGroupCrossClusterRejectsDifferentVolumeBacking(t *testing.T) {
	p, _, node, pvc, pv := groupFixture()
	p.Spec.SourceCluster = "source"
	now := metav1.Now()
	fences := []api.SourcePodFenceStatus{{PodName: "trainer-0", SourcePodUID: "old", ObservedGeneration: p.Generation, Phase: "SourceGone", DeleteRequestedAt: &now, GoneObservedAt: &now}}
	p.Status.GroupControl = &api.GroupControlStatus{VolumeServer: "other-server", VolumePath: "/shared"}
	receipt, err := groupcontract.Encode(p, fences)
	if err != nil {
		t.Fatal(err)
	}
	p.Annotations = map[string]string{groupcontract.FenceAnnotation: receipt}
	p.Status.GroupControl = nil
	c := groupClient(t, p, node, pvc, pv)
	r := NewReconciler(c, c, "target")
	r.GroupControlImage = "control"
	reconcileGroup(t, r, p)
	if !strings.Contains(p.Status.Message, "backing differs") {
		t.Fatalf("different volume accepted: %+v", p.Status)
	}
}
func TestGroupJobRejectsForeignOwnerAndWrongReceipt(t *testing.T) {
	p, _, _, pvc, pv := groupFixture()
	job := desiredGroupJob(p, "control", "prepare")
	job.OwnerReferences[0].UID = "foreign"
	c := groupClient(t, p, pvc, pv, job)
	r := NewReconciler(c, c, "target")
	r.GroupControlImage = "control"
	if _, _, _, err := r.groupJob(context.Background(), p, "prepare"); err == nil {
		t.Fatal("adopted foreign job")
	}
}
