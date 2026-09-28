package member

import (
	"context"
	"fmt"
	api "github.com/GProjectdev/Stateful-Migration-Operator-with-PV/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"reflect"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"strings"
	"testing"
	"time"
)

func TestReconcilerPhasesAndNoAdoption(t *testing.T) {
	cases := []struct {
		name, want string
		present    bool
		change     func(*api.RestorePlan, *corev1.Pod, *corev1.Node)
	}{
		{"prepared without creating pods", "Prepared", false, func(*api.RestorePlan, *corev1.Pod, *corev1.Node) {}},
		{"awaiting archives", "AwaitingArtifacts", false, func(p *api.RestorePlan, _ *corev1.Pod, _ *corev1.Node) { p.Status.Artifacts = nil }},
		{"expired archives", "AwaitingArtifacts", false, func(p *api.RestorePlan, _ *corev1.Pod, _ *corev1.Node) {
			p.Status.Artifacts[0].CheckedAt = metav1.NewTime(time.Now().Add(-3 * time.Minute))
		}},
		{"failed archives", "Failed", false, func(p *api.RestorePlan, _ *corev1.Pod, _ *corev1.Node) { p.Status.Artifacts[0].Verified = false }},
		{"uncertified node", "Failed", false, func(_ *api.RestorePlan, _ *corev1.Pod, n *corev1.Node) { n.Labels = nil }},
		{"missing node", "Failed", false, func(_ *api.RestorePlan, _ *corev1.Pod, n *corev1.Node) { n.Name = "other-node" }},
		{"pending bound pod", "Prepared", true, func(_ *api.RestorePlan, p *corev1.Pod, _ *corev1.Node) { p.Status.Phase = corev1.PodPending }},
		{"running ready", "Running", true, func(*api.RestorePlan, *corev1.Pod, *corev1.Node) {}},
		{"running unready", "Prepared", true, func(_ *api.RestorePlan, p *corev1.Pod, _ *corev1.Node) { p.Status.Conditions = nil }},
		{"unbound running pod", "Failed", true, func(_ *api.RestorePlan, p *corev1.Pod, _ *corev1.Node) { delete(p.Annotations, PlanUIDAnnotation) }},
		{"wrong plan uid", "Failed", true, func(_ *api.RestorePlan, p *corev1.Pod, _ *corev1.Node) { p.Annotations[PlanUIDAnnotation] = "other" }},
		{"wrong generation", "Failed", true, func(_ *api.RestorePlan, p *corev1.Pod, _ *corev1.Node) { p.Annotations[PlanGenerationAnnotation] = "2" }},
		{"wrong node", "Failed", true, func(_ *api.RestorePlan, p *corev1.Pod, _ *corev1.Node) { p.Spec.NodeName = "other" }},
		{"missing restore", "Failed", true, func(_ *api.RestorePlan, p *corev1.Pod, _ *corev1.Node) {
			delete(p.Annotations, api.RestoreAnnotationPrefix+"main")
		}},
		{"failed container", "Failed", true, func(_ *api.RestorePlan, p *corev1.Pod, _ *corev1.Node) {
			p.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "main", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}}}}
		}},
		{"failed pod", "Failed", true, func(_ *api.RestorePlan, p *corev1.Pod, _ *corev1.Node) { p.Status.Phase = corev1.PodFailed }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan, pod, node := fixture()
			if err := NewWebhook(testClient(t, plan, node), "target").Apply(context.Background(), pod); err != nil {
				t.Fatal(err)
			}
			inject(pod)
			pod.UID = "pod-uid"
			pod.Spec.NodeName = "node-a"
			pod.Status.Phase = corev1.PodRunning
			pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
			tc.change(plan, pod, node)
			objects := []client.Object{plan, node}
			if tc.present {
				objects = append(objects, pod)
			}
			c := testClient(t, objects...)
			r := NewReconciler(c, c, "target")
			key := client.ObjectKeyFromObject(plan)
			var storedPlan api.RestorePlan
			if err := c.Get(context.Background(), key, &storedPlan); err != nil {
				t.Fatal(err)
			}
			var before corev1.Pod
			if tc.present {
				if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), &before); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
				t.Fatal(err)
			}
			var got api.RestorePlan
			if err := c.Get(context.Background(), key, &got); err != nil {
				t.Fatal(err)
			}
			if got.Status.Phase != tc.want {
				t.Fatalf("want %s, got %s (%s)", tc.want, got.Status.Phase, got.Status.Message)
			}
			if !reflect.DeepEqual(got.Status.Artifacts, storedPlan.Status.Artifacts) {
				t.Fatal("independent artifact reports changed")
			}
			if tc.want == "Running" && !strings.Contains(got.Status.Message, "not attested") {
				t.Fatal("Running overclaims restore")
			}
			var after corev1.Pod
			err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), &after)
			if tc.present {
				if err != nil || !reflect.DeepEqual(&before, &after) {
					t.Fatal("controller adopted or modified a Pod")
				}
			} else if !apierrors.IsNotFound(err) {
				t.Fatal("controller created a Pod")
			}
		})
	}
}

type conflictClient struct {
	client.Client
	collided bool
}
type conflictStatus struct {
	client.SubResourceWriter
	owner *conflictClient
}

func (c *conflictClient) Status() client.SubResourceWriter {
	return &conflictStatus{SubResourceWriter: c.Client.Status(), owner: c}
}
func (s *conflictStatus) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	if !s.owner.collided {
		s.owner.collided = true
		var latest api.RestorePlan
		if err := s.owner.Client.Get(ctx, client.ObjectKeyFromObject(obj), &latest); err != nil {
			return err
		}
		latest.Status.Artifacts = append(latest.Status.Artifacts, api.ArtifactStatus{NodeName: "independent-node", ObservedGeneration: 3, Verified: true, DurableRef: "file-store:jobs/sha256/" + strings.Repeat("a", 64), CheckedAt: metav1.Now()})
		if err := s.owner.Client.Status().Update(ctx, &latest); err != nil {
			return err
		}
		return apierrors.NewConflict(schema.GroupResource{Group: api.GroupVersion.Group, Resource: "restoreplans"}, obj.GetName(), fmt.Errorf("concurrent artifact report"))
	}
	return s.SubResourceWriter.Update(ctx, obj, opts...)
}

func TestStatusConflictPreservesIndependentReports(t *testing.T) {
	plan, _, node := fixture()
	base := testClient(t, plan, node)
	c := &conflictClient{Client: base}
	r := NewReconciler(c, base, "target")
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(plan)}); err != nil {
		t.Fatal(err)
	}
	var got api.RestorePlan
	if err := base.Get(context.Background(), client.ObjectKeyFromObject(plan), &got); err != nil {
		t.Fatal(err)
	}
	if !c.collided || len(got.Status.Artifacts) != 2 || got.Status.Artifacts[1].NodeName != "independent-node" || got.Status.Phase != "Prepared" {
		t.Fatalf("lost concurrent status: %+v", got.Status)
	}
}

func TestPodWatchFindsUnlabelledConflictingPod(t *testing.T) {
	plan, pod, node := fixture()
	pod.Labels = nil
	c := testClient(t, plan, node)
	r := NewReconciler(c, c, "target")
	requests := r.plansForPod(context.Background(), pod)
	if len(requests) != 1 || requests[0].Name != plan.Name {
		t.Fatal("conflicting Pod event not mapped")
	}
	pod.Namespace = "other"
	if len(r.plansForPod(context.Background(), pod)) != 0 {
		t.Fatal("cross namespace mapping")
	}
}

func partialFixture() (*api.RestorePlan, *corev1.Pod, *corev1.Node) {
	plan, pod, node := fixture()
	plan.Spec.SourceCluster = "spot-cluster"
	plan.Spec.TargetCluster = "spot-cluster"
	plan.Spec.SourceFenced = false
	plan.Spec.WorkloadRef = api.WorkloadReference{APIVersion: "apps/v1", Kind: "StatefulSet", Name: "trainer", UID: "workload-uid"}
	plan.Spec.Pods[0].SourcePod = "trainer-1"
	plan.Spec.Pods[0].TargetPod = "trainer-1"
	plan.Spec.Pods[0].Rank = 1
	plan.Spec.Pods[0].SourcePodUID = "source-uid"
	plan.Spec.PartialRestore = &api.PartialRestoreSpec{TargetRanks: []int64{1}, PreventPeriodicResume: true, PreservedSurvivors: []api.SurvivorEvidence{{Rank: 0, PodName: "trainer-0", PodUID: "survivor-uid", NodeName: "node-survivor", Generation: 7, PauseLockPath: "/checkpoint/rank0/pause-lock"}}}
	pod.Name = "trainer-1"
	pod.Labels = map[string]string{WorkloadUIDLabel: "workload-uid"}
	pod.Annotations = map[string]string{InjectAnnotation: "true"}
	controller := true
	pod.OwnerReferences = []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "StatefulSet", Name: "trainer", UID: "member-workload-uid", Controller: &controller}}
	node.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}
	return plan, pod, node
}

func partialAdmissionObjects() []client.Object {
	replicas := int32(2)
	sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "trainer", Namespace: "jobs", UID: "member-workload-uid", Labels: map[string]string{WorkloadUIDLabel: "workload-uid"}}, Spec: appsv1.StatefulSetSpec{Replicas: &replicas, Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{WorkloadUIDLabel: "workload-uid"}, Annotations: map[string]string{InjectAnnotation: "true"}}}}}
	endpoints := &corev1.Endpoints{ObjectMeta: metav1.ObjectMeta{Name: WebhookServiceName, Namespace: WebhookServiceNamespace}, Subsets: []corev1.EndpointSubset{{Addresses: []corev1.EndpointAddress{{IP: "10.0.0.10"}}, Ports: []corev1.EndpointPort{{Name: "webhook", Port: 9443}}}}}
	return []client.Object{sts, endpoints}
}

func withPartialAdmission(objects ...client.Object) []client.Object {
	out := append([]client.Object{}, objects...)
	out = append(out, partialAdmissionObjects()...)
	return out
}

func TestPartialSourceFenceActuatorDeletesOnlyUIDMatchedStagedSourcePod(t *testing.T) {
	plan, pod, node := partialFixture()
	pod.UID = "source-uid"
	pod.Spec.NodeName = "node-a"
	c := testClient(t, withPartialAdmission(plan, node, pod)...)
	r := NewReconciler(c, c, "spot-cluster")
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(plan)}); err != nil {
		t.Fatal(err)
	}
	var old corev1.Pod
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), &old); !apierrors.IsNotFound(err) {
		t.Fatalf("source pod was not gracefully deleted by fake client: %v", err)
	}
	var got api.RestorePlan
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(plan), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != "Prepared" || len(got.Status.SourceFences) != 1 || got.Status.SourceFences[0].Phase != "DeleteRequested" || got.Status.SourceFences[0].DeleteRequestedAt == nil {
		t.Fatalf("unexpected fence status: phase=%s fences=%+v", got.Status.Phase, got.Status.SourceFences)
	}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(plan)}); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(plan), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.SourceFences[0].Phase != "SourceGone" || got.Status.SourceFences[0].GoneObservedAt == nil {
		t.Fatalf("source UID gone proof missing: %+v", got.Status.SourceFences)
	}
}

func TestPartialSourceFenceRefusesAbsentNameWithoutPriorDeleteProof(t *testing.T) {
	plan, _, node := partialFixture()
	c := testClient(t, withPartialAdmission(plan, node)...)
	r := NewReconciler(c, c, "spot-cluster")
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(plan)}); err != nil {
		t.Fatal(err)
	}
	var got api.RestorePlan
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(plan), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != "Failed" || got.Status.SourceFences[0].Phase != "Refused" {
		t.Fatalf("absent source name should not prove fencing: %+v", got.Status)
	}
}

func TestPartialSourceFenceRefusesUnsafeUIDCollisionsBeforeDelete(t *testing.T) {
	cases := []struct {
		name   string
		change func(*api.RestorePlan, *corev1.Pod)
	}{
		{"uid mismatch", func(_ *api.RestorePlan, p *corev1.Pod) { p.UID = "other-uid" }},
		{"replayed plan uid", func(p *api.RestorePlan, pod *corev1.Pod) {
			pod.UID = "new-uid"
			pod.Annotations[PlanUIDAnnotation] = "old-plan"
			pod.Annotations[PlanGenerationAnnotation] = fmt.Sprint(p.Generation)
		}},
		{"replayed generation", func(p *api.RestorePlan, pod *corev1.Pod) {
			pod.UID = "new-uid"
			pod.Annotations[PlanUIDAnnotation] = string(p.UID)
			pod.Annotations[PlanGenerationAnnotation] = fmt.Sprint(p.Generation + 1)
		}},
		{"unstaged source", func(p *api.RestorePlan, pod *corev1.Pod) {
			pod.UID = "source-uid"
			p.Status.Artifacts = nil
		}},
		{"wrong owner", func(_ *api.RestorePlan, pod *corev1.Pod) {
			pod.UID = "source-uid"
			pod.Spec.NodeName = "node-a"
			pod.OwnerReferences[0].UID = "other-workload"
		}},
		{"unready source node", func(_ *api.RestorePlan, pod *corev1.Pod) {
			pod.UID = "source-uid"
			pod.Spec.NodeName = "node-a"
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan, pod, node := partialFixture()
			tc.change(plan, pod)
			if tc.name == "unready source node" {
				node.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionFalse}}
			}
			c := testClient(t, withPartialAdmission(plan, node, pod)...)
			r := NewReconciler(c, c, "spot-cluster")
			if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(plan)}); err != nil {
				t.Fatal(err)
			}
			var stillThere corev1.Pod
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), &stillThere); err != nil {
				t.Fatalf("pod was deleted despite refusal: %v", err)
			}
			var got api.RestorePlan
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(plan), &got); err != nil {
				t.Fatal(err)
			}
			if tc.name == "unstaged source" {
				if got.Status.Phase != "AwaitingArtifacts" || got.Status.SourceFences[0].Phase != "Pending" {
					t.Fatalf("unstaged source should wait without delete: %+v", got.Status)
				}
				return
			}
			if got.Status.Phase != "Failed" || got.Status.SourceFences[0].Phase != "Refused" {
				t.Fatalf("unsafe collision not refused: %+v", got.Status)
			}
		})
	}
}

func TestPartialReplacementPodAfterFenceMustRemainBoundToPlanAndNode(t *testing.T) {
	plan, pod, node := partialFixture()
	now := metav1.Now()
	plan.Status.SourceFences = []api.SourcePodFenceStatus{{PodName: "trainer-1", SourcePodUID: "source-uid", ObservedGeneration: plan.Generation, Phase: "DeleteRequested", DeleteRequestedAt: &now}}
	if err := NewWebhook(testClient(t, plan, node), "spot-cluster").Apply(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	inject(pod)
	pod.UID = "replacement-uid"
	pod.Spec.NodeName = "node-a"
	pod.Status.Phase = corev1.PodRunning
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	c := testClient(t, withPartialAdmission(plan, node, pod)...)
	r := NewReconciler(c, c, "spot-cluster")
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(plan)}); err != nil {
		t.Fatal(err)
	}
	var got api.RestorePlan
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(plan), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != "Running" || got.Status.SourceFences[0].Phase != "SourceGone" || got.Status.Pods[0].UID != "replacement-uid" {
		t.Fatalf("replacement proof not accepted: %+v", got.Status)
	}
}

func TestPartialReplacementPodWithoutPriorFenceIsReplay(t *testing.T) {
	plan, pod, node := partialFixture()
	if err := NewWebhook(testClient(t, plan, node), "spot-cluster").Apply(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	pod.UID = "replacement-uid"
	c := testClient(t, withPartialAdmission(plan, node, pod)...)
	r := NewReconciler(c, c, "spot-cluster")
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(plan)}); err != nil {
		t.Fatal(err)
	}
	var got api.RestorePlan
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(plan), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != "Failed" || got.Status.SourceFences[0].Phase != "Refused" {
		t.Fatalf("replacement replay accepted without prior source fence: %+v", got.Status)
	}
}
