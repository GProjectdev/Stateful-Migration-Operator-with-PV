package member

import (
	"context"
	"fmt"
	api "github.com/GProjectdev/Stateful-Migration-Operator-with-PV/api/v1alpha1"
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
