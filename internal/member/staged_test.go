package member

import (
	"context"
	"fmt"
	api "github.com/GProjectdev/Stateful-Migration-Operator-with-PV/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"testing"
	"time"
)

func TestStagedLauncherBeforeReadiness(t *testing.T) {
	for _, mode := range []string{"staged", "probe-fails", "no-transport", "source-not-fenced", "archive-stale", "wrong-plan", "restarted", "survivor-changed", "target-changed", "plan-changed", "ready"} {
		t.Run(mode, func(t *testing.T) {
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
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: plan.Spec.Pods[0].Archives[0].ContainerName, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}
			survivor := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: plan.Namespace, Name: "trainer-0", UID: "survivor-uid"}, Spec: corev1.PodSpec{NodeName: "node-survivor"}}
			switch mode {
			case "source-not-fenced":
				plan.Status.SourceFences = nil
			case "archive-stale":
				plan.Status.Artifacts[0].CheckedAt = metav1.NewTime(time.Now().Add(-time.Hour))
			case "wrong-plan":
				pod.Annotations[PlanUIDAnnotation] = "other"
			case "restarted":
				pod.Status.ContainerStatuses[0].RestartCount = 1
			case "survivor-changed":
				survivor.UID = "other"
			case "ready":
				pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
			}
			c := testClient(t, withPartialAdmission(plan, node, pod, survivor)...)
			r := NewReconciler(c, c, "spot-cluster")
			calls := 0
			if mode != "no-transport" {
				r.StageProbe = func(ctx context.Context, p *corev1.Pod, container string, command []string) error {
					calls++
					if p.UID != pod.UID || container == "" || len(command) != 5 || command[1] != "-S" {
						t.Fatal("unbound probe")
					}
					if mode == "probe-fails" {
						return fmt.Errorf("wrong checkpoint")
					}
					if mode == "target-changed" {
						if err := c.Delete(ctx, p); err != nil {
							t.Fatal(err)
						}
						replacement := p.DeepCopy()
						replacement.UID = "different"
						replacement.ResourceVersion = ""
						if err := c.Create(ctx, replacement); err != nil {
							t.Fatal(err)
						}
					}
					if mode == "plan-changed" {
						var current api.RestorePlan
						if err := c.Get(ctx, client.ObjectKeyFromObject(plan), &current); err != nil {
							t.Fatal(err)
						}
						current.Generation++
						if err := c.Update(ctx, &current); err != nil {
							t.Fatal(err)
						}
					}
					return nil
				}
			}
			phase, _, statuses, fences, err := r.evaluate(context.Background(), plan)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "staged" {
				if phase != "StagedReady" || calls != 1 || statuses[0].Phase != "Staged" || fences[0].Phase != "SourceGone" {
					t.Fatalf("missing staging: %s %+v", phase, statuses)
				}
			} else if mode == "ready" {
				if phase != "Running" || calls != 0 {
					t.Fatalf("ready path changed: %s calls=%d", phase, calls)
				}
			} else if phase == "StagedReady" || phase == "Running" {
				t.Fatalf("unsafe evidence accepted: %s", phase)
			}
			if mode == "source-not-fenced" || mode == "archive-stale" || mode == "wrong-plan" || mode == "restarted" || mode == "survivor-changed" {
				if calls != 0 {
					t.Fatal("probe before safety gates")
				}
			}
		})
	}
}
