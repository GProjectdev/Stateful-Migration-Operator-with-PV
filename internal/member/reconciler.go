package member

import (
	"context"
	"fmt"
	api "github.com/GProjectdev/Stateful-Migration-Operator-with-PV/api/v1alpha1"
	"github.com/GProjectdev/Stateful-Migration-Operator-with-PV/internal/artifact"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/util/retry"
	"reflect"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"time"
)

type Reconciler struct {
	Client      client.Client
	Reader      client.Reader
	ClusterName string
}

func NewReconciler(c client.Client, reader client.Reader, clusterName string) *Reconciler {
	return &Reconciler{Client: c, Reader: reader, ClusterName: clusterName}
}
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Client == nil || r.Reader == nil || r.ClusterName == "" {
		return fmt.Errorf("local client, uncached reader and cluster name required")
	}
	return ctrl.NewControllerManagedBy(mgr).Named("member-restore").For(&api.RestorePlan{}).Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(r.plansForPod)).Complete(r)
}

func (r *Reconciler) plansForPod(ctx context.Context, obj client.Object) []reconcile.Request {
	var plans api.RestorePlanList
	if err := r.Reader.List(ctx, &plans, client.InNamespace(obj.GetNamespace())); err != nil {
		return nil
	}
	requests := []reconcile.Request{}
	for _, plan := range plans.Items {
		if plan.Spec.TargetCluster != r.ClusterName {
			continue
		}
		for _, mapping := range plan.Spec.Pods {
			if mapping.TargetPod == obj.GetName() {
				requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&plan)})
				break
			}
		}
	}
	return requests
}

func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var plan api.RestorePlan
		if err := r.Reader.Get(ctx, req.NamespacedName, &plan); err != nil {
			return client.IgnoreNotFound(err)
		}
		if plan.Spec.TargetCluster != r.ClusterName || !plan.DeletionTimestamp.IsZero() {
			return nil
		}
		before := plan.DeepCopy().Status
		phase, message, pods, err := r.evaluate(ctx, &plan)
		if err != nil {
			return err
		}
		// Each retry starts with a fresh object, retaining independent node reports.
		plan.Status.ObservedGeneration = plan.Generation
		plan.Status.Phase, plan.Status.Message, plan.Status.Pods = phase, message, pods
		if reflect.DeepEqual(before, plan.Status) {
			return nil
		}
		return r.Client.Status().Update(ctx, &plan)
	})
	return ctrl.Result{RequeueAfter: 30 * time.Second}, err
}

func (r *Reconciler) evaluate(ctx context.Context, plan *api.RestorePlan) (string, string, []api.PodStatus, error) {
	if err := validatePlan(plan, r.ClusterName); err != nil {
		return "Failed", err.Error(), nil, nil
	}
	prepared, running := true, true
	failure := ""
	statuses := make([]api.PodStatus, 0, len(plan.Spec.Pods))
	for _, mapping := range plan.Spec.Pods {
		var node corev1.Node
		if err := r.Reader.Get(ctx, client.ObjectKey{Name: mapping.TargetNode}, &node); err != nil {
			if !apierrors.IsNotFound(err) {
				return "", "", nil, err
			}
			failure = "target node does not exist"
		} else if node.Labels[RuntimeCapabilityLabel] != "true" {
			failure = "target node lacks admin-certified restore-from-file capability"
		}
		if !artifact.Fresh(plan, mapping.TargetNode, time.Now()) {
			prepared = false
		}
		for _, report := range plan.Status.Artifacts {
			if report.NodeName == mapping.TargetNode && report.ObservedGeneration == plan.Generation && !report.Verified {
				failure = "target archive verification failed"
			}
		}
		var pod corev1.Pod
		err := r.Reader.Get(ctx, client.ObjectKey{Namespace: plan.Namespace, Name: mapping.TargetPod}, &pod)
		if apierrors.IsNotFound(err) {
			running = false
			statuses = append(statuses, api.PodStatus{Name: mapping.TargetPod, Phase: "Pending", Message: "waiting for externally created Pod"})
			continue
		}
		if err != nil {
			return "", "", nil, err
		}
		status := api.PodStatus{Name: pod.Name, UID: string(pod.UID), Phase: string(pod.Status.Phase)}
		if err := verifyBoundPod(plan, &mapping, &pod, true); err != nil {
			status.Phase, status.Message = "Failed", err.Error()
			failure = err.Error()
		}
		if !pod.DeletionTimestamp.IsZero() {
			status.Phase, status.Message = "Failed", "planned Pod is deleting"
			failure = status.Message
		}
		if pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
			status.Phase, status.Message = "Failed", "planned Pod terminated"
			failure = status.Message
		}
		for _, cs := range append(append([]corev1.ContainerStatus{}, pod.Status.InitContainerStatuses...), pod.Status.ContainerStatuses...) {
			if (cs.State.Terminated != nil && cs.State.Terminated.ExitCode != 0) || (cs.State.Waiting != nil && (cs.State.Waiting.Reason == "CrashLoopBackOff" || cs.State.Waiting.Reason == "CreateContainerError" || cs.State.Waiting.Reason == "RunContainerError" || cs.State.Waiting.Reason == "ImagePullBackOff")) {
				status.Phase, status.Message = "Failed", "container startup or execution failed"
				failure = status.Message
			}
		}
		ready := false
		for _, condition := range pod.Status.Conditions {
			if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
				ready = true
			}
		}
		if pod.Status.Phase != corev1.PodRunning || !ready || pod.Spec.NodeName != mapping.TargetNode {
			running = false
		}
		statuses = append(statuses, status)
	}
	if failure != "" {
		return "Failed", failure, statuses, nil
	}
	if !prepared {
		return "AwaitingArtifacts", "waiting for fresh current-generation node archive reports", statuses, nil
	}
	if running {
		return "Running", "all planned Pods are Running and Ready; CRIU restore success is not attested", statuses, nil
	}
	return "Prepared", "target archives verified; waiting for planned Pods to become Running and Ready", statuses, nil
}
