package member

import (
	"context"
	"fmt"
	"strconv"

	api "github.com/GProjectdev/Stateful-Migration-Operator-with-PV/api/v1alpha1"
	"github.com/GProjectdev/Stateful-Migration-Operator-with-PV/internal/artifact"
	"github.com/GProjectdev/Stateful-Migration-Operator-with-PV/internal/groupcontract"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"reflect"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"time"
)

const WebhookServiceName = "stateful-restore-webhook"
const WebhookServiceNamespace = "stateful-migration-system"

type Reconciler struct {
	StageProbe        func(context.Context, *corev1.Pod, string, []string) error
	GroupControlImage string
	Client            client.Client
	Reader            client.Reader
	ClusterName       string
}

func NewReconciler(c client.Client, reader client.Reader, clusterName string) *Reconciler {
	return &Reconciler{Client: c, Reader: reader, ClusterName: clusterName}
}
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Client == nil || r.Reader == nil || r.ClusterName == "" {
		return fmt.Errorf("local client, uncached reader and cluster name required")
	}
	if r.StageProbe == nil {
		probe, err := newStageProbe(mgr.GetConfig())
		if err != nil {
			return err
		}
		r.StageProbe = probe
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
		if (plan.Spec.TargetCluster != r.ClusterName && !(plan.Spec.GroupRestore != nil && plan.Spec.SourceCluster == r.ClusterName)) || !plan.DeletionTimestamp.IsZero() {
			return nil
		}
		before := plan.DeepCopy().Status
		if plan.Spec.TargetCluster == r.ClusterName && groupcontract.TargetVerified(&plan) {
			return nil
		}
		phase, message, pods, sourceFences, err := r.evaluate(ctx, &plan)
		if err != nil {
			return err
		}
		// Each retry starts with a fresh object, retaining independent node reports.
		plan.Status.ObservedGeneration = plan.Generation
		plan.Status.Phase, plan.Status.Message, plan.Status.Pods = phase, message, pods
		plan.Status.SourceFences = sourceFences
		if reflect.DeepEqual(before, plan.Status) {
			return nil
		}
		return r.Client.Status().Update(ctx, &plan)
	})
	return ctrl.Result{RequeueAfter: 30 * time.Second}, err
}

func (r *Reconciler) evaluate(ctx context.Context, plan *api.RestorePlan) (string, string, []api.PodStatus, []api.SourcePodFenceStatus, error) {
	if plan.Spec.GroupRestore != nil {
		if err := validatePlan(plan, plan.Spec.TargetCluster); err != nil {
			return "Failed", err.Error(), nil, plan.Status.SourceFences, nil
		}
		if err := api.ValidateGroup(plan.Spec.GroupRestore, plan.Spec.PartialRestore, plan.Spec.WorkloadRef, plan.Spec.Pods); err != nil {
			return "Failed", err.Error(), nil, plan.Status.SourceFences, nil
		}
		if plan.Spec.SourceCluster == r.ClusterName {
			done, fences, err := r.fenceGroup(ctx, plan)
			if err != nil {
				return "AwaitingSourceFence", err.Error(), nil, plan.Status.SourceFences, nil
			}
			plan.Status.SourceFences = fences
			if !done {
				return "SourceFencing", "waiting for every current source UID to be fenced", nil, fences, nil
			}
			if plan.Spec.TargetCluster != r.ClusterName {
				return "SourceFenced", "all current source world UIDs fenced", nil, fences, nil
			}
		}
		if err := groupSourceReceipt(plan); err != nil {
			return "AwaitingSourceFence", err.Error(), nil, plan.Status.SourceFences, nil
		}
		done, err := r.prepareGroup(ctx, plan)
		if err != nil {
			return "Preparing", err.Error(), nil, plan.Status.SourceFences, nil
		}
		if !done {
			return "Preparing", "waiting for operation-owned prepare Job", nil, plan.Status.SourceFences, nil
		}
	}
	if err := validatePlan(plan, r.ClusterName); err != nil {
		return "Failed", err.Error(), nil, nil, nil
	}
	if plan.Spec.LocalPodRestore {
		if err := validateLocalCheckpoint(ctx, r.Reader, plan); err != nil {
			return "Failed", err.Error(), nil, nil, nil
		}
	}
	prepared, running := true, true
	staged := plan.Spec.PartialRestore != nil
	failure := ""
	statuses := make([]api.PodStatus, 0, len(plan.Spec.Pods))
	sourceFences := []api.SourcePodFenceStatus(nil)
	if plan.Spec.GroupRestore != nil {
		sourceFences = plan.Status.SourceFences
	}
	partial := plan.Spec.PartialRestore != nil || plan.Spec.LocalPodRestore
	for _, mapping := range plan.Spec.Pods {
		mappingReady := true
		var node corev1.Node
		if err := r.Reader.Get(ctx, client.ObjectKey{Name: mapping.TargetNode}, &node); err != nil {
			if !apierrors.IsNotFound(err) {
				return "", "", nil, nil, err
			}
			failure = "target node does not exist"
			mappingReady = false
		} else if node.Labels[RuntimeCapabilityLabel] != "true" {
			failure = "target node lacks admin-certified restore-from-file capability"
			mappingReady = false
		}
		if !artifact.Fresh(plan, mapping.TargetNode, time.Now()) {
			prepared = false
			mappingReady = false
		}
		for _, report := range plan.Status.Artifacts {
			if report.NodeName == mapping.TargetNode && report.ObservedGeneration == plan.Generation && !report.Verified {
				failure = "target archive verification failed"
				mappingReady = false
			}
		}
		var pod corev1.Pod
		err := r.Reader.Get(ctx, client.ObjectKey{Namespace: plan.Namespace, Name: mapping.TargetPod}, &pod)
		if partial {
			fence, fenced, fail, err := r.ensureSourceFenced(ctx, plan, &mapping, mappingReady, previousSourceFence(plan, &mapping), &pod, err)
			sourceFences = append(sourceFences, fence)
			if err != nil {
				return "", "", nil, nil, err
			}
			if fail != "" {
				status := api.PodStatus{Name: mapping.TargetPod, Phase: "Failed", Message: fail}
				if err == nil {
					status.UID = string(pod.UID)
				}
				statuses = append(statuses, status)
				failure = fail
				continue
			}
			if !fenced {
				running = false
				statuses = append(statuses, api.PodStatus{Name: mapping.TargetPod, Phase: "SourceFencing", Message: fence.Message})
				continue
			}
		}
		if apierrors.IsNotFound(err) {
			running = false
			statuses = append(statuses, api.PodStatus{Name: mapping.TargetPod, Phase: "Pending", Message: "waiting for externally created Pod"})
			continue
		}
		if err != nil {
			return "", "", nil, nil, err
		}
		status := api.PodStatus{Name: pod.Name, UID: string(pod.UID), Phase: string(pod.Status.Phase)}
		if err := verifyBoundPod(plan, &mapping, &pod, true); err != nil {
			status.Phase, status.Message = "Failed", err.Error()
			failure = err.Error()
		}
		if plan.Spec.GroupRestore != nil {
			if err := groupPVCMatches(plan, &pod); err != nil {
				status.Phase, status.Message = "Failed", err.Error()
				failure = err.Error()
			}
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
		if staged && !ready && failure == "" && mappingReady {
			if err := r.probeStagedTarget(ctx, plan, &mapping, &pod, sourceFences); err != nil {
				status.Message = "waiting for staged launcher evidence: " + err.Error()
			} else {
				status.Phase = "Staged"
			}
		}
		statuses = append(statuses, status)
	}
	if failure != "" {
		return "Failed", failure, statuses, sourceFences, nil
	}
	if !prepared {
		return "AwaitingArtifacts", "waiting for fresh current-generation node archive reports", statuses, sourceFences, nil
	}
	if staged && !running && allTargetsStaged(plan, statuses) {
		return "StagedReady", "restored launchers staged; waiting for restore-owned round release", statuses, sourceFences, nil
	}
	if running {
		if plan.Spec.GroupRestore != nil {
			done, err := r.resumeGroup(ctx, plan)
			if err != nil {
				return "Resuming", err.Error(), statuses, sourceFences, nil
			}
			if !done {
				return "Resuming", "waiting for operation-owned resume Job", statuses, sourceFences, nil
			}
		}
		return "Running", "all planned Pods are Running and Ready; CRIU restore success is not attested", statuses, sourceFences, nil
	}
	return "Prepared", "target archives verified; waiting for planned Pods to become Running and Ready", statuses, sourceFences, nil
}

func previousSourceFence(plan *api.RestorePlan, mapping *api.RestorePod) *api.SourcePodFenceStatus {
	for i := range plan.Status.SourceFences {
		fence := &plan.Status.SourceFences[i]
		if fence.PodName == mapping.TargetPod && fence.SourcePodUID == mapping.SourcePodUID && fence.ObservedGeneration == plan.Generation {
			return fence
		}
	}
	return nil
}

func (r *Reconciler) ensureSourceFenced(ctx context.Context, plan *api.RestorePlan, mapping *api.RestorePod, staged bool, previous *api.SourcePodFenceStatus, pod *corev1.Pod, podErr error) (api.SourcePodFenceStatus, bool, string, error) {
	fence := api.SourcePodFenceStatus{PodName: mapping.TargetPod, SourcePodUID: mapping.SourcePodUID, ObservedGeneration: plan.Generation, Phase: "Pending", Message: "waiting for fresh archive, survivor evidence, certified target node, and ready source node before source Pod deletion"}
	if previous != nil {
		fence.DeleteRequestedAt = previous.DeleteRequestedAt
		fence.GoneObservedAt = previous.GoneObservedAt
	}
	if mapping.SourcePodUID == "" {
		fence.Phase, fence.Message = "Refused", "partial restore mapping is missing sourcePodUID"
		return fence, false, fence.Message, nil
	}
	if apierrors.IsNotFound(podErr) {
		if previous == nil || previous.DeleteRequestedAt == nil {
			fence.Phase, fence.Message = "Refused", "source Pod name is absent without a controller-owned delete request"
			return fence, false, fence.Message, nil
		}
		if fence.GoneObservedAt == nil {
			now := metav1.Now()
			fence.GoneObservedAt = &now
		}
		fence.Phase, fence.Message = "SourceGone", "source Pod UID is gone after controller-owned UID-precondition delete"
		return fence, true, "", nil
	}
	if podErr != nil {
		return fence, false, "", podErr
	}
	if string(pod.UID) == mapping.SourcePodUID {
		if !staged {
			return fence, false, "", nil
		}
		// Persist local deletion intent before the external side effect so a
		// restart after Delete cannot lose the UID-bound fencing provenance.
		if plan.Spec.LocalPodRestore && fence.DeleteRequestedAt == nil {
			now := metav1.Now()
			fence.Phase, fence.Message, fence.DeleteRequestedAt = "DeleteRequested", "persisting UID-bound source deletion intent", &now
			return fence, false, "", nil
		}
		if err := validateSourcePodForDeletion(ctx, r.Reader, plan, mapping, pod); err != nil {
			fence.Phase, fence.Message = "Refused", err.Error()
			return fence, false, fence.Message, nil
		}
		if !pod.DeletionTimestamp.IsZero() {
			fence.Phase, fence.Message = "DeleteRequested", "source Pod UID is already deleting after controller-owned request"
			return fence, false, "", nil
		}
		uid := types.UID(mapping.SourcePodUID)
		grace := int64(30)
		if err := r.Client.Delete(ctx, pod, client.GracePeriodSeconds(grace), client.Preconditions{UID: &uid}); err != nil && !apierrors.IsNotFound(err) {
			return fence, false, "", err
		}
		now := metav1.Now()
		fence.Phase, fence.Message, fence.DeleteRequestedAt = "DeleteRequested", "UID-precondition graceful delete requested for source Pod", &now
		return fence, false, "", nil
	}
	if pod.Annotations[PlanUIDAnnotation] == string(plan.UID) && pod.Annotations[PlanGenerationAnnotation] == strconv.FormatInt(plan.Generation, 10) {
		if previous == nil || previous.DeleteRequestedAt == nil {
			fence.Phase, fence.Message = "Refused", "replacement Pod appeared before a controller-owned source delete request"
			return fence, false, fence.Message, nil
		}
		if fence.GoneObservedAt == nil {
			now := metav1.Now()
			fence.GoneObservedAt = &now
		}
		fence.Phase, fence.Message = "SourceGone", "replacement Pod is bound to current restore plan after old source UID was deleted"
		return fence, true, "", nil
	}
	fence.Phase, fence.Message = "Refused", "mapped Pod UID collision before source UID was fenced"
	return fence, false, fence.Message, nil
}

func validateSourcePodForDeletion(ctx context.Context, reader client.Reader, plan *api.RestorePlan, mapping *api.RestorePod, pod *corev1.Pod) error {
	if pod.Spec.NodeName == "" {
		return fmt.Errorf("source Pod is not bound to a node")
	}
	if mapping.SourceNode != "" && pod.Spec.NodeName != mapping.SourceNode {
		return fmt.Errorf("source Pod node does not match checkpoint evidence")
	}
	memberWorkloadUID, err := validateAdmissionReady(ctx, reader, plan)
	if err != nil {
		return err
	}
	if !sourcePodOwnedByWorkload(plan, pod, memberWorkloadUID) {
		return fmt.Errorf("source Pod owner does not match restore workload identity")
	}
	var node corev1.Node
	if err := reader.Get(ctx, client.ObjectKey{Name: pod.Spec.NodeName}, &node); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("source Pod node does not exist")
		}
		return err
	}
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady && condition.Status == corev1.ConditionTrue {
			return nil
		}
	}
	return fmt.Errorf("source Pod node is not Ready")
}

func validateAdmissionReady(ctx context.Context, reader client.Reader, plan *api.RestorePlan) (string, error) {
	if plan.Spec.WorkloadRef.Kind != "StatefulSet" {
		return plan.Spec.WorkloadRef.UID, nil
	}
	var sts appsv1.StatefulSet
	if err := reader.Get(ctx, client.ObjectKey{Namespace: plan.Namespace, Name: plan.Spec.WorkloadRef.Name}, &sts); err != nil {
		if apierrors.IsNotFound(err) {
			return "", fmt.Errorf("restore workload StatefulSet does not exist")
		}
		return "", err
	}
	if !sts.DeletionTimestamp.IsZero() {
		return "", fmt.Errorf("restore workload StatefulSet is deleting")
	}
	originUID := sts.Labels[WorkloadUIDLabel]
	if originUID == "" {
		originUID = string(sts.UID)
	}
	if originUID != plan.Spec.WorkloadRef.UID {
		return "", fmt.Errorf("restore workload StatefulSet origin UID mismatch")
	}
	if sts.Spec.Template.Labels[WorkloadUIDLabel] != plan.Spec.WorkloadRef.UID {
		return "", fmt.Errorf("StatefulSet template lacks workload UID label required by partial restore admission")
	}
	if sts.Spec.Template.Annotations[InjectAnnotation] != "true" {
		return "", fmt.Errorf("StatefulSet template lacks fluidcr inject opt-in required before source deletion")
	}
	var endpoints corev1.Endpoints
	if err := reader.Get(ctx, client.ObjectKey{Namespace: WebhookServiceNamespace, Name: WebhookServiceName}, &endpoints); err != nil {
		if apierrors.IsNotFound(err) {
			return "", fmt.Errorf("restore webhook endpoints are not available")
		}
		return "", err
	}
	for _, subset := range endpoints.Subsets {
		if len(subset.Addresses) == 0 || len(subset.Ports) == 0 {
			continue
		}
		for _, port := range subset.Ports {
			if port.Port == 9443 || port.Name == "webhook" || port.Port == 443 {
				return string(sts.UID), nil
			}
		}
	}
	return "", fmt.Errorf("restore webhook endpoints have no ready webhook backend")
}

func sourcePodOwnedByWorkload(plan *api.RestorePlan, pod *corev1.Pod, memberWorkloadUID string) bool {
	if plan.Spec.WorkloadRef.Kind == "Pod" {
		if plan.Spec.LocalPodRestore && string(pod.UID) != plan.Spec.WorkloadRef.UID {
			return false
		}
		return pod.Name == plan.Spec.WorkloadRef.Name && len(pod.OwnerReferences) == 0
	}
	owner := metav1.GetControllerOf(pod)
	if owner == nil || owner.APIVersion != plan.Spec.WorkloadRef.APIVersion || owner.Kind != plan.Spec.WorkloadRef.Kind || owner.Name != plan.Spec.WorkloadRef.Name {
		return false
	}
	if memberWorkloadUID != "" && string(owner.UID) != memberWorkloadUID {
		return false
	}
	return plan.Spec.WorkloadRef.UID == "" || pod.Labels[WorkloadUIDLabel] == plan.Spec.WorkloadRef.UID
}
