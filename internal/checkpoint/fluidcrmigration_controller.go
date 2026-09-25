/*
Copyright 2026 Leehun.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package checkpoint

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	fluidcrv1alpha1 "github.com/GProjectdev/Stateful-Migration-Operator-with-PV/api/fluidcr/v1alpha1"
	"github.com/GProjectdev/Stateful-Migration-Operator-with-PV/internal/ctrlapi"
)

const (
	// FinalizerName guards in-flight migrations so the controller can resume a
	// paused workload before the resource disappears.
	FinalizerName = "fluidcrmigration.fluidcr.dcnlab.com/finalizer"

	conditionReady       = "Ready"
	defaultTimeoutSecs   = 300
	waitRequeueInterval  = 15 * time.Second
	statusUpdateAttempts = 5
)

// CtrlAPI is the subset of the in-pod FluidCR control API the controller uses.
type CtrlAPI interface {
	Checkpoint(ctx context.Context, podIP string, port int, timeout time.Duration) (map[string]string, error)
	Resume(ctx context.Context, podIP string, port int, timeout time.Duration) (map[string]string, error)
}

// KubeletAPI is the subset of the kubelet checkpoint API the controller uses.
type KubeletAPI interface {
	Checkpoint(ctx context.Context, hostIP, namespace, pod, container string, timeout time.Duration) (string, error)
}

// FluidCRMigrationReconciler reconciles a FluidCRMigration object.
type FluidCRMigrationReconciler struct {
	client.Client
	Scheme        *runtime.Scheme
	CtrlClient    CtrlAPI
	KubeletClient KubeletAPI
	// APIReader should be mgr.GetAPIReader() for uncached identity checks.
	APIReader client.Reader
}

// target captures everything the workflow needs about a single pod.
type target struct {
	podUID    types.UID
	podName   string
	namespace string
	podIP     string
	hostIP    string
	container string
	port      int
}

// +kubebuilder:rbac:groups=fluidcr.dcnlab.com,resources=fluidcrmigrations,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=fluidcr.dcnlab.com,resources=fluidcrmigrations/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=fluidcr.dcnlab.com,resources=fluidcrmigrations/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=nodes/checkpoint,verbs=create
// +kubebuilder:rbac:groups=apps,resources=deployments;statefulsets,verbs=get;list;watch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch

// Reconcile drives a FluidCRMigration through its checkpoint(+resume) workflow.
func (r *FluidCRMigrationReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var migration fluidcrv1alpha1.FluidCRMigration
	if err := r.Get(ctx, req.NamespacedName, &migration); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if migration.DeletionTimestamp != nil {
		return r.reconcileDelete(ctx, &migration)
	}

	if !controllerutil.ContainsFinalizer(&migration, FinalizerName) {
		controllerutil.AddFinalizer(&migration, FinalizerName)
		if err := r.Update(ctx, &migration); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Single-shot: do nothing once terminal for the current spec generation.
	if isTerminalPhase(migration.Status.Phase) && migration.Status.ObservedGeneration == migration.Generation {
		return ctrl.Result{}, nil
	}

	log.Info("reconciling FluidCRMigration", "workload", migration.Spec.WorkloadRef.Name, "phase", migration.Status.Phase)
	return r.reconcileWorkflow(ctx, &migration)
}

// reconcileWorkflow advances a migration through its phases in a single pass,
// persisting status between phases so a controller restart can resume.
func (r *FluidCRMigrationReconciler) reconcileWorkflow(ctx context.Context, m *fluidcrv1alpha1.FluidCRMigration) (ctrl.Result, error) {
	if m.Generation != 1 || (m.Status.ObservedGeneration != 0 && m.Status.ObservedGeneration != m.Generation) {
		return r.markFailed(ctx, m, "checkpoint is generation-1 one-shot; create a new FluidCRMigration")
	}
	if r.CtrlClient == nil || r.KubeletClient == nil {
		return ctrl.Result{}, fmt.Errorf("checkpoint clients must be configured in the member process")
	}

	pods, err := r.resolveTargetPods(ctx, m)
	if err != nil {
		// Workload not found yet / transient list error: wait and retry.
		return r.markWaiting(ctx, m, fmt.Sprintf("waiting for workload pods: %v", err))
	}
	if len(pods) == 0 {
		return r.markWaiting(ctx, m, "no Running FluidCR-injected pods found for workload")
	}

	targets := make([]target, 0, len(pods))
	if len(m.Status.Pods) > 0 {
		if len(m.Status.Pods) != len(pods) {
			return r.markFailed(ctx, m, "checkpoint pod set changed; create a new FluidCRMigration")
		}
		for i := range pods {
			ps := getPodStatus(m, pods[i].Name)
			if ps == nil || ps.PodUID == "" || ps.PodUID != string(pods[i].UID) {
				return r.markFailed(ctx, m, "checkpoint pod UID changed or missing; create a new FluidCRMigration")
			}
		}
	}
	for i := range pods {
		pod := &pods[i]
		container, err := resolveContainerName(pod, m.Spec.Container)
		if err != nil {
			// Configuration error: terminal.
			return r.markFailed(ctx, m, err.Error())
		}
		port := resolveCtrlPort(pod, container, m.Spec.CtrlPort)
		ps := ensurePodStatus(m, pod.Name, pod.Spec.NodeName, pod.Status.PodIP)
		ps.PodUID = string(pod.UID)
		if ps.Phase == "" {
			ps.Phase = fluidcrv1alpha1.PodPhasePending
		}
		targets = append(targets, target{
			podUID:    pod.UID,
			podName:   pod.Name,
			namespace: pod.Namespace,
			podIP:     pod.Status.PodIP,
			hostIP:    pod.Status.HostIP,
			container: container,
			port:      port,
		})
	}

	if m.Status.StartTime == nil {
		now := metav1.Now()
		m.Status.StartTime = &now
	}
	m.Status.ObservedGeneration = m.Generation

	// Phase 1: application checkpoint (concurrent fan-out is mandatory so
	// distributed-training ranks do not deadlock on a collective barrier).
	appTargets := filterTargets(targets, m, func(ps *fluidcrv1alpha1.PodMigrationStatus) bool {
		return ps.Phase == fluidcrv1alpha1.PodPhasePending
	})
	if len(appTargets) > 0 {
		m.Status.Phase = fluidcrv1alpha1.PhaseAppCheckpointing
		m.Status.Message = fmt.Sprintf("signalling application checkpoint on %d pod(s)", len(appTargets))
		if err := r.saveStatus(ctx, m); err != nil {
			return ctrl.Result{}, err
		}
		outcomes := r.appCheckpoint(ctx, appTargets, timeoutOf(m.Spec.AppCheckpointTimeoutSeconds))
		for _, t := range appTargets {
			ps := getPodStatus(m, t.podName)
			oc := outcomes[t.podName]
			if oc.err != nil {
				ps.Phase = fluidcrv1alpha1.PodPhaseFailed
				ps.Message = fmt.Sprintf("app checkpoint: %v", oc.err)
				continue
			}
			ps.Phase = fluidcrv1alpha1.PodPhaseAppCheckpointed
			ps.AppCheckpointResult = oc.summary
			ps.Message = ""
		}
		if err := r.saveStatus(ctx, m); err != nil {
			return ctrl.Result{}, err
		}
	}
	appFailed := anyPodFailed(m)

	// Phase 2: container checkpoint (CRIU). Skipped entirely if any pod failed
	// the application checkpoint, since a partial set is not consistent.
	if !appFailed {
		ckptTargets := filterTargets(targets, m, func(ps *fluidcrv1alpha1.PodMigrationStatus) bool {
			return podRank(ps.Phase) >= podRank(fluidcrv1alpha1.PodPhaseAppCheckpointed) &&
				podRank(ps.Phase) < podRank(fluidcrv1alpha1.PodPhaseContainerCheckpointed)
		})
		if len(ckptTargets) > 0 {
			m.Status.Phase = fluidcrv1alpha1.PhaseContainerCheckpointing
			m.Status.Message = fmt.Sprintf("creating CRIU container checkpoint on %d pod(s)", len(ckptTargets))
			if err := r.saveStatus(ctx, m); err != nil {
				return ctrl.Result{}, err
			}
			outcomes := r.containerCheckpoint(ctx, ckptTargets, timeoutOf(m.Spec.KubeletTimeoutSeconds))
			for _, t := range ckptTargets {
				ps := getPodStatus(m, t.podName)
				oc := outcomes[t.podName]
				if oc.err != nil {
					ps.Phase = fluidcrv1alpha1.PodPhaseFailed
					ps.Message = fmt.Sprintf("container checkpoint: %v", oc.err)
					continue
				}
				now := metav1.Now()
				ps.CheckpointFiles = append(ps.CheckpointFiles, fluidcrv1alpha1.CheckpointFile{
					ContainerName:  oc.container,
					FilePath:       oc.path,
					CheckpointTime: &now,
				})
				ps.Phase = fluidcrv1alpha1.PodPhaseContainerCheckpointed
				ps.Message = ""
			}
			if err := r.saveStatus(ctx, m); err != nil {
				return ctrl.Result{}, err
			}
		}
	}
	checkpointFailed := anyPodFailed(m)

	// Phase 3: resume in place (best effort). Always attempted for pods whose
	// application checkpoint succeeded, even when a later step failed, so a
	// paused workload is never left stuck.
	var resumeErrs []string
	if shouldResume(m) {
		// Resume every pod whose application checkpoint actually ran (workers are
		// paused on the lock), even if a later step failed for it, so the job is
		// never left stuck. A pod that failed AT the application checkpoint has no
		// AppCheckpointResult and is therefore skipped.
		resumeTargets := filterTargets(targets, m, func(ps *fluidcrv1alpha1.PodMigrationStatus) bool {
			return ps.AppCheckpointResult != "" && ps.Phase != fluidcrv1alpha1.PodPhaseResumed
		})
		if len(resumeTargets) > 0 {
			m.Status.Phase = fluidcrv1alpha1.PhaseResuming
			m.Status.Message = fmt.Sprintf("resuming %d pod(s) in place", len(resumeTargets))
			if err := r.saveStatus(ctx, m); err != nil {
				return ctrl.Result{}, err
			}
			outcomes := r.resume(ctx, resumeTargets, timeoutOf(m.Spec.AppCheckpointTimeoutSeconds))
			for _, t := range resumeTargets {
				ps := getPodStatus(m, t.podName)
				if err := outcomes[t.podName]; err != nil {
					resumeErrs = append(resumeErrs, fmt.Sprintf("%s: %v", t.podName, err))
					if ps.Phase != fluidcrv1alpha1.PodPhaseFailed {
						ps.Message = fmt.Sprintf("resume: %v", err)
					}
					continue
				}
				if ps.Phase != fluidcrv1alpha1.PodPhaseFailed {
					ps.Phase = fluidcrv1alpha1.PodPhaseResumed
				}
			}
			if err := r.saveStatus(ctx, m); err != nil {
				return ctrl.Result{}, err
			}
		}
	}

	now := metav1.Now()
	m.Status.CompletionTime = &now
	switch {
	case checkpointFailed:
		msg := "checkpoint failed; see pod statuses"
		if len(resumeErrs) > 0 {
			msg += "; resume errors: " + strings.Join(resumeErrs, "; ")
		}
		return r.finalize(ctx, m, fluidcrv1alpha1.PhaseFailed, "CheckpointFailed", msg)
	case len(resumeErrs) > 0:
		return r.finalize(ctx, m, fluidcrv1alpha1.PhaseFailed, "ResumeFailed",
			"resume failed (workers may be paused): "+strings.Join(resumeErrs, "; "))
	default:
		msg := "checkpoint completed"
		if shouldResume(m) {
			msg += " and workload resumed in place"
		}
		return r.finalize(ctx, m, fluidcrv1alpha1.PhaseCompleted, "CheckpointCompleted", msg)
	}
}

// reconcileDelete best-effort resumes any still-paused pods, then drops the finalizer.
func (r *FluidCRMigrationReconciler) reconcileDelete(ctx context.Context, m *fluidcrv1alpha1.FluidCRMigration) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	if controllerutil.ContainsFinalizer(m, FinalizerName) {
		if shouldResume(m) && len(m.Status.Pods) > 0 {
			if pods, err := r.resolveTargetPods(ctx, m); err == nil {
				var resumeTargets []target
				for i := range pods {
					pod := &pods[i]
					ps := getPodStatus(m, pod.Name)
					if ps == nil || ps.PodUID == "" || ps.PodUID != string(pod.UID) || ps.AppCheckpointResult == "" {
						continue
					}
					container, cerr := resolveContainerName(pod, m.Spec.Container)
					if cerr != nil {
						return ctrl.Result{}, cerr
					}
					resumeTargets = append(resumeTargets, target{
						podUID:  pod.UID,
						podName: pod.Name, namespace: pod.Namespace, podIP: pod.Status.PodIP,
						container: container, port: resolveCtrlPort(pod, container, m.Spec.CtrlPort),
					})
				}
				if len(resumeTargets) > 0 {
					log.Info("best-effort resume on delete", "pods", len(resumeTargets))
					for pod, err := range r.resume(ctx, resumeTargets, timeoutOf(m.Spec.AppCheckpointTimeoutSeconds)) {
						if err != nil {
							return ctrl.Result{}, fmt.Errorf("resume on delete %s: %w", pod, err)
						}
					}
				}
			} else {
				return ctrl.Result{}, fmt.Errorf("resolve pods for deletion cleanup: %w", err)
			}
		}
		controllerutil.RemoveFinalizer(m, FinalizerName)
		if err := r.Update(ctx, m); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{}, nil
}

// appCheckpoint signals the in-pod control API on every target concurrently.
func (r *FluidCRMigrationReconciler) appCheckpoint(ctx context.Context, targets []target, timeout time.Duration) map[string]appOutcome {
	out := make(map[string]appOutcome, len(targets))
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := range targets {
		t := targets[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			var results map[string]string
			err := r.validateTarget(ctx, t)
			if err == nil {
				results, err = r.CtrlClient.Checkpoint(ctx, t.podIP, t.port, timeout)
			}
			mu.Lock()
			out[t.podName] = appOutcome{summary: ctrlapi.SummarizeResults(results), err: err}
			mu.Unlock()
		}()
	}
	wg.Wait()
	return out
}

// containerCheckpoint invokes the kubelet CRIU checkpoint API on every target.
func (r *FluidCRMigrationReconciler) containerCheckpoint(ctx context.Context, targets []target, timeout time.Duration) map[string]ckptOutcome {
	out := make(map[string]ckptOutcome, len(targets))
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := range targets {
		t := targets[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			var path string
			err := r.validateTarget(ctx, t)
			if err == nil {
				path, err = r.KubeletClient.Checkpoint(ctx, t.hostIP, t.namespace, t.podName, t.container, timeout)
				if err == nil && strings.TrimSpace(path) == "" {
					err = fmt.Errorf("empty checkpoint path")
				}
			}
			mu.Lock()
			out[t.podName] = ckptOutcome{container: t.container, path: path, err: err}
			mu.Unlock()
		}()
	}
	wg.Wait()
	return out
}

// resume releases the checkpoint locks on every target concurrently.
func (r *FluidCRMigrationReconciler) resume(ctx context.Context, targets []target, timeout time.Duration) map[string]error {
	out := make(map[string]error, len(targets))
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := range targets {
		t := targets[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := r.validateTarget(ctx, t)
			if err == nil {
				_, err = r.CtrlClient.Resume(ctx, t.podIP, t.port, timeout)
			}
			mu.Lock()
			out[t.podName] = err
			mu.Unlock()
		}()
	}
	wg.Wait()
	return out
}

type appOutcome struct {
	summary string
	err     error
}

type ckptOutcome struct {
	container string
	path      string
	err       error
}

// resolveTargetPods lists the Running, FluidCR-injected pods owned by the
// referenced workload. A Pod reference resolves to that single pod directly.
func (r *FluidCRMigrationReconciler) resolveTargetPods(ctx context.Context, m *fluidcrv1alpha1.FluidCRMigration) ([]corev1.Pod, error) {
	ns := workloadNamespace(m)
	expectedVersion := map[string]string{"Pod": "v1", "StatefulSet": "apps/v1", "Deployment": "apps/v1", "Job": "batch/v1"}[m.Spec.WorkloadRef.Kind]
	if expectedVersion == "" || m.Spec.WorkloadRef.APIVersion != expectedVersion {
		return nil, fmt.Errorf("unsupported workload apiVersion/kind")
	}

	if m.Spec.WorkloadRef.Kind == "Pod" {
		var pod corev1.Pod
		key := types.NamespacedName{Namespace: ns, Name: m.Spec.WorkloadRef.Name}
		if err := r.Get(ctx, key, &pod); err != nil {
			return nil, err
		}
		if !isEligiblePod(&pod) {
			return nil, nil
		}
		return []corev1.Pod{pod}, nil
	}

	selector, err := r.workloadSelector(ctx, m)
	if err != nil {
		return nil, err
	}
	sel, err := metav1.LabelSelectorAsSelector(selector)
	if err != nil {
		return nil, fmt.Errorf("invalid workload selector: %w", err)
	}

	var podList corev1.PodList
	if err := r.List(ctx, &podList,
		client.InNamespace(ns),
		client.MatchingLabelsSelector{Selector: sel},
	); err != nil {
		return nil, err
	}

	var pods []corev1.Pod
	for i := range podList.Items {
		p := podList.Items[i]
		owned, err := r.ownedByWorkload(ctx, &p, m)
		if err != nil {
			return nil, err
		}
		if !owned {
			continue
		}
		if !isEligiblePod(&p) {
			continue
		}
		pods = append(pods, p)
	}
	if m.Spec.WorkloadRef.Kind == "StatefulSet" {
		var s appsv1.StatefulSet
		if err := r.Get(ctx, client.ObjectKey{Namespace: ns, Name: m.Spec.WorkloadRef.Name}, &s); err != nil {
			return nil, err
		}
		expected := int32(1)
		if s.Spec.Replicas != nil {
			expected = *s.Spec.Replicas
		}
		if expected <= 0 || s.Status.ObservedGeneration != s.Generation || s.Status.Replicas != expected ||
			s.Status.CurrentReplicas != expected || s.Status.ReadyReplicas != expected || s.Status.AvailableReplicas != expected ||
			s.Status.UpdatedReplicas != expected || s.Status.CurrentRevision != s.Status.UpdateRevision || int32(len(pods)) != expected {
			return nil, fmt.Errorf("StatefulSet snapshot incomplete or rollout in progress: expected %d, eligible %d", expected, len(pods))
		}
	}
	return pods, nil
}

// isEligiblePod reports whether a pod is a valid checkpoint target: Running,
// not terminating, and wired by the FluidCR webhook.
func isEligiblePod(p *corev1.Pod) bool {
	return p.UID != "" && p.DeletionTimestamp == nil && p.Status.Phase == corev1.PodRunning && isInjected(p) &&
		p.Status.PodIP != "" && p.Status.HostIP != "" && p.Spec.NodeName != ""
}

// workloadNamespace returns the namespace the referenced workload lives in,
// defaulting to the FluidCRMigration's own namespace.
func workloadNamespace(m *fluidcrv1alpha1.FluidCRMigration) string {
	if ns := m.Spec.WorkloadRef.Namespace; ns != "" {
		return ns
	}
	return m.Namespace
}

// workloadSelector resolves the pod selector of the referenced workload.
func (r *FluidCRMigrationReconciler) workloadSelector(ctx context.Context, m *fluidcrv1alpha1.FluidCRMigration) (*metav1.LabelSelector, error) {
	ref := m.Spec.WorkloadRef
	key := types.NamespacedName{Namespace: workloadNamespace(m), Name: ref.Name}
	switch ref.Kind {
	case "Deployment":
		var d appsv1.Deployment
		if err := r.Get(ctx, key, &d); err != nil {
			return nil, err
		}
		return d.Spec.Selector, nil
	case "StatefulSet":
		var s appsv1.StatefulSet
		if err := r.Get(ctx, key, &s); err != nil {
			return nil, err
		}
		return s.Spec.Selector, nil
	case "Job":
		var j batchv1.Job
		if err := r.Get(ctx, key, &j); err != nil {
			return nil, err
		}
		return j.Spec.Selector, nil
	default:
		return nil, fmt.Errorf("unsupported workload kind %q", ref.Kind)
	}
}

// markWaiting records a non-terminal Pending state and requeues.
func (r *FluidCRMigrationReconciler) markWaiting(ctx context.Context, m *fluidcrv1alpha1.FluidCRMigration, msg string) (ctrl.Result, error) {
	m.Status.Phase = fluidcrv1alpha1.PhasePending
	m.Status.Message = msg
	m.Status.ObservedGeneration = m.Generation
	setReadyCondition(m, metav1.ConditionFalse, "WaitingForPods", msg)
	if err := r.saveStatus(ctx, m); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: waitRequeueInterval}, nil
}

// markFailed records a terminal failure (configuration error).
func (r *FluidCRMigrationReconciler) markFailed(ctx context.Context, m *fluidcrv1alpha1.FluidCRMigration, msg string) (ctrl.Result, error) {
	if m.Status.CompletionTime == nil {
		now := metav1.Now()
		m.Status.CompletionTime = &now
	}
	m.Status.ObservedGeneration = m.Generation
	return r.finalize(ctx, m, fluidcrv1alpha1.PhaseFailed, "CheckpointFailed", msg)
}

// finalize writes the terminal phase, message and Ready condition.
func (r *FluidCRMigrationReconciler) finalize(ctx context.Context, m *fluidcrv1alpha1.FluidCRMigration, phase fluidcrv1alpha1.MigrationPhase, reason, msg string) (ctrl.Result, error) {
	m.Status.Phase = phase
	m.Status.Message = msg
	m.Status.ObservedGeneration = m.Generation
	status := metav1.ConditionFalse
	if phase == fluidcrv1alpha1.PhaseCompleted {
		status = metav1.ConditionTrue
	}
	setReadyCondition(m, status, reason, msg)
	if err := r.saveStatus(ctx, m); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// saveStatus writes the status subresource, retrying on optimistic-lock conflicts.
func (r *FluidCRMigrationReconciler) saveStatus(ctx context.Context, m *fluidcrv1alpha1.FluidCRMigration) error {
	key := client.ObjectKeyFromObject(m)
	for i := 0; i < statusUpdateAttempts; i++ {
		var latest fluidcrv1alpha1.FluidCRMigration
		reader := r.APIReader
		if reader == nil {
			reader = r.Client
		}
		if gerr := reader.Get(ctx, key, &latest); gerr != nil {
			return gerr
		}
		if latest.UID != m.UID || latest.Generation != m.Generation || !reflect.DeepEqual(latest.Spec, m.Spec) {
			return fmt.Errorf("migration identity/spec changed during status update; refusing stale work")
		}
		clusters := latest.Status.Clusters
		latest.Status = m.Status
		latest.Status.Clusters = clusters
		err := r.Status().Update(ctx, &latest)
		if err == nil {
			*m = latest
			return nil
		}
		if !apierrors.IsConflict(err) {
			return err
		}
	}
	return fmt.Errorf("status update failed after %d conflict retries", statusUpdateAttempts)
}

// SetupWithManager sets up the controller with the Manager.
func (r *FluidCRMigrationReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.KubeletClient == nil {
		return fmt.Errorf("member checkpoint mode requires a kubelet client")
	}
	if r.APIReader == nil {
		r.APIReader = mgr.GetAPIReader()
	}
	if r.CtrlClient == nil {
		r.CtrlClient = ctrlapi.NewClient()
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&fluidcrv1alpha1.FluidCRMigration{}).
		Named("fluidcrmigration").
		Complete(r)
}

// ---- helpers ----

func setReadyCondition(m *fluidcrv1alpha1.FluidCRMigration, status metav1.ConditionStatus, reason, msg string) {
	meta.SetStatusCondition(&m.Status.Conditions, metav1.Condition{
		Type:               conditionReady,
		Status:             status,
		Reason:             reason,
		Message:            msg,
		ObservedGeneration: m.Generation,
	})
}

func ensurePodStatus(m *fluidcrv1alpha1.FluidCRMigration, podName, nodeName, podIP string) *fluidcrv1alpha1.PodMigrationStatus {
	for i := range m.Status.Pods {
		if m.Status.Pods[i].PodName == podName {
			m.Status.Pods[i].NodeName = nodeName
			m.Status.Pods[i].PodIP = podIP
			return &m.Status.Pods[i]
		}
	}
	m.Status.Pods = append(m.Status.Pods, fluidcrv1alpha1.PodMigrationStatus{
		PodName:  podName,
		NodeName: nodeName,
		PodIP:    podIP,
		Phase:    fluidcrv1alpha1.PodPhasePending,
	})
	return &m.Status.Pods[len(m.Status.Pods)-1]
}

func getPodStatus(m *fluidcrv1alpha1.FluidCRMigration, podName string) *fluidcrv1alpha1.PodMigrationStatus {
	for i := range m.Status.Pods {
		if m.Status.Pods[i].PodName == podName {
			return &m.Status.Pods[i]
		}
	}
	return nil
}

func filterTargets(targets []target, m *fluidcrv1alpha1.FluidCRMigration, keep func(*fluidcrv1alpha1.PodMigrationStatus) bool) []target {
	var out []target
	for _, t := range targets {
		ps := getPodStatus(m, t.podName)
		if ps != nil && keep(ps) {
			out = append(out, t)
		}
	}
	return out
}

func anyPodFailed(m *fluidcrv1alpha1.FluidCRMigration) bool {
	for i := range m.Status.Pods {
		if m.Status.Pods[i].Phase == fluidcrv1alpha1.PodPhaseFailed {
			return true
		}
	}
	return false
}

func podRank(p fluidcrv1alpha1.PodPhase) int {
	switch p {
	case fluidcrv1alpha1.PodPhaseAppCheckpointed:
		return 1
	case fluidcrv1alpha1.PodPhaseContainerCheckpointed:
		return 2
	case fluidcrv1alpha1.PodPhaseResumed:
		return 3
	case fluidcrv1alpha1.PodPhaseFailed:
		return -1
	default:
		return 0
	}
}

func isTerminalPhase(p fluidcrv1alpha1.MigrationPhase) bool {
	return p == fluidcrv1alpha1.PhaseCompleted || p == fluidcrv1alpha1.PhaseFailed
}

func shouldResume(m *fluidcrv1alpha1.FluidCRMigration) bool {
	return m.Spec.Resume == nil || *m.Spec.Resume
}

func timeoutOf(secs int32) time.Duration {
	if secs <= 0 {
		secs = defaultTimeoutSecs
	}
	return time.Duration(secs) * time.Second
}
