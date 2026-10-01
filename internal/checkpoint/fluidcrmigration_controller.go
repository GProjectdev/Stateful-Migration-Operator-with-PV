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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strconv"
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

var errSurvivorEvidencePending = errors.New("survivor pause evidence pending")

const (
	// FinalizerName guards in-flight migrations so the controller can resume a
	// paused workload before the resource disappears.
	FinalizerName = "fluidcrmigration.fluidcr.dcnlab.com/finalizer"

	conditionReady                = "Ready"
	defaultTimeoutSecs            = 300
	waitRequeueInterval           = 15 * time.Second
	statusUpdateAttempts          = 5
	AnnotationCheckpointID        = "training.dcnlab.com/checkpoint-id"
	AnnotationRestoreOwnedResume  = "training.dcnlab.com/restore-owned-resume"
	AnnotationScheduledParentName = "training.dcnlab.com/scheduled-parent-name"
	AnnotationScheduledParentUID  = "training.dcnlab.com/scheduled-parent-uid"
	LabelRole                     = "training.dcnlab.com/role"
	LabelScheduledParent          = "training.dcnlab.com/scheduled-parent"
	LabelWorkloadUID              = "training.dcnlab.com/workload-uid"
	RoleCheckpointEvidence        = "checkpoint-evidence"
)

// CtrlAPI is the subset of the in-pod FluidCR control API the controller uses.
type CtrlAPI interface {
	Checkpoint(ctx context.Context, podIP string, port int, timeout time.Duration, checkpointID string) (map[string]string, error)
	CheckpointRanks(ctx context.Context, podIP string, port int, timeout time.Duration, checkpointID string, ranks []int64) (map[string]string, error)
	Runtime(ctx context.Context, podIP string, port int, timeout time.Duration) (ctrlapi.RuntimeStatus, error)
	Resume(ctx context.Context, podIP string, port int, timeout time.Duration) (map[string]string, error)
	ResumeOwned(ctx context.Context, podIP string, port int, timeout time.Duration, checkpointID string, generation int64) (map[string]string, error)
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
	PodExecutor   PodExecutor
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
	rank      int64
}

var statefulOrdinal = regexp.MustCompile(`^(.*)-([0-9]+)$`)

// +kubebuilder:rbac:groups=fluidcr.dcnlab.com,resources=fluidcrmigrations,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=fluidcr.dcnlab.com,resources=fluidcrmigrations/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=fluidcr.dcnlab.com,resources=fluidcrmigrations/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods/exec,verbs=create
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

	if migration.Labels[LabelRole] == RoleCheckpointEvidence {
		return ctrl.Result{}, nil
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

	if migration.Spec.Schedule != nil {
		return r.reconcileSchedule(ctx, &migration)
	}

	if restoreOwnedResumeRequested(&migration) && restoreOwnedResumePending(&migration) {
		return r.reconcileRestoreOwnedResume(ctx, &migration)
	}

	// Single-shot once terminal for the current spec generation.
	if isTerminalPhase(migration.Status.Phase) && migration.Status.ObservedGeneration == migration.Generation {
		return ctrl.Result{}, nil
	}

	log.Info("reconciling FluidCRMigration", "workload", migration.Spec.WorkloadRef.Name, "phase", migration.Status.Phase)
	return r.reconcileWorkflow(ctx, &migration)
}

func (r *FluidCRMigrationReconciler) reconcileSchedule(ctx context.Context, parent *fluidcrv1alpha1.FluidCRMigration) (ctrl.Result, error) {
	if parent.Spec.Schedule == nil {
		return ctrl.Result{}, nil
	}
	if parent.Spec.Schedule.IntervalSeconds <= 0 {
		return r.markFailed(ctx, parent, "schedule intervalSeconds must be positive")
	}
	if parent.Spec.PartialCheckpoint != nil {
		return r.markFailed(ctx, parent, "scheduled checkpoints require full-rank checkpoint; partialCheckpoint is not allowed")
	}
	if !shouldResume(parent) {
		return r.markFailed(ctx, parent, "scheduled checkpoints require spec.resume=true or omitted")
	}
	children, err := r.scheduledChildren(ctx, parent)
	if err != nil {
		return ctrl.Result{}, err
	}
	if pendingScheduleReservation(parent.Status.CurrentRun) {
		if child := childByName(children, parent.Status.CurrentRun.Name); child != nil {
			if err := validateReservedScheduledChild(parent, parent.Status.CurrentRun, child); err != nil {
				return ctrl.Result{}, err
			}
			blocking := activeScheduledChild(children) != nil
			result, err := r.updateScheduleFromCurrentChild(ctx, parent, child, blocking)
			if err != nil || blocking {
				return result, err
			}
		} else {
			return r.materializeReservedScheduledChild(ctx, parent)
		}
	}
	if pinnedCurrentRun(parent.Status.CurrentRun) {
		child := childByName(children, parent.Status.CurrentRun.Name)
		if child == nil {
			return r.blockScheduleOnPinnedCurrentRun(ctx, parent, fmt.Sprintf("waiting for pinned scheduled child %s to appear", parent.Status.CurrentRun.Name))
		}
		if string(child.UID) != parent.Status.CurrentRun.UID {
			return r.blockScheduleOnPinnedCurrentRun(ctx, parent, fmt.Sprintf("waiting for pinned scheduled child %s UID %s, found UID %s", child.Name, parent.Status.CurrentRun.UID, child.UID))
		}
	}
	active := activeScheduledChild(children)
	current := active
	if current == nil && parent.Status.CurrentRun != nil {
		current = childByName(children, parent.Status.CurrentRun.Name)
	}
	if current == nil {
		current = latestScheduledChild(children)
	}
	if current != nil {
		result, err := r.updateScheduleFromCurrentChild(ctx, parent, current, active != nil)
		if err != nil || active != nil {
			return result, err
		}
	}
	if !parent.Spec.Schedule.Enabled {
		parent.Status.ObservedGeneration = parent.Generation
		parent.Status.Phase = fluidcrv1alpha1.PhasePaused
		parent.Status.Message = "schedule paused"
		setReadyCondition(parent, metav1.ConditionFalse, "SchedulePaused", parent.Status.Message)
		return ctrl.Result{}, r.saveStatus(ctx, parent)
	}
	if wait := scheduleWait(parent, children); wait > 0 {
		parent.Status.ObservedGeneration = parent.Generation
		parent.Status.Phase = fluidcrv1alpha1.PhasePending
		parent.Status.Message = "waiting for next scheduled checkpoint interval"
		setReadyCondition(parent, metav1.ConditionTrue, "ScheduleIdle", parent.Status.Message)
		if err := r.saveStatus(ctx, parent); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: wait}, nil
	}
	if conflict := r.conflictingWorkloadMigration(ctx, parent, children); conflict != "" {
		parent.Status.ObservedGeneration = parent.Generation
		parent.Status.Phase = fluidcrv1alpha1.PhasePending
		parent.Status.Message = conflict
		setReadyCondition(parent, metav1.ConditionFalse, "ScheduleBlocked", conflict)
		return ctrl.Result{RequeueAfter: waitRequeueInterval}, r.saveStatus(ctx, parent)
	}
	parent.Status.CurrentRun = scheduledRunReservation(parent)
	parent.Status.ObservedGeneration = parent.Generation
	parent.Status.Phase = fluidcrv1alpha1.PhasePending
	parent.Status.Message = fmt.Sprintf("reserved scheduled child %s", parent.Status.CurrentRun.Name)
	setReadyCondition(parent, metav1.ConditionFalse, "ScheduleActive", parent.Status.Message)
	if err := r.saveStatus(ctx, parent); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: time.Second}, nil
}

func (r *FluidCRMigrationReconciler) blockScheduleOnPinnedCurrentRun(ctx context.Context, parent *fluidcrv1alpha1.FluidCRMigration, message string) (ctrl.Result, error) {
	parent.Status.ObservedGeneration = parent.Generation
	parent.Status.Phase = fluidcrv1alpha1.PhasePending
	parent.Status.Message = message
	setReadyCondition(parent, metav1.ConditionFalse, "ScheduleBlocked", message)
	return ctrl.Result{RequeueAfter: waitRequeueInterval}, r.saveStatus(ctx, parent)
}

func (r *FluidCRMigrationReconciler) updateScheduleFromCurrentChild(ctx context.Context, parent *fluidcrv1alpha1.FluidCRMigration, current *fluidcrv1alpha1.FluidCRMigration, blocking bool) (ctrl.Result, error) {
	parent.Status.CurrentRun = scheduledRunReference(current, false)
	if durableFullCheckpointComplete(current) && !sameRunReference(parent.Status.LastSuccessfulFullCheckpoint, current) {
		parent.Status.LastSuccessfulCheckpoint = scheduledRunReference(current, true)
		parent.Status.LastSuccessfulFullCheckpoint = scheduledRunReference(current, true)
	}
	if !blocking {
		return ctrl.Result{}, nil
	}
	parent.Status.ObservedGeneration = parent.Generation
	parent.Status.Phase = fluidcrv1alpha1.PhasePending
	if isTerminalPhase(current.Status.Phase) {
		parent.Status.Message = fmt.Sprintf("last child %s finished with phase %s", current.Name, current.Status.Phase)
	} else {
		parent.Status.Message = fmt.Sprintf("waiting for child %s to finish", current.Name)
	}
	setReadyCondition(parent, metav1.ConditionFalse, "ScheduleActive", parent.Status.Message)
	if err := r.saveStatus(ctx, parent); err != nil {
		return ctrl.Result{}, err
	}
	if blocking {
		return ctrl.Result{RequeueAfter: waitRequeueInterval}, nil
	}
	return ctrl.Result{}, nil
}

func (r *FluidCRMigrationReconciler) materializeReservedScheduledChild(ctx context.Context, parent *fluidcrv1alpha1.FluidCRMigration) (ctrl.Result, error) {
	child, err := scheduledChildForReservation(parent, parent.Status.CurrentRun)
	if err != nil {
		return ctrl.Result{}, err
	}
	if err := r.Create(ctx, child); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return ctrl.Result{}, err
		}
		var existing fluidcrv1alpha1.FluidCRMigration
		if getErr := r.Get(ctx, client.ObjectKeyFromObject(child), &existing); getErr != nil {
			return ctrl.Result{}, getErr
		}
		if err := validateReservedScheduledChild(parent, parent.Status.CurrentRun, &existing); err != nil {
			return ctrl.Result{}, err
		}
		child = &existing
	}
	parent.Status.CurrentRun = scheduledRunReference(child, false)
	parent.Status.ObservedGeneration = parent.Generation
	parent.Status.Phase = fluidcrv1alpha1.PhasePending
	parent.Status.Message = fmt.Sprintf("created scheduled child %s", child.Name)
	setReadyCondition(parent, metav1.ConditionFalse, "ScheduleActive", parent.Status.Message)
	if err := r.saveStatus(ctx, parent); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: time.Second}, nil
}

func (r *FluidCRMigrationReconciler) scheduledChildren(ctx context.Context, parent *fluidcrv1alpha1.FluidCRMigration) ([]fluidcrv1alpha1.FluidCRMigration, error) {
	var list fluidcrv1alpha1.FluidCRMigrationList
	if err := r.List(ctx, &list, client.InNamespace(parent.Namespace), client.MatchingLabels{LabelScheduledParent: string(parent.UID)}); err != nil {
		return nil, err
	}
	children := list.Items[:0]
	for i := range list.Items {
		child := list.Items[i]
		if controlledBy(&child, parent) {
			children = append(children, child)
		}
	}
	sort.Slice(children, func(i, j int) bool {
		return children[i].CreationTimestamp.Before(&children[j].CreationTimestamp) || (children[i].CreationTimestamp.Equal(&children[j].CreationTimestamp) && children[i].Name < children[j].Name)
	})
	return children, nil
}

func controlledBy(child, parent *fluidcrv1alpha1.FluidCRMigration) bool {
	return child.Labels[LabelScheduledParent] == string(parent.UID) &&
		child.Annotations[AnnotationScheduledParentUID] == string(parent.UID) &&
		child.Annotations[AnnotationScheduledParentName] == parent.Name
}

func activeScheduledChild(children []fluidcrv1alpha1.FluidCRMigration) *fluidcrv1alpha1.FluidCRMigration {
	for i := range children {
		if !durableFullCheckpointComplete(&children[i]) {
			return &children[i]
		}
	}
	return nil
}

func latestScheduledChild(children []fluidcrv1alpha1.FluidCRMigration) *fluidcrv1alpha1.FluidCRMigration {
	if len(children) == 0 {
		return nil
	}
	return &children[len(children)-1]
}

func childByName(children []fluidcrv1alpha1.FluidCRMigration, name string) *fluidcrv1alpha1.FluidCRMigration {
	for i := range children {
		if children[i].Name == name {
			return &children[i]
		}
	}
	return nil
}

func (r *FluidCRMigrationReconciler) conflictingWorkloadMigration(ctx context.Context, parent *fluidcrv1alpha1.FluidCRMigration, ownChildren []fluidcrv1alpha1.FluidCRMigration) string {
	own := map[string]bool{}
	for i := range ownChildren {
		own[ownChildren[i].Name] = true
	}
	var list fluidcrv1alpha1.FluidCRMigrationList
	if err := r.List(ctx, &list, client.InNamespace(parent.Namespace)); err != nil {
		return fmt.Sprintf("waiting for workload migration inventory: %v", err)
	}
	for i := range list.Items {
		other := &list.Items[i]
		if other.Name == parent.Name || own[other.Name] || other.Labels[LabelRole] == RoleCheckpointEvidence {
			continue
		}
		if !sameWorkloadRef(parent.Spec.WorkloadRef, other.Spec.WorkloadRef, parent.Namespace) {
			continue
		}
		if sameWorkloadMigrationBlocksPeriodic(other) {
			return fmt.Sprintf("waiting for same-workload FluidCRMigration %s/%s", other.Namespace, other.Name)
		}
	}
	return ""
}

func sameWorkloadMigrationBlocksPeriodic(other *fluidcrv1alpha1.FluidCRMigration) bool {
	if other.Spec.Schedule != nil {
		return true
	}
	if completedPartialSurvivorsReleased(other) {
		return false
	}
	if durableFullCheckpointComplete(other) && shouldResume(other) {
		return false
	}
	return true
}

func completedPartialSurvivorsReleased(m *fluidcrv1alpha1.FluidCRMigration) bool {
	if m.Spec.PartialCheckpoint == nil || m.Status.Phase != fluidcrv1alpha1.PhaseCompleted || m.Status.ObservedGeneration != m.Generation || m.Status.CompletionTime == nil {
		return false
	}
	condition := meta.FindStatusCondition(m.Status.Conditions, "SurvivorReleased")
	return condition != nil && condition.Status == metav1.ConditionTrue && condition.Reason == "Released" && condition.ObservedGeneration == m.Generation
}

func sameWorkloadRef(a, b fluidcrv1alpha1.WorkloadReference, defaultNamespace string) bool {
	return a.UID == b.UID && a.APIVersion == b.APIVersion && a.Kind == b.Kind && a.Name == b.Name && workloadRefNamespace(a, defaultNamespace) == workloadRefNamespace(b, defaultNamespace)
}

func workloadRefNamespace(ref fluidcrv1alpha1.WorkloadReference, defaultNamespace string) string {
	if ref.Namespace != "" {
		return ref.Namespace
	}
	return defaultNamespace
}
func scheduleWait(parent *fluidcrv1alpha1.FluidCRMigration, children []fluidcrv1alpha1.FluidCRMigration) time.Duration {
	if len(children) == 0 {
		return 0
	}
	latest := children[len(children)-1]
	base := latest.CreationTimestamp.Time
	if latest.Status.CompletionTime != nil {
		base = latest.Status.CompletionTime.Time
	}
	if base.IsZero() {
		return 0
	}
	due := base.Add(time.Duration(parent.Spec.Schedule.IntervalSeconds) * time.Second)
	return time.Until(due)
}

func pendingScheduleReservation(ref *fluidcrv1alpha1.ScheduledRunReference) bool {
	return ref != nil && ref.Name != "" && ref.CheckpointID != "" && ref.UID == ""
}

func pinnedCurrentRun(ref *fluidcrv1alpha1.ScheduledRunReference) bool {
	return ref != nil && ref.Name != "" && ref.UID != ""
}

func scheduledRunReservation(parent *fluidcrv1alpha1.FluidCRMigration) *fluidcrv1alpha1.ScheduledRunReference {
	checkpointID := newScheduledCheckpointID(parent)
	now := metav1.Now()
	return &fluidcrv1alpha1.ScheduledRunReference{
		Name:         childName(parent.Name, checkpointID),
		CheckpointID: checkpointID,
		Phase:        fluidcrv1alpha1.PhasePending,
		StartTime:    &now,
	}
}

func newScheduledCheckpointID(parent *fluidcrv1alpha1.FluidCRMigration) string {
	createdAt := time.Now().UTC().UnixNano()
	parentUID := string(parent.UID)
	return fmt.Sprintf("%s-%s-g%d-t%d", parent.Name, shortID(parentUID), parent.Generation, createdAt)
}

func scheduledChildFor(parent *fluidcrv1alpha1.FluidCRMigration, _ ...int) *fluidcrv1alpha1.FluidCRMigration {
	return mustScheduledChildForReservation(parent, scheduledRunReservation(parent))
}

func scheduledChildForReservation(parent *fluidcrv1alpha1.FluidCRMigration, ref *fluidcrv1alpha1.ScheduledRunReference) (*fluidcrv1alpha1.FluidCRMigration, error) {
	if ref == nil || ref.Name == "" || ref.CheckpointID == "" {
		return nil, fmt.Errorf("scheduled child reservation is incomplete")
	}
	if wantName := childName(parent.Name, ref.CheckpointID); wantName != ref.Name {
		return nil, fmt.Errorf("scheduled child reservation name %q does not match checkpointID %q", ref.Name, ref.CheckpointID)
	}
	return mustScheduledChildForReservation(parent, ref), nil
}

func mustScheduledChildForReservation(parent *fluidcrv1alpha1.FluidCRMigration, ref *fluidcrv1alpha1.ScheduledRunReference) *fluidcrv1alpha1.FluidCRMigration {
	parentUID := string(parent.UID)
	spec := *parent.Spec.DeepCopy()
	spec.Schedule = nil
	annotations := map[string]string{AnnotationCheckpointID: ref.CheckpointID, AnnotationScheduledParentName: parent.Name, AnnotationScheduledParentUID: parentUID}
	labels := map[string]string{LabelScheduledParent: parentUID}
	return &fluidcrv1alpha1.FluidCRMigration{
		TypeMeta:   metav1.TypeMeta{APIVersion: fluidcrv1alpha1.GroupVersion.String(), Kind: "FluidCRMigration"},
		ObjectMeta: metav1.ObjectMeta{Name: ref.Name, Namespace: parent.Namespace, Annotations: annotations, Labels: labels},
		Spec:       spec,
	}
}

func validateReservedScheduledChild(parent *fluidcrv1alpha1.FluidCRMigration, ref *fluidcrv1alpha1.ScheduledRunReference, child *fluidcrv1alpha1.FluidCRMigration) error {
	want, err := scheduledChildForReservation(parent, ref)
	if err != nil {
		return err
	}
	if !controlledBy(child, parent) || child.Annotations[AnnotationCheckpointID] != ref.CheckpointID {
		return fmt.Errorf("existing scheduled child %s/%s is not owned by reserved parent UID/checkpointID", child.Namespace, child.Name)
	}
	if !reflect.DeepEqual(child.Spec, want.Spec) {
		return fmt.Errorf("existing scheduled child %s/%s spec does not match reserved execution spec", child.Namespace, child.Name)
	}
	return nil
}
func shortID(value string) string {
	if value == "" {
		value = "nouid"
	}
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])[:8]
}

func childName(parentName, checkpointID string) string {
	sum := sha256.Sum256([]byte(checkpointID))
	suffix := hex.EncodeToString(sum[:])[:10]
	prefix := strings.Trim(parentName, "-")
	if len(prefix) > 45 {
		prefix = prefix[:45]
	}
	return strings.Trim(prefix, "-") + "-" + suffix
}

func rawSnapshot(value any) runtime.RawExtension {
	data, _ := json.Marshal(value)
	return runtime.RawExtension{Raw: data}
}

func sameRunReference(ref *fluidcrv1alpha1.ScheduledRunReference, child *fluidcrv1alpha1.FluidCRMigration) bool {
	return ref != nil && ref.UID == string(child.UID) && ref.CheckpointID == strings.TrimSpace(child.Annotations[AnnotationCheckpointID])
}

func scheduledRunReference(child *fluidcrv1alpha1.FluidCRMigration, includeResult bool) *fluidcrv1alpha1.ScheduledRunReference {
	ref := &fluidcrv1alpha1.ScheduledRunReference{
		Name: child.Name, UID: string(child.UID), CheckpointID: strings.TrimSpace(child.Annotations[AnnotationCheckpointID]),
		Phase: child.Status.Phase, StartTime: child.Status.StartTime, CompletionTime: child.Status.CompletionTime,
	}
	if includeResult {
		spec := *child.Spec.DeepCopy()
		spec.Schedule = nil
		status := *child.Status.DeepCopy()
		status.CurrentRun = nil
		status.LastSuccessfulCheckpoint = nil
		status.LastSuccessfulFullCheckpoint = nil
		ref.Result = &fluidcrv1alpha1.ScheduledRunResult{Spec: rawSnapshot(spec), Status: rawSnapshot(status)}
	}
	return ref
}

func durableFullCheckpointComplete(child *fluidcrv1alpha1.FluidCRMigration) bool {
	if child.Spec.PartialCheckpoint != nil || child.Status.Phase != fluidcrv1alpha1.PhaseCompleted || child.Status.ObservedGeneration != child.Generation || child.Status.CompletionTime == nil {
		return false
	}
	checkpointID := strings.TrimSpace(child.Annotations[AnnotationCheckpointID])
	if checkpointID == "" || len(child.Status.Pods) == 0 {
		return false
	}
	for _, pod := range child.Status.Pods {
		if pod.CheckpointID != checkpointID || len(pod.CheckpointFiles) == 0 {
			return false
		}
		for _, file := range pod.CheckpointFiles {
			if file.CheckpointID != checkpointID || file.SHA256 == "" || file.DurableRef == "" || file.ExportedAt == "" {
				return false
			}
		}
	}
	return true
}
func (r *FluidCRMigrationReconciler) reconcileRestoreOwnedResume(ctx context.Context, m *fluidcrv1alpha1.FluidCRMigration) (ctrl.Result, error) {
	if m.Status.Phase != fluidcrv1alpha1.PhaseCompleted || m.Status.ObservedGeneration != m.Generation {
		return r.markSurvivorRelease(ctx, m, metav1.ConditionFalse, "CheckpointIncomplete", "restore-owned release requires a completed current-generation checkpoint")
	}
	if m.Spec.PartialCheckpoint == nil {
		return r.markSurvivorRelease(ctx, m, metav1.ConditionFalse, "ReleaseRejected", "restore-owned resume requires partial checkpoint status")
	}
	checkpointID, err := checkpointIDFor(m)
	if err != nil {
		return r.markSurvivorRelease(ctx, m, metav1.ConditionFalse, "ReleaseRejected", err.Error())
	}
	pods, err := r.resolveTargetPods(ctx, m)
	if err != nil {
		return r.markSurvivorRelease(ctx, m, metav1.ConditionFalse, "ReleasePending", fmt.Sprintf("waiting for survivor pods before restore-owned resume: %v", err))
	}
	byName := map[string]corev1.Pod{}
	for i := range pods {
		byName[pods[i].Name] = pods[i]
	}
	var release []target
	var generation int64
	for i := range m.Status.Pods {
		ps := &m.Status.Pods[i]
		if ps.Phase != fluidcrv1alpha1.PodPhaseSurvivorPaused {
			continue
		}
		if ps.SurvivorEvidence == nil || ps.SurvivorEvidence.Generation <= 0 {
			return r.markSurvivorRelease(ctx, m, metav1.ConditionFalse, "ReleaseRejected", "survivor generation evidence missing before restore-owned resume")
		}
		if generation == 0 {
			generation = ps.SurvivorEvidence.Generation
		} else if generation != ps.SurvivorEvidence.Generation {
			return r.markSurvivorRelease(ctx, m, metav1.ConditionFalse, "ReleaseRejected", "survivor generations differ before restore-owned resume")
		}
		pod, ok := byName[ps.PodName]
		if !ok || string(pod.UID) != ps.PodUID {
			return r.markSurvivorRelease(ctx, m, metav1.ConditionFalse, "ReleasePending", "waiting for UID-matched survivor pod before restore-owned resume")
		}
		container, err := resolveContainerName(&pod, m.Spec.Container)
		if err != nil {
			return r.markSurvivorRelease(ctx, m, metav1.ConditionFalse, "ReleaseRejected", err.Error())
		}
		port := resolveCtrlPort(&pod, container, m.Spec.CtrlPort)
		if err := r.validateLiveSurvivorEvidence(ctx, ps, &pod, port, timeoutOf(m.Spec.AppCheckpointTimeoutSeconds), checkpointID); err != nil {
			return r.markSurvivorRelease(ctx, m, metav1.ConditionFalse, "ReleasePending", fmt.Sprintf("waiting for live survivor evidence before restore-owned resume: %v", err))
		}
		release = append(release, target{podUID: pod.UID, podName: pod.Name, namespace: pod.Namespace, podIP: pod.Status.PodIP, hostIP: pod.Status.HostIP, container: container, port: port, rank: ps.Rank})
	}
	if len(release) == 0 {
		return ctrl.Result{}, nil
	}
	if _, err := r.markSurvivorRelease(ctx, m, metav1.ConditionFalse, "Releasing", fmt.Sprintf("restore-owned scoped resume for %d survivor pod(s)", len(release))); err != nil {
		return ctrl.Result{}, err
	}
	outcomes := r.resumeOwned(ctx, release, timeoutOf(m.Spec.AppCheckpointTimeoutSeconds), checkpointID, generation)
	var errs []string
	for _, t := range release {
		ps := getPodStatus(m, t.podName)
		if err := outcomes[t.podName]; err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", t.podName, err))
			if ps != nil {
				ps.Message = fmt.Sprintf("restore-owned resume: %v", err)
			}
			continue
		}
		if ps != nil {
			ps.Phase = fluidcrv1alpha1.PodPhaseResumed
			ps.Message = "restore-owned survivor release completed"
		}
	}
	if len(errs) > 0 {
		return r.markSurvivorRelease(ctx, m, metav1.ConditionFalse, "ReleasePending", "restore-owned resume retry pending: "+strings.Join(errs, "; "))
	}
	return r.markSurvivorRelease(ctx, m, metav1.ConditionTrue, "Released", "restore-owned survivors resumed after target replacement")
}

func (r *FluidCRMigrationReconciler) markSurvivorRelease(ctx context.Context, m *fluidcrv1alpha1.FluidCRMigration, status metav1.ConditionStatus, reason, message string) (ctrl.Result, error) {
	// Archive completion and its timestamp are immutable during later release.
	meta.SetStatusCondition(&m.Status.Conditions, metav1.Condition{
		Type: "SurvivorReleased", Status: status, Reason: reason, Message: message,
		ObservedGeneration: m.Generation, LastTransitionTime: metav1.Now(),
	})
	result := ctrl.Result{}
	if status != metav1.ConditionTrue {
		result.RequeueAfter = waitRequeueInterval
	}
	return result, r.saveStatus(ctx, m)
}

func (r *FluidCRMigrationReconciler) validateLiveSurvivorEvidence(ctx context.Context, ps *fluidcrv1alpha1.PodMigrationStatus, pod *corev1.Pod, port int, timeout time.Duration, checkpointID string) error {
	if ps == nil || ps.SurvivorEvidence == nil {
		return fmt.Errorf("survivor status evidence missing")
	}
	status, err := r.CtrlClient.Runtime(ctx, pod.Status.PodIP, port, timeout)
	if err != nil {
		return err
	}
	if status.Rank != ps.Rank {
		return fmt.Errorf("runtime rank mismatch: got %d want %d", status.Rank, ps.Rank)
	}
	if status.CheckpointID != checkpointID {
		return fmt.Errorf("runtime checkpointID mismatch")
	}
	evidence := status.SurvivorEvidence
	if evidence.Generation == 0 && strings.TrimSpace(evidence.PauseLockPath) == "" && evidence.PauseLockPID == 0 {
		state := strings.ToLower(strings.TrimSpace(status.State))
		if state == "running" || state == "resumed" {
			return nil
		}
		return fmt.Errorf("runtime survivor pause-lock evidence missing")
	}
	if evidence.Generation != ps.SurvivorEvidence.Generation || evidence.PauseLockPath != ps.SurvivorEvidence.PauseLockPath || evidence.PauseLockPID != ps.SurvivorEvidence.PauseLockPID || strings.TrimSpace(evidence.ObservedAt) == "" {
		return fmt.Errorf("runtime survivor pause-lock evidence mismatch")
	}
	return nil
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
	partialTargets, partial, err := partialTargetSet(m)
	if err != nil {
		return r.markFailed(ctx, m, err.Error())
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
		rank, err := resolvePodRank(m, pod)
		if err != nil {
			return r.markFailed(ctx, m, err.Error())
		}
		container, err := resolveContainerName(pod, m.Spec.Container)
		if err != nil {
			// Configuration error: terminal.
			return r.markFailed(ctx, m, err.Error())
		}
		port := resolveCtrlPort(pod, container, m.Spec.CtrlPort)
		ps := ensurePodStatus(m, pod.Name, pod.Spec.NodeName, pod.Status.PodIP)
		ps.PodUID = string(pod.UID)
		ps.Rank = rank
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
			rank:      rank,
		})
	}
	if partial && !targetRanksPresent(targets, partialTargets) {
		return r.markFailed(ctx, m, "partial checkpoint target ranks do not match eligible pods")
	}
	checkpointID, err := checkpointIDFor(m)
	if err != nil {
		return r.markFailed(ctx, m, err.Error())
	}

	if m.Status.StartTime == nil {
		now := metav1.Now()
		m.Status.StartTime = &now
	}
	m.Status.ObservedGeneration = m.Generation

	// Phase 1: application checkpoint (concurrent fan-out is mandatory for full
	// checkpoints so distributed-training ranks do not deadlock on a collective
	// barrier). Partial checkpoints trigger the manifest once; the runtime role
	// split makes target ranks exit and survivors park.
	appTargets := filterTargets(targets, m, func(ps *fluidcrv1alpha1.PodMigrationStatus) bool {
		return ps.Phase == fluidcrv1alpha1.PodPhasePending
	})
	if len(appTargets) > 0 {
		// Preflight the entire world before issuing any collective checkpoint signal.
		if len(appTargets) == len(targets) {
			for _, t := range appTargets {
				ready, err := r.CtrlClient.Runtime(ctx, t.podIP, t.port, 3*time.Second)
				if err != nil || !ready.CheckpointReady || ready.State != "Running" {
					message := fmt.Sprintf("waiting for GPU worker readiness on %s", t.podName)
					if err != nil {
						message += ": " + err.Error()
					}
					if appCheckpointTimedOut(m) {
						return r.markFailed(ctx, m, message+"; readiness timeout")
					}
					m.Status.Message = message
					if err := r.saveStatus(ctx, m); err != nil {
						return ctrl.Result{}, err
					}
					return ctrl.Result{RequeueAfter: 3 * time.Second}, nil
				}
			}
		}
		m.Status.Phase = fluidcrv1alpha1.PhaseAppCheckpointing
		m.Status.Message = fmt.Sprintf("signalling application checkpoint on %d pod(s)", len(appTargets))
		if err := r.saveStatus(ctx, m); err != nil {
			return ctrl.Result{}, err
		}
		if partial {
			outcomes := r.partialAppCheckpoint(ctx, appTargets, timeoutOf(m.Spec.AppCheckpointTimeoutSeconds), checkpointID, m.Spec.PartialCheckpoint.TargetRanks)
			for _, t := range appTargets {
				ps := getPodStatus(m, t.podName)
				oc := outcomes[t.podName]
				if oc.err != nil {
					ps.Phase = fluidcrv1alpha1.PodPhaseFailed
					ps.Message = fmt.Sprintf("partial app checkpoint: %v", oc.err)
					continue
				}
				ps.AppCheckpointResult = oc.summary
				if partialTargets[t.rank] {
					ps.Phase = fluidcrv1alpha1.PodPhaseAppCheckpointed
				} else {
					ps.Phase = fluidcrv1alpha1.PodPhaseAppCheckpointed
					ps.Message = "waiting for survivor pause evidence"
					continue
				}
				ps.Message = ""
			}
		} else {
			outcomes := r.appCheckpoint(ctx, appTargets, timeoutOf(m.Spec.AppCheckpointTimeoutSeconds), checkpointID)
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
		}
		if err := r.saveStatus(ctx, m); err != nil {
			return ctrl.Result{}, err
		}
	}
	if partial {
		waiting := false
		for _, t := range targets {
			if partialTargets[t.rank] {
				continue
			}
			ps := getPodStatus(m, t.podName)
			if ps.Phase != fluidcrv1alpha1.PodPhaseAppCheckpointed {
				continue
			}
			if err := r.recordSurvivorEvidence(ctx, t, ps, timeoutOf(m.Spec.AppCheckpointTimeoutSeconds)); err != nil {
				if errors.Is(err, errSurvivorEvidencePending) && !appCheckpointTimedOut(m) {
					ps.Message = fmt.Sprintf("survivor evidence: %v", err)
					waiting = true
					continue
				}
				ps.Phase = fluidcrv1alpha1.PodPhaseFailed
				ps.Message = fmt.Sprintf("survivor evidence: %v", err)
				continue
			}
			ps.Phase = fluidcrv1alpha1.PodPhaseSurvivorPaused
			ps.Message = ""
		}
		if waiting {
			m.Status.Phase = fluidcrv1alpha1.PhaseAppCheckpointing
			m.Status.Message = "waiting for survivor pause evidence"
			if err := r.saveStatus(ctx, m); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
	}
	appFailed := anyPodFailed(m)

	// Phase 2: container checkpoint (CRIU). Skipped entirely if any pod failed
	// the application checkpoint, since a partial set is not consistent.
	if !appFailed {
		ckptTargets := filterTargets(targets, m, func(ps *fluidcrv1alpha1.PodMigrationStatus) bool {
			return podRank(ps.Phase) >= podRank(fluidcrv1alpha1.PodPhaseAppCheckpointed) &&
				podRank(ps.Phase) < podRank(fluidcrv1alpha1.PodPhaseContainerCheckpointed) &&
				(!partial || partialTargets[ps.Rank])
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
				ps.CheckpointID = checkpointID
				ps.CheckpointFiles = append(ps.CheckpointFiles, fluidcrv1alpha1.CheckpointFile{
					CheckpointID:   checkpointID,
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
			outcomes := r.resume(ctx, m, resumeTargets, timeoutOf(m.Spec.AppCheckpointTimeoutSeconds))
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
			if pods, err := r.resolveDeletionPods(ctx, m); err == nil {
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
					for pod, err := range r.resume(ctx, m, resumeTargets, timeoutOf(m.Spec.AppCheckpointTimeoutSeconds)) {
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

// Cleanup follows checkpointed identities even when the workload no longer exists.
func (r *FluidCRMigrationReconciler) resolveDeletionPods(ctx context.Context, m *fluidcrv1alpha1.FluidCRMigration) ([]corev1.Pod, error) {
	var pods []corev1.Pod
	for _, recorded := range m.Status.Pods {
		if recorded.PodUID == "" || recorded.AppCheckpointResult == "" {
			continue
		}
		var pod corev1.Pod
		err := r.Get(ctx, client.ObjectKey{Namespace: m.Namespace, Name: recorded.PodName}, &pod)
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if string(pod.UID) != recorded.PodUID || pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		pods = append(pods, pod)
	}
	return pods, nil
}

// appCheckpoint signals the in-pod control API on every target concurrently.
func (r *FluidCRMigrationReconciler) appCheckpoint(ctx context.Context, targets []target, timeout time.Duration, checkpointID string) map[string]appOutcome {
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
				results, err = r.CtrlClient.Checkpoint(ctx, t.podIP, t.port, timeout, checkpointID)
			}
			mu.Lock()
			out[t.podName] = appOutcome{summary: ctrlapi.SummarizeResults(results), err: err}
			mu.Unlock()
		}()
	}
	wg.Wait()
	return out
}

func (r *FluidCRMigrationReconciler) partialAppCheckpoint(ctx context.Context, targets []target, timeout time.Duration, checkpointID string, ranks []int64) map[string]appOutcome {
	out := make(map[string]appOutcome, len(targets))
	if len(targets) == 0 {
		return out
	}
	// The shared round marker deduplicates signalling, but readiness is local
	// to each target Pod. Never treat signal delivery as checkpoint completion.
	var results map[string]string
	var err error
	for _, t := range targets {
		isTarget := false
		for _, rank := range ranks {
			isTarget = isTarget || t.rank == rank
		}
		if !isTarget {
			continue
		}
		results, err = r.CtrlClient.CheckpointRanks(ctx, t.podIP, t.port, timeout, checkpointID, ranks)
		if err == nil && len(results) == 0 {
			err = fmt.Errorf("target %s returned no checkpoint readiness", t.podName)
		}
		for _, result := range results {
			if err == nil && result != "checkpoint-ready" {
				err = fmt.Errorf("target %s is not checkpoint-ready: %s", t.podName, result)
			}
		}
		if err != nil {
			break
		}
	}
	summary := ctrlapi.SummarizeResults(results)
	for _, t := range targets {
		out[t.podName] = appOutcome{summary: summary, err: err}
	}
	return out
}

func (r *FluidCRMigrationReconciler) recordSurvivorEvidence(ctx context.Context, t target, ps *fluidcrv1alpha1.PodMigrationStatus, timeout time.Duration) error {
	status, err := r.CtrlClient.Runtime(ctx, t.podIP, t.port, timeout)
	if err != nil {
		return fmt.Errorf("%w: runtime status: %v", errSurvivorEvidencePending, err)
	}
	if status.Rank != t.rank {
		return fmt.Errorf("runtime rank mismatch: got %d want %d", status.Rank, t.rank)
	}
	evidence := status.SurvivorEvidence
	if evidence.Generation <= 0 || strings.TrimSpace(evidence.PauseLockPath) == "" || evidence.PauseLockPID <= 0 || strings.TrimSpace(evidence.ObservedAt) == "" {
		return fmt.Errorf("%w", errSurvivorEvidencePending)
	}
	ps.SurvivorEvidence = &fluidcrv1alpha1.SurvivorEvidence{
		Generation:    evidence.Generation,
		PauseLockPath: evidence.PauseLockPath,
		PauseLockPID:  evidence.PauseLockPID,
		ObservedAt:    evidence.ObservedAt,
	}
	return nil
}

func appCheckpointTimedOut(m *fluidcrv1alpha1.FluidCRMigration) bool {
	if m.Status.StartTime == nil {
		return false
	}
	return time.Now().After(m.Status.StartTime.Add(timeoutOf(m.Spec.AppCheckpointTimeoutSeconds)))
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
func (r *FluidCRMigrationReconciler) resume(ctx context.Context, m *fluidcrv1alpha1.FluidCRMigration, targets []target, timeout time.Duration) map[string]error {
	out := make(map[string]error, len(targets))
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := range targets {
		t := targets[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := r.validateTarget(ctx, t)
			if !shouldResume(m) || m.Spec.PartialCheckpoint != nil {
				err = fmt.Errorf("generic resume is forbidden for retained or partial checkpoints")
			}
			if err == nil {
				_, err = r.CtrlClient.Resume(ctx, t.podIP, t.port, timeout)
				if isConnectionRefused(err) && r.PodExecutor != nil {
					err = r.resumeWithExec(ctx, m, t, timeout)
				}
			}
			mu.Lock()
			out[t.podName] = err
			mu.Unlock()
		}()
	}
	wg.Wait()
	return out
}

func (r *FluidCRMigrationReconciler) resumeOwned(ctx context.Context, targets []target, timeout time.Duration, checkpointID string, generation int64) map[string]error {
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
				_, err = r.CtrlClient.ResumeOwned(ctx, t.podIP, t.port, timeout, checkpointID, generation)
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
		if err := validateManagedWorkloadUID(&pod, m); err != nil {
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
	// Require a fully Ready, stable StatefulSet only when taking the initial
	// pod snapshot. Once status has bound the workflow to exact Pod UIDs, a
	// partial checkpoint intentionally makes the target and survivors NotReady;
	// reapplying the initial readiness gate would prevent finalization forever.
	if m.Spec.WorkloadRef.Kind == "StatefulSet" && len(m.Status.Pods) == 0 {
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
		if err := validateManagedWorkloadUID(&d, m); err != nil {
			return nil, err
		}
		return d.Spec.Selector, nil
	case "StatefulSet":
		var s appsv1.StatefulSet
		if err := r.Get(ctx, key, &s); err != nil {
			return nil, err
		}
		if err := validateManagedWorkloadUID(&s, m); err != nil {
			return nil, err
		}
		return s.Spec.Selector, nil
	case "Job":
		var j batchv1.Job
		if err := r.Get(ctx, key, &j); err != nil {
			return nil, err
		}
		if err := validateManagedWorkloadUID(&j, m); err != nil {
			return nil, err
		}
		return j.Spec.Selector, nil
	default:
		return nil, fmt.Errorf("unsupported workload kind %q", ref.Kind)
	}
}

func validateManagedWorkloadUID(obj client.Object, m *fluidcrv1alpha1.FluidCRMigration) error {
	want := strings.TrimSpace(m.Spec.WorkloadRef.UID)
	if want == "" {
		return nil
	}
	got := strings.TrimSpace(obj.GetLabels()[LabelWorkloadUID])
	if got != want {
		return fmt.Errorf("workload %s/%s %s mismatch: label %s=%q, want %q", obj.GetNamespace(), obj.GetName(), LabelWorkloadUID, LabelWorkloadUID, got, want)
	}
	return nil
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
		desired := *m.Status.DeepCopy()
		desired.Clusters = clusters
		if reflect.DeepEqual(latest.Status, desired) {
			*m = latest
			return nil
		}
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
	if r.PodExecutor == nil {
		var err error
		r.PodExecutor, err = newPodExecutor(mgr.GetConfig())
		if err != nil {
			return err
		}
	}
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

func partialTargetSet(m *fluidcrv1alpha1.FluidCRMigration) (map[int64]bool, bool, error) {
	if m.Spec.PartialCheckpoint == nil {
		return nil, false, nil
	}
	if m.Spec.Resume == nil || *m.Spec.Resume {
		return nil, true, fmt.Errorf("partial checkpoint requires spec.resume=false")
	}
	if len(m.Spec.PartialCheckpoint.TargetRanks) == 0 {
		return nil, true, fmt.Errorf("partial checkpoint targetRanks are required")
	}
	targets := map[int64]bool{}
	for _, rank := range m.Spec.PartialCheckpoint.TargetRanks {
		if rank < 0 || targets[rank] {
			return nil, true, fmt.Errorf("partial checkpoint targetRanks must be unique non-negative values")
		}
		targets[rank] = true
	}
	return targets, true, nil
}

func targetRanksPresent(targets []target, targetRanks map[int64]bool) bool {
	found := map[int64]bool{}
	for _, t := range targets {
		if targetRanks[t.rank] {
			found[t.rank] = true
		}
	}
	return len(found) == len(targetRanks)
}

func resolvePodRank(m *fluidcrv1alpha1.FluidCRMigration, pod *corev1.Pod) (int64, error) {
	if m.Spec.PartialCheckpoint == nil {
		return 0, nil
	}
	switch m.Spec.WorkloadRef.Kind {
	case "Pod":
		return 0, nil
	case "StatefulSet":
		matches := statefulOrdinal.FindStringSubmatch(pod.Name)
		if len(matches) != 3 || matches[1] != m.Spec.WorkloadRef.Name {
			return 0, fmt.Errorf("cannot resolve rank from StatefulSet pod %s", pod.Name)
		}
		rank, err := strconv.ParseInt(matches[2], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("cannot parse StatefulSet rank from pod %s", pod.Name)
		}
		return rank, nil
	default:
		return 0, fmt.Errorf("partial checkpoint requires Pod or StatefulSet workload rank mapping")
	}
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
	case fluidcrv1alpha1.PodPhaseSurvivorPaused:
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

func restoreOwnedResumeRequested(m *fluidcrv1alpha1.FluidCRMigration) bool {
	return m.Annotations[AnnotationRestoreOwnedResume] == "true"
}

func restoreOwnedResumePending(m *fluidcrv1alpha1.FluidCRMigration) bool {
	if m.Spec.PartialCheckpoint == nil {
		return false
	}
	for i := range m.Status.Pods {
		if m.Status.Pods[i].Phase == fluidcrv1alpha1.PodPhaseSurvivorPaused {
			return true
		}
	}
	return false
}

func checkpointIDFor(m *fluidcrv1alpha1.FluidCRMigration) (string, error) {
	checkpointID := strings.TrimSpace(m.Annotations[AnnotationCheckpointID])
	if checkpointID == "" {
		return "", fmt.Errorf("missing required %s annotation", AnnotationCheckpointID)
	}
	if len(checkpointID) > 128 || !validCheckpointID(checkpointID) {
		return "", fmt.Errorf("invalid %s annotation", AnnotationCheckpointID)
	}
	return checkpointID, nil
}

func validCheckpointID(value string) bool {
	for i, r := range value {
		ok := r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '.' || r == '_' || r == ':' || r == '-'
		if !ok || (i == 0 && !(r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z' || r >= '0' && r <= '9')) {
			return false
		}
	}
	return value != ""
}

func timeoutOf(secs int32) time.Duration {
	if secs <= 0 {
		secs = defaultTimeoutSecs
	}
	return time.Duration(secs) * time.Second
}
