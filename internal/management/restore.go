package management

import (
	"context"
	"crypto/sha256"
	"fmt"
	"path"
	"reflect"
	"regexp"
	"strings"
	"time"

	api "github.com/GProjectdev/Stateful-Migration-Operator-with-PV/api/v1alpha1"
	errors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

var checkpointGVK = schema.GroupVersionKind{Group: "fluidcr.dcnlab.com", Version: "v1alpha1", Kind: "FluidCRMigration"}
var policyGVK = schema.GroupVersionKind{Group: "policy.karmada.io", Version: "v1alpha1", Kind: "PropagationPolicy"}
var trainingRuntimeGVK = schema.GroupVersionKind{Group: "training.dcnlab.com", Version: "v1alpha1", Kind: "TrainingRuntime"}
var digestPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)

const annotationRestoreOwnedResume = "training.dcnlab.com/restore-owned-resume"

// RestoreReconciler uses only the Karmada control-plane client and reader.
type RestoreReconciler struct {
	Client       client.Client
	APIReader    client.Reader
	PollInterval time.Duration
}

func (r *RestoreReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Client == nil {
		r.Client = mgr.GetClient()
	}
	if r.APIReader == nil {
		r.APIReader = mgr.GetAPIReader()
	}
	return ctrl.NewControllerManagedBy(mgr).For(&api.RestoreRequest{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).Complete(r)
}
func (r *RestoreReconciler) reader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

func (r *RestoreReconciler) Reconcile(ctx context.Context, key ctrl.Request) (ctrl.Result, error) {
	req := &api.RestoreRequest{}
	if err := r.reader().Get(ctx, key.NamespacedName, req); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !req.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}
	interval := r.PollInterval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	finish := func(phase, message, plan string) (ctrl.Result, error) {
		before := req.DeepCopy()
		req.Status.Phase, req.Status.Message, req.Status.PlanName = phase, message, plan
		req.Status.ObservedGeneration = req.Generation
		if !reflect.DeepEqual(before.Status, req.Status) {
			if err := r.Client.Status().Patch(ctx, req, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{RequeueAfter: interval}, nil
	}
	if err := validateRequest(req); err != nil {
		return finish("Failed", err.Error(), req.Status.PlanName)
	}
	cp := &unstructured.Unstructured{}
	cp.SetGroupVersionKind(checkpointGVK)
	if err := r.reader().Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: req.Spec.CheckpointRef.Name}, cp); err != nil {
		if errors.IsNotFound(err) {
			return finish("AwaitingCheckpoint", "checkpoint not found", req.Status.PlanName)
		}
		return ctrl.Result{}, err
	}
	ready, err := validateCheckpoint(req, cp)
	if err != nil {
		return finish("Failed", err.Error(), req.Status.PlanName)
	}
	if !ready {
		return finish("AwaitingCheckpoint", "waiting for current source checkpoint aggregation", req.Status.PlanName)
	}
	desired, err := desiredPlan(req, cp)
	if err != nil {
		return finish("Failed", err.Error(), req.Status.PlanName)
	}
	plan := &api.RestorePlan{}
	err = r.reader().Get(ctx, client.ObjectKeyFromObject(desired), plan)
	if errors.IsNotFound(err) {
		if err = r.Client.Create(ctx, desired); err != nil && !errors.IsAlreadyExists(err) {
			return ctrl.Result{}, err
		}
		if err = r.reader().Get(ctx, client.ObjectKeyFromObject(desired), plan); err != nil {
			return ctrl.Result{}, err
		}
	} else if err != nil {
		return ctrl.Result{}, err
	}
	if !ownedBy(plan, req) || !reflect.DeepEqual(plan.Spec, desired.Spec) || !plan.DeletionTimestamp.IsZero() {
		return finish("Failed", "RestorePlan ownership or immutable spec conflict", desired.Name)
	}
	policy := desiredPolicy(req, plan.Name)
	actual := &unstructured.Unstructured{}
	actual.SetGroupVersionKind(policyGVK)
	err = r.reader().Get(ctx, client.ObjectKeyFromObject(policy), actual)
	if errors.IsNotFound(err) {
		if err = r.Client.Create(ctx, policy); err != nil && !errors.IsAlreadyExists(err) {
			return ctrl.Result{}, err
		}
		if err = r.reader().Get(ctx, client.ObjectKeyFromObject(policy), actual); err != nil {
			return ctrl.Result{}, err
		}
	} else if err != nil {
		return ctrl.Result{}, err
	}
	if !ownedBy(actual, req) || !policyMatches(actual, policy) || !actual.GetDeletionTimestamp().IsZero() {
		return finish("Failed", "PropagationPolicy ownership or spec drift", plan.Name)
	}
	phase, message := "Preparing", "waiting for current target plan aggregation"
	matches := 0
	for _, s := range plan.Status.Clusters {
		if s.ClusterName != req.Spec.TargetCluster {
			continue
		}
		matches++
		if s.ObservedGeneration != plan.Generation || plan.Generation <= 0 {
			continue
		}
		switch s.Phase {
		case "AwaitingArtifacts", "Preparing", "Prepared", "Failed":
			phase, message = s.Phase, s.Message
		case "Running":
			verification, err := r.validateTargetRuntime(ctx, req, plan, s.Pods, s.SourceFences)
			if err == nil {
				phase, message = "Verified", "target runtime evidence verified"
				req.Status.Verification = stableVerification(req, verification)
				break
			}
			if req.Spec.PartialRestore != nil {
				verification, readyErr := partialRestoreReadyVerification(req, plan, s.Pods, s.SourceFences)
				if readyErr == nil {
					if err := r.authorizeRestoreOwnedResume(ctx, cp); err != nil {
						return ctrl.Result{}, err
					}
					phase, message = "RestoreReady", "target native restore and source fence evidence verified; scoped survivor release authorized"
					req.Status.Verification = stableVerification(req, verification)
					break
				}
			}
			phase, message = "Running", s.Message
		}
	}
	if matches > 1 {
		return finish("Failed", "duplicate target cluster aggregation", plan.Name)
	}
	return finish(phase, message, plan.Name)
}

func (r *RestoreReconciler) authorizeRestoreOwnedResume(ctx context.Context, cp *unstructured.Unstructured) error {
	if cp.GetAnnotations()[annotationRestoreOwnedResume] == "true" {
		return nil
	}
	before := cp.DeepCopy()
	annotations := map[string]string{}
	for k, v := range cp.GetAnnotations() {
		annotations[k] = v
	}
	annotations[annotationRestoreOwnedResume] = "true"
	cp.SetAnnotations(annotations)
	return r.Client.Patch(ctx, cp, client.MergeFrom(before))
}

func (r *RestoreReconciler) validateTargetRuntime(ctx context.Context, req *api.RestoreRequest, plan *api.RestorePlan, targetPods []api.PodStatus, sourceFences []api.SourcePodFenceStatus) (*api.RestoreVerification, error) {
	tr, err := r.resolveTrainingRuntime(ctx, req)
	if err != nil {
		return nil, err
	}
	clusters, found, err := unstructured.NestedSlice(tr.Object, "status", "clusters")
	if err != nil || !found {
		return nil, fmt.Errorf("waiting for target TrainingRuntime aggregation")
	}
	var runtimeStatus map[string]interface{}
	for _, raw := range clusters {
		cluster, ok := raw.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("invalid TrainingRuntime cluster entry")
		}
		if cluster["clusterName"] != req.Spec.TargetCluster {
			continue
		}
		observedGeneration, ok := int64From(cluster["observedGeneration"])
		if !ok || observedGeneration != tr.GetGeneration() {
			return nil, fmt.Errorf("TrainingRuntime observedGeneration mismatch")
		}
		if runtimeStatus != nil {
			return nil, fmt.Errorf("duplicate target TrainingRuntime aggregation")
		}
		status, ok := cluster["status"].(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("target TrainingRuntime report is missing status")
		}
		runtimeStatus = status
	}
	if runtimeStatus == nil {
		return nil, fmt.Errorf("waiting for target TrainingRuntime report")
	}
	if req.Spec.TrainingRuntimeRef.UID != "" && string(tr.GetUID()) != req.Spec.TrainingRuntimeRef.UID {
		return nil, fmt.Errorf("TrainingRuntime UID mismatch")
	}
	if err := validateRuntimeStatus(req, plan, targetPods, runtimeStatus, time.Now()); err != nil {
		return nil, err
	}
	verifiedAt := metav1.Now()
	verification := &api.RestoreVerification{RequestUID: string(req.UID), Operation: req.Annotations["training.dcnlab.com/recovery-operation"], CheckpointID: req.Spec.CheckpointRef.CheckpointID, VerifiedAt: verifiedAt, TrainingRuntimeRef: api.RuntimeReference{Name: tr.GetName(), UID: string(tr.GetUID())}, SourceCluster: req.Spec.SourceCluster, TargetCluster: req.Spec.TargetCluster, SourceFenced: req.Spec.SourceFenced}
	if req.Spec.PartialRestore != nil {
		partial, sourceFence, survivors, err := partialVerification(req, plan, targetPods, sourceFences, verifiedAt.Time.Format(time.RFC3339))
		if err != nil {
			return nil, err
		}
		verification.PartialRestore = partial
		verification.SourceFence = sourceFence
		verification.PreservedSurvivors = append([]api.SurvivorEvidence{}, req.Spec.PartialRestore.PreservedSurvivors...)
		verification.Survivors = survivors
	}
	return verification, nil
}

func partialRestoreReadyVerification(req *api.RestoreRequest, plan *api.RestorePlan, targetPods []api.PodStatus, sourceFences []api.SourcePodFenceStatus) (*api.RestoreVerification, error) {
	if req.Spec.PartialRestore == nil {
		return nil, fmt.Errorf("partial restore required")
	}
	for _, pod := range targetPods {
		if pod.UID == "" || pod.Phase != "Running" {
			return nil, fmt.Errorf("target native restored pod evidence incomplete")
		}
	}
	now := metav1.Now()
	partial, sourceFence, survivors, err := partialVerification(req, plan, targetPods, sourceFences, now.Time.UTC().Format(time.RFC3339))
	if err != nil {
		return nil, err
	}
	return &api.RestoreVerification{RequestUID: string(req.UID), Operation: req.Annotations["training.dcnlab.com/recovery-operation"], CheckpointID: req.Spec.CheckpointRef.CheckpointID, VerifiedAt: now, TrainingRuntimeRef: req.Spec.TrainingRuntimeRef, SourceCluster: req.Spec.SourceCluster, TargetCluster: req.Spec.TargetCluster, SourceFenced: req.Spec.SourceFenced, SourceFence: sourceFence, PartialRestore: partial, PreservedSurvivors: append([]api.SurvivorEvidence{}, req.Spec.PartialRestore.PreservedSurvivors...), Survivors: survivors}, nil
}

func (r *RestoreReconciler) resolveTrainingRuntime(ctx context.Context, req *api.RestoreRequest) (*unstructured.Unstructured, error) {
	if req.Spec.TrainingRuntimeRef.Name != "" {
		tr := &unstructured.Unstructured{}
		tr.SetGroupVersionKind(trainingRuntimeGVK)
		if err := r.reader().Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: req.Spec.TrainingRuntimeRef.Name}, tr); err != nil {
			if errors.IsNotFound(err) {
				return nil, fmt.Errorf("waiting for TrainingRuntime telemetry")
			}
			return nil, err
		}
		return tr, nil
	}
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(trainingRuntimeGVK.GroupVersion().WithKind("TrainingRuntimeList"))
	if err := r.reader().List(ctx, list, client.InNamespace(req.Namespace)); err != nil {
		return nil, err
	}
	var match *unstructured.Unstructured
	for i := range list.Items {
		item := &list.Items[i]
		uid, _, _ := unstructured.NestedString(item.Object, "spec", "workloadRef", "uid")
		if uid != req.Spec.WorkloadRef.UID {
			continue
		}
		if match != nil {
			return nil, fmt.Errorf("multiple TrainingRuntime objects match workload UID")
		}
		copy := item.DeepCopy()
		match = copy
	}
	if match == nil {
		return nil, fmt.Errorf("waiting for unique TrainingRuntime telemetry")
	}
	return match, nil
}

func stableVerification(req *api.RestoreRequest, next *api.RestoreVerification) *api.RestoreVerification {
	if next == nil {
		return nil
	}
	old := req.Status.Verification
	if req.Status.ObservedGeneration == req.Generation && old != nil && old.RequestUID == next.RequestUID && old.CheckpointID == next.CheckpointID && old.TrainingRuntimeRef == next.TrainingRuntimeRef && old.SourceCluster == next.SourceCluster && old.TargetCluster == next.TargetCluster && old.SourceFenced == next.SourceFenced && reflect.DeepEqual(old.PartialRestore, next.PartialRestore) && reflect.DeepEqual(old.PreservedSurvivors, next.PreservedSurvivors) && !old.VerifiedAt.IsZero() {
		kept := *old
		return &kept
	}
	return next
}

func partialVerification(req *api.RestoreRequest, plan *api.RestorePlan, targetPods []api.PodStatus, sourceFences []api.SourcePodFenceStatus, observedAt string) (*api.PartialRestoreVerification, api.SourceFenceEvidence, []api.SurvivorStateEvidence, error) {
	operation := req.Annotations["training.dcnlab.com/recovery-operation"]
	targetUIDs := map[string]string{}
	for _, pod := range targetPods {
		if pod.UID != "" {
			targetUIDs[pod.Name] = pod.UID
		}
	}
	fences := map[string]api.SourcePodFenceStatus{}
	for _, fence := range sourceFences {
		if fence.ObservedGeneration == plan.Generation && fence.SourcePodUID != "" {
			fences[fence.PodName] = fence
		}
	}
	partial := &api.PartialRestoreVerification{PreventPeriodicResume: true, TargetRanks: make([]api.PartialRestoreTargetEvidence, 0, len(plan.Spec.Pods))}
	sourceFence := api.SourceFenceEvidence{Fenced: true, Operation: operation, ObservedAt: observedAt}
	for _, mapping := range plan.Spec.Pods {
		fence, ok := fences[mapping.TargetPod]
		if !ok || fence.Phase != "SourceGone" || fence.GoneObservedAt == nil || fence.DeleteRequestedAt == nil || fence.SourcePodUID != mapping.SourcePodUID {
			return nil, api.SourceFenceEvidence{}, nil, fmt.Errorf("waiting for source fence evidence")
		}
		if sourceFence.EvidenceID == "" || fence.SourcePodUID < sourceFence.EvidenceID {
			sourceFence.EvidenceID = fence.SourcePodUID + "-gone"
			if fence.GoneObservedAt != nil {
				sourceFence.ObservedAt = fence.GoneObservedAt.Time.UTC().Format(time.RFC3339)
			}
		}
		if len(mapping.Archives) != 1 || restoreArchiveEvidence(mapping.Archives[0]) == "" {
			return nil, api.SourceFenceEvidence{}, nil, fmt.Errorf("target rank archive evidence incomplete")
		}
		uid := targetUIDs[mapping.TargetPod]
		if uid == "" {
			return nil, api.SourceFenceEvidence{}, nil, fmt.Errorf("target pod UID evidence missing")
		}
		partial.TargetRanks = append(partial.TargetRanks, api.PartialRestoreTargetEvidence{Rank: mapping.Rank, TargetPodUID: uid, CheckpointID: req.Spec.CheckpointRef.CheckpointID, ArchiveEvidenceID: restoreArchiveEvidence(mapping.Archives[0])})
	}
	survivors := make([]api.SurvivorStateEvidence, 0, len(req.Spec.PartialRestore.PreservedSurvivors))
	for _, survivor := range req.Spec.PartialRestore.PreservedSurvivors {
		seenAt := firstNonEmpty(survivor.ObservedAt, observedAt)
		survivors = append(survivors, api.SurvivorStateEvidence{Rank: survivor.Rank, PodUID: survivor.PodUID, StateEvidence: api.StateEvidence{Kind: "pause-lock", ObservedAt: seenAt}})
	}
	return partial, sourceFence, survivors, nil
}

func restoreArchiveEvidence(a api.Archive) string {
	if strings.TrimSpace(a.ArchiveEvidenceID) != "" {
		return a.ArchiveEvidenceID
	}
	if strings.TrimSpace(a.DurableRef) != "" {
		return a.DurableRef
	}
	if strings.TrimSpace(a.SHA256) != "" {
		return "sha256:" + a.SHA256
	}
	return ""
}

func validateRuntimeStatus(req *api.RestoreRequest, plan *api.RestorePlan, targetPods []api.PodStatus, status map[string]interface{}, now time.Time) error {
	if status["phase"] != "Running" {
		return fmt.Errorf("target runtime is not Running")
	}
	if status["checkpointID"] != req.Spec.CheckpointRef.CheckpointID {
		return fmt.Errorf("target runtime checkpointID mismatch")
	}
	if status["workloadUID"] != req.Spec.WorkloadRef.UID {
		return fmt.Errorf("target runtime workloadUID mismatch")
	}
	expectedWorld := len(plan.Spec.Pods)
	if plan.Spec.PartialRestore != nil {
		expectedWorld += len(plan.Spec.PartialRestore.PreservedSurvivors)
	}
	world, ok := intFrom(status["worldSize"])
	if !ok || world <= 0 || world != expectedWorld {
		return fmt.Errorf("target runtime worldSize mismatch")
	}
	ready, ok := intFrom(status["readyRanks"])
	if !ok || ready != world {
		return fmt.Errorf("target runtime ranks are incomplete")
	}
	rootObserved, err := parseObservedAt(status["observedAt"])
	if err != nil || stale(rootObserved, now) {
		return fmt.Errorf("target runtime root observation is stale")
	}
	pods, ok := status["pods"].([]interface{})
	if !ok || len(pods) != world {
		return fmt.Errorf("target runtime pod samples are incomplete")
	}
	allowed := map[string]string{}
	for _, p := range targetPods {
		if p.UID != "" {
			allowed[p.Name] = p.UID
		}
	}
	if len(allowed) != len(plan.Spec.Pods) {
		return fmt.Errorf("target pod identity status is incomplete")
	}
	survivors := map[string]api.SurvivorEvidence{}
	if plan.Spec.PartialRestore != nil {
		for _, survivor := range plan.Spec.PartialRestore.PreservedSurvivors {
			survivors[survivor.PodName] = survivor
		}
	}
	seenRanks := map[int]bool{}
	seenNames := map[string]bool{}
	seenTargets, seenSurvivors := 0, 0
	minStep := int64(0)
	for _, raw := range pods {
		pod, ok := raw.(map[string]interface{})
		if !ok {
			return fmt.Errorf("invalid target runtime pod sample")
		}
		name, _ := pod["name"].(string)
		uid, _ := pod["uid"].(string)
		if name == "" || seenNames[name] {
			return fmt.Errorf("duplicate target runtime pod sample")
		}
		seenNames[name] = true
		rank, ok := intFrom(pod["rank"])
		if !ok || rank < 0 || rank >= world || seenRanks[rank] {
			return fmt.Errorf("target runtime rank set is invalid")
		}
		seenRanks[rank] = true
		if wantUID := allowed[name]; wantUID != "" {
			if wantUID != uid {
				return fmt.Errorf("target runtime pod identity mismatch")
			}
			seenTargets++
		} else if survivor, ok := survivors[name]; ok {
			if survivor.PodUID != uid || survivor.Rank != int64(rank) {
				return fmt.Errorf("target runtime survivor identity mismatch")
			}
			seenSurvivors++
		} else {
			return fmt.Errorf("target runtime pod identity mismatch")
		}
		if pod["checkpointID"] != req.Spec.CheckpointRef.CheckpointID {
			return fmt.Errorf("target runtime pod checkpointID mismatch")
		}
		step, ok := int64From(pod["globalStep"])
		if !ok || step <= 0 {
			return fmt.Errorf("target runtime progress is not positive")
		}
		previousStep, ok := int64From(pod["previousGlobalStep"])
		if !ok || previousStep < 0 || previousStep >= step {
			return fmt.Errorf("target runtime rank did not progress across samples")
		}
		previousObserved, err := parseObservedAt(pod["previousObservedAt"])
		if err != nil || stale(previousObserved, now) {
			return fmt.Errorf("target runtime previous pod sample is stale")
		}
		if minStep == 0 || step < minStep {
			minStep = step
		}
		observed, err := parseObservedAt(pod["observedAt"])
		if err != nil || stale(observed, now) {
			return fmt.Errorf("target runtime pod sample is stale")
		}
		if !previousObserved.Before(observed) {
			return fmt.Errorf("target runtime previous pod sample is not older than current sample")
		}
	}
	globalStep, ok := int64From(status["globalStep"])
	if !ok || globalStep != minStep {
		return fmt.Errorf("target runtime globalStep does not match rank minimum")
	}
	if seenTargets != len(allowed) || seenSurvivors != len(survivors) {
		return fmt.Errorf("target runtime partial rank set is incomplete")
	}
	return nil
}

func intFrom(v interface{}) (int, bool) {
	n, ok := int64From(v)
	return int(n), ok && n <= int64(^uint(0)>>1)
}

func int64From(v interface{}) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int:
		return int64(n), true
	case float64:
		if n == float64(int64(n)) {
			return int64(n), true
		}
	}
	return 0, false
}

func parseObservedAt(v interface{}) (time.Time, error) {
	s, ok := v.(string)
	if !ok || s == "" {
		return time.Time{}, fmt.Errorf("missing observedAt")
	}
	return time.Parse(time.RFC3339, s)
}

func stale(t time.Time, now time.Time) bool {
	age := now.Sub(t)
	return age < 0 || age > 30*time.Second
}

// ValidateRestoreCheckpoint rechecks the immutable operation before releasing dispatch.
func ValidateRestoreCheckpoint(req *api.RestoreRequest, cp *unstructured.Unstructured) error {
	if err := validateRequest(req); err != nil {
		return err
	}
	ready, err := validateCheckpoint(req, cp)
	if err != nil {
		return err
	}
	if !ready {
		return fmt.Errorf("current source checkpoint is not completed")
	}
	return nil
}

func ownedBy(obj client.Object, req *api.RestoreRequest) bool {
	owner := metav1.GetControllerOf(obj)
	return owner != nil && owner.APIVersion == api.GroupVersion.String() && owner.Kind == "RestoreRequest" && owner.Name == req.Name && owner.UID == req.UID
}
func ownerReference(req *api.RestoreRequest) []metav1.OwnerReference {
	yes := true
	return []metav1.OwnerReference{{APIVersion: api.GroupVersion.String(), Kind: "RestoreRequest", Name: req.Name, UID: req.UID, Controller: &yes}}
}
func desiredPlan(req *api.RestoreRequest, cps ...*unstructured.Unstructured) (*api.RestorePlan, error) {
	sum := sha256.Sum256([]byte(req.UID))
	s := req.DeepCopy().Spec
	if s.SourceCluster == s.TargetCluster && len(cps) > 0 && cps[0] != nil {
		pods, err := enrichPartialArchives(req, cps[0])
		if err != nil {
			return nil, err
		}
		s.Pods = pods
	}
	return &api.RestorePlan{TypeMeta: metav1.TypeMeta{APIVersion: api.GroupVersion.String(), Kind: "RestorePlan"}, ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("restore-%x", sum[:20]), Namespace: req.Namespace, OwnerReferences: ownerReference(req)}, Spec: api.RestorePlanSpec{RequestUID: string(req.UID), CheckpointRef: s.CheckpointRef, WorkloadRef: s.WorkloadRef, TrainingRuntimeRef: s.TrainingRuntimeRef, SourceCluster: s.SourceCluster, TargetCluster: s.TargetCluster, SourceFenced: s.SourceFenced, VolumesReady: s.VolumesReady, Pods: s.Pods, PartialRestore: s.PartialRestore}}, nil
}

func mustDesiredPlan(req *api.RestoreRequest) *api.RestorePlan {
	plan, err := desiredPlan(req)
	if err != nil {
		panic(err)
	}
	return plan
}

func enrichPartialArchives(req *api.RestoreRequest, cp *unstructured.Unstructured) ([]api.RestorePod, error) {
	out := restoreCopyPods(req.Spec.Pods)
	sourcePods, err := checkpointSourcePods(req, cp)
	if err != nil {
		return nil, err
	}
	for i := range out {
		mapping := &out[i]
		status := sourcePods[mapping.SourcePod]
		if status == nil {
			return nil, fmt.Errorf("checkpoint status missing source pod %s", mapping.SourcePod)
		}
		if got, _, _ := unstructured.NestedString(status, "podUID"); got != mapping.SourcePodUID {
			return nil, fmt.Errorf("checkpoint source pod UID mismatch")
		}
		files, _, err := unstructured.NestedSlice(status, "checkpointFiles")
		if err != nil || len(files) != 1 || len(mapping.Archives) != 1 {
			return nil, fmt.Errorf("same-cluster partial restore requires exactly one checkpoint file per target rank")
		}
		file, ok := files[0].(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("invalid checkpoint file evidence")
		}
		archive := mapping.Archives[0]
		container, _, _ := unstructured.NestedString(file, "containerName")
		filePath, _, _ := unstructured.NestedString(file, "filePath")
		sha, _, _ := unstructured.NestedString(file, "sha256")
		durable, _, _ := unstructured.NestedString(file, "durableRef")
		if archive.SHA256 != "" && sha != "" && archive.SHA256 != sha {
			return nil, fmt.Errorf("checkpoint archive sha256 mismatch")
		}
		if archive.DurableRef != "" && durable != "" && archive.DurableRef != durable {
			return nil, fmt.Errorf("checkpoint archive durableRef mismatch")
		}
		if container == "" || filePath == "" || sha == "" {
			return nil, fmt.Errorf("checkpoint archive container/path/sha evidence incomplete")
		}
		archive.ContainerName = firstNonEmpty(archive.ContainerName, container)
		archive.SourcePath = firstNonEmpty(archive.SourcePath, filePath)
		archive.SHA256 = firstNonEmpty(archive.SHA256, sha)
		archive.TargetPath = firstNonEmpty(archive.TargetPath, deterministicRestoreTargetPath(sha))
		archive.DurableRef = firstNonEmpty(archive.DurableRef, durable)
		if archive.ArchiveEvidenceID == "" {
			archive.ArchiveEvidenceID, _, _ = unstructured.NestedString(file, "archiveEvidenceID")
		}
		if archive.ArchiveEvidenceID == "" && sha != "" {
			archive.ArchiveEvidenceID = "sha256:" + sha
		}
		if archive.DurableRef == "" || archive.ContainerName != container || archive.SourcePath != filePath || !validRestoreTargetPath(archive.TargetPath, sha) {
			return nil, fmt.Errorf("same-cluster archive evidence is incomplete")
		}
		mapping.Archives[0] = archive
	}
	return out, nil
}

func deterministicRestoreTargetPath(sha string) string {
	if sha == "" {
		return ""
	}
	return "/var/lib/kubelet/checkpoints/" + sha + ".tar"
}

func validRestoreTargetPath(pathValue, sha string) bool {
	if !strings.HasPrefix(pathValue, "/var/lib/kubelet/checkpoints/") || !validPath(pathValue) {
		return false
	}
	if sha == "" {
		return true
	}
	return pathValue == deterministicRestoreTargetPath(sha)
}

func restoreCopyPods(in []api.RestorePod) []api.RestorePod {
	out := append([]api.RestorePod{}, in...)
	for i := range out {
		out[i].Archives = append([]api.Archive{}, in[i].Archives...)
	}
	return out
}

func checkpointSourcePods(req *api.RestoreRequest, cp *unstructured.Unstructured) (map[string]map[string]interface{}, error) {
	clusters, _, err := unstructured.NestedSlice(cp.Object, "status", "clusters")
	if err != nil {
		return nil, fmt.Errorf("invalid checkpoint cluster aggregation")
	}
	for _, entry := range clusters {
		cluster, ok := entry.(map[string]interface{})
		if !ok || cluster["clusterName"] != req.Spec.SourceCluster {
			continue
		}
		pods, _, err := unstructured.NestedSlice(cluster, "pods")
		if err != nil {
			return nil, fmt.Errorf("invalid checkpoint pod aggregation")
		}
		out := map[string]map[string]interface{}{}
		for _, raw := range pods {
			pod, ok := raw.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("invalid checkpoint pod status")
			}
			name, _, _ := unstructured.NestedString(pod, "podName")
			if name != "" {
				out[name] = pod
			}
		}
		return out, nil
	}
	return nil, fmt.Errorf("source checkpoint cluster status missing")
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
func desiredPolicy(req *api.RestoreRequest, name string) *unstructured.Unstructured {
	clusters := []interface{}{req.Spec.SourceCluster}
	if req.Spec.TargetCluster != req.Spec.SourceCluster {
		clusters = append(clusters, req.Spec.TargetCluster)
	}
	p := &unstructured.Unstructured{Object: map[string]interface{}{"spec": map[string]interface{}{
		"resourceSelectors": []interface{}{map[string]interface{}{"apiVersion": api.GroupVersion.String(), "kind": "RestorePlan", "namespace": req.Namespace, "name": name}},
		"placement":         map[string]interface{}{"clusterAffinity": map[string]interface{}{"clusterNames": clusters}},
	}}}
	p.SetGroupVersionKind(policyGVK)
	p.SetName(name)
	p.SetNamespace(req.Namespace)
	p.SetOwnerReferences(ownerReference(req))
	return p
}
func validName(s string) bool { return s != "" && len(validation.IsDNS1123Subdomain(s)) == 0 }

// Admission may materialize these no-op defaults; all routing remains exact.
func policyMatches(actual, desired *unstructured.Unstructured) bool {
	a, ok := actual.DeepCopy().Object["spec"].(map[string]interface{})
	if !ok {
		return false
	}
	d := desired.Object["spec"].(map[string]interface{})
	// Karmada injects only these two health tolerations. They cannot widen
	// the singleton cluster affinity; other tolerations remain a conflict.
	if placement, ok := a["placement"].(map[string]interface{}); ok {
		if raw, exists := placement["clusterTolerations"]; exists {
			tolerations, ok := raw.([]interface{})
			if !ok {
				return false
			}
			seen := map[string]bool{}
			for _, raw := range tolerations {
				tol, ok := raw.(map[string]interface{})
				if !ok {
					return false
				}
				key, ok := tol["key"].(string)
				if !ok || (key != "cluster.karmada.io/not-ready" && key != "cluster.karmada.io/unreachable") || seen[key] {
					return false
				}
				seen[key] = true
				if tol["operator"] != "Exists" || tol["effect"] != "NoExecute" {
					return false
				}
				seconds, ok := tol["tolerationSeconds"].(int64)
				if !ok || seconds < 0 {
					return false
				}
				for k, v := range tol {
					switch k {
					case "key", "operator", "effect", "tolerationSeconds":
					case "value":
						if v != "" {
							return false
						}
					default:
						return false
					}
				}
			}
			delete(placement, "clusterTolerations")
		}
	}
	if !reflect.DeepEqual(a["resourceSelectors"], d["resourceSelectors"]) || !reflect.DeepEqual(a["placement"], d["placement"]) {
		return false
	}
	defaults := map[string]interface{}{"priority": int64(0), "preemption": "Never", "conflictResolution": "Abort", "propagateDeps": false, "schedulerName": "default-scheduler", "preserveResourcesOnDeletion": false}
	for k, v := range a {
		if k == "resourceSelectors" || k == "placement" {
			continue
		}
		want, ok := defaults[k]
		if !ok || !reflect.DeepEqual(v, want) {
			return false
		}
	}
	return true
}
func validPath(s string) bool {
	return strings.HasPrefix(s, "/") && s != "/" && path.Clean(s) == s && !strings.ContainsAny(s, "\\\x00\r\n")
}

func validatePartialRestore(p *api.PartialRestoreSpec, sourceFenced bool, podCount int) (map[int64]bool, map[string]api.SurvivorEvidence, error) {
	if p == nil {
		return nil, nil, fmt.Errorf("same-cluster restore requires partialRestore")
	}
	if sourceFenced {
		return nil, nil, fmt.Errorf("same-cluster partial restore requires sourceFenced=false and UID-bound pod evidence")
	}
	if !p.PreventPeriodicResume {
		return nil, nil, fmt.Errorf("same-cluster partial restore must prevent periodic resume interference")
	}
	if len(p.TargetRanks) == 0 || len(p.TargetRanks) != podCount || len(p.PreservedSurvivors) == 0 {
		return nil, nil, fmt.Errorf("partialRestore target ranks and preserved survivors are required")
	}
	targets := map[int64]bool{}
	for _, rank := range p.TargetRanks {
		if rank < 0 || targets[rank] {
			return nil, nil, fmt.Errorf("partialRestore target ranks must be unique non-negative values")
		}
		targets[rank] = true
	}
	survivors := map[string]api.SurvivorEvidence{}
	seenSurvivorRanks := map[int64]bool{}
	for _, survivor := range p.PreservedSurvivors {
		if survivor.Rank < 0 || targets[survivor.Rank] || seenSurvivorRanks[survivor.Rank] {
			return nil, nil, fmt.Errorf("partialRestore survivor ranks must be unique and outside target ranks")
		}
		if !validName(survivor.PodName) || !validName(survivor.NodeName) || strings.TrimSpace(survivor.PodUID) == "" || survivor.Generation <= 0 || !validPath(survivor.PauseLockPath) || path.Base(survivor.PauseLockPath) != "pause-lock" {
			return nil, nil, fmt.Errorf("partialRestore survivor evidence is incomplete")
		}
		seenSurvivorRanks[survivor.Rank] = true
		survivors[survivor.PodName] = survivor
	}
	if len(survivors) != len(p.PreservedSurvivors) {
		return nil, nil, fmt.Errorf("partialRestore survivor pod names must be unique")
	}
	return targets, survivors, nil
}

func validateRequest(req *api.RestoreRequest) error {
	s := req.Spec
	if req.UID == "" || !validName(s.CheckpointRef.Name) || strings.TrimSpace(s.CheckpointRef.UID) == "" || strings.TrimSpace(s.CheckpointRef.CheckpointID) == "" || s.CheckpointRef.Generation <= 0 {
		return fmt.Errorf("checkpoint reference, stable checkpointID and request UID are required")
	}
	if !s.VolumesReady {
		return fmt.Errorf("volumesReady must be true")
	}
	if !validName(s.SourceCluster) || !validName(s.TargetCluster) {
		return fmt.Errorf("valid source and target clusters are required")
	}
	sameCluster := s.SourceCluster == s.TargetCluster
	var partialTargets map[int64]bool
	if sameCluster {
		var err error
		partialTargets, _, err = validatePartialRestore(s.PartialRestore, s.SourceFenced, len(s.Pods))
		if err != nil {
			return err
		}
	} else {
		if !s.SourceFenced {
			return fmt.Errorf("sourceFenced must be true for cross-cluster restore")
		}
		if s.PartialRestore != nil {
			return fmt.Errorf("partialRestore is only supported for same-cluster restore")
		}
	}
	if !validName(s.WorkloadRef.Name) || strings.TrimSpace(s.WorkloadRef.UID) == "" || (!sameCluster && !validName(s.TrainingRuntimeRef.Name)) || (s.TrainingRuntimeRef.Name != "" && !validName(s.TrainingRuntimeRef.Name)) || !((s.WorkloadRef.Kind == "StatefulSet" && s.WorkloadRef.APIVersion == "apps/v1") || (s.WorkloadRef.Kind == "Pod" && s.WorkloadRef.APIVersion == "v1")) {
		return fmt.Errorf("only stable StatefulSet or Pod identities with management UID are supported")
	}
	if len(s.Pods) == 0 {
		return fmt.Errorf("pod mappings are required")
	}
	seen := map[string]bool{}
	targets := map[string]bool{}
	ordinal := regexp.MustCompile(`^` + regexp.QuoteMeta(s.WorkloadRef.Name) + `-(0|[1-9][0-9]*)$`)
	for _, p := range s.Pods {
		if !validName(p.SourcePod) || p.SourcePod != p.TargetPod || seen[p.SourcePod] || !validName(p.SourceNode) || !validName(p.TargetNode) {
			return fmt.Errorf("pod mappings must have unique stable identities, source nodes and target nodes")
		}
		if sameCluster {
			if !partialTargets[p.Rank] || strings.TrimSpace(p.SourcePodUID) == "" {
				return fmt.Errorf("same-cluster partial restore requires explicit target rank and sourcePodUID evidence")
			}
		} else if strings.TrimSpace(p.SourcePodUID) != "" {
			return fmt.Errorf("sourcePodUID is reserved for same-cluster partial restore")
		}
		seen[p.SourcePod] = true
		if (s.WorkloadRef.Kind == "Pod" && p.SourcePod != s.WorkloadRef.Name) || (s.WorkloadRef.Kind == "StatefulSet" && !ordinal.MatchString(p.SourcePod)) {
			return fmt.Errorf("pod identity does not match workload")
		}
		if len(p.Archives) != 1 {
			return fmt.Errorf("each FluidCR pod must map its single selected container archive")
		}
		for _, a := range p.Archives {
			if !digestPattern.MatchString(a.SHA256) {
				return fmt.Errorf("invalid archive SHA256")
			}
			if sameCluster && (a.ContainerName == "" || a.SourcePath == "" || a.TargetPath == "") {
				if strings.TrimSpace(a.DurableRef) == "" {
					return fmt.Errorf("same-cluster minimal archive evidence requires durableRef")
				}
				continue
			}
			if len(validation.IsDNS1123Label(a.ContainerName)) != 0 || a.ContainerName == "" || !validPath(a.SourcePath) || !validPath(a.TargetPath) || !strings.HasPrefix(a.TargetPath, "/var/lib/kubelet/checkpoints/") {
				return fmt.Errorf("invalid archive container, path or SHA256")
			}
			key := p.TargetNode + "\x00" + a.TargetPath
			if targets[key] {
				return fmt.Errorf("duplicate target archive path on node")
			}
			targets[key] = true
		}
	}
	return nil
}
func validateCheckpoint(req *api.RestoreRequest, cp *unstructured.Unstructured) (bool, error) {
	s := req.Spec
	if string(cp.GetUID()) != s.CheckpointRef.UID || cp.GetGeneration() != s.CheckpointRef.Generation || !cp.GetDeletionTimestamp().IsZero() {
		return false, fmt.Errorf("checkpoint UID or generation mismatch, or checkpoint deleting")
	}
	resume, found, err := unstructured.NestedBool(cp.Object, "spec", "resume")
	if err != nil || !found {
		return false, fmt.Errorf("checkpoint spec.resume must be explicit")
	}
	partialTargets := map[int64]bool(nil)
	survivors := map[string]api.SurvivorEvidence(nil)
	if s.SourceCluster == s.TargetCluster {
		var vErr error
		partialTargets, survivors, vErr = validatePartialRestore(s.PartialRestore, s.SourceFenced, len(s.Pods))
		if vErr != nil {
			return false, vErr
		}
		if resume {
			return false, fmt.Errorf("same-cluster partial checkpoint must set spec.resume=false")
		}
	}
	if cp.GetAnnotations()["training.dcnlab.com/checkpoint-id"] != s.CheckpointRef.CheckpointID {
		return false, fmt.Errorf("checkpoint annotation checkpoint-id mismatch")
	}
	for k, want := range map[string]string{"apiVersion": s.WorkloadRef.APIVersion, "kind": s.WorkloadRef.Kind, "name": s.WorkloadRef.Name, "namespace": req.Namespace} {
		got, _, err := unstructured.NestedString(cp.Object, "spec", "workloadRef", k)
		if err != nil {
			return false, fmt.Errorf("invalid checkpoint workloadRef.%s", k)
		}
		if k == "namespace" && got == "" {
			got = cp.GetNamespace()
		}
		if got != want {
			return false, fmt.Errorf("checkpoint workloadRef.%s mismatch", k)
		}
	}
	clusters, _, err := unstructured.NestedSlice(cp.Object, "status", "clusters")
	if err != nil {
		return false, fmt.Errorf("invalid checkpoint cluster aggregation")
	}
	var source map[string]interface{}
	for _, entry := range clusters {
		c, ok := entry.(map[string]interface{})
		if !ok {
			return false, fmt.Errorf("invalid checkpoint cluster entry")
		}
		if c["clusterName"] == s.SourceCluster {
			if source != nil {
				return false, fmt.Errorf("duplicate source cluster aggregation")
			}
			source = c
		}
	}
	if source == nil {
		return false, nil
	}
	gen, _, _ := unstructured.NestedInt64(source, "observedGeneration")
	if gen != cp.GetGeneration() {
		return false, nil
	}
	if source["phase"] == "Failed" {
		return false, fmt.Errorf("source checkpoint failed")
	}
	if source["phase"] != "Completed" {
		return false, nil
	}
	pods, found, err := unstructured.NestedSlice(source, "pods")
	if err != nil || !found || len(pods) < len(s.Pods)+len(survivors) {
		return false, fmt.Errorf("source pod mappings are incomplete")
	}
	mapped := map[string]api.RestorePod{}
	for _, p := range s.Pods {
		mapped[p.SourcePod] = p
	}
	seen := map[string]bool{}
	seenSurvivors := map[string]bool{}
	for _, entry := range pods {
		p, ok := entry.(map[string]interface{})
		if !ok {
			return false, fmt.Errorf("invalid source pod status")
		}
		name, _, _ := unstructured.NestedString(p, "podName")
		mapping, ok := mapped[name]
		if !ok {
			if survivor, ok := survivors[name]; ok {
				if seenSurvivors[name] {
					return false, fmt.Errorf("duplicate survivor pod evidence")
				}
				if err := validateSurvivorCheckpointEvidence(p, survivor); err != nil {
					return false, err
				}
				seenSurvivors[name] = true
				continue
			}
			return false, fmt.Errorf("source pod mappings are not a bijection")
		}
		if seen[name] {
			return false, fmt.Errorf("source pod mappings are not a bijection")
		}
		seen[name] = true
		if len(partialTargets) > 0 {
			if !partialTargets[mapping.Rank] || p["phase"] != "ContainerCheckpointed" {
				return false, fmt.Errorf("target rank lacks stopped container checkpoint evidence")
			}
			podUID, _, _ := unstructured.NestedString(p, "podUID")
			if podUID == "" || podUID != mapping.SourcePodUID {
				return false, fmt.Errorf("source pod UID precondition mismatch")
			}
		} else if p["phase"] != "ContainerCheckpointed" && p["phase"] != "Resumed" {
			return false, fmt.Errorf("source pod is not checkpointed or resumed from an immutable checkpoint")
		}
		files, _, err := unstructured.NestedSlice(p, "checkpointFiles")
		if err != nil || len(files) != 1 {
			return false, fmt.Errorf("all selected container archives must be mapped")
		}
		file, ok := files[0].(map[string]interface{})
		if !ok {
			return false, fmt.Errorf("invalid checkpoint file")
		}
		a := mapping.Archives[0]
		nodeName, _, _ := unstructured.NestedString(p, "nodeName")
		if nodeName != mapping.SourceNode {
			return false, fmt.Errorf("source pod node mismatch")
		}
		fileContainer, _ := file["containerName"].(string)
		filePath, _ := file["filePath"].(string)
		if fileContainer == "" || filePath == "" {
			return false, fmt.Errorf("source container or checkpoint path missing")
		}
		if (a.ContainerName != "" && fileContainer != a.ContainerName) || (a.SourcePath != "" && filePath != a.SourcePath) {
			return false, fmt.Errorf("source container or checkpoint path mismatch")
		}
		if fileSHA, ok := file["sha256"].(string); ok && fileSHA != "" && fileSHA != a.SHA256 {
			return false, fmt.Errorf("source checkpoint sha256 mismatch")
		}
		if durableRef, ok := file["durableRef"].(string); ok && durableRef != "" {
			if a.DurableRef != "" && durableRef != a.DurableRef {
				return false, fmt.Errorf("source checkpoint durableRef mismatch")
			}
		}
	}
	if len(seen) != len(s.Pods) || len(seenSurvivors) != len(survivors) {
		return false, fmt.Errorf("source pod mappings are incomplete")
	}
	return true, nil
}

func validateSurvivorCheckpointEvidence(p map[string]interface{}, want api.SurvivorEvidence) error {
	if p["phase"] != "SurvivorPaused" && p["phase"] != "Resumed" {
		return fmt.Errorf("survivor pod is not paused or restore-resumed")
	}
	rank, ok := intFrom(p["rank"])
	if !ok || int64(rank) != want.Rank {
		return fmt.Errorf("survivor rank mismatch")
	}
	for field, expected := range map[string]string{"podUID": want.PodUID, "nodeName": want.NodeName} {
		got, _, _ := unstructured.NestedString(p, field)
		if got != expected {
			return fmt.Errorf("survivor %s mismatch", field)
		}
	}
	evidence, found, err := unstructured.NestedMap(p, "survivorEvidence")
	if err != nil || !found {
		return fmt.Errorf("survivor evidence missing")
	}
	generation, ok := int64From(evidence["generation"])
	if !ok || generation != want.Generation {
		return fmt.Errorf("survivor generation mismatch")
	}
	if evidence["pauseLockPath"] != want.PauseLockPath {
		return fmt.Errorf("survivor pause-lock path mismatch")
	}
	pid, ok := int64From(evidence["pauseLockPID"])
	if !ok || pid <= 0 {
		return fmt.Errorf("survivor pause-lock PID missing")
	}
	if observed, ok := evidence["observedAt"].(string); !ok || strings.TrimSpace(observed) == "" {
		return fmt.Errorf("survivor observedAt missing")
	}
	return nil
}
