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
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var checkpointGVK = schema.GroupVersionKind{Group: "fluidcr.dcnlab.com", Version: "v1alpha1", Kind: "FluidCRMigration"}
var policyGVK = schema.GroupVersionKind{Group: "policy.karmada.io", Version: "v1alpha1", Kind: "PropagationPolicy"}
var digestPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)

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
	return ctrl.NewControllerManagedBy(mgr).For(&api.RestoreRequest{}).Complete(r)
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
	desired := desiredPlan(req)
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
		case "AwaitingArtifacts", "Preparing", "Prepared", "Running", "Failed":
			phase, message = s.Phase, s.Message
		}
	}
	if matches > 1 {
		return finish("Failed", "duplicate target cluster aggregation", plan.Name)
	}
	return finish(phase, message, plan.Name)
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
func desiredPlan(req *api.RestoreRequest) *api.RestorePlan {
	sum := sha256.Sum256([]byte(req.UID))
	s := req.DeepCopy().Spec
	return &api.RestorePlan{TypeMeta: metav1.TypeMeta{APIVersion: api.GroupVersion.String(), Kind: "RestorePlan"}, ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("restore-%x", sum[:20]), Namespace: req.Namespace, OwnerReferences: ownerReference(req)}, Spec: api.RestorePlanSpec{RequestUID: string(req.UID), CheckpointRef: s.CheckpointRef, WorkloadRef: s.WorkloadRef, SourceCluster: s.SourceCluster, TargetCluster: s.TargetCluster, SourceFenced: s.SourceFenced, VolumesReady: s.VolumesReady, Pods: s.Pods}}
}
func desiredPolicy(req *api.RestoreRequest, name string) *unstructured.Unstructured {
	p := &unstructured.Unstructured{Object: map[string]interface{}{"spec": map[string]interface{}{
		"resourceSelectors": []interface{}{map[string]interface{}{"apiVersion": api.GroupVersion.String(), "kind": "RestorePlan", "namespace": req.Namespace, "name": name}},
		"placement":         map[string]interface{}{"clusterAffinity": map[string]interface{}{"clusterNames": []interface{}{req.Spec.TargetCluster}}},
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
func validateRequest(req *api.RestoreRequest) error {
	s := req.Spec
	if req.UID == "" || !validName(s.CheckpointRef.Name) || strings.TrimSpace(s.CheckpointRef.UID) == "" || s.CheckpointRef.Generation <= 0 {
		return fmt.Errorf("checkpoint reference and request UID are required")
	}
	if !s.SourceFenced || !s.VolumesReady {
		return fmt.Errorf("sourceFenced and volumesReady must be true")
	}
	if !validName(s.SourceCluster) || !validName(s.TargetCluster) || s.SourceCluster == s.TargetCluster {
		return fmt.Errorf("distinct valid source and target clusters are required")
	}
	if !validName(s.WorkloadRef.Name) || !((s.WorkloadRef.Kind == "StatefulSet" && s.WorkloadRef.APIVersion == "apps/v1") || (s.WorkloadRef.Kind == "Pod" && s.WorkloadRef.APIVersion == "v1")) {
		return fmt.Errorf("only stable StatefulSet or Pod identities are supported")
	}
	if len(s.Pods) == 0 {
		return fmt.Errorf("pod mappings are required")
	}
	seen := map[string]bool{}
	targets := map[string]bool{}
	ordinal := regexp.MustCompile(`^` + regexp.QuoteMeta(s.WorkloadRef.Name) + `-(0|[1-9][0-9]*)$`)
	for _, p := range s.Pods {
		if !validName(p.SourcePod) || p.SourcePod != p.TargetPod || seen[p.SourcePod] || !validName(p.TargetNode) {
			return fmt.Errorf("pod mappings must have unique stable identities and target nodes")
		}
		seen[p.SourcePod] = true
		if (s.WorkloadRef.Kind == "Pod" && p.SourcePod != s.WorkloadRef.Name) || (s.WorkloadRef.Kind == "StatefulSet" && !ordinal.MatchString(p.SourcePod)) {
			return fmt.Errorf("pod identity does not match workload")
		}
		if len(p.Archives) != 1 {
			return fmt.Errorf("each FluidCR pod must map its single selected container archive")
		}
		for _, a := range p.Archives {
			if len(validation.IsDNS1123Label(a.ContainerName)) != 0 || a.ContainerName == "" || !validPath(a.SourcePath) || !validPath(a.TargetPath) || !strings.HasPrefix(a.TargetPath, "/var/lib/kubelet/checkpoints/") || !digestPattern.MatchString(a.SHA256) {
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
	if err != nil || !found || resume {
		return false, fmt.Errorf("checkpoint spec.resume must explicitly be false")
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
	if err != nil || !found || len(pods) != len(s.Pods) {
		return false, fmt.Errorf("source pod mappings are incomplete")
	}
	mapped := map[string]api.RestorePod{}
	for _, p := range s.Pods {
		mapped[p.SourcePod] = p
	}
	seen := map[string]bool{}
	for _, entry := range pods {
		p, ok := entry.(map[string]interface{})
		if !ok {
			return false, fmt.Errorf("invalid source pod status")
		}
		name, _, _ := unstructured.NestedString(p, "podName")
		mapping, ok := mapped[name]
		if !ok || seen[name] {
			return false, fmt.Errorf("source pod mappings are not a bijection")
		}
		seen[name] = true
		if p["phase"] != "ContainerCheckpointed" {
			return false, fmt.Errorf("source pod is not ContainerCheckpointed")
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
		if file["containerName"] != a.ContainerName || file["filePath"] != a.SourcePath {
			return false, fmt.Errorf("source container or checkpoint path mismatch")
		}
	}
	return true, nil
}
