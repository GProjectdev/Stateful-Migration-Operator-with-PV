package suspension

import (
	"context"
	"fmt"
	"reflect"
	"time"

	api "github.com/GProjectdev/Stateful-Migration-Operator-with-PV/api/v1alpha1"
	"github.com/GProjectdev/Stateful-Migration-Operator-with-PV/internal/management"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	RequestAnnotation     = "migration.dcnlab.com/restore-request"
	RequestUIDAnnotation  = "migration.dcnlab.com/restore-request-uid"
	PVAnnotation          = "migration.dcnlab.com/pv-migration"
	PVUIDAnnotation       = "migration.dcnlab.com/pv-migration-uid"
	ReleasedUIDAnnotation = "migration.dcnlab.com/dispatch-released-for"
)

var bindingGVK = schema.GroupVersionKind{Group: "work.karmada.io", Version: "v1alpha2", Kind: "ResourceBinding"}

type Reconciler struct {
	Client    client.Client
	APIReader client.Reader
}

func object(version, kind string) *unstructured.Unstructured {
	o := &unstructured.Unstructured{}
	o.SetGroupVersionKind(schema.FromAPIVersionAndKind(version, kind))
	return o
}
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Client == nil {
		r.Client = mgr.GetClient()
	}
	if r.APIReader == nil {
		r.APIReader = mgr.GetAPIReader()
	}
	return ctrl.NewControllerManagedBy(mgr).Named("migration-suspension").
		For(object(bindingGVK.GroupVersion().String(), bindingGVK.Kind)).Complete(r)
}
func str(o map[string]interface{}, fields ...string) string {
	s, _, _ := unstructured.NestedString(o, fields...)
	return s
}
func integer(o map[string]interface{}, fields ...string) int64 {
	n, _, _ := unstructured.NestedInt64(o, fields...)
	return n
}
func truth(o map[string]interface{}, fields ...string) bool {
	b, _, _ := unstructured.NestedBool(o, fields...)
	return b
}
func ready(phase string) bool {
	return phase == "Prepared" || phase == "Running" || phase == "Verified"
}

// A conflict restarts all checks using uncached reads, not just the final patch.
func (r *Reconciler) Reconcile(ctx context.Context, key ctrl.Request) (ctrl.Result, error) {
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	enrolled := false
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		rb := object(bindingGVK.GroupVersion().String(), bindingGVK.Kind)
		if err := reader.Get(ctx, key.NamespacedName, rb); err != nil {
			return client.IgnoreNotFound(err)
		}
		if rb.GetAnnotations()[RequestAnnotation] == "" || !rb.GetDeletionTimestamp().IsZero() {
			return nil
		}
		enrolled = true
		if uid := rb.GetAnnotations()[RequestUIDAnnotation]; uid != "" && rb.GetAnnotations()[ReleasedUIDAnnotation] == uid {
			return nil
		}
		if !truth(rb.Object, "spec", "suspension", "dispatching") {
			return nil
		}
		if err := check(ctx, reader, rb); err != nil {
			ctrl.LoggerFrom(ctx).Info("dispatch remains suspended", "binding", key.NamespacedName, "reason", err.Error())
			return nil
		}
		before := rb.DeepCopy()
		unstructured.RemoveNestedField(rb.Object, "spec", "suspension", "dispatching")
		annotations := rb.GetAnnotations()
		annotations[ReleasedUIDAnnotation] = annotations[RequestUIDAnnotation]
		rb.SetAnnotations(annotations)
		return r.Client.Patch(ctx, rb, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
	})
	if enrolled {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, err
	}
	return ctrl.Result{}, err
}
func check(ctx context.Context, reader client.Reader, rb *unstructured.Unstructured) error {
	fail := func(s string) error { return fmt.Errorf("%s", s) }
	suspension, _, err := unstructured.NestedMap(rb.Object, "spec", "suspension")
	if err != nil {
		return err
	}
	for k := range suspension {
		if k != "dispatching" && k != "scheduling" {
			return fail("unsupported per-cluster or nested suspension")
		}
	}
	a := rb.GetAnnotations()
	if a[RequestUIDAnnotation] == "" || a[PVAnnotation] == "" || a[PVUIDAnnotation] == "" {
		return fail("operation names and UIDs required")
	}
	req := &api.RestoreRequest{}
	if err := reader.Get(ctx, client.ObjectKey{Namespace: rb.GetNamespace(), Name: a[RequestAnnotation]}, req); err != nil {
		return err
	}
	s := req.Spec
	if string(req.UID) != a[RequestUIDAnnotation] || !req.DeletionTimestamp.IsZero() || req.Generation <= 0 ||
		req.Status.ObservedGeneration != req.Generation || !ready(req.Status.Phase) {
		return fail("current ready RestoreRequest required")
	}
	if s.WorkloadRef.APIVersion != "apps/v1" || s.WorkloadRef.Kind != "StatefulSet" {
		return fail("automatic gate supports StatefulSet only")
	}
	for k, want := range map[string]string{"apiVersion": "apps/v1", "kind": "StatefulSet", "name": s.WorkloadRef.Name, "namespace": req.Namespace} {
		if str(rb.Object, "spec", "resource", k) != want {
			return fail("binding workload reference mismatch")
		}
	}
	sts := &appsv1.StatefulSet{}
	if err := reader.Get(ctx, client.ObjectKey{Namespace: req.Namespace, Name: s.WorkloadRef.Name}, sts); err != nil {
		return err
	}
	if sts.UID == "" || !sts.DeletionTimestamp.IsZero() || str(rb.Object, "spec", "resource", "uid") != string(sts.UID) {
		return fail("binding workload UID mismatch")
	}
	if req.Status.PlanName == "" || sts.Spec.Template.Labels[api.PlanLabel] != req.Status.PlanName {
		return fail("workload restore-plan label mismatch")
	}
	clusters, _, err := unstructured.NestedSlice(rb.Object, "spec", "clusters")
	if err != nil || len(clusters) != 1 {
		return fail("binding must select only target")
	}
	cluster, ok := clusters[0].(map[string]interface{})
	if !ok || str(cluster, "name") != s.TargetCluster {
		return fail("binding target mismatch")
	}
	replicas := int32(1)
	if sts.Spec.Replicas != nil {
		replicas = *sts.Spec.Replicas
	}
	if replicas <= 0 || int(replicas) != len(s.Pods) {
		return fail("all StatefulSet replicas must be restored")
	}
	if n, found, err := unstructured.NestedInt64(cluster, "replicas"); err != nil || (found && n != int64(replicas)) {
		return fail("binding target replica allocation mismatch")
	}
	start := int32(0)
	if sts.Spec.Ordinals != nil {
		start = sts.Spec.Ordinals.Start
	}
	expectedPods := map[string]bool{}
	for i := int32(0); i < replicas; i++ {
		expectedPods[fmt.Sprintf("%s-%d", sts.Name, start+i)] = true
	}
	for _, p := range s.Pods {
		if !expectedPods[p.SourcePod] || p.SourcePod != p.TargetPod {
			return fail("restore mapping does not cover current StatefulSet ordinals")
		}
		delete(expectedPods, p.SourcePod)
	}
	if len(expectedPods) != 0 {
		return fail("missing ordinal")
	}
	plan := &api.RestorePlan{}
	if err := reader.Get(ctx, client.ObjectKey{Namespace: req.Namespace, Name: req.Status.PlanName}, plan); err != nil {
		return err
	}
	owner := metav1.GetControllerOf(plan)
	want := api.RestorePlanSpec{RequestUID: string(req.UID), CheckpointRef: s.CheckpointRef, WorkloadRef: s.WorkloadRef, TrainingRuntimeRef: s.TrainingRuntimeRef, SourceCluster: s.SourceCluster, TargetCluster: s.TargetCluster, SourceFenced: s.SourceFenced, VolumesReady: s.VolumesReady, Pods: s.Pods}
	if plan.UID == "" || plan.Generation <= 0 || !plan.DeletionTimestamp.IsZero() || owner == nil || owner.UID != req.UID || owner.Name != req.Name ||
		owner.Kind != "RestoreRequest" || owner.APIVersion != api.GroupVersion.String() || !reflect.DeepEqual(plan.Spec, want) {
		return fail("plan ownership or spec mismatch")
	}
	if len(plan.Status.Clusters) != 1 {
		return fail("one current target plan report required")
	}
	report := plan.Status.Clusters[0]
	if report.ClusterName != s.TargetCluster || report.ObservedGeneration != plan.Generation || !ready(report.Phase) || report.Phase != req.Status.Phase {
		return fail("plan target not currently prepared")
	}
	cp := object("fluidcr.dcnlab.com/v1alpha1", "FluidCRMigration")
	if err := reader.Get(ctx, client.ObjectKey{Namespace: req.Namespace, Name: s.CheckpointRef.Name}, cp); err != nil {
		return err
	}
	if err := management.ValidateRestoreCheckpoint(req, cp); err != nil {
		return err
	}
	pv := object(api.GroupVersion.String(), "PVMigration")
	if err := reader.Get(ctx, client.ObjectKey{Namespace: req.Namespace, Name: a[PVAnnotation]}, pv); err != nil {
		return err
	}
	if string(pv.GetUID()) != a[PVUIDAnnotation] || !pv.GetDeletionTimestamp().IsZero() || pv.GetGeneration() <= 0 ||
		integer(pv.Object, "status", "observedGeneration") != pv.GetGeneration() || str(pv.Object, "status", "phase") != "Completed" ||
		str(pv.Object, "status", "planHash") == "" || !truth(pv.Object, "spec", "sourceFenced") {
		return fail("current completed PVMigration required")
	}
	if str(pv.Object, "spec", "sourceCluster") != s.SourceCluster || str(pv.Object, "spec", "targetCluster") != s.TargetCluster ||
		str(pv.Object, "spec", "resourceBinding") != rb.GetName() {
		return fail("PV migration route mismatch")
	}
	md := object(api.GroupVersion.String(), "PVMetadata")
	mdName := str(pv.Object, "spec", "metadataRef")
	if mdName == "" {
		return fail("PVMetadata reference required")
	}
	if err := reader.Get(ctx, client.ObjectKey{Namespace: req.Namespace, Name: mdName}, md); err != nil {
		return err
	}
	if !md.GetDeletionTimestamp().IsZero() || str(md.Object, "spec", "sourceCluster") != s.SourceCluster ||
		str(md.Object, "spec", "workloadRef", "name") != sts.Name || str(md.Object, "spec", "workloadRef", "uid") != string(sts.UID) {
		return fail("PVMetadata workload identity mismatch")
	}
	expectedClaims := map[string]bool{}
	for _, t := range sts.Spec.VolumeClaimTemplates {
		for _, p := range s.Pods {
			expectedClaims[t.Name+"-"+p.SourcePod] = true
		}
	}
	if len(expectedClaims) == 0 {
		return fail("automatic PV gate requires volumeClaimTemplates")
	}
	volumes, _, err := unstructured.NestedSlice(pv.Object, "spec", "volumes")
	if err != nil || len(volumes) != len(expectedClaims) {
		return fail("PV mapping must cover every restoring claim")
	}
	for _, raw := range volumes {
		v, ok := raw.(map[string]interface{})
		if !ok {
			return fail("invalid PV mapping")
		}
		source, target := str(v, "sourcePVC"), str(v, "targetPVC")
		if !expectedClaims[source] || target != source {
			return fail("PV mapping must preserve all current ordinal claims")
		}
		delete(expectedClaims, source)
	}
	works, _, err := unstructured.NestedSlice(pv.Object, "status", "works")
	if err != nil || len(works) != len(volumes) {
		return fail("incomplete PV Work evidence")
	}
	seen := map[string]bool{}
	for _, raw := range works {
		w, ok := raw.(map[string]interface{})
		if !ok {
			return fail("invalid PV Work")
		}
		name, ns := str(w, "name"), str(w, "namespace")
		key := ns + "/" + name
		if name == "" || ns != "karmada-es-"+s.TargetCluster || seen[key] || !truth(w, "applied") || !truth(w, "detached") {
			return fail("PV Work not applied and detached")
		}
		seen[key] = true
	}
	return nil
}
