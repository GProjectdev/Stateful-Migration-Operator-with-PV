package member

import (
	"context"
	"encoding/json"
	"fmt"
	api "github.com/GProjectdev/Stateful-Migration-Operator-with-PV/api/v1alpha1"
	"github.com/GProjectdev/Stateful-Migration-Operator-with-PV/internal/groupcontract"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"time"
)

// Group fencing is driven by current source identities, never historical archive
// Pod UIDs. An API-deleted Pod on an unreachable node is not proof of fencing.
func (r *Reconciler) fenceGroup(ctx context.Context, plan *api.RestorePlan) (bool, []api.SourcePodFenceStatus, error) {
	g := plan.Spec.GroupRestore
	if err := r.recordGroupVolume(ctx, plan); err != nil {
		return false, nil, err
	}
	var all corev1.PodList
	if err := r.Reader.List(ctx, &all, client.InNamespace(plan.Namespace)); err != nil {
		return false, nil, err
	}
	current := map[string]api.GroupSourcePod{}
	for _, p := range g.SourcePods {
		current[p.PodName] = p
	}
	for _, p := range all.Items {
		owner := metav1.GetControllerOf(&p)
		if owner == nil || owner.Kind != "StatefulSet" || owner.Name != plan.Spec.WorkloadRef.Name {
			continue
		}
		source, ok := current[p.Name]
		if !ok {
			return false, nil, fmt.Errorf("unexpected source world Pod %s; refuse incomplete fencing", p.Name)
		}
		if string(p.UID) != source.PodUID && p.Annotations[PlanUIDAnnotation] != string(plan.UID) {
			return false, nil, fmt.Errorf("current source Pod UID changed: %s", p.Name)
		}
	}
	result := make([]api.SourcePodFenceStatus, 0, len(g.SourcePods))
	complete := true
	for _, source := range g.SourcePods {
		f := api.SourcePodFenceStatus{PodName: source.PodName, SourcePodUID: source.PodUID, ObservedGeneration: plan.Generation, Phase: "Pending"}
		for _, old := range plan.Status.SourceFences {
			if old.PodName == source.PodName && old.SourcePodUID == source.PodUID && old.ObservedGeneration == plan.Generation {
				f = old
			}
		}
		if f.Phase == "SourceGone" {
			result = append(result, f)
			continue
		}
		if f.DeleteRequestedAt == nil {
			var original corev1.Pod
			err := r.Reader.Get(ctx, client.ObjectKey{Namespace: plan.Namespace, Name: source.PodName}, &original)
			if apierrors.IsNotFound(err) {
				fenced, e := r.groupNodeFenced(ctx, plan, source)
				if e != nil {
					return false, nil, e
				}
				if !fenced {
					return false, nil, fmt.Errorf("missing source UID requires positive provider fencing receipt")
				}
			} else if err != nil {
				return false, nil, err
			} else if string(original.UID) != source.PodUID {
				return false, nil, fmt.Errorf("source UID changed before delete intent")
			}
			now := metav1.Now()
			f.DeleteRequestedAt = &now
			f.Phase = "DeleteRequested"
			f.Message = "persisting group UID-bound deletion intent"
			result = append(result, f)
			complete = false
			continue
		}
		providerFenced, err := r.groupNodeFenced(ctx, plan, source)
		if err != nil {
			return false, nil, err
		}
		var pod corev1.Pod
		err = r.Reader.Get(ctx, client.ObjectKey{Namespace: plan.Namespace, Name: source.PodName}, &pod)
		if err != nil && !apierrors.IsNotFound(err) {
			return false, nil, err
		}
		if err == nil && string(pod.UID) != source.PodUID {
			return false, nil, fmt.Errorf("source Pod identity changed before fence completed")
		}
		if !providerFenced {
			var node corev1.Node
			if e := r.Reader.Get(ctx, client.ObjectKey{Name: source.NodeName}, &node); e != nil {
				return false, nil, fmt.Errorf("source node unavailable; provider fencing receipt required: %w", e)
			}
			ready := false
			for _, cond := range node.Status.Conditions {
				if cond.Type == corev1.NodeReady && cond.Status == corev1.ConditionTrue {
					age := time.Since(cond.LastHeartbeatTime.Time)
					ready = age >= 0 && age < 2*time.Minute
					if !ready {
						var lease coordinationv1.Lease
						if err := r.Reader.Get(ctx, client.ObjectKey{Namespace: "kube-node-lease", Name: node.Name}, &lease); err == nil && lease.Spec.RenewTime != nil {
							age = time.Since(lease.Spec.RenewTime.Time)
							ready = age >= 0 && age < 90*time.Second
						}
					}
				}
			}
			if !ready {
				return false, nil, fmt.Errorf("source node not freshly Ready; provider fencing receipt required")
			}
		}
		if apierrors.IsNotFound(err) {
			now := metav1.Now()
			f.GoneObservedAt = &now
			f.Phase = "SourceGone"
			f.Message = "current source UID absent after persisted delete intent and node liveness/provider fencing check"
		} else {
			owner := metav1.GetControllerOf(&pod)
			if pod.Spec.NodeName != source.NodeName || owner == nil || owner.Kind != "StatefulSet" || owner.Name != plan.Spec.WorkloadRef.Name || (pod.Labels[WorkloadUIDLabel] != g.SourceWorldUID && string(owner.UID) != g.SourceWorldUID) {
				return false, nil, fmt.Errorf("source Pod ownership/node mismatch")
			}
			grace := int64(30)
			if providerFenced {
				grace = 0
			}
			uid := types.UID(source.PodUID)
			if pod.DeletionTimestamp.IsZero() || providerFenced {
				if e := r.Client.Delete(ctx, &pod, client.GracePeriodSeconds(grace), client.Preconditions{UID: &uid}); e != nil && !apierrors.IsNotFound(e) {
					return false, nil, e
				}
			}
			f.Phase = "DeleteRequested"
			complete = false
		}
		result = append(result, f)
	}
	return complete, result, nil
}

func (r *Reconciler) groupNodeFenced(ctx context.Context, plan *api.RestorePlan, source api.GroupSourcePod) (bool, error) {
	ref := source.NodeProvisionRef
	if ref == nil {
		return false, nil
	}
	np := &unstructured.Unstructured{}
	np.SetGroupVersionKind(schema.GroupVersionKind{Group: "ml.dcn.ssu.ac.kr", Version: "v1alpha1", Kind: "NodeProvision"})
	if err := r.Reader.Get(ctx, client.ObjectKey{Namespace: plan.Namespace, Name: ref.Name}, np); err != nil {
		return false, err
	}
	// A propagated object's management UID is not its member identity.
	if string(np.GetUID()) != ref.UID {
		return false, fmt.Errorf("source NodeProvision member UID mismatch")
	}
	s, _, _ := unstructured.NestedMap(np.Object, "spec", "fence")
	f, _, _ := unstructured.NestedMap(np.Object, "status", "fence")
	instance, _, _ := unstructured.NestedString(np.Object, "status", "instanceId")
	node, _, _ := unstructured.NestedString(np.Object, "status", "nodeName")
	if instance != ref.InstanceID || node != source.NodeName {
		return false, fmt.Errorf("source NodeProvision instance/node mismatch")
	}
	if f["phase"] != "Fenced" {
		return false, nil
	}
	gen, _, _ := unstructured.NestedInt64(f, "observedGeneration")
	if gen != np.GetGeneration() || f["operationUID"] != plan.Spec.GroupRestore.OperationUID || f["instanceID"] != ref.InstanceID || s["operationUID"] != f["operationUID"] || s["instanceID"] != f["instanceID"] {
		return false, fmt.Errorf("provider fence receipt is stale or belongs to another operation")
	}
	observed, _, _ := unstructured.NestedString(f, "observedAt")
	if _, err := time.Parse(time.RFC3339, observed); err != nil {
		return false, fmt.Errorf("provider fence receipt has no observation time")
	}
	return true, nil
}

func groupPrepared(plan *api.RestorePlan) bool {
	s := plan.Status.GroupControl
	return s != nil && s.OperationUID == plan.Spec.GroupRestore.OperationUID && s.CheckpointID == plan.Spec.CheckpointRef.CheckpointID && s.CheckpointGeneration > 0 && s.PrepareJobUID != "" && s.PreparedAt != nil && plan.Status.ObservedGeneration == plan.Generation
}
func groupSourceReceipt(plan *api.RestorePlan) error {
	if plan.Spec.SourceCluster == plan.Spec.TargetCluster {
		return groupcontract.ValidateFences(plan, plan.Status.SourceFences)
	}
	_, err := groupcontract.Decode(plan)
	return err
}
func (r *Reconciler) prepareGroup(ctx context.Context, plan *api.RestorePlan) (bool, error) {
	if err := groupSourceReceipt(plan); err != nil {
		return false, err
	}
	if err := r.recordGroupVolume(ctx, plan); err != nil {
		return false, err
	}
	if plan.Spec.SourceCluster != plan.Spec.TargetCluster {
		var receipt groupcontract.FenceReceipt
		if err := json.Unmarshal([]byte(plan.Annotations[groupcontract.FenceAnnotation]), &receipt); err != nil {
			return false, err
		}
		if receipt.VolumeServer != plan.Status.GroupControl.VolumeServer || receipt.VolumePath != plan.Status.GroupControl.VolumePath {
			return false, fmt.Errorf("target shared PVC NFS backing differs from fenced source")
		}
	}
	if r.GroupControlImage == "" {
		return false, fmt.Errorf("--group-control-image is required")
	}
	result, uid, done, err := r.groupJob(ctx, plan, "prepare")
	if err != nil || !done {
		return false, err
	}
	if plan.Status.GroupControl == nil {
		plan.Status.GroupControl = &api.GroupControlStatus{}
	}
	s := plan.Status.GroupControl
	if s.CheckpointGeneration != 0 && s.CheckpointGeneration != result.Generation {
		return false, fmt.Errorf("prepared generation changed")
	}
	s.OperationUID = plan.Spec.GroupRestore.OperationUID
	s.CheckpointID = plan.Spec.CheckpointRef.CheckpointID
	s.CheckpointGeneration = result.Generation
	s.PrepareJobUID = uid
	if s.PreparedAt == nil {
		now := metav1.Now()
		s.PreparedAt = &now
	}
	return true, nil
}
func (r *Reconciler) resumeGroup(ctx context.Context, plan *api.RestorePlan) (bool, error) {
	if !groupPrepared(plan) {
		return false, fmt.Errorf("current group prepare receipt required")
	}
	result, uid, done, err := r.groupJob(ctx, plan, "resume")
	if err != nil || !done {
		return false, err
	}
	if result.Generation != plan.Status.GroupControl.CheckpointGeneration {
		return false, fmt.Errorf("resume generation mismatch")
	}
	plan.Status.GroupControl.ResumeJobUID = uid
	if plan.Status.GroupControl.ResumedAt == nil {
		now := metav1.Now()
		plan.Status.GroupControl.ResumedAt = &now
	}
	return true, nil
}
func groupPVCMatches(plan *api.RestorePlan, pod *corev1.Pod) error {
	g := plan.Spec.GroupRestore
	for _, c := range pod.Spec.Containers {
		if c.Name != plan.Spec.Pods[0].Archives[0].ContainerName {
			continue
		}
		for _, m := range c.VolumeMounts {
			if m.MountPath != g.CheckpointRoot {
				continue
			}
			for _, v := range pod.Spec.Volumes {
				if v.Name == m.Name && v.PersistentVolumeClaim != nil && v.PersistentVolumeClaim.ClaimName == g.SharedPVC && !v.PersistentVolumeClaim.ReadOnly && !m.ReadOnly && m.SubPath == "" && m.SubPathExpr == "" {
					return nil
				}
			}
		}
	}
	return fmt.Errorf("group target must mount the complete shared PVC at checkpointRoot")
}
func (r *Reconciler) recordGroupVolume(ctx context.Context, plan *api.RestorePlan) error {
	var pvc corev1.PersistentVolumeClaim
	if err := r.Reader.Get(ctx, client.ObjectKey{Namespace: plan.Namespace, Name: plan.Spec.GroupRestore.SharedPVC}, &pvc); err != nil {
		return err
	}
	if pvc.Status.Phase != corev1.ClaimBound || pvc.Spec.VolumeName == "" || !pvc.DeletionTimestamp.IsZero() {
		return fmt.Errorf("group PVC not Bound")
	}
	var pv corev1.PersistentVolume
	if err := r.Reader.Get(ctx, client.ObjectKey{Name: pvc.Spec.VolumeName}, &pv); err != nil {
		return err
	}
	if pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.UID != pvc.UID || pv.Spec.ClaimRef.Namespace != pvc.Namespace || pv.Spec.ClaimRef.Name != pvc.Name || pv.Spec.NFS == nil || pv.Spec.NFS.Server == "" || pv.Spec.NFS.Path == "" || pv.Spec.NFS.ReadOnly || !pv.DeletionTimestamp.IsZero() {
		return fmt.Errorf("group requires UID-bound writable NFS PV")
	}
	if plan.Status.GroupControl == nil {
		plan.Status.GroupControl = &api.GroupControlStatus{OperationUID: plan.Spec.GroupRestore.OperationUID, CheckpointID: plan.Spec.CheckpointRef.CheckpointID}
	}
	s := plan.Status.GroupControl
	if s.VolumePVCUID != "" && (s.VolumePVCUID != string(pvc.UID) || s.VolumePVUID != string(pv.UID) || s.VolumeServer != pv.Spec.NFS.Server || s.VolumePath != pv.Spec.NFS.Path) {
		return fmt.Errorf("group PVC/PV identity changed during operation")
	}
	s.VolumePVCUID = string(pvc.UID)
	s.VolumePVUID = string(pv.UID)
	s.VolumeServer = pv.Spec.NFS.Server
	s.VolumePath = pv.Spec.NFS.Path
	return nil
}
