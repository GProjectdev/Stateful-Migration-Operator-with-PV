package checkpoint

import (
	"context"
	"fmt"
	fluidcrv1alpha1 "github.com/GProjectdev/Stateful-Migration-Operator-with-PV/api/fluidcr/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (r *FluidCRMigrationReconciler) validateTarget(ctx context.Context, t target) error {
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	var pod corev1.Pod
	if err := reader.Get(ctx, client.ObjectKey{Namespace: t.namespace, Name: t.podName}, &pod); err != nil {
		return err
	}
	if t.podUID == "" || pod.UID != t.podUID || !isEligiblePod(&pod) || pod.Status.PodIP != t.podIP || (t.hostIP != "" && pod.Status.HostIP != t.hostIP) {
		return fmt.Errorf("pod %s identity or endpoint changed", t.podName)
	}
	return nil
}

func (r *FluidCRMigrationReconciler) ownedByWorkload(ctx context.Context, pod *corev1.Pod, m *fluidcrv1alpha1.FluidCRMigration) (bool, error) {
	ref := m.Spec.WorkloadRef
	key := client.ObjectKey{Namespace: workloadNamespace(m), Name: ref.Name}
	owner := metav1.GetControllerOf(pod)
	if owner == nil || owner.UID == "" {
		return false, nil
	}
	var workload client.Object
	switch ref.Kind {
	case "StatefulSet":
		workload = &appsv1.StatefulSet{}
	case "Job":
		workload = &batchv1.Job{}
	case "Deployment":
		if owner.Kind != "ReplicaSet" || owner.APIVersion != "apps/v1" {
			return false, nil
		}
		var rs appsv1.ReplicaSet
		if err := r.Get(ctx, client.ObjectKey{Namespace: pod.Namespace, Name: owner.Name}, &rs); err != nil {
			return false, err
		}
		if rs.UID != owner.UID {
			return false, nil
		}
		owner = metav1.GetControllerOf(&rs)
		if owner == nil {
			return false, nil
		}
		workload = &appsv1.Deployment{}
	default:
		return false, fmt.Errorf("unsupported workload kind %q", ref.Kind)
	}
	if err := r.Get(ctx, key, workload); err != nil {
		return false, err
	}
	return workload.GetUID() != "" && owner.UID == workload.GetUID() && owner.Name == ref.Name && owner.Kind == ref.Kind && owner.APIVersion == ref.APIVersion, nil
}
