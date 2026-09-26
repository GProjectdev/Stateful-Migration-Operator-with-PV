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
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	fluidcrv1alpha1 "github.com/GProjectdev/Stateful-Migration-Operator-with-PV/api/fluidcr/v1alpha1"
)

// fakeCtrl is an in-memory CtrlAPI that records calls and can inject errors per pod IP.
type fakeCtrl struct {
	mu              sync.Mutex
	checkpointCalls []string
	checkpointIDs   []string
	resumeCalls     []string
	checkpointErr   map[string]error
}

func (f *fakeCtrl) Checkpoint(_ context.Context, podIP string, _ int, _ time.Duration, checkpointID string) (map[string]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.checkpointCalls = append(f.checkpointCalls, podIP)
	f.checkpointIDs = append(f.checkpointIDs, checkpointID)
	if err := f.checkpointErr[podIP]; err != nil {
		return nil, err
	}
	return map[string]string{"1234": "checkpoint-ready"}, nil
}

func (f *fakeCtrl) Resume(_ context.Context, podIP string, _ int, _ time.Duration) (map[string]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.resumeCalls = append(f.resumeCalls, podIP)
	return map[string]string{"lock": "lock-removed"}, nil
}

func (f *fakeCtrl) counts() (checkpoint, resume int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.checkpointCalls), len(f.resumeCalls)
}

// fakeKubelet is an in-memory KubeletAPI that records calls and can inject errors per pod name.
type fakeKubelet struct {
	mu    sync.Mutex
	calls []string
	err   map[string]error
}

func (f *fakeKubelet) Checkpoint(_ context.Context, _, namespace, pod, container string, _ time.Duration) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, pod)
	if err := f.err[pod]; err != nil {
		return "", err
	}
	return fmt.Sprintf("%s/checkpoint-%s_%s-%s-20260623.tar", "/var/lib/kubelet/checkpoints", namespace, pod, container), nil
}

func (f *fakeKubelet) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := fluidcrv1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("add fluidcr scheme: %v", err)
	}
	return s
}

func newTestPod(name, podIP, hostIP string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			UID:             types.UID("uid-" + name),
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(newTestReplicaSet("default"), appsv1.SchemeGroupVersion.WithKind("ReplicaSet"))},
			Namespace:       "default",
			Labels:          map[string]string{"app": "trainer"},
			Annotations:     map[string]string{AnnotationInjected: "true"},
		},
		Spec: corev1.PodSpec{
			NodeName: "node-" + name,
			Containers: []corev1.Container{{
				Name: "trainer",
				Env:  []corev1.EnvVar{{Name: EnvCtrlPort, Value: "8298"}},
			}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: podIP, HostIP: hostIP},
	}
}

func newTestDeployment() *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "trainer", Namespace: "default", UID: "deployment-uid"},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "trainer"}},
		},
	}
}

func newTestMigration() *fluidcrv1alpha1.FluidCRMigration {
	return &fluidcrv1alpha1.FluidCRMigration{
		ObjectMeta: metav1.ObjectMeta{Name: "mig", Namespace: "default", Generation: 1, Annotations: map[string]string{AnnotationCheckpointID: "mig-round-001"}},
		Spec: fluidcrv1alpha1.FluidCRMigrationSpec{
			WorkloadRef: fluidcrv1alpha1.WorkloadReference{
				APIVersion: "apps/v1", Kind: "Deployment", Name: "trainer",
			},
		},
	}
}

func newTestReplicaSet(namespace string) *appsv1.ReplicaSet {
	return &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{
		Name: "trainer-rs", Namespace: namespace, UID: "rs-uid",
		OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(newTestDeployment(), appsv1.SchemeGroupVersion.WithKind("Deployment"))},
	}}
}

// reconcileToCompletion drives Reconcile until the migration reaches a terminal phase.
func reconcileToCompletion(t *testing.T, r *FluidCRMigrationReconciler, c client.Client) fluidcrv1alpha1.FluidCRMigration {
	t.Helper()
	ctx := context.Background()
	req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "mig"}}
	var got fluidcrv1alpha1.FluidCRMigration
	for i := 0; i < 8; i++ {
		if _, err := r.Reconcile(ctx, req); err != nil {
			t.Fatalf("reconcile iteration %d: %v", i, err)
		}
		if err := c.Get(ctx, req.NamespacedName, &got); err != nil {
			t.Fatalf("get migration: %v", err)
		}
		if isTerminalPhase(got.Status.Phase) {
			return got
		}
	}
	t.Fatalf("migration did not reach a terminal phase, last phase=%q", got.Status.Phase)
	return got
}

func TestReconcile_HappyPath(t *testing.T) {
	s := newTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(newTestDeployment(), newTestPod("p0", "10.0.0.1", "192.168.0.1"), newTestPod("p1", "10.0.0.2", "192.168.0.2"), newTestMigration()).
		WithObjects(newTestReplicaSet("default"), newTestReplicaSet("other")).
		WithStatusSubresource(&fluidcrv1alpha1.FluidCRMigration{}).
		Build()
	fc := &fakeCtrl{}
	fk := &fakeKubelet{}
	r := &FluidCRMigrationReconciler{Client: c, Scheme: s, CtrlClient: fc, KubeletClient: fk}

	got := reconcileToCompletion(t, r, c)

	if got.Status.Phase != fluidcrv1alpha1.PhaseCompleted {
		t.Fatalf("phase = %q, want Completed (message: %s)", got.Status.Phase, got.Status.Message)
	}
	if len(got.Status.Pods) != 2 {
		t.Fatalf("got %d pod statuses, want 2", len(got.Status.Pods))
	}
	for _, ps := range got.Status.Pods {
		if ps.Phase != fluidcrv1alpha1.PodPhaseResumed {
			t.Errorf("pod %s phase = %q, want Resumed", ps.PodName, ps.Phase)
		}
		if len(ps.CheckpointFiles) != 1 || ps.CheckpointFiles[0].FilePath == "" {
			t.Errorf("pod %s checkpoint files = %+v, want one non-empty file", ps.PodName, ps.CheckpointFiles)
		}
	}
	if cp, rs := fc.counts(); cp != 2 || rs != 2 {
		t.Errorf("ctrl calls: checkpoint=%d resume=%d, want 2 and 2", cp, rs)
	}
	for _, id := range fc.checkpointIDs {
		if id != "mig-round-001" {
			t.Errorf("checkpointID = %q, want mig-round-001", id)
		}
	}
	if n := fk.count(); n != 2 {
		t.Errorf("kubelet checkpoint calls = %d, want 2", n)
	}
}

func TestReconcile_ResumeWithoutResumeFlag(t *testing.T) {
	s := newTestScheme(t)
	mig := newTestMigration()
	noResume := false
	mig.Spec.Resume = &noResume
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(newTestDeployment(), newTestPod("p0", "10.0.0.1", "192.168.0.1"), mig).
		WithObjects(newTestReplicaSet("default"), newTestReplicaSet("other")).
		WithStatusSubresource(&fluidcrv1alpha1.FluidCRMigration{}).
		Build()
	fc := &fakeCtrl{}
	fk := &fakeKubelet{}
	r := &FluidCRMigrationReconciler{Client: c, Scheme: s, CtrlClient: fc, KubeletClient: fk}

	got := reconcileToCompletion(t, r, c)

	if got.Status.Phase != fluidcrv1alpha1.PhaseCompleted {
		t.Fatalf("phase = %q, want Completed", got.Status.Phase)
	}
	if _, rs := fc.counts(); rs != 0 {
		t.Errorf("resume calls = %d, want 0 when resume disabled", rs)
	}
	if got.Status.Pods[0].Phase != fluidcrv1alpha1.PodPhaseContainerCheckpointed {
		t.Errorf("pod phase = %q, want ContainerCheckpointed", got.Status.Pods[0].Phase)
	}
}

// TestReconcile_ResumeOnContainerFailure verifies the safeguard: when a CRIU
// container checkpoint fails for a pod whose application checkpoint succeeded,
// the controller still resumes it so the workload is not left paused.
func TestReconcile_ResumeOnContainerFailure(t *testing.T) {
	s := newTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(newTestDeployment(), newTestPod("p0", "10.0.0.1", "192.168.0.1"), newTestPod("p1", "10.0.0.2", "192.168.0.2"), newTestMigration()).
		WithObjects(newTestReplicaSet("default"), newTestReplicaSet("other")).
		WithStatusSubresource(&fluidcrv1alpha1.FluidCRMigration{}).
		Build()
	fc := &fakeCtrl{}
	fk := &fakeKubelet{err: map[string]error{"p0": fmt.Errorf("kubelet boom")}}
	r := &FluidCRMigrationReconciler{Client: c, Scheme: s, CtrlClient: fc, KubeletClient: fk}

	got := reconcileToCompletion(t, r, c)

	if got.Status.Phase != fluidcrv1alpha1.PhaseFailed {
		t.Fatalf("phase = %q, want Failed", got.Status.Phase)
	}
	// Both pods completed the application checkpoint, so both must be resumed.
	if _, rs := fc.counts(); rs != 2 {
		t.Errorf("resume calls = %d, want 2 (safeguard resumes both app-checkpointed pods)", rs)
	}
	p0 := podStatusByName(got.Status.Pods, "p0")
	if p0 == nil || p0.Phase != fluidcrv1alpha1.PodPhaseFailed {
		t.Errorf("p0 phase = %v, want Failed", p0)
	}
}

func TestReconcile_AppCheckpointFailureSkipsContainerCheckpoint(t *testing.T) {
	s := newTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(newTestDeployment(), newTestPod("p0", "10.0.0.1", "192.168.0.1"), newTestMigration()).
		WithObjects(newTestReplicaSet("default"), newTestReplicaSet("other")).
		WithStatusSubresource(&fluidcrv1alpha1.FluidCRMigration{}).
		Build()
	fc := &fakeCtrl{checkpointErr: map[string]error{"10.0.0.1": fmt.Errorf("ctrl boom")}}
	fk := &fakeKubelet{}
	r := &FluidCRMigrationReconciler{Client: c, Scheme: s, CtrlClient: fc, KubeletClient: fk}

	got := reconcileToCompletion(t, r, c)

	if got.Status.Phase != fluidcrv1alpha1.PhaseFailed {
		t.Fatalf("phase = %q, want Failed", got.Status.Phase)
	}
	if n := fk.count(); n != 0 {
		t.Errorf("kubelet checkpoint calls = %d, want 0 (skipped after app checkpoint failure)", n)
	}
	// The pod never paused (app checkpoint failed), so it must not be resumed.
	if _, rs := fc.counts(); rs != 0 {
		t.Errorf("resume calls = %d, want 0 (pod never paused)", rs)
	}
}

func TestReconcile_WaitsWhenNoPods(t *testing.T) {
	s := newTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(newTestDeployment(), newTestMigration()).
		WithObjects(newTestReplicaSet("default"), newTestReplicaSet("other")).
		WithStatusSubresource(&fluidcrv1alpha1.FluidCRMigration{}).
		Build()
	r := &FluidCRMigrationReconciler{Client: c, Scheme: s, CtrlClient: &fakeCtrl{}, KubeletClient: &fakeKubelet{}}

	ctx := context.Background()
	req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "mig"}}
	// First reconcile adds the finalizer; second runs the workflow.
	for i := 0; i < 2; i++ {
		res, err := r.Reconcile(ctx, req)
		if err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		_ = res
	}
	var got fluidcrv1alpha1.FluidCRMigration
	if err := c.Get(ctx, req.NamespacedName, &got); err != nil {
		t.Fatalf("get migration: %v", err)
	}
	if got.Status.Phase != fluidcrv1alpha1.PhasePending {
		t.Errorf("phase = %q, want Pending (waiting for pods)", got.Status.Phase)
	}
}

// TestReconcile_PodWorkloadRef verifies that a Pod-kind workloadRef targets
// that single pod directly, with no selector resolution.
func TestReconcile_PodWorkloadRef(t *testing.T) {
	s := newTestScheme(t)
	mig := newTestMigration()
	mig.Spec.WorkloadRef = fluidcrv1alpha1.WorkloadReference{
		APIVersion: "v1", Kind: "Pod", Name: "p0",
	}
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(newTestPod("p0", "10.0.0.1", "192.168.0.1"), mig).
		WithObjects(newTestReplicaSet("default"), newTestReplicaSet("other")).
		WithStatusSubresource(&fluidcrv1alpha1.FluidCRMigration{}).
		Build()
	fc := &fakeCtrl{}
	fk := &fakeKubelet{}
	r := &FluidCRMigrationReconciler{Client: c, Scheme: s, CtrlClient: fc, KubeletClient: fk}

	got := reconcileToCompletion(t, r, c)

	if got.Status.Phase != fluidcrv1alpha1.PhaseCompleted {
		t.Fatalf("phase = %q, want Completed (message: %s)", got.Status.Phase, got.Status.Message)
	}
	if len(got.Status.Pods) != 1 || got.Status.Pods[0].PodName != "p0" {
		t.Fatalf("pod statuses = %+v, want single p0", got.Status.Pods)
	}
	if cp, rs := fc.counts(); cp != 1 || rs != 1 {
		t.Errorf("ctrl calls: checkpoint=%d resume=%d, want 1 and 1", cp, rs)
	}
}

// TestReconcile_WorkloadRefNamespace verifies that workloadRef.namespace
// overrides the FluidCRMigration's own namespace when resolving the workload.
func TestReconcile_WorkloadRefNamespace(t *testing.T) {
	s := newTestScheme(t)
	dep := newTestDeployment()
	dep.Namespace = "other"
	pod := newTestPod("p0", "10.0.0.1", "192.168.0.1")
	pod.Namespace = "other"
	mig := newTestMigration()
	mig.Spec.WorkloadRef.Namespace = "other"
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(dep, pod, mig).
		WithObjects(newTestReplicaSet("default"), newTestReplicaSet("other")).
		WithStatusSubresource(&fluidcrv1alpha1.FluidCRMigration{}).
		Build()
	fc := &fakeCtrl{}
	fk := &fakeKubelet{}
	r := &FluidCRMigrationReconciler{Client: c, Scheme: s, CtrlClient: fc, KubeletClient: fk}

	got := reconcileToCompletion(t, r, c)

	if got.Status.Phase != fluidcrv1alpha1.PhaseCompleted {
		t.Fatalf("phase = %q, want Completed (message: %s)", got.Status.Phase, got.Status.Message)
	}
	if len(got.Status.Pods) != 1 {
		t.Fatalf("got %d pod statuses, want 1 (pod in 'other' namespace)", len(got.Status.Pods))
	}
}

func podStatusByName(pods []fluidcrv1alpha1.PodMigrationStatus, name string) *fluidcrv1alpha1.PodMigrationStatus {
	for i := range pods {
		if pods[i].PodName == name {
			return &pods[i]
		}
	}
	return nil
}
