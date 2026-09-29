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
	"github.com/GProjectdev/Stateful-Migration-Operator-with-PV/internal/ctrlapi"
)

// fakeCtrl is an in-memory CtrlAPI that records calls and can inject errors per pod IP.
type fakeCtrl struct {
	readinessBlocked        bool
	mu                      sync.Mutex
	checkpointCalls         []string
	partialCalls            []string
	checkpointIDs           []string
	resumeCalls             []string
	resumeOwnedCalls        []string
	resumeOwnedCheckpointID string
	resumeOwnedGeneration   int64
	checkpointErr           map[string]error
	resumeOwnedErr          map[string]error
	runtime                 map[string]ctrlapi.RuntimeStatus
	runtimeSequence         map[string][]ctrlapi.RuntimeStatus
	partialResult           string
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

func (f *fakeCtrl) CheckpointRanks(_ context.Context, podIP string, _ int, _ time.Duration, checkpointID string, _ []int64) (map[string]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.partialCalls = append(f.partialCalls, podIP)
	f.checkpointIDs = append(f.checkpointIDs, checkpointID)
	if err := f.checkpointErr[podIP]; err != nil {
		return nil, err
	}
	if f.partialResult != "" {
		return map[string]string{"1234": f.partialResult}, nil
	}
	return map[string]string{"1234": "checkpoint-ready"}, nil
}

func (f *fakeCtrl) Runtime(_ context.Context, podIP string, _ int, timeout time.Duration) (ctrlapi.RuntimeStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if timeout == 3*time.Second {
		return ctrlapi.RuntimeStatus{State: "Running", CheckpointReady: !f.readinessBlocked}, nil
	}
	if statuses := f.runtimeSequence[podIP]; len(statuses) > 0 {
		status := statuses[0]
		f.runtimeSequence[podIP] = statuses[1:]
		return status, nil
	}
	if f.runtime != nil {
		if status, ok := f.runtime[podIP]; ok {
			return status, nil
		}
	}
	return ctrlapi.RuntimeStatus{}, fmt.Errorf("runtime status missing for %s", podIP)
}

func (f *fakeCtrl) Resume(_ context.Context, podIP string, _ int, _ time.Duration) (map[string]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.resumeCalls = append(f.resumeCalls, podIP)
	return map[string]string{"lock": "lock-removed"}, nil
}

func (f *fakeCtrl) ResumeOwned(_ context.Context, podIP string, _ int, _ time.Duration, checkpointID string, generation int64) (map[string]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.resumeOwnedCalls = append(f.resumeOwnedCalls, podIP)
	f.resumeOwnedCheckpointID = checkpointID
	f.resumeOwnedGeneration = generation
	if err := f.resumeOwnedErr[podIP]; err != nil {
		return nil, err
	}
	return map[string]string{"lock": "removed"}, nil
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

func newTestStatefulSet() *appsv1.StatefulSet {
	replicas := int32(2)
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "trainer", Namespace: "default", UID: "statefulset-uid", Generation: 1},
		Spec:       appsv1.StatefulSetSpec{Replicas: &replicas, Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "trainer"}}},
		Status:     appsv1.StatefulSetStatus{ObservedGeneration: 1, Replicas: 2, CurrentReplicas: 2, ReadyReplicas: 2, AvailableReplicas: 2, UpdatedReplicas: 2, CurrentRevision: "rev", UpdateRevision: "rev"},
	}
}

func newStatefulPod(name, podIP, hostIP string) *corev1.Pod {
	p := newTestPod(name, podIP, hostIP)
	p.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(newTestStatefulSet(), appsv1.SchemeGroupVersion.WithKind("StatefulSet"))}
	return p
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
		if ps.CheckpointID != "mig-round-001" {
			t.Errorf("pod %s checkpointID = %q, want mig-round-001", ps.PodName, ps.CheckpointID)
		}
		if len(ps.CheckpointFiles) != 1 || ps.CheckpointFiles[0].FilePath == "" {
			t.Errorf("pod %s checkpoint files = %+v, want one non-empty file", ps.PodName, ps.CheckpointFiles)
		} else if ps.CheckpointFiles[0].CheckpointID != "mig-round-001" {
			t.Errorf("pod %s archive checkpointID = %q, want mig-round-001", ps.PodName, ps.CheckpointFiles[0].CheckpointID)
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

func TestReconcile_PartialCheckpointTargetsOnlySelectedRank(t *testing.T) {
	s := newTestScheme(t)
	mig := newTestMigration()
	noResume := false
	mig.Spec.Resume = &noResume
	mig.Spec.WorkloadRef = fluidcrv1alpha1.WorkloadReference{APIVersion: "apps/v1", Kind: "StatefulSet", Name: "trainer"}
	mig.Spec.PartialCheckpoint = &fluidcrv1alpha1.PartialCheckpointSpec{TargetRanks: []int64{1}}
	fc := &fakeCtrl{runtime: map[string]ctrlapi.RuntimeStatus{"10.0.0.1": {Rank: 0, WorldSize: 2, CheckpointID: "mig-round-001", SurvivorEvidence: ctrlapi.SurvivorEvidence{Generation: 7, PauseLockPath: "/checkpoint/rank0/pause-lock", PauseLockPID: 1234, ObservedAt: "now"}}}}
	fk := &fakeKubelet{}
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(newTestStatefulSet(), newStatefulPod("trainer-0", "10.0.0.1", "192.168.0.1"), newStatefulPod("trainer-1", "10.0.0.2", "192.168.0.2"), mig).
		WithStatusSubresource(&fluidcrv1alpha1.FluidCRMigration{}).
		Build()
	r := &FluidCRMigrationReconciler{Client: c, Scheme: s, CtrlClient: fc, KubeletClient: fk}

	got := reconcileToCompletion(t, r, c)

	if got.Status.Phase != fluidcrv1alpha1.PhaseCompleted {
		t.Fatalf("phase = %q: %s", got.Status.Phase, got.Status.Message)
	}
	if len(fc.partialCalls) != 1 || fc.partialCalls[0] != "10.0.0.2" {
		t.Fatalf("partial calls = %v, want target rank pod", fc.partialCalls)
	}
	if n := fk.count(); n != 1 {
		t.Fatalf("kubelet checkpoint calls = %d, want target rank only", n)
	}
	if _, rs := fc.counts(); rs != 0 {
		t.Fatalf("resume calls = %d, want none for partial", rs)
	}
	survivor := podStatusByName(got.Status.Pods, "trainer-0")
	if survivor == nil || survivor.Phase != fluidcrv1alpha1.PodPhaseSurvivorPaused || survivor.SurvivorEvidence == nil {
		t.Fatalf("survivor status = %+v", survivor)
	}
	target := podStatusByName(got.Status.Pods, "trainer-1")
	if target == nil || target.Phase != fluidcrv1alpha1.PodPhaseContainerCheckpointed || len(target.CheckpointFiles) != 1 {
		t.Fatalf("target status = %+v", target)
	}
}

func TestPartialCheckpointConfirmsEveryTarget(t *testing.T) {
	fc := &fakeCtrl{}
	r := &FluidCRMigrationReconciler{CtrlClient: fc}
	targets := []target{{podName: "p0", podIP: "10.0.0.1", rank: 0}, {podName: "p1", podIP: "10.0.0.2", rank: 1}, {podName: "p2", podIP: "10.0.0.3", rank: 2}}
	out := r.partialAppCheckpoint(context.Background(), targets, time.Second, "round", []int64{0, 2})
	if len(fc.partialCalls) != 2 || fc.partialCalls[0] != "10.0.0.1" || fc.partialCalls[1] != "10.0.0.3" {
		t.Fatalf("calls: %v", fc.partialCalls)
	}
	for _, result := range out {
		if result.err != nil {
			t.Fatal(result.err)
		}
	}
	fc.checkpointErr = map[string]error{"10.0.0.3": context.DeadlineExceeded}
	out = r.partialAppCheckpoint(context.Background(), targets, time.Second, "round-next", []int64{0, 2})
	for _, result := range out {
		if result.err == nil {
			t.Fatal("later target timeout must reject the entire round")
		}
	}
}

func TestPartialCheckpointRejectsUnreadyTargetBeforeCRIU(t *testing.T) {
	for _, result := range []string{"checkpoint-signalled", "survivor-parked", "timeout-waiting-lock"} {
		t.Run(result, func(t *testing.T) {
			s := newTestScheme(t)
			mig := newTestMigration()
			noResume := false
			mig.Spec.Resume = &noResume
			mig.Spec.WorkloadRef = fluidcrv1alpha1.WorkloadReference{APIVersion: "apps/v1", Kind: "StatefulSet", Name: "trainer"}
			mig.Spec.PartialCheckpoint = &fluidcrv1alpha1.PartialCheckpointSpec{TargetRanks: []int64{1}}
			fc := &fakeCtrl{partialResult: result}
			fk := &fakeKubelet{}
			c := fake.NewClientBuilder().WithScheme(s).
				WithObjects(newTestStatefulSet(), newStatefulPod("trainer-0", "10.0.0.1", "192.168.0.1"), newStatefulPod("trainer-1", "10.0.0.2", "192.168.0.2"), mig).
				WithStatusSubresource(&fluidcrv1alpha1.FluidCRMigration{}).Build()
			r := &FluidCRMigrationReconciler{Client: c, Scheme: s, CtrlClient: fc, KubeletClient: fk}
			got := reconcileToCompletion(t, r, c)
			if got.Status.Phase != fluidcrv1alpha1.PhaseFailed || fk.count() != 0 {
				t.Fatalf("phase=%s CRIU calls=%d", got.Status.Phase, fk.count())
			}
		})
	}
}

func TestReconcile_PartialCheckpointFinalizesAfterPodsBecomeNotReady(t *testing.T) {
	s := newTestScheme(t)
	sts := newTestStatefulSet()
	sts.Status.ReadyReplicas = 0
	sts.Status.AvailableReplicas = 0

	mig := newTestMigration()
	noResume := false
	mig.Spec.Resume = &noResume
	mig.Spec.WorkloadRef = fluidcrv1alpha1.WorkloadReference{APIVersion: "apps/v1", Kind: "StatefulSet", Name: "trainer"}
	mig.Spec.PartialCheckpoint = &fluidcrv1alpha1.PartialCheckpointSpec{TargetRanks: []int64{0}}
	mig.Status = fluidcrv1alpha1.FluidCRMigrationStatus{
		Phase:              fluidcrv1alpha1.PhaseContainerCheckpointing,
		ObservedGeneration: 1,
		Pods: []fluidcrv1alpha1.PodMigrationStatus{
			{
				PodName:             "trainer-0",
				PodUID:              "uid-trainer-0",
				NodeName:            "node-trainer-0",
				PodIP:               "10.0.0.1",
				Phase:               fluidcrv1alpha1.PodPhaseContainerCheckpointed,
				AppCheckpointResult: "1 checkpoint-signalled",
				CheckpointID:        "mig-round-001",
				CheckpointFiles: []fluidcrv1alpha1.CheckpointFile{{
					CheckpointID:  "mig-round-001",
					ContainerName: "trainer",
					FilePath:      "/var/lib/kubelet/checkpoints/checkpoint-trainer-0.tar",
				}},
			},
			{
				PodName:             "trainer-1",
				PodUID:              "uid-trainer-1",
				NodeName:            "node-trainer-1",
				PodIP:               "10.0.0.2",
				Rank:                1,
				Phase:               fluidcrv1alpha1.PodPhaseSurvivorPaused,
				AppCheckpointResult: "1 checkpoint-signalled",
				SurvivorEvidence: &fluidcrv1alpha1.SurvivorEvidence{
					Generation:    7,
					PauseLockPath: "/checkpoint/trainer-1/pause-lock",
					PauseLockPID:  274,
					ObservedAt:    "now",
				},
			},
		},
	}

	fc := &fakeCtrl{}
	fk := &fakeKubelet{}
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(sts, newStatefulPod("trainer-0", "10.0.0.1", "192.168.0.1"), newStatefulPod("trainer-1", "10.0.0.2", "192.168.0.2"), mig).
		WithStatusSubresource(&fluidcrv1alpha1.FluidCRMigration{}).
		Build()
	r := &FluidCRMigrationReconciler{Client: c, Scheme: s, CtrlClient: fc, KubeletClient: fk}

	got := reconcileToCompletion(t, r, c)

	if got.Status.Phase != fluidcrv1alpha1.PhaseCompleted {
		t.Fatalf("phase = %q, want Completed (message: %s)", got.Status.Phase, got.Status.Message)
	}
	if cp, rs := fc.counts(); cp != 0 || rs != 0 || fk.count() != 0 {
		t.Fatalf("completed work was repeated: checkpoint=%d resume=%d kubelet=%d", cp, rs, fk.count())
	}
}

func TestReconcile_PartialCheckpointWaitsForSurvivorEvidence(t *testing.T) {
	s := newTestScheme(t)
	mig := newTestMigration()
	noResume := false
	mig.Spec.Resume = &noResume
	mig.Spec.WorkloadRef = fluidcrv1alpha1.WorkloadReference{APIVersion: "apps/v1", Kind: "StatefulSet", Name: "trainer"}
	mig.Spec.PartialCheckpoint = &fluidcrv1alpha1.PartialCheckpointSpec{TargetRanks: []int64{1}}
	fc := &fakeCtrl{runtimeSequence: map[string][]ctrlapi.RuntimeStatus{"10.0.0.1": {
		{Rank: 0, WorldSize: 2, CheckpointID: "mig-round-001"},
		{Rank: 0, WorldSize: 2, CheckpointID: "mig-round-001", SurvivorEvidence: ctrlapi.SurvivorEvidence{Generation: 7, PauseLockPath: "/checkpoint/rank0/pause-lock", PauseLockPID: 1234, ObservedAt: "now"}},
	}}}
	fk := &fakeKubelet{}
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(newTestStatefulSet(), newStatefulPod("trainer-0", "10.0.0.1", "192.168.0.1"), newStatefulPod("trainer-1", "10.0.0.2", "192.168.0.2"), mig).
		WithStatusSubresource(&fluidcrv1alpha1.FluidCRMigration{}).
		Build()
	r := &FluidCRMigrationReconciler{Client: c, Scheme: s, CtrlClient: fc, KubeletClient: fk}

	got := reconcileToCompletion(t, r, c)

	if got.Status.Phase != fluidcrv1alpha1.PhaseCompleted {
		t.Fatalf("phase = %q: %s", got.Status.Phase, got.Status.Message)
	}
	if len(fc.partialCalls) != 1 {
		t.Fatalf("partial checkpoint calls = %d, want one signal", len(fc.partialCalls))
	}
	survivor := podStatusByName(got.Status.Pods, "trainer-0")
	if survivor == nil || survivor.Phase != fluidcrv1alpha1.PodPhaseSurvivorPaused || survivor.SurvivorEvidence == nil {
		t.Fatalf("survivor status = %+v", survivor)
	}
}

func TestReconcile_RestoreOwnedResumeReleasesSurvivorAfterAuthorization(t *testing.T) {
	s := newTestScheme(t)
	mig := newTestMigration()
	noResume := false
	mig.Spec.Resume = &noResume
	mig.Spec.WorkloadRef = fluidcrv1alpha1.WorkloadReference{APIVersion: "apps/v1", Kind: "StatefulSet", Name: "trainer"}
	mig.Spec.PartialCheckpoint = &fluidcrv1alpha1.PartialCheckpointSpec{TargetRanks: []int64{1}}
	fc := &fakeCtrl{runtime: map[string]ctrlapi.RuntimeStatus{"10.0.0.1": {Rank: 0, WorldSize: 2, CheckpointID: "mig-round-001", SurvivorEvidence: ctrlapi.SurvivorEvidence{Generation: 7, PauseLockPath: "/checkpoint/rank0/pause-lock", PauseLockPID: 1234, ObservedAt: "now"}}}}
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(newTestStatefulSet(), newStatefulPod("trainer-0", "10.0.0.1", "192.168.0.1"), newStatefulPod("trainer-1", "10.0.0.2", "192.168.0.2"), mig).
		WithStatusSubresource(&fluidcrv1alpha1.FluidCRMigration{}).
		Build()
	r := &FluidCRMigrationReconciler{Client: c, Scheme: s, CtrlClient: fc, KubeletClient: &fakeKubelet{}}

	got := reconcileToCompletion(t, r, c)
	if len(fc.resumeOwnedCalls) != 0 {
		t.Fatalf("restore-owned resume before authorization: %v", fc.resumeOwnedCalls)
	}
	got.Annotations[AnnotationRestoreOwnedResume] = "true"
	if err := c.Update(context.Background(), &got); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "mig"}}); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "mig"}, &got); err != nil {
		t.Fatal(err)
	}
	if len(fc.resumeOwnedCalls) != 1 || fc.resumeOwnedCalls[0] != "10.0.0.1" {
		t.Fatalf("restore-owned resume calls = %v, want survivor only", fc.resumeOwnedCalls)
	}
	if fc.resumeOwnedCheckpointID != "mig-round-001" || fc.resumeOwnedGeneration != 7 {
		t.Fatalf("restore-owned resume contract = checkpointID %q generation %d", fc.resumeOwnedCheckpointID, fc.resumeOwnedGeneration)
	}
	survivor := podStatusByName(got.Status.Pods, "trainer-0")
	if survivor == nil || survivor.Phase != fluidcrv1alpha1.PodPhaseResumed {
		t.Fatalf("survivor after release = %+v", survivor)
	}
	target := podStatusByName(got.Status.Pods, "trainer-1")
	if target == nil || target.Phase != fluidcrv1alpha1.PodPhaseContainerCheckpointed {
		t.Fatalf("target phase changed during survivor release: %+v", target)
	}
}

func TestReconcile_RestoreOwnedResumeRetryStaysOwnedLane(t *testing.T) {
	s := newTestScheme(t)
	mig := newTestMigration()
	noResume := false
	mig.Spec.Resume = &noResume
	mig.Spec.WorkloadRef = fluidcrv1alpha1.WorkloadReference{APIVersion: "apps/v1", Kind: "StatefulSet", Name: "trainer"}
	mig.Spec.PartialCheckpoint = &fluidcrv1alpha1.PartialCheckpointSpec{TargetRanks: []int64{1}}
	fc := &fakeCtrl{
		runtime:        map[string]ctrlapi.RuntimeStatus{"10.0.0.1": {Rank: 0, WorldSize: 2, CheckpointID: "mig-round-001", SurvivorEvidence: ctrlapi.SurvivorEvidence{Generation: 7, PauseLockPath: "/checkpoint/rank0/pause-lock", PauseLockPID: 1234, ObservedAt: "now"}}},
		resumeOwnedErr: map[string]error{"10.0.0.1": fmt.Errorf("runtime busy")},
	}
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(newTestStatefulSet(), newStatefulPod("trainer-0", "10.0.0.1", "192.168.0.1"), newStatefulPod("trainer-1", "10.0.0.2", "192.168.0.2"), mig).
		WithStatusSubresource(&fluidcrv1alpha1.FluidCRMigration{}).
		Build()
	r := &FluidCRMigrationReconciler{Client: c, Scheme: s, CtrlClient: fc, KubeletClient: &fakeKubelet{}}

	got := reconcileToCompletion(t, r, c)
	got.Annotations[AnnotationRestoreOwnedResume] = "true"
	if err := c.Update(context.Background(), &got); err != nil {
		t.Fatal(err)
	}
	req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "mig"}}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(context.Background(), req.NamespacedName, &got); err != nil {
		t.Fatal(err)
	}
	if len(fc.resumeOwnedCalls) != 1 || fc.resumeOwnedCalls[0] != "10.0.0.1" {
		t.Fatalf("restore-owned retry first call = %v", fc.resumeOwnedCalls)
	}
	if got.Status.Phase != fluidcrv1alpha1.PhaseCompleted || got.Status.CompletionTime == nil {
		t.Fatalf("retry status = %q %q", got.Status.Phase, got.Status.Message)
	}
	var pendingRelease bool
	for _, condition := range got.Status.Conditions {
		if condition.Type == "SurvivorReleased" && condition.Status == metav1.ConditionFalse && condition.Reason == "ReleasePending" {
			pendingRelease = true
		}
	}
	if !pendingRelease {
		t.Fatalf("release wait must be separate from checkpoint completion: %+v", got.Status.Conditions)
	}
	if _, generic := fc.counts(); generic != 0 {
		t.Fatalf("generic resume calls during owned retry = %d", generic)
	}

	delete(fc.resumeOwnedErr, "10.0.0.1")
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(context.Background(), req.NamespacedName, &got); err != nil {
		t.Fatal(err)
	}
	if len(fc.resumeOwnedCalls) != 2 || got.Status.Phase != fluidcrv1alpha1.PhaseCompleted {
		t.Fatalf("owned retry did not complete: calls=%v status=%q %q", fc.resumeOwnedCalls, got.Status.Phase, got.Status.Message)
	}
	if _, generic := fc.counts(); generic != 0 {
		t.Fatalf("generic resume calls after owned retry = %d", generic)
	}
}

func TestReconcile_RestoreOwnedResumeRetryAfterLostStatusAcceptsReleasedRuntime(t *testing.T) {
	s := newTestScheme(t)
	mig := newTestMigration()
	noResume := false
	mig.Spec.Resume = &noResume
	mig.Spec.WorkloadRef = fluidcrv1alpha1.WorkloadReference{APIVersion: "apps/v1", Kind: "StatefulSet", Name: "trainer"}
	mig.Spec.PartialCheckpoint = &fluidcrv1alpha1.PartialCheckpointSpec{TargetRanks: []int64{1}}
	fc := &fakeCtrl{runtime: map[string]ctrlapi.RuntimeStatus{"10.0.0.1": {Rank: 0, WorldSize: 2, CheckpointID: "mig-round-001", SurvivorEvidence: ctrlapi.SurvivorEvidence{Generation: 7, PauseLockPath: "/checkpoint/rank0/pause-lock", PauseLockPID: 1234, ObservedAt: "now"}}}}
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(newTestStatefulSet(), newStatefulPod("trainer-0", "10.0.0.1", "192.168.0.1"), newStatefulPod("trainer-1", "10.0.0.2", "192.168.0.2"), mig).
		WithStatusSubresource(&fluidcrv1alpha1.FluidCRMigration{}).
		Build()
	r := &FluidCRMigrationReconciler{Client: c, Scheme: s, CtrlClient: fc, KubeletClient: &fakeKubelet{}}

	got := reconcileToCompletion(t, r, c)
	got.Annotations[AnnotationRestoreOwnedResume] = "true"
	if err := c.Update(context.Background(), &got); err != nil {
		t.Fatal(err)
	}
	req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "mig"}}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if len(fc.resumeOwnedCalls) != 1 {
		t.Fatalf("initial restore-owned resume calls = %v", fc.resumeOwnedCalls)
	}

	// Simulate the API status write being lost after the scoped release reached
	// FluidCR: persisted status still asks for release, but /runtime no longer
	// exposes the pause-lock manifest because the survivor is already running.
	if err := c.Get(context.Background(), req.NamespacedName, &got); err != nil {
		t.Fatal(err)
	}
	got.Status.Phase = fluidcrv1alpha1.PhaseCompleted
	for i := range got.Status.Pods {
		if got.Status.Pods[i].PodName == "trainer-0" {
			got.Status.Pods[i].Phase = fluidcrv1alpha1.PodPhaseSurvivorPaused
			got.Status.Pods[i].Message = "persisted before release completion"
		}
	}
	if err := c.Status().Update(context.Background(), &got); err != nil {
		t.Fatal(err)
	}
	fc.runtime["10.0.0.1"] = ctrlapi.RuntimeStatus{Rank: 0, WorldSize: 2, State: "Running", CheckpointID: "mig-round-001"}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(context.Background(), req.NamespacedName, &got); err != nil {
		t.Fatal(err)
	}
	if len(fc.resumeOwnedCalls) != 2 || got.Status.Phase != fluidcrv1alpha1.PhaseCompleted {
		t.Fatalf("idempotent owned retry did not complete: calls=%v status=%q %q", fc.resumeOwnedCalls, got.Status.Phase, got.Status.Message)
	}
	if _, generic := fc.counts(); generic != 0 {
		t.Fatalf("generic resume calls after lost-status retry = %d", generic)
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
