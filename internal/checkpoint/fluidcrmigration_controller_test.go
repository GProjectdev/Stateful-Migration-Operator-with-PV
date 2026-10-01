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
	"strings"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
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

func TestScheduleParentCreatesChildWithoutExecutingCheckpoint(t *testing.T) {
	s := newTestScheme(t)
	parent := newTestMigration()
	parent.Name = "sched"
	parent.UID = "parent-uid"
	parent.Annotations = nil
	parent.Spec.Schedule = &fluidcrv1alpha1.MigrationScheduleSpec{Enabled: true, IntervalSeconds: 60}
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(newTestDeployment(), newTestPod("p0", "10.0.0.1", "192.168.0.1"), parent).
		WithObjects(newTestReplicaSet("default"), newTestReplicaSet("other")).
		WithStatusSubresource(&fluidcrv1alpha1.FluidCRMigration{}).
		Build()
	fc, fk := &fakeCtrl{}, &fakeKubelet{}
	r := &FluidCRMigrationReconciler{Client: c, Scheme: s, CtrlClient: fc, KubeletClient: fk}
	req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "sched"}}
	for i := 0; i < 3; i++ {
		if _, err := r.Reconcile(context.Background(), req); err != nil {
			t.Fatal(err)
		}
	}
	var list fluidcrv1alpha1.FluidCRMigrationList
	if err := c.List(context.Background(), &list, client.InNamespace("default"), client.MatchingLabels{LabelScheduledParent: "parent-uid"}); err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 1 {
		t.Fatalf("children = %d, want 1", len(list.Items))
	}
	child := list.Items[0]
	if child.Spec.Schedule != nil || child.Annotations[AnnotationCheckpointID] == "" {
		t.Fatalf("child contract = schedule %#v annotations %#v", child.Spec.Schedule, child.Annotations)
	}
	if cp, rs := fc.counts(); cp != 0 || rs != 0 || fk.count() != 0 {
		t.Fatalf("parent executed checkpoint: checkpoint=%d resume=%d kubelet=%d", cp, rs, fk.count())
	}
	var got fluidcrv1alpha1.FluidCRMigration
	if err := c.Get(context.Background(), req.NamespacedName, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.ObservedGeneration != got.Generation || got.Status.CurrentRun == nil || got.Status.CurrentRun.Name != child.Name {
		t.Fatalf("parent status = %+v", got.Status)
	}
}

func TestScheduleReservationRetryAfterChildCreateStatusFailureIsDeterministic(t *testing.T) {
	s := newTestScheme(t)
	parent := newTestMigration()
	parent.Name = "sched-retry"
	parent.UID = "parent-retry-uid"
	parent.Annotations = nil
	parent.Spec.Schedule = &fluidcrv1alpha1.MigrationScheduleSpec{Enabled: true, IntervalSeconds: 60}
	var createdChildren int
	failMaterializedStatus := true
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(newTestDeployment(), newTestPod("p0", "10.0.0.1", "192.168.0.1"), parent).
		WithObjects(newTestReplicaSet("default"), newTestReplicaSet("other")).
		WithStatusSubresource(&fluidcrv1alpha1.FluidCRMigration{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if mig, ok := obj.(*fluidcrv1alpha1.FluidCRMigration); ok && mig.Name != "sched-retry" {
					createdChildren++
					mig.UID = types.UID("child-retry-uid")
				}
				return c.Create(ctx, obj, opts...)
			},
			SubResourceUpdate: func(ctx context.Context, c client.Client, subResourceName string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				if subResourceName == "status" && failMaterializedStatus {
					if mig, ok := obj.(*fluidcrv1alpha1.FluidCRMigration); ok && mig.Name == "sched-retry" && mig.Status.CurrentRun != nil && mig.Status.CurrentRun.UID != "" {
						failMaterializedStatus = false
						return fmt.Errorf("injected parent status save failure")
					}
				}
				return c.Status().Update(ctx, obj, opts...)
			},
		}).
		Build()
	r := &FluidCRMigrationReconciler{Client: c, Scheme: s, CtrlClient: &fakeCtrl{}, KubeletClient: &fakeKubelet{}}
	req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "sched-retry"}}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	var got fluidcrv1alpha1.FluidCRMigration
	if err := c.Get(context.Background(), req.NamespacedName, &got); err != nil {
		t.Fatal(err)
	}
	reserved := got.Status.CurrentRun
	if reserved == nil || reserved.Name == "" || reserved.CheckpointID == "" || reserved.UID != "" {
		t.Fatalf("reservation status = %+v", got.Status.CurrentRun)
	}
	if _, err := r.Reconcile(context.Background(), req); err == nil {
		t.Fatal("second reconcile unexpectedly saved parent status")
	}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	var list fluidcrv1alpha1.FluidCRMigrationList
	if err := c.List(context.Background(), &list, client.InNamespace("default"), client.MatchingLabels{LabelScheduledParent: "parent-retry-uid"}); err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 1 || createdChildren != 1 {
		t.Fatalf("children=%d createCalls=%d, want exactly one child/create", len(list.Items), createdChildren)
	}
	child := list.Items[0]
	if child.Name != reserved.Name || child.Annotations[AnnotationCheckpointID] != reserved.CheckpointID {
		t.Fatalf("child identity = %s/%s, want %s/%s", child.Name, child.Annotations[AnnotationCheckpointID], reserved.Name, reserved.CheckpointID)
	}
}

func TestSchedulePendingReservationValidatesExistingChildBeforeAdopting(t *testing.T) {
	s := newTestScheme(t)
	parent := newTestMigration()
	parent.Name = "sched-adopt"
	parent.UID = "parent-adopt-uid"
	parent.Finalizers = []string{FinalizerName}
	parent.Annotations = nil
	parent.Spec.Schedule = &fluidcrv1alpha1.MigrationScheduleSpec{Enabled: true, IntervalSeconds: 60}
	reservation := scheduledRunReservation(parent)
	parent.Status.CurrentRun = reservation
	child := mustScheduledChildForReservation(parent, reservation)
	child.Spec.Container = "tampered"
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(parent, child).
		WithStatusSubresource(&fluidcrv1alpha1.FluidCRMigration{}).
		Build()
	r := &FluidCRMigrationReconciler{Client: c, Scheme: s, CtrlClient: &fakeCtrl{}, KubeletClient: &fakeKubelet{}}
	_, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: parent.Name}})
	if err == nil || !strings.Contains(err.Error(), "spec does not match reserved execution spec") {
		t.Fatalf("expected reserved child validation error, got %v", err)
	}
}

func TestSameWorkloadRefNormalizesOmittedNamespace(t *testing.T) {
	a := fluidcrv1alpha1.WorkloadReference{UID: "workload-uid", APIVersion: "apps/v1", Kind: "Deployment", Name: "trainer"}
	b := a
	b.Namespace = "default"
	if !sameWorkloadRef(a, b, "default") {
		t.Fatal("empty namespace and explicit object namespace must conflict as the same workload")
	}
	b.Namespace = "other"
	if sameWorkloadRef(a, b, "default") {
		t.Fatal("different explicit namespace must not match")
	}
}

func TestSameWorkloadPartialCompletedUnblocksOnlyAfterSurvivorRelease(t *testing.T) {
	now := metav1.Now()
	other := newTestMigration()
	other.Spec.PartialCheckpoint = &fluidcrv1alpha1.PartialCheckpointSpec{TargetRanks: []int64{1}}
	other.Generation = 3
	other.Status.Phase = fluidcrv1alpha1.PhaseCompleted
	other.Status.ObservedGeneration = 3
	other.Status.CompletionTime = &now
	if !sameWorkloadMigrationBlocksPeriodic(other) {
		t.Fatal("completed partial without explicit survivor release must still block periodic checkpoints")
	}
	meta.SetStatusCondition(&other.Status.Conditions, metav1.Condition{Type: "SurvivorReleased", Status: metav1.ConditionTrue, Reason: "Released", ObservedGeneration: 3, LastTransitionTime: now})
	if sameWorkloadMigrationBlocksPeriodic(other) {
		t.Fatal("completed partial with explicit survivor release should not block periodic checkpoints")
	}
}

func TestSameWorkloadDurableFullResumeFalseStillBlocksPeriodic(t *testing.T) {
	now := metav1.Now()
	resumeFalse := false
	other := newTestMigration()
	other.Spec.Resume = &resumeFalse
	other.Annotations[AnnotationCheckpointID] = "checkpoint-001"
	other.Generation = 2
	other.Status = fluidcrv1alpha1.FluidCRMigrationStatus{
		ObservedGeneration: 2,
		Phase:              fluidcrv1alpha1.PhaseCompleted,
		CompletionTime:     &now,
		Pods: []fluidcrv1alpha1.PodMigrationStatus{{
			PodName:      "p0",
			PodUID:       "pod-uid",
			CheckpointID: "checkpoint-001",
			CheckpointFiles: []fluidcrv1alpha1.CheckpointFile{{
				CheckpointID:  "checkpoint-001",
				ContainerName: "trainer",
				FilePath:      "/var/lib/kubelet/checkpoints/a.tar",
				SHA256:        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				DurableRef:    "file-store:default/sha256/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				ExportedAt:    "2026-09-26T00:00:00Z",
			}},
		}},
	}
	if !durableFullCheckpointComplete(other) {
		t.Fatal("test setup must be durable full complete")
	}
	if !sameWorkloadMigrationBlocksPeriodic(other) {
		t.Fatal("durable full checkpoint with resume=false leaves workload paused and must block periodic overlap")
	}
}

func TestScheduleDisabledAcknowledgesPauseWithoutSpawning(t *testing.T) {
	s := newTestScheme(t)
	parent := newTestMigration()
	parent.Name = "sched"
	parent.UID = "parent-uid"
	parent.Annotations = nil
	parent.Spec.Schedule = &fluidcrv1alpha1.MigrationScheduleSpec{Enabled: false, IntervalSeconds: 60}
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(parent).WithStatusSubresource(&fluidcrv1alpha1.FluidCRMigration{}).Build()
	r := &FluidCRMigrationReconciler{Client: c, Scheme: s, CtrlClient: &fakeCtrl{}, KubeletClient: &fakeKubelet{}}
	req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "sched"}}
	for i := 0; i < 2; i++ {
		if _, err := r.Reconcile(context.Background(), req); err != nil {
			t.Fatal(err)
		}
	}
	var got fluidcrv1alpha1.FluidCRMigration
	if err := c.Get(context.Background(), req.NamespacedName, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.ObservedGeneration != got.Generation || got.Status.Message != "schedule paused" {
		t.Fatalf("pause ack status = %+v", got.Status)
	}
	var list fluidcrv1alpha1.FluidCRMigrationList
	if err := c.List(context.Background(), &list, client.InNamespace("default")); err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 1 {
		t.Fatalf("paused schedule spawned children: %d objects", len(list.Items))
	}
}

func TestSchedulePausedSecondReconcileDoesNotRewriteStatus(t *testing.T) {
	s := newTestScheme(t)
	parent := newTestMigration()
	parent.Name = "sched-paused-stable"
	parent.UID = "parent-paused-stable-uid"
	parent.Finalizers = []string{FinalizerName}
	parent.Annotations = nil
	parent.Spec.Schedule = &fluidcrv1alpha1.MigrationScheduleSpec{Enabled: false, IntervalSeconds: 60}
	var statusWrites int
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(parent).
		WithStatusSubresource(&fluidcrv1alpha1.FluidCRMigration{}).
		WithInterceptorFuncs(interceptor.Funcs{SubResourceUpdate: func(ctx context.Context, c client.Client, subResourceName string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if subResourceName == "status" {
				statusWrites++
			}
			return c.Status().Update(ctx, obj, opts...)
		}}).
		Build()
	r := &FluidCRMigrationReconciler{Client: c, Scheme: s, CtrlClient: &fakeCtrl{}, KubeletClient: &fakeKubelet{}}
	req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: parent.Name}}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if statusWrites != 1 {
		t.Fatalf("initial pause status writes = %d, want 1", statusWrites)
	}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if statusWrites != 1 {
		t.Fatalf("second pause reconcile rewrote status: writes=%d", statusWrites)
	}
}

func TestSchedulePinnedCurrentRunMissingChildBlocksPauseAck(t *testing.T) {
	s := newTestScheme(t)
	parent := newTestMigration()
	parent.Name = "sched-missing-current"
	parent.UID = "parent-missing-current-uid"
	parent.Finalizers = []string{FinalizerName}
	parent.Annotations = nil
	parent.Spec.Schedule = &fluidcrv1alpha1.MigrationScheduleSpec{Enabled: false, IntervalSeconds: 60}
	parent.Status.CurrentRun = &fluidcrv1alpha1.ScheduledRunReference{Name: "missing-child", UID: "child-uid", CheckpointID: "checkpoint-001"}
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(parent).WithStatusSubresource(&fluidcrv1alpha1.FluidCRMigration{}).Build()
	r := &FluidCRMigrationReconciler{Client: c, Scheme: s, CtrlClient: &fakeCtrl{}, KubeletClient: &fakeKubelet{}}
	req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: parent.Name}}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	var got fluidcrv1alpha1.FluidCRMigration
	if err := c.Get(context.Background(), req.NamespacedName, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != fluidcrv1alpha1.PhasePending || got.Status.Message == "schedule paused" || got.Status.CurrentRun == nil || got.Status.CurrentRun.Name != "missing-child" {
		t.Fatalf("pinned missing child should block without pause ack/new run: %+v", got.Status)
	}
}

func TestSchedulePinnedCurrentRunUIDMismatchBlocksReplacement(t *testing.T) {
	s := newTestScheme(t)
	parent := newTestMigration()
	parent.Name = "sched-replaced-current"
	parent.UID = "parent-replaced-current-uid"
	parent.Finalizers = []string{FinalizerName}
	parent.Annotations = nil
	parent.Spec.Schedule = &fluidcrv1alpha1.MigrationScheduleSpec{Enabled: true, IntervalSeconds: 1}
	child := scheduledChildFor(parent)
	child.Name = "replaced-child"
	child.UID = "new-child-uid"
	parent.Status.CurrentRun = &fluidcrv1alpha1.ScheduledRunReference{Name: child.Name, UID: "old-child-uid", CheckpointID: child.Annotations[AnnotationCheckpointID]}
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(parent, child).WithStatusSubresource(&fluidcrv1alpha1.FluidCRMigration{}).Build()
	r := &FluidCRMigrationReconciler{Client: c, Scheme: s, CtrlClient: &fakeCtrl{}, KubeletClient: &fakeKubelet{}}
	req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: parent.Name}}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	var got fluidcrv1alpha1.FluidCRMigration
	if err := c.Get(context.Background(), req.NamespacedName, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != fluidcrv1alpha1.PhasePending || got.Status.CurrentRun == nil || got.Status.CurrentRun.UID != "old-child-uid" || !strings.Contains(got.Status.Message, "found UID") {
		t.Fatalf("pinned UID mismatch should block without adopting replacement: %+v", got.Status)
	}
	var list fluidcrv1alpha1.FluidCRMigrationList
	if err := c.List(context.Background(), &list, client.InNamespace("default"), client.MatchingLabels{LabelScheduledParent: "parent-replaced-current-uid"}); err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 1 {
		t.Fatalf("UID mismatch spawned a new child: %d", len(list.Items))
	}
}

func TestScheduleActiveChildPreventsOverlapAndGenerationAppliesNextRun(t *testing.T) {
	s := newTestScheme(t)
	parent := newTestMigration()
	parent.Name = "sched"
	parent.UID = "parent-uid"
	parent.Generation = 2
	parent.Annotations = nil
	parent.Finalizers = []string{FinalizerName}
	parent.Spec.Container = "next-container"
	parent.Spec.Schedule = &fluidcrv1alpha1.MigrationScheduleSpec{Enabled: true, IntervalSeconds: 1}
	child := scheduledChildFor(parent, 1)
	child.Name = "active-child"
	child.UID = "child-uid"
	child.Spec.Container = "old-container"
	child.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(parent, fluidcrv1alpha1.GroupVersion.WithKind("FluidCRMigration"))}
	child.Status.Phase = fluidcrv1alpha1.PhaseAppCheckpointing
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(parent, child).WithStatusSubresource(&fluidcrv1alpha1.FluidCRMigration{}).Build()
	r := &FluidCRMigrationReconciler{Client: c, Scheme: s, CtrlClient: &fakeCtrl{}, KubeletClient: &fakeKubelet{}}
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "sched"}}); err != nil {
		t.Fatal(err)
	}
	var list fluidcrv1alpha1.FluidCRMigrationList
	if err := c.List(context.Background(), &list, client.InNamespace("default"), client.MatchingLabels{LabelScheduledParent: "parent-uid"}); err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 1 || list.Items[0].Spec.Container != "old-container" {
		t.Fatalf("overlap/generation contract broken: %+v", list.Items)
	}
	var got fluidcrv1alpha1.FluidCRMigration
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "sched"}, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.ObservedGeneration != 2 || got.Status.CurrentRun == nil || got.Status.CurrentRun.Phase != fluidcrv1alpha1.PhaseAppCheckpointing {
		t.Fatalf("active child status = %+v", got.Status)
	}
}

func TestScheduleDurableFullChildUpdatesSuccessSnapshot(t *testing.T) {
	s := newTestScheme(t)
	parent := newTestMigration()
	parent.Name = "sched"
	parent.UID = "parent-uid"
	parent.Annotations = nil
	parent.Finalizers = []string{FinalizerName}
	parent.Spec.Schedule = &fluidcrv1alpha1.MigrationScheduleSpec{Enabled: false, IntervalSeconds: 60}
	child := scheduledChildFor(parent, 1)
	child.Name = "done-child"
	child.UID = "child-uid"
	child.Generation = 1
	child.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(parent, fluidcrv1alpha1.GroupVersion.WithKind("FluidCRMigration"))}
	checkpointID := child.Annotations[AnnotationCheckpointID]
	now := metav1.Now()
	child.Status = fluidcrv1alpha1.FluidCRMigrationStatus{ObservedGeneration: 1, Phase: fluidcrv1alpha1.PhaseCompleted, StartTime: &now, CompletionTime: &now, Pods: []fluidcrv1alpha1.PodMigrationStatus{{PodName: "p0", PodUID: "pod-uid", CheckpointID: checkpointID, Phase: fluidcrv1alpha1.PodPhaseResumed, CheckpointFiles: []fluidcrv1alpha1.CheckpointFile{{CheckpointID: checkpointID, ContainerName: "trainer", FilePath: "/var/lib/kubelet/checkpoints/a.tar", SHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", DurableRef: "file-store:default/sha256/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", ExportedAt: "2026-09-26T00:00:00Z"}}}}}
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(parent, child).WithStatusSubresource(&fluidcrv1alpha1.FluidCRMigration{}).Build()
	r := &FluidCRMigrationReconciler{Client: c, Scheme: s, CtrlClient: &fakeCtrl{}, KubeletClient: &fakeKubelet{}}
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "sched"}}); err != nil {
		t.Fatal(err)
	}
	var got fluidcrv1alpha1.FluidCRMigration
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "sched"}, &got); err != nil {
		t.Fatal(err)
	}
	ref := got.Status.LastSuccessfulFullCheckpoint
	if ref == nil || ref.Name != child.Name || ref.UID != "child-uid" || ref.CheckpointID != checkpointID || ref.CompletionTime == nil || ref.Result == nil {
		t.Fatalf("success ref = %+v", ref)
	}
	if strings.Contains(string(ref.Result.Spec.Raw), "schedule") || !strings.Contains(string(ref.Result.Status.Raw), "durableRef") {
		t.Fatalf("snapshot missing immutable evidence: spec=%s status=%s", ref.Result.Spec.Raw, ref.Result.Status.Raw)
	}
}

func TestScheduleIdleAfterDurableChildSecondReconcileDoesNotRewriteStatus(t *testing.T) {
	s := newTestScheme(t)
	parent := newTestMigration()
	parent.Name = "sched-idle-stable"
	parent.UID = "parent-idle-stable-uid"
	parent.Annotations = nil
	parent.Finalizers = []string{FinalizerName}
	parent.Spec.Schedule = &fluidcrv1alpha1.MigrationScheduleSpec{Enabled: true, IntervalSeconds: 3600}
	child := scheduledChildFor(parent)
	child.Name = "done-idle-stable-child"
	child.UID = "child-idle-stable-uid"
	child.Generation = 1
	checkpointID := child.Annotations[AnnotationCheckpointID]
	now := metav1.Now()
	child.CreationTimestamp = now
	child.Status = fluidcrv1alpha1.FluidCRMigrationStatus{ObservedGeneration: 1, Phase: fluidcrv1alpha1.PhaseCompleted, StartTime: &now, CompletionTime: &now, Pods: []fluidcrv1alpha1.PodMigrationStatus{{PodName: "p0", PodUID: "pod-uid", CheckpointID: checkpointID, Phase: fluidcrv1alpha1.PodPhaseResumed, CheckpointFiles: []fluidcrv1alpha1.CheckpointFile{{CheckpointID: checkpointID, ContainerName: "trainer", FilePath: "/var/lib/kubelet/checkpoints/a.tar", SHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", DurableRef: "file-store:default/sha256/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", ExportedAt: "2026-09-26T00:00:00Z"}}}}}
	var statusWrites int
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(parent, child).
		WithStatusSubresource(&fluidcrv1alpha1.FluidCRMigration{}).
		WithInterceptorFuncs(interceptor.Funcs{SubResourceUpdate: func(ctx context.Context, c client.Client, subResourceName string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if subResourceName == "status" {
				statusWrites++
			}
			return c.Status().Update(ctx, obj, opts...)
		}}).
		Build()
	r := &FluidCRMigrationReconciler{Client: c, Scheme: s, CtrlClient: &fakeCtrl{}, KubeletClient: &fakeKubelet{}}
	req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: parent.Name}}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if statusWrites != 1 {
		t.Fatalf("initial idle status writes = %d, want 1", statusWrites)
	}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if statusWrites != 1 {
		t.Fatalf("second idle reconcile rewrote status: writes=%d", statusWrites)
	}
}

func TestScheduleSuccessSnapshotFreezesSameChild(t *testing.T) {
	s := newTestScheme(t)
	parent := newTestMigration()
	parent.Name = "sched-freeze"
	parent.UID = "parent-freeze-uid"
	parent.Annotations = nil
	parent.Finalizers = []string{FinalizerName}
	parent.Spec.Schedule = &fluidcrv1alpha1.MigrationScheduleSpec{Enabled: false, IntervalSeconds: 60}
	child := scheduledChildFor(parent)
	child.Name = "done-freeze-child"
	child.UID = "freeze-child-uid"
	child.Generation = 1
	checkpointID := child.Annotations[AnnotationCheckpointID]
	now := metav1.Now()
	child.Status = fluidcrv1alpha1.FluidCRMigrationStatus{ObservedGeneration: 1, Phase: fluidcrv1alpha1.PhaseCompleted, StartTime: &now, CompletionTime: &now, Pods: []fluidcrv1alpha1.PodMigrationStatus{{PodName: "p0", PodUID: "pod-uid", CheckpointID: checkpointID, Phase: fluidcrv1alpha1.PodPhaseResumed, CheckpointFiles: []fluidcrv1alpha1.CheckpointFile{{CheckpointID: checkpointID, ContainerName: "trainer", FilePath: "/var/lib/kubelet/checkpoints/a.tar", SHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", DurableRef: "file-store:default/sha256/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", ExportedAt: "2026-09-26T00:00:00Z"}}}}}
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(parent, child).WithStatusSubresource(&fluidcrv1alpha1.FluidCRMigration{}).Build()
	r := &FluidCRMigrationReconciler{Client: c, Scheme: s, CtrlClient: &fakeCtrl{}, KubeletClient: &fakeKubelet{}}
	req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: parent.Name}}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	var got fluidcrv1alpha1.FluidCRMigration
	if err := c.Get(context.Background(), req.NamespacedName, &got); err != nil {
		t.Fatal(err)
	}
	first := string(got.Status.LastSuccessfulFullCheckpoint.Result.Status.Raw)
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(child), child); err != nil {
		t.Fatal(err)
	}
	child.Status.Conditions = []metav1.Condition{{Type: "ExporterObserved", Status: metav1.ConditionTrue, Reason: "Later", Message: "later", LastTransitionTime: metav1.Now()}}
	child.Status.Pods[0].CheckpointFiles[0].ExportedAt = "2026-09-26T00:05:00Z"
	if err := c.Status().Update(context.Background(), child); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(context.Background(), req.NamespacedName, &got); err != nil {
		t.Fatal(err)
	}
	if second := string(got.Status.LastSuccessfulFullCheckpoint.Result.Status.Raw); second != first {
		t.Fatalf("snapshot changed for same child:\nfirst=%s\nsecond=%s", first, second)
	}
}
