package checkpoint

import (
	"context"
	"errors"
	"fmt"
	corev1 "k8s.io/api/core/v1"
	"strings"
	"syscall"
	"testing"
	"time"

	fluidcrv1alpha1 "github.com/GProjectdev/Stateful-Migration-Operator-with-PV/api/fluidcr/v1alpha1"
	"github.com/GProjectdev/Stateful-Migration-Operator-with-PV/internal/ctrlapi"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type offlineCtrl struct {
	fakeCtrl
	err          error
	afterResume  func()
	runtimeState string
}

func (f *offlineCtrl) Resume(context.Context, string, int, time.Duration) (map[string]string, error) {
	if f.afterResume != nil {
		f.afterResume()
	}
	return nil, f.err
}
func (f *offlineCtrl) Runtime(context.Context, string, int, time.Duration) (ctrlapi.RuntimeStatus, error) {
	return ctrlapi.RuntimeStatus{State: f.runtimeState}, nil
}

type fakeExec struct {
	calls   int
	command []string
	err     error
}

func (f *fakeExec) Execute(_ context.Context, ns, pod, container string, command []string) error {
	f.calls++
	f.command = command
	if ns != "default" || pod != "p0" || container != "trainer" {
		return fmt.Errorf("wrong exec target")
	}
	return f.err
}

func TestResumeExecFallback(t *testing.T) {
	for _, name := range []string{"success", "container-failed", "http-success", "http-error", "partial", "retained", "uid-changed", "unbound", "exec-denied", "api-not-running", "missing-path", "unresolved-path", "outside-root", "readonly"} {
		t.Run(name, func(t *testing.T) {
			p := newTestPod("p0", "10.0.0.1", "192.0.2.1")
			p.Spec.Containers[0].Env = append(p.Spec.Containers[0].Env, corev1.EnvVar{Name: "FLUIDCR_CHECKPOINT_DIR", Value: "/checkpoint"}, corev1.EnvVar{Name: "FLUIDCR_CHECKPOINT_PATH", Value: "/checkpoint/$(POD_NAME)/latest.pt"})
			p.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{Name: "checkpoint", MountPath: "/checkpoint"}}
			switch name {
			case "missing-path":
				p.Spec.Containers[0].Env[2].Value = ""
			case "unresolved-path":
				p.Spec.Containers[0].Env[2].Value = "/checkpoint/$(UNKNOWN)/latest.pt"
			case "outside-root":
				p.Spec.Containers[0].Env[2].Value = "/elsewhere/latest.pt"
			case "readonly":
				p.Spec.Containers[0].VolumeMounts[0].ReadOnly = true
			}
			c := fake.NewClientBuilder().WithScheme(newTestScheme(t)).WithObjects(p).Build()
			m := newTestMigration()
			m.Status.Pods = []fluidcrv1alpha1.PodMigrationStatus{{PodName: p.Name, PodUID: string(p.UID), CheckpointID: "mig-round-001", AppCheckpointResult: "1 checkpoint-ready"}}
			f := &offlineCtrl{err: fmt.Errorf("control API: %w", syscall.ECONNREFUSED), runtimeState: "Running"}
			e := &fakeExec{}
			r := &FluidCRMigrationReconciler{Client: c, APIReader: c, CtrlClient: f, PodExecutor: e}
			targetPod := target{podName: p.Name, podUID: p.UID, namespace: p.Namespace, podIP: p.Status.PodIP, hostIP: p.Status.HostIP, container: "trainer", port: 8298}
			wantCalls, wantErr := 1, false
			switch name {
			case "missing-path", "unresolved-path", "outside-root", "readonly":
				wantCalls = 0
				wantErr = true
			case "container-failed":
				m.Status.Pods[0].CheckpointID = ""
				m.Status.Pods[0].Phase = fluidcrv1alpha1.PodPhaseFailed
			case "http-success":
				f.err = nil
				wantCalls = 0
			case "http-error":
				f.err = errors.New("HTTP 409 denied")
				wantCalls = 0
				wantErr = true
			case "partial":
				m.Spec.PartialCheckpoint = &fluidcrv1alpha1.PartialCheckpointSpec{}
				wantCalls = 0
				wantErr = true
			case "retained":
				v := false
				m.Spec.Resume = &v
				wantCalls = 0
				wantErr = true
			case "uid-changed":
				f.afterResume = func() {
					p.UID = "new-uid"
					if err := c.Update(context.Background(), p); err != nil {
						t.Fatal(err)
					}
				}
				wantCalls = 0
				wantErr = true
			case "unbound":
				m.Status.Pods[0].CheckpointID = "old"
				wantCalls = 0
				wantErr = true
			case "exec-denied":
				e.err = errors.New("forbidden pods/exec")
				wantErr = true
			case "api-not-running":
				f.runtimeState = "CheckpointReady"
				wantErr = true
			}
			result := r.resume(context.Background(), m, []target{targetPod}, 30*time.Millisecond)
			if (result[p.Name] != nil) != wantErr || e.calls != wantCalls {
				t.Fatalf("error=%v calls=%d", result[p.Name], e.calls)
			}
			if e.calls > 0 && !strings.Contains(strings.Join(e.command[:5], " "), "FLUIDCR_CHECKPOINT_PATH=/checkpoint/p0/latest.pt") {
				t.Fatal("missing resolved current-pod checkpoint path")
			}
			if e.calls > 0 && e.command[len(e.command)-1] != "mig-round-001" {
				t.Fatal("missing checkpoint binding")
			}
		})
	}
}
