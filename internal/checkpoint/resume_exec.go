package checkpoint

import (
	"bytes"
	"context"
	_ "embed"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"syscall"
	"time"

	fluidcrv1alpha1 "github.com/GProjectdev/Stateful-Migration-Operator-with-PV/api/fluidcr/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
)

// PodExecutor is the non-HTTP recovery transport for a quiesced launcher.
type PodExecutor interface {
	Execute(context.Context, string, string, string, []string) error
}

type podExecutor struct {
	config *rest.Config
	client kubernetes.Interface
}

func newPodExecutor(config *rest.Config) (PodExecutor, error) {
	c, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	return &podExecutor{config: config, client: c}, nil
}

func (e *podExecutor) Execute(ctx context.Context, namespace, pod, container string, command []string) error {
	req := e.client.CoreV1().RESTClient().Post().Namespace(namespace).Resource("pods").Name(pod).SubResource("exec").VersionedParams(&corev1.PodExecOptions{Container: container, Command: command, Stdout: true, Stderr: true}, scheme.ParameterCodec)
	executor, err := remotecommand.NewSPDYExecutor(e.config, http.MethodPost, req.URL())
	if err != nil {
		return err
	}
	var stderr bytes.Buffer
	if err := executor.StreamWithContext(ctx, remotecommand.StreamOptions{Stdout: io.Discard, Stderr: &stderr}); err != nil {
		return fmt.Errorf("scoped resume exec: %w: %s", err, stderr.String())
	}
	return nil
}

//go:embed resume_in_place.py
var resumeInPlaceScript string

func isConnectionRefused(err error) bool { return errors.Is(err, syscall.ECONNREFUSED) }

func (r *FluidCRMigrationReconciler) resumeWithExec(ctx context.Context, m *fluidcrv1alpha1.FluidCRMigration, t target, timeout time.Duration) error {
	if !shouldResume(m) || m.Spec.PartialCheckpoint != nil {
		return fmt.Errorf("exec resume requires an in-place full checkpoint")
	}
	id := strings.TrimSpace(m.Annotations[AnnotationCheckpointID])
	ps := getPodStatus(m, t.podName)
	if id == "" || ps == nil || ps.PodUID != string(t.podUID) || (ps.CheckpointID != "" && ps.CheckpointID != id) || ps.AppCheckpointResult == "" {
		return fmt.Errorf("exec resume requires UID-bound checkpoint evidence")
	}
	if err := r.validateTarget(ctx, t); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	command := []string{"env", "PYTHONPATH=/opt/fluidcr", "python3", "-c", resumeInPlaceScript, id}
	if err := r.PodExecutor.Execute(ctx, t.namespace, t.podName, t.container, command); err != nil {
		return err
	}
	// Lock removal alone is not evidence that the launcher restarted its API.
	for {
		if err := r.validateTarget(ctx, t); err != nil {
			return err
		}
		status, err := r.CtrlClient.Runtime(ctx, t.podIP, t.port, 5*time.Second)
		if err == nil && strings.EqualFold(status.State, "Running") {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("resume API did not return Running: %w", ctx.Err())
		case <-time.After(time.Second):
		}
	}
}
