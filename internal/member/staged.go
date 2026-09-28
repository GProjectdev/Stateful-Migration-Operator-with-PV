package member

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	api "github.com/GProjectdev/Stateful-Migration-Operator-with-PV/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

//go:embed staged_probe.py
var stagedProbeScript string

func newStageProbe(config *rest.Config) (func(context.Context, *corev1.Pod, string, []string) error, error) {
	c, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	return func(ctx context.Context, pod *corev1.Pod, container string, command []string) error {
		req := c.CoreV1().RESTClient().Post().Namespace(pod.Namespace).Resource("pods").Name(pod.Name).SubResource("exec").VersionedParams(&corev1.PodExecOptions{Container: container, Command: command, Stdout: true, Stderr: true}, scheme.ParameterCodec)
		e, err := remotecommand.NewSPDYExecutor(config, http.MethodPost, req.URL())
		if err != nil {
			return err
		}
		var stderr bytes.Buffer
		if err := e.StreamWithContext(ctx, remotecommand.StreamOptions{Stdout: io.Discard, Stderr: &stderr}); err != nil {
			return fmt.Errorf("staged probe: %w: %s", err, stderr.String())
		}
		return nil
	}, nil
}

func allTargetsStaged(plan *api.RestorePlan, pods []api.PodStatus) bool {
	if len(pods) != len(plan.Spec.Pods) || len(pods) == 0 {
		return false
	}
	for _, pod := range pods {
		if pod.UID == "" || pod.Message != "" || (pod.Phase != "Running" && pod.Phase != "Staged") {
			return false
		}
	}
	return true
}

func (r *Reconciler) probeStagedTarget(ctx context.Context, plan *api.RestorePlan, mapping *api.RestorePod, pod *corev1.Pod, fences []api.SourcePodFenceStatus) error {
	if r.StageProbe == nil || plan.Spec.PartialRestore == nil || pod.Status.Phase != corev1.PodRunning || len(mapping.Archives) != 1 {
		return fmt.Errorf("partial restored Running container and probe transport required")
	}
	if pod.UID == "" || string(pod.UID) == mapping.SourcePodUID || !pod.DeletionTimestamp.IsZero() || pod.Spec.NodeName != mapping.TargetNode {
		return fmt.Errorf("replacement UID required")
	}
	if err := verifyBoundPod(plan, mapping, pod, true); err != nil {
		return err
	}
	fenced := false
	for _, f := range fences {
		if f.PodName == pod.Name && f.SourcePodUID == mapping.SourcePodUID && f.ObservedGeneration == plan.Generation && f.Phase == "SourceGone" && f.DeleteRequestedAt != nil && f.GoneObservedAt != nil {
			fenced = true
		}
	}
	if !fenced {
		return fmt.Errorf("controller-owned source deletion evidence required")
	}
	container := mapping.Archives[0].ContainerName
	running := false
	for _, s := range pod.Status.ContainerStatuses {
		if s.Name == container && s.State.Running != nil && s.RestartCount == 0 {
			running = true
		}
	}
	if !running {
		return fmt.Errorf("restore container must be running without restart")
	}
	partial := plan.Spec.PartialRestore
	if len(partial.PreservedSurvivors) == 0 {
		return fmt.Errorf("survivor generation required")
	}
	generation := partial.PreservedSurvivors[0].Generation
	for _, s := range partial.PreservedSurvivors {
		if generation <= 0 || s.Generation != generation {
			return fmt.Errorf("survivor generations differ")
		}
		var live corev1.Pod
		if err := r.Reader.Get(ctx, client.ObjectKey{Namespace: plan.Namespace, Name: s.PodName}, &live); err != nil {
			return err
		}
		if string(live.UID) != s.PodUID || live.Spec.NodeName != s.NodeName || !live.DeletionTimestamp.IsZero() {
			return fmt.Errorf("survivor identity changed")
		}
	}
	payload, err := json.Marshal(map[string]interface{}{
		"checkpointID": plan.Spec.CheckpointRef.CheckpointID, "rank": mapping.Rank,
		"sourcePodUID": mapping.SourcePodUID, "sourceNode": mapping.SourceNode,
		"podName": mapping.SourcePod, "workloadUID": plan.Spec.WorkloadRef.UID,
		"generation": generation, "targets": partial.TargetRanks,
	})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	// No imported payload/sitecustomize and no lock mutation. The existing
	// checkpoint controller owns the operation-scoped survivor resume.
	if err := r.StageProbe(ctx, pod, container, []string{"python3", "-S", "-c", stagedProbeScript, string(payload)}); err != nil {
		return err
	}
	var live corev1.Pod
	if err := r.Reader.Get(ctx, client.ObjectKeyFromObject(pod), &live); err != nil {
		return err
	}
	if live.UID != pod.UID || !live.DeletionTimestamp.IsZero() {
		return fmt.Errorf("target UID changed during probe")
	}
	if live.Status.Phase != corev1.PodRunning || live.Spec.NodeName != mapping.TargetNode {
		return fmt.Errorf("target stopped or moved during probe")
	}
	if err := verifyBoundPod(plan, mapping, &live, true); err != nil {
		return err
	}
	stillRunning := false
	for _, s := range live.Status.ContainerStatuses {
		if s.Name == container && s.State.Running != nil && s.RestartCount == 0 {
			stillRunning = true
		}
	}
	if !stillRunning {
		return fmt.Errorf("restore container changed during probe")
	}
	var current api.RestorePlan
	if err := r.Reader.Get(ctx, client.ObjectKeyFromObject(plan), &current); err != nil {
		return err
	}
	if current.UID != plan.UID || current.Generation != plan.Generation || !current.DeletionTimestamp.IsZero() {
		return fmt.Errorf("plan changed during probe")
	}
	return nil
}
