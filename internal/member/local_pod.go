package member

import (
	"context"
	"fmt"
	"strings"

	api "github.com/GProjectdev/Stateful-Migration-Operator-with-PV/api/v1alpha1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Local plans are intentionally not produced by management RestoreRequests.
// Their request provenance is the actual member checkpoint UID, not a made-up request.
func validateLocalPodPlan(p *api.RestorePlan) error {
	s := p.Spec
	if s.SourceCluster != s.TargetCluster || s.PartialRestore != nil || s.SourceFenced || s.WorkloadRef.Kind != "Pod" || s.WorkloadRef.APIVersion != "v1" || len(s.Pods) != 1 {
		return fmt.Errorf("localPodRestore requires a same-cluster standalone Pod without partialRestore or sourceFenced assertion")
	}
	m := s.Pods[0]
	if s.WorkloadRef.UID == "" || m.SourcePodUID != s.WorkloadRef.UID || m.SourcePod != s.WorkloadRef.Name || m.TargetPod != m.SourcePod || m.SourceNode == "" || m.Rank != 0 {
		return fmt.Errorf("localPodRestore requires exact standalone source Pod UID, name, node and rank zero")
	}
	if s.RequestUID != s.CheckpointRef.UID || strings.TrimSpace(s.CheckpointRef.CheckpointID) == "" || len(m.Archives) != 1 || m.Archives[0].SourcePath == "" {
		return fmt.Errorf("localPodRestore requires member checkpoint UID provenance and one source archive")
	}
	return nil
}

func validateLocalCheckpoint(ctx context.Context, reader client.Reader, p *api.RestorePlan) error {
	if err := validateLocalPodPlan(p); err != nil {
		return err
	}
	s := p.Spec
	cp := &unstructured.Unstructured{}
	cp.SetAPIVersion("fluidcr.dcnlab.com/v1alpha1")
	cp.SetKind("FluidCRMigration")
	if err := reader.Get(ctx, client.ObjectKey{Namespace: p.Namespace, Name: s.CheckpointRef.Name}, cp); err != nil {
		return fmt.Errorf("local checkpoint unavailable: %w", err)
	}
	if string(cp.GetUID()) != s.CheckpointRef.UID || cp.GetGeneration() != s.CheckpointRef.Generation || !cp.GetDeletionTimestamp().IsZero() || cp.GetAnnotations()["training.dcnlab.com/checkpoint-id"] != s.CheckpointRef.CheckpointID {
		return fmt.Errorf("local checkpoint identity or generation mismatch")
	}
	resume, found, err := unstructured.NestedBool(cp.Object, "spec", "resume")
	if err != nil || !found || resume {
		return fmt.Errorf("local checkpoint must explicitly set resume=false")
	}
	for key, expected := range map[string]string{"apiVersion": "v1", "kind": "Pod", "name": s.WorkloadRef.Name, "uid": s.WorkloadRef.UID} {
		got, _, _ := unstructured.NestedString(cp.Object, "spec", "workloadRef", key)
		if got != expected {
			return fmt.Errorf("local checkpoint workloadRef.%s mismatch", key)
		}
	}
	phase, _, _ := unstructured.NestedString(cp.Object, "status", "phase")
	gen, _, _ := unstructured.NestedInt64(cp.Object, "status", "observedGeneration")
	if phase != "Completed" || gen != cp.GetGeneration() {
		return fmt.Errorf("local checkpoint is not Completed at the current generation")
	}
	pods, _, err := unstructured.NestedSlice(cp.Object, "status", "pods")
	if err != nil || len(pods) != 1 {
		return fmt.Errorf("local checkpoint must contain exactly one Pod report")
	}
	report, ok := pods[0].(map[string]interface{})
	m := s.Pods[0]
	if !ok || report["podUID"] != m.SourcePodUID || report["podName"] != m.SourcePod || report["nodeName"] != m.SourceNode || report["phase"] != "ContainerCheckpointed" {
		return fmt.Errorf("local checkpoint source Pod evidence mismatch or already resumed")
	}
	files, _, err := unstructured.NestedSlice(report, "checkpointFiles")
	if err != nil || len(files) != 1 {
		return fmt.Errorf("local checkpoint must contain exactly one archive")
	}
	f, ok := files[0].(map[string]interface{})
	a := m.Archives[0]
	if !ok || f["filePath"] != a.SourcePath || f["containerName"] != a.ContainerName || f["sha256"] != a.SHA256 || a.DurableRef == "" || f["durableRef"] != a.DurableRef {
		return fmt.Errorf("local checkpoint archive path, SHA256 or durableRef missing or mismatched; wait for artifact export")
	}
	return nil
}
