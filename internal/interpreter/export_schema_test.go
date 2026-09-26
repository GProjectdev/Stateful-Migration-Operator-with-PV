package interpreter_test

import (
	"reflect"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"

	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema/pruning"
)

func TestExportedCheckpointEvidenceSurvivesMemberAndKarmadaSchema(t *testing.T) {
	crd := restoreLoadCRD(t, "fluidcrmigrations.yaml")
	structural, err := structuralschema.NewStructural(restoreSchema(t, crd))
	if err != nil {
		t.Fatal(err)
	}
	digest := strings.Repeat("a", 64)
	status := map[string]any{
		"observedGeneration": int64(1), "phase": "Completed",
		"pods": []any{map[string]any{
			"podName": "trainer-0", "podUID": "member-pod-uid", "nodeName": "node-1", "phase": "Resumed",
			"checkpointFiles": []any{map[string]any{
				"containerName": "trainer", "filePath": "/var/lib/kubelet/checkpoints/round.tar",
				"sha256": digest, "durableRef": "file-store:default/sha256/" + digest, "exportedAt": "2026-09-26T00:00:00Z",
			}},
		}},
	}
	obj := map[string]any{
		"apiVersion": "fluidcr.dcnlab.com/v1alpha1", "kind": "FluidCRMigration",
		"metadata": map[string]any{"name": "round", "namespace": "default", "generation": int64(1)},
		"spec":     map[string]any{"workloadRef": map[string]any{"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "trainer", "uid": "mgmt-sts-uid"}},
		"status":   status,
	}
	expected := runtime.DeepCopyJSONValue(status["pods"])
	pruning.Prune(obj, structural, true)
	if !reflect.DeepEqual(obj["status"].(map[string]any)["pods"], expected) {
		t.Fatal("member schema pruned durable archive evidence")
	}
	if obj["spec"].(map[string]any)["workloadRef"].(map[string]any)["uid"] != "mgmt-sts-uid" {
		t.Fatal("workload UID pruned")
	}
	restoreAssertValid(t, restoreNewValidator(t, crd), obj, nil)
	reflected := ricCall(t, "fluidcrmigration", "statusReflection", "ReflectStatus", obj)
	result := ricCall(t, "fluidcrmigration", "statusAggregation", "AggregateStatus", obj, []any{map[string]any{"clusterName": "aws", "status": reflected}}).(map[string]any)
	pruning.Prune(result, structural, true)
	clusters := result["status"].(map[string]any)["clusters"].([]any)
	if !reflect.DeepEqual(clusters[0].(map[string]any)["pods"], expected) {
		t.Fatal("RIC lost durable archive evidence")
	}
}
