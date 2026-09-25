package v1alpha1

import (
	"encoding/json"
	"k8s.io/apimachinery/pkg/runtime"
	"testing"
)

func TestPodUIDSerializationAndClusterDeepCopy(t *testing.T) {
	original := &FluidCRMigration{Status: FluidCRMigrationStatus{
		Pods:     []PodMigrationStatus{{PodName: "p0", PodUID: "uid-1"}},
		Clusters: []ClusterMigrationStatus{{ClusterName: "member", Pods: []runtime.RawExtension{{Raw: []byte(`{"podUID":"uid-1"}`)}}}},
	}}
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	pod := decoded["status"].(map[string]any)["pods"].([]any)[0].(map[string]any)
	if pod["podUID"] != "uid-1" {
		t.Fatalf("podUID not serialized correctly: %s", data)
	}
	copied := original.DeepCopy()
	copied.Status.Clusters[0].Pods[0].Raw[0] = 'x'
	if original.Status.Clusters[0].Pods[0].Raw[0] != '{' {
		t.Fatal("cluster raw status aliases original")
	}
}
