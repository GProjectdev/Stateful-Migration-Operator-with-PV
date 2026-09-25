package deployment

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
)

func TestTwoReplicaExampleContracts(t *testing.T) {
	files, err := filepath.Glob(filepath.Join("..", "..", "config", "samples", "two-replica", "*.yaml"))
	if err != nil || len(files) == 0 {
		t.Fatalf("two-replica examples missing: %v", err)
	}
	counts := map[string]int{}
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		decoder := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(data), 4096)
		for {
			var raw map[string]interface{}
			if err := decoder.Decode(&raw); err == io.EOF {
				break
			} else if err != nil {
				t.Fatalf("%s: %v", file, err)
			}
			if len(raw) == 0 {
				continue
			}
			obj := &unstructured.Unstructured{Object: raw}
			counts[obj.GetKind()]++
			switch obj.GetKind() {
			case "StatefulSet":
				n, _, _ := unstructured.NestedFieldNoCopy(raw, "spec", "replicas")
				if n != int64(2) && n != float64(2) {
					t.Fatalf("%s: expected exactly two replicas", file)
				}
				for _, field := range []string{"whenDeleted", "whenScaled"} {
					v, _, _ := unstructured.NestedString(raw, "spec", "persistentVolumeClaimRetentionPolicy", field)
					if v != "Retain" {
						t.Fatalf("%s: PVC retention %s must be Retain", file, field)
					}
				}
			case "PVMigration", "RestoreRequest":
				fenced, _, _ := unstructured.NestedBool(raw, "spec", "sourceFenced")
				if fenced {
					t.Fatalf("%s: sample must not attest source fencing", file)
				}
				field := "volumes"
				if obj.GetKind() == "RestoreRequest" {
					field = "pods"
					ready, _, _ := unstructured.NestedBool(raw, "spec", "volumesReady")
					if ready {
						t.Fatalf("%s: sample must not attest volume readiness", file)
					}
				}
				items, _, _ := unstructured.NestedSlice(raw, "spec", field)
				if len(items) != 2 {
					t.Fatalf("%s: need both ordinal mappings", file)
				}
			case "PersistentVolume":
				policy, _, _ := unstructured.NestedString(raw, "spec", "persistentVolumeReclaimPolicy")
				if policy != "Retain" {
					t.Fatalf("%s: PV must retain source data", file)
				}
			case "FluidCRMigration":
				resume, found, _ := unstructured.NestedBool(raw, "spec", "resume")
				if !found || resume {
					t.Fatalf("%s: checkpoint must explicitly keep source paused", file)
				}
			}
		}
	}
	for _, kind := range []string{"StatefulSet", "PVMigration", "RestoreRequest", "FluidCRMigration"} {
		if counts[kind] != 1 {
			t.Fatalf("expected one %s example; found %d", kind, counts[kind])
		}
	}
}
