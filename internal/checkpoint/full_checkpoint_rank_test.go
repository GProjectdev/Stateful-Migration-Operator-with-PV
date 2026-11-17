package checkpoint

import (
	"testing"

	api "github.com/GProjectdev/Stateful-Migration-Operator-with-PV/api/fluidcr/v1alpha1"
)

func TestFullStatefulCheckpointRetainsDistinctRanks(t *testing.T) {
	m := &api.FluidCRMigration{}
	m.Spec.WorkloadRef.Kind = "StatefulSet"
	m.Spec.WorkloadRef.Name = "trainer"
	for name, want := range map[string]int64{"trainer-0": 0, "trainer-1": 1, "trainer-12": 12} {
		got, err := resolvePodRank(m, newTestPod(name, "10.0.0.1", "10.0.0.2"))
		if err != nil || got != want {
			t.Fatalf("full checkpoint rank %s: got %d, err %v, want %d", name, got, err, want)
		}
	}
	if _, err := resolvePodRank(m, newTestPod("other-1", "10.0.0.1", "10.0.0.2")); err == nil {
		t.Fatal("foreign StatefulSet ordinal accepted")
	}
}
