package management

import (
	"encoding/json"
	"testing"

	fluidcr "github.com/GProjectdev/Stateful-Migration-Operator-with-PV/api/fluidcr/v1alpha1"
	api "github.com/GProjectdev/Stateful-Migration-Operator-with-PV/api/v1alpha1"
	"k8s.io/apimachinery/pkg/runtime"
)

func TestSurvivorRankSurvivesWireRoundTrip(t *testing.T) {
	for _, rank := range []int64{0, 1} {
		status := fluidcr.PodMigrationStatus{
			Rank: rank, PodName: "survivor", PodUID: "uid", NodeName: "node",
			Phase: fluidcr.PodPhaseSurvivorPaused,
			SurvivorEvidence: &fluidcr.SurvivorEvidence{
				Generation: 7, PauseLockPath: "/checkpoint/survivor/pause-lock",
				PauseLockPID: 42, ObservedAt: "2026-09-28T18:31:41Z",
			},
		}
		want := api.SurvivorEvidence{Rank: rank, PodUID: "uid", NodeName: "node",
			Generation: 7, PauseLockPath: "/checkpoint/survivor/pause-lock"}
		raw, err := json.Marshal(status)
		if err != nil {
			t.Fatal(err)
		}
		var wire map[string]interface{}
		if err := json.Unmarshal(raw, &wire); err != nil {
			t.Fatal(err)
		}
		if _, ok := wire["rank"]; !ok {
			t.Fatalf("rank %d omitted: %s", rank, raw)
		}
		// API unstructured objects use int64 rather than JSON's float64 numbers.
		var decoded fluidcr.PodMigrationStatus
		if err := json.Unmarshal(raw, &decoded); err != nil {
			t.Fatal(err)
		}
		obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&decoded)
		if err != nil {
			t.Fatal(err)
		}
		if err := validateSurvivorCheckpointEvidence(obj, want); err != nil {
			t.Fatalf("rank %d: %v", rank, err)
		}
		delete(obj, "rank")
		if err := validateSurvivorCheckpointEvidence(obj, want); err == nil {
			t.Fatal("missing rank accepted")
		}
		obj["rank"] = rank + 1
		if err := validateSurvivorCheckpointEvidence(obj, want); err == nil {
			t.Fatal("wrong rank accepted")
		}
	}
}
