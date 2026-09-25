package interpreter_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	lua "github.com/yuin/gopher-lua"
	"sigs.k8s.io/yaml"
)

var ricNames = []string{"fluidcrmigration", "restoreplan"}

func ricDocument(t *testing.T, name string) map[string]any {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "config", "karmada", "ric", name+"_resource_interpreter.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := yaml.Unmarshal(b, &doc); err != nil {
		t.Fatal(err)
	}
	return doc
}

func ricCall(t *testing.T, name, operation, function string, args ...any) any {
	t.Helper()
	doc := ricDocument(t, name)
	script := doc["spec"].(map[string]any)["customizations"].(map[string]any)[operation].(map[string]any)["luaScript"].(string)
	L := lua.NewState()
	defer L.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	L.SetContext(ctx)
	if err := L.DoString(script); err != nil {
		t.Fatal(err)
	}
	values := make([]lua.LValue, len(args))
	for i, arg := range args {
		// Match Karmada's JSON number representation.
		b, err := json.Marshal(arg)
		if err != nil {
			t.Fatal(err)
		}
		var normalized any
		if err := json.Unmarshal(b, &normalized); err != nil {
			t.Fatal(err)
		}
		values[i] = ricToLua(L, normalized)
	}
	if err := L.CallByParam(lua.P{Fn: L.GetGlobal(function), NRet: 1, Protect: true}, values...); err != nil {
		t.Fatal(err)
	}
	return ricFromLua(L.Get(-1))
}

func ricToLua(L *lua.LState, value any) lua.LValue {
	switch v := value.(type) {
	case nil:
		return lua.LNil
	case bool:
		return lua.LBool(v)
	case string:
		return lua.LString(v)
	case float64:
		return lua.LNumber(v)
	case []any:
		out := L.NewTable()
		for _, item := range v {
			out.Append(ricToLua(L, item))
		}
		return out
	case map[string]any:
		out := L.NewTable()
		for key, item := range v {
			out.RawSetString(key, ricToLua(L, item))
		}
		return out
	default:
		panic(fmt.Sprintf("unexpected JSON value %T", v))
	}
}

func ricFromLua(value lua.LValue) any {
	switch v := value.(type) {
	case *lua.LNilType:
		return nil
	case lua.LBool:
		return bool(v)
	case lua.LString:
		return string(v)
	case lua.LNumber:
		return float64(v)
	case *lua.LTable:
		// Karmada serializes empty Lua tables as objects, never empty arrays.
		if v.Len() > 0 {
			out := make([]any, v.Len())
			for i := range out {
				out[i] = ricFromLua(v.RawGetInt(i + 1))
			}
			return out
		}
		out := map[string]any{}
		v.ForEach(func(k, v lua.LValue) { out[k.String()] = ricFromLua(v) })
		return out
	default:
		panic(fmt.Sprintf("unexpected Lua value %T", v))
	}
}

func ricDesired(name string) map[string]any {
	kind, api := "RestorePlan", "migration.dcnlab.com/v1alpha1"
	if name == "fluidcrmigration" {
		kind, api = "FluidCRMigration", "fluidcr.dcnlab.com/v1alpha1"
	}
	return map[string]any{
		"apiVersion": api, "kind": kind,
		"metadata": map[string]any{"name": "test", "generation": 1},
		"spec":     map[string]any{"sourceCluster": "source", "targetCluster": "target", "checkpointRef": map[string]any{"generation": 9}},
		"status": map[string]any{"phase": "ManagementOwned", "observedGeneration": 1, "message": "local message", "artifacts": []any{map[string]any{"nodeName": "keep", "verified": true}},
			"clusters": []any{map[string]any{"clusterName": "old", "phase": "Completed", "observedGeneration": 1}}},
	}
}

func ricItem(cluster string, generation any, phase string) map[string]any {
	return map[string]any{"clusterName": cluster, "status": map[string]any{"observedGeneration": generation, "phase": phase, "message": cluster + " report"}}
}

func ricAggregate(t *testing.T, name string, desired map[string]any, reports any) map[string]any {
	t.Helper()
	got := ricCall(t, name, "statusAggregation", "AggregateStatus", desired, reports).(map[string]any)
	if got["kind"] != desired["kind"] || !reflect.DeepEqual(got["metadata"], ricNormalize(desired["metadata"])) ||
		!reflect.DeepEqual(got["spec"], ricNormalize(desired["spec"])) {
		t.Fatalf("whole desired object not preserved: %#v", got)
	}
	status := got["status"].(map[string]any)
	original, _ := desired["status"].(map[string]any)
	for key, value := range original {
		if key != "clusters" && !reflect.DeepEqual(status[key], ricNormalize(value)) {
			t.Fatalf("local status.%s overwritten: %#v", key, status)
		}
	}
	return status
}

func ricNormalize(value any) any {
	b, _ := json.Marshal(value)
	var out any
	_ = json.Unmarshal(b, &out)
	return out
}

func TestRICTargetsAndOperations(t *testing.T) {
	for _, name := range ricNames {
		doc := ricDocument(t, name)
		desired := ricDesired(name)
		if doc["apiVersion"] != "config.karmada.io/v1alpha1" || doc["kind"] != "ResourceInterpreterCustomization" {
			t.Fatal(doc)
		}
		spec := doc["spec"].(map[string]any)
		target := spec["target"].(map[string]any)
		if target["kind"] != desired["kind"] || target["apiVersion"] != desired["apiVersion"] {
			t.Fatal(target)
		}
		operations := spec["customizations"].(map[string]any)
		if len(operations) != 3 {
			t.Fatalf("unexpected mutation/deletion operations: %v", operations)
		}
	}
}

func TestRICHealthRequiresCurrentPositiveGeneration(t *testing.T) {
	for _, name := range ricNames {
		for _, phase := range []string{"Completed", "Prepared", "Running", "Pending", "Failed", ""} {
			for _, observed := range []any{nil, 0, 1, 2, "1"} {
				for _, generation := range []any{nil, 0, 1, 2, "1"} {
					t.Run(fmt.Sprintf("%s/%s/observed-%v/desired-%v", name, phase, observed, generation), func(t *testing.T) {
						obj := map[string]any{"metadata": map[string]any{"generation": generation}, "status": map[string]any{"observedGeneration": observed, "phase": phase}}
						success := name == "fluidcrmigration" && phase == "Completed" || name == "restoreplan" && (phase == "Prepared" || phase == "Running")
						g, valid := generation.(int)
						want := valid && g > 0 && observed == generation && success
						if got := ricCall(t, name, "healthInterpretation", "InterpretHealth", obj); got != want {
							t.Fatalf("got %v want %v", got, want)
						}
					})
				}
			}
		}
		for _, obj := range []any{nil, map[string]any{}, map[string]any{"metadata": map[string]any{"generation": 1}}} {
			if got := ricCall(t, name, "healthInterpretation", "InterpretHealth", obj); got != false {
				t.Fatalf("missing status healthy: %v", got)
			}
		}
	}
}

func TestRICReflectionPreservesStatusAndOmitsRecursiveAndEmptyLists(t *testing.T) {
	for _, name := range ricNames {
		status := map[string]any{
			"phase": "Completed", "observedGeneration": 1, "message": "checkpoint result",
			"startTime": "2026-09-25T00:00:00Z", "completionTime": "2026-09-25T00:01:00Z",
			"pods": []any{map[string]any{"podName": "db-0", "name": "db-0", "podUID": "source-uid", "uid": "target-uid", "nodeName": "node-a", "podIP": "10.0.0.1", "phase": "Completed", "message": "ok", "appCheckpointResult": "saved",
				"checkpointFiles": []any{map[string]any{"containerName": "db", "filePath": "/checkpoints/a.tar", "checkpointTime": "2026-09-25T00:00:30Z"}}}},
			"conditions": []any{map[string]any{"type": "Ready", "status": "True"}},
			"artifacts":  []any{map[string]any{"nodeName": "node-a", "verified": true}},
			"clusters":   []any{map[string]any{"clusterName": "recursive"}},
		}
		got := ricCall(t, name, "statusReflection", "ReflectStatus", map[string]any{"status": status}).(map[string]any)
		delete(status, "clusters")
		if !reflect.DeepEqual(got, ricNormalize(status)) {
			t.Fatalf("%s lost status fidelity: %#v", name, got)
		}
		empty := map[string]any{"status": map[string]any{"pods": []any{map[string]any{"name": "db-0", "checkpointFiles": []any{}}}, "conditions": []any{}, "artifacts": []any{}, "clusters": []any{}}}
		got = ricCall(t, name, "statusReflection", "ReflectStatus", empty).(map[string]any)
		if len(got) != 1 || len(got["pods"].([]any)[0].(map[string]any)) != 1 {
			t.Fatalf("empty lists serialized as objects: %#v", got)
		}
		for _, obj := range []any{nil, map[string]any{}, map[string]any{"status": map[string]any{}}} {
			if got := ricCall(t, name, "statusReflection", "ReflectStatus", obj); !reflect.DeepEqual(got, map[string]any{}) {
				t.Fatal(got)
			}
		}
	}
}

func TestRICAggregationRejectsMissingStaleDuplicateAndUnexpectedReports(t *testing.T) {
	for _, name := range ricNames {
		cases := []struct {
			name    string
			reports any
		}{
			{"nil", nil}, {"empty", []any{}},
			{"missing status", []any{map[string]any{"clusterName": "source"}}},
			{"empty status", []any{map[string]any{"clusterName": "source", "status": map[string]any{}}}},
			{"stale", []any{ricItem("source", 0, "Completed")}},
			{"future", []any{ricItem("source", 2, "Completed")}},
			{"checkpoint generation is not plan generation", []any{ricItem("source", 9, "Completed")}},
			{"string generation", []any{ricItem("source", "1", "Completed")}},
			{"unexpected", []any{ricItem("unexpected", 1, "Completed")}},
			{"empty cluster", []any{ricItem("", 1, "Completed")}},
			{"missing cluster", []any{map[string]any{"status": map[string]any{"observedGeneration": 1, "phase": "Completed"}}}},
			{"duplicate", []any{ricItem("source", 1, "Completed"), ricItem("source", 1, "Failed")}},
			{"duplicate missing", []any{ricItem("source", 1, "Completed"), map[string]any{"clusterName": "source"}}},
		}
		for _, tc := range cases {
			t.Run(name+"/"+tc.name, func(t *testing.T) {
				status := ricAggregate(t, name, ricDesired(name), tc.reports)
				if _, exists := status["clusters"]; exists {
					t.Fatalf("invalid reports reused as clusters: %#v", status)
				}
				b, _ := json.Marshal(status)
				if strings.Contains(string(b), "\"clusters\"") {
					t.Fatal(string(b))
				}
			})
		}
		for _, generation := range []any{nil, 0, "1"} {
			desired := ricDesired(name)
			if generation == nil {
				delete(desired["metadata"].(map[string]any), "generation")
			} else {
				desired["metadata"].(map[string]any)["generation"] = generation
			}
			status := ricAggregate(t, name, desired, []any{ricItem("source", 1, "Completed")})
			if _, exists := status["clusters"]; exists {
				t.Fatal(status)
			}
		}
	}
}

func TestRICAggregationKeepsSourceAndTargetSeparateAndDeterministic(t *testing.T) {
	for _, name := range ricNames {
		source := ricItem("source", 1, "Completed")
		target := ricItem("target", 1, "Failed")
		sourceStatus := source["status"].(map[string]any)
		sourceStatus["clusterName"] = "spoofed"
		sourceStatus["clusters"] = []any{map[string]any{"clusterName": "recursive"}}
		sourceStatus["pods"] = []any{map[string]any{"name": "db-0", "podName": "db-0", "uid": "source-id", "podUID": "source-id", "nodeName": "source-node", "phase": "Completed", "checkpointFiles": []any{map[string]any{"containerName": "db", "filePath": "/source/archive.tar", "checkpointTime": "2026-09-25T00:00:00Z"}}}}
		target["status"].(map[string]any)["pods"] = []any{map[string]any{"name": "db-0", "uid": "target-id", "phase": "Failed", "message": "waiting"}}
		reports := []any{target, ricItem("unrelated", 1, "Completed"), source}
		first := ricAggregate(t, name, ricDesired(name), reports)
		second := ricAggregate(t, name, ricDesired(name), []any{source, target, reports[1]})
		if !reflect.DeepEqual(first, second) {
			t.Fatalf("order-dependent result: %#v / %#v", first, second)
		}
		clusters := first["clusters"].([]any)
		if len(clusters) != 2 {
			t.Fatal(clusters)
		}
		for i, want := range []string{"source", "target"} {
			cluster := clusters[i].(map[string]any)
			if cluster["clusterName"] != want {
				t.Fatal(clusters)
			}
			if _, exists := cluster["clusters"]; exists {
				t.Fatalf("recursive aggregation: %v", cluster)
			}
			original := sourceStatus
			if i == 1 {
				original = target["status"].(map[string]any)
			}
			for _, key := range []string{"phase", "message", "observedGeneration", "pods"} {
				if !reflect.DeepEqual(cluster[key], ricNormalize(original[key])) {
					t.Fatalf("%s %s lost fidelity: %#v", want, key, cluster)
				}
			}
		}
		// A duplicate in one member cannot erase or substitute another member.
		status := ricAggregate(t, name, ricDesired(name), []any{source, source, target})
		remaining := status["clusters"].([]any)
		if len(remaining) != 1 || remaining[0].(map[string]any)["clusterName"] != "target" {
			t.Fatal(remaining)
		}
	}
}

func TestRICAggregationFreshFailuresReplacePreviousSuccess(t *testing.T) {
	for _, name := range ricNames {
		desired := ricDesired(name)
		desired["status"].(map[string]any)["clusters"] = []any{map[string]any{"clusterName": "source", "phase": "Completed", "observedGeneration": 1, "pods": []any{map[string]any{"name": "old"}}}}
		status := ricAggregate(t, name, desired, []any{ricItem("source", 1, "Failed")})
		cluster := status["clusters"].([]any)[0].(map[string]any)
		if cluster["phase"] != "Failed" || cluster["pods"] != nil {
			t.Fatalf("retained stale success: %#v", cluster)
		}
		fresh := ricItem("source", 1, "Pending")
		fresh["status"].(map[string]any)["pods"] = []any{}
		status = ricAggregate(t, name, desired, []any{fresh})
		if _, exists := status["clusters"].([]any)[0].(map[string]any)["pods"]; exists {
			t.Fatal(status)
		}
	}
}

func TestCheckpointWithoutClusterFieldsUsesBindingIdentity(t *testing.T) {
	desired := ricDesired("fluidcrmigration")
	desired["spec"] = map[string]any{"workloadRef": map[string]any{"name": "db"}}
	status := ricAggregate(t, "fluidcrmigration", desired, []any{ricItem("target", 1, "Failed"), ricItem("source", 1, "Completed")})
	clusters := status["clusters"].([]any)
	if len(clusters) != 2 || clusters[0].(map[string]any)["clusterName"] != "source" || clusters[1].(map[string]any)["clusterName"] != "target" {
		t.Fatal(clusters)
	}
}

func TestRICEmptyTableSerializationMatchesKarmada(t *testing.T) {
	L := lua.NewState()
	defer L.Close()
	b, err := json.Marshal(ricFromLua(L.NewTable()))
	if err != nil || string(b) != "{}" {
		t.Fatalf("empty Lua table = %s, %v", b, err)
	}
}
