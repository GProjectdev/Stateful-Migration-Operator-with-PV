package interpreter_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	api "github.com/GProjectdev/Stateful-Migration-Operator-with-PV/api/v1alpha1"
	apiextensions "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apivalidation "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/validation"
	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema/pruning"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

func TestRestoreSchemasCoverSharedAPITypes(t *testing.T) {
	for _, tc := range []struct {
		file string
		spec any
	}{
		{"restorerequests.yaml", api.RestoreRequestSpec{}},
		{"restoreplans.yaml", api.RestorePlanSpec{}},
	} {
		t.Run(tc.file, func(t *testing.T) {
			schema := restoreSchema(t, restoreLoadCRD(t, tc.file))
			schemaCheckType(t, "spec", reflect.TypeOf(tc.spec), schema.Properties["spec"])
			schemaCheckType(t, "status", reflect.TypeOf(api.RestoreStatus{}), schema.Properties["status"])
		})
	}
}

func TestCheckpointCRDStructuralCELAndStatusFidelity(t *testing.T) {
	crd := restoreLoadCRD(t, "fluidcrmigrations.yaml")
	if errs := apivalidation.ValidateCustomResourceDefinition(context.Background(), crd); len(errs) > 0 {
		t.Fatalf("checkpoint CRD structural/CEL validation: %v", errs.ToAggregate())
	}
	root := restoreSchema(t, crd)
	immutable := false
	for _, rule := range root.Properties["spec"].XValidations {
		if rule.Rule == "self == oldSelf" {
			immutable = true
		}
	}
	if !immutable {
		t.Fatal("checkpoint spec must be immutable")
	}
	structural, err := structuralschema.NewStructural(root)
	if err != nil {
		t.Fatal(err)
	}
	memberStatus := map[string]any{
		"observedGeneration": 1, "phase": "Completed", "message": "saved",
		"startTime": "2026-09-25T00:00:00Z", "completionTime": "2026-09-25T00:01:00Z",
		"pods": []any{map[string]any{
			"podName": "db-0", "podUID": "source-uid", "nodeName": "source-node",
			"podIP": "10.0.0.1", "phase": "Completed", "message": "saved", "appCheckpointResult": "ok",
			"checkpointFiles": []any{map[string]any{"containerName": "db", "filePath": "/var/lib/kubelet/checkpoints/a.tar", "checkpointTime": "2026-09-25T00:00:30Z"}},
		}},
	}
	reflected := ricCall(t, "fluidcrmigration", "statusReflection", "ReflectStatus", map[string]any{"status": memberStatus})
	desired := map[string]any{
		"apiVersion": "fluidcr.dcnlab.com/v1alpha1", "kind": "FluidCRMigration",
		"metadata": map[string]any{"name": "checkpoint", "namespace": "default", "generation": 1},
		"spec":     map[string]any{"workloadRef": map[string]any{"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "db"}},
	}
	result := ricCall(t, "fluidcrmigration", "statusAggregation", "AggregateStatus", desired, []any{
		map[string]any{"clusterName": "source", "status": reflected},
		map[string]any{"clusterName": "target", "status": map[string]any{"observedGeneration": 1, "phase": "Failed", "message": "target only"}},
	})
	data, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	var obj unstructured.Unstructured
	if err := obj.UnmarshalJSON(data); err != nil {
		t.Fatal(err)
	}
	before := obj.DeepCopy()
	pruning.Prune(obj.Object, structural, true)
	if !reflect.DeepEqual(before.Object, obj.Object) {
		t.Fatalf("checkpoint aggregate schema prunes reflected fields: before=%s after=%v", data, obj.Object)
	}
	restoreAssertValid(t, restoreNewValidator(t, crd), obj.Object, nil)
}

func schemaCheckType(t *testing.T, path string, typ reflect.Type, schema apiextensions.JSONSchemaProps) {
	t.Helper()
	if typ == reflect.TypeOf(metav1.Time{}) {
		if schema.Type != "string" || schema.Format != "date-time" {
			t.Fatalf("%s must be date-time", path)
		}
		return
	}
	switch typ.Kind() {
	case reflect.Pointer:
		schemaCheckType(t, path, typ.Elem(), schema)
	case reflect.Struct:
		if schema.Type != "object" {
			t.Fatalf("%s must be an object", path)
		}
		fields := map[string]bool{}
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			tag := strings.Split(field.Tag.Get("json"), ",")
			name := tag[0]
			if name == "" || name == "-" {
				continue
			}
			fields[name] = true
			child, ok := schema.Properties[name]
			if !ok {
				t.Fatalf("%s.%s is missing from schema", path, name)
			}
			optional := false
			for _, option := range tag[1:] {
				if option == "omitempty" {
					optional = true
				}
			}
			if !optional {
				required := false
				for _, item := range schema.Required {
					if item == name {
						required = true
					}
				}
				if !required {
					t.Fatalf("%s.%s must be required to match API", path, name)
				}
			}
			schemaCheckType(t, path+"."+name, field.Type, child)
		}
		for name := range schema.Properties {
			if !fields[name] {
				t.Fatalf("%s.%s is absent from shared API", path, name)
			}
		}
	case reflect.Slice:
		if schema.Type != "array" || schema.Items == nil || schema.Items.Schema == nil || schema.MaxItems == nil {
			t.Fatalf("%s must be a bounded array", path)
		}
		schemaCheckType(t, path+"[]", typ.Elem(), *schema.Items.Schema)
	case reflect.String:
		if schema.Type != "string" || schema.MaxLength == nil {
			t.Fatalf("%s must be a bounded string", path)
		}
	case reflect.Int64:
		if schema.Type != "integer" || schema.Format != "int64" {
			t.Fatalf("%s must be int64", path)
		}
	case reflect.Bool:
		if schema.Type != "boolean" {
			t.Fatalf("%s must be boolean", path)
		}
	default:
		t.Fatalf("unhandled API type %s at %s", typ, path)
	}
}

func TestRestorePlanActualLuaOutputPassesSchema(t *testing.T) {
	validator := restoreNewValidator(t, restoreLoadCRD(t, "restoreplans.yaml"))
	for _, reports := range []any{
		nil, []any{},
		[]any{ricItem("target", 1, "Prepared")},
		[]any{ricItem("source", 1, "Failed"), ricItem("target", 1, "Running")},
		[]any{ricItem("target", 0, "Running")},
		[]any{ricItem("target", 1, "Running"), ricItem("target", 1, "Prepared")},
	} {
		desired := restoreObject("restoreplans.yaml")
		desired["metadata"].(map[string]any)["generation"] = int64(1)
		result := ricCall(t, "restoreplan", "statusAggregation", "AggregateStatus", desired, reports)
		data, err := json.Marshal(result)
		if err != nil {
			t.Fatal(err)
		}
		var obj unstructured.Unstructured
		if err := obj.UnmarshalJSON(data); err != nil {
			t.Fatal(err)
		}
		restoreAssertValid(t, validator, obj.Object, nil)
	}
}

func TestRestoreRichVerificationStatusSurvivesSchema(t *testing.T) {
	richVerification := map[string]any{
		"requestUID":         "restore-request-uid",
		"operation":          "replacement-op",
		"checkpointID":       "round-a",
		"verifiedAt":         "2026-09-26T00:00:00Z",
		"trainingRuntimeRef": map[string]any{"name": "database-runtime", "uid": "runtime-uid"},
		"sourceCluster":      "source",
		"targetCluster":      "target",
		"sourceFenced":       true,
		"sourceFence":        map[string]any{"fenced": true, "operation": "replacement-op", "evidenceID": "fence-1", "observedAt": "2026-09-26T00:00:01Z"},
		"partialRestore": map[string]any{
			"preventPeriodicResume": true,
			"targetRanks": []any{map[string]any{
				"rank": int64(0), "targetPodUID": "target-pod-uid", "checkpointID": "round-a", "archiveEvidenceID": "archive-1",
			}},
		},
		"preservedSurvivors": []any{map[string]any{
			"rank": int64(1), "podName": "database-1", "podUID": "survivor-pod-uid", "nodeName": "node-b",
			"generation": int64(7), "pauseLockPath": "/var/lib/checkpoints/database-1/pause-lock", "observedAt": "2026-09-26T00:00:02Z",
		}},
		"survivors": []any{map[string]any{
			"rank": int64(1), "podUID": "survivor-pod-uid",
			"stateEvidence": map[string]any{"kind": "pause-lock", "observedAt": "2026-09-26T00:00:03Z"},
		}},
	}

	for _, file := range []string{"restorerequests.yaml", "restoreplans.yaml"} {
		t.Run(file, func(t *testing.T) {
			crd := restoreLoadCRD(t, file)
			structural, err := structuralschema.NewStructural(restoreSchema(t, crd))
			if err != nil {
				t.Fatal(err)
			}
			obj := restoreObject(file)
			obj["status"].(map[string]any)["verification"] = runtime.DeepCopyJSONValue(richVerification)
			expected := runtime.DeepCopyJSONValue(richVerification)

			pruning.Prune(obj, structural, true)
			got := obj["status"].(map[string]any)["verification"]
			if !reflect.DeepEqual(got, expected) {
				t.Fatalf("rich verification pruned: got=%#v want=%#v", got, expected)
			}
			restoreAssertValid(t, restoreNewValidator(t, crd), obj, nil)
		})
	}
}

func TestSystemSpotReplacementRestoreFixtureSurvivesRestoreRequestSchema(t *testing.T) {
	fixture := filepath.Join("..", "..", "..", "System", "docs", "fixtures", "spot-replacement-restore-request.json")
	data, err := os.ReadFile(fixture)
	if err != nil {
		t.Fatal(err)
	}
	var obj map[string]any
	if err := json.Unmarshal(data, &obj); err != nil {
		t.Fatal(err)
	}
	if obj["apiVersion"] != "migration.dcnlab.com/v1alpha1" || obj["kind"] != "RestoreRequest" {
		t.Fatalf("fixture identity = %v/%v, want migration RestoreRequest", obj["apiVersion"], obj["kind"])
	}
	archiveBefore := restoreFirstFixtureArchive(t, obj)
	if _, exists := archiveBefore["filePath"]; exists {
		t.Fatalf("System fixture uses filePath, which is pruned by RestoreRequest CRD: %#v", archiveBefore)
	}
	if archiveBefore["archiveEvidenceID"] == nil && (archiveBefore["sourcePath"] == nil || archiveBefore["targetPath"] == nil) {
		t.Fatalf("System fixture archive lacks admitted locator evidence before pruning: %#v", archiveBefore)
	}

	crd := restoreLoadCRD(t, "restorerequests.yaml")
	structural, err := structuralschema.NewStructural(restoreSchema(t, crd))
	if err != nil {
		t.Fatal(err)
	}
	pruning.Prune(obj, structural, true)
	restoreAssertValid(t, restoreNewValidator(t, crd), obj, nil)

	data, err = json.Marshal(obj)
	if err != nil {
		t.Fatal(err)
	}
	var typed api.RestoreRequest
	if err := json.Unmarshal(data, &typed); err != nil {
		t.Fatal(err)
	}
	if len(typed.Spec.Pods) == 0 || len(typed.Spec.Pods[0].Archives) == 0 {
		t.Fatal("typed RestoreRequest lost pod archive evidence")
	}
	archive := typed.Spec.Pods[0].Archives[0]
	if archive.ArchiveEvidenceID == "" && (archive.SourcePath == "" || archive.TargetPath == "") {
		t.Fatalf("typed RestoreRequest archive lacks admitted locator evidence: %#v", archive)
	}
}

func restoreFirstFixtureArchive(t *testing.T, obj map[string]any) map[string]any {
	t.Helper()
	spec, ok := obj["spec"].(map[string]any)
	if !ok {
		t.Fatal("fixture spec missing")
	}
	pods, ok := spec["pods"].([]any)
	if !ok || len(pods) == 0 {
		t.Fatal("fixture pods missing")
	}
	pod, ok := pods[0].(map[string]any)
	if !ok {
		t.Fatalf("fixture pod shape = %#v", pods[0])
	}
	archives, ok := pod["archives"].([]any)
	if !ok || len(archives) == 0 {
		t.Fatal("fixture archives missing")
	}
	archive, ok := archives[0].(map[string]any)
	if !ok {
		t.Fatalf("fixture archive shape = %#v", archives[0])
	}
	return archive
}
