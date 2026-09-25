package interpreter_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	apiextensions "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsvalidation "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/validation"
	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema/cel"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema/listtype"
	apiservervalidation "k8s.io/apiextensions-apiserver/pkg/apiserver/validation"
	"k8s.io/apimachinery/pkg/runtime"
	celconfig "k8s.io/apiserver/pkg/apis/cel"
	"sigs.k8s.io/yaml"
)

type restoreCRDValidator struct {
	structural   *structuralschema.Structural
	openAPI      apiservervalidation.SchemaValidator
	celValidator *cel.Validator
}

func TestRestoreCRDsAreStructuralAndValidateObjects(t *testing.T) {
	for _, file := range []string{"restorerequests.yaml", "restoreplans.yaml"} {
		t.Run(file, func(t *testing.T) {
			crd := restoreLoadCRD(t, file)
			if errs := apiextensionsvalidation.ValidateCustomResourceDefinition(context.Background(), crd); len(errs) > 0 {
				t.Fatalf("CRD is not structurally valid: %v", errs.ToAggregate())
			}

			validator := restoreNewValidator(t, crd)
			valid := restoreObject(file)
			restoreAssertValid(t, validator, valid, nil)
			restoreAssertValid(t, validator, restoreRICClusterStatusObject(file), nil)
			restoreAssertValid(t, validator, restoreRealKubeletPathObject(file), nil)

			cases := []struct {
				name string
				edit func(map[string]any)
				want string
			}{
				{
					name: "positive checkpoint generation",
					edit: func(obj map[string]any) { restoreMap(t, obj, "spec", "checkpointRef")["generation"] = int64(0) },
					want: "greater than or equal to 1",
				},
				{
					name: "only statefulset or pod workload refs",
					edit: func(obj map[string]any) {
						ref := restoreMap(t, obj, "spec", "workloadRef")
						ref["apiVersion"] = "apps/v1"
						ref["kind"] = "Pod"
					},
					want: "workloadRef must target apps/v1 StatefulSet or v1 Pod",
				},
				{
					name: "pods are keyed by targetPod",
					edit: func(obj map[string]any) {
						pods := restoreSlice(t, obj, "spec", "pods")
						pod := restoreCloneMap(t, pods[0].(map[string]any))
						pod["sourcePod"] = "source-1"
						restoreMap(t, obj, "spec")["pods"] = append(pods, pod)
					},
					want: "Duplicate value",
				},
				{
					name: "archives are keyed by containerName",
					edit: func(obj map[string]any) {
						archives := restoreSlice(t, obj, "spec", "pods", "archives")
						archive := restoreCloneMap(t, archives[0].(map[string]any))
						archive["sourcePath"] = "/var/lib/checkpoints/other.tar"
						restoreMap(t, restoreSlice(t, obj, "spec", "pods")[0].(map[string]any))["archives"] = append(archives, archive)
					},
					want: "Duplicate value",
				},
				{
					name: "lowercase sha256 hex64",
					edit: func(obj map[string]any) {
						restoreMap(t, obj, "spec", "pods", "archives")["sha256"] = strings.Repeat("A", 64)
					},
					want: "should match",
				},
				{
					name: "safe absolute source paths",
					edit: func(obj map[string]any) {
						restoreMap(t, obj, "spec", "pods", "archives")["sourcePath"] = "/var/lib/../escape.tar"
					},
					want: "should match",
				},
				{
					name: "safe absolute target paths",
					edit: func(obj map[string]any) {
						restoreMap(t, obj, "spec", "pods", "archives")["targetPath"] = "var/lib/kubelet/checkpoints/relative.tar"
					},
					want: "should match",
				},
				{
					name: "artifacts are keyed by nodeName",
					edit: func(obj map[string]any) {
						artifacts := restoreSlice(t, obj, "status", "artifacts")
						restoreMap(t, obj, "status")["artifacts"] = append(artifacts, restoreCloneMap(t, artifacts[0].(map[string]any)))
					},
					want: "Duplicate value",
				},
				{
					name: "bounded pod count",
					edit: func(obj map[string]any) {
						pods := make([]any, 65)
						base := restoreSlice(t, obj, "spec", "pods")[0].(map[string]any)
						for i := range pods {
							pod := restoreCloneMap(t, base)
							pod["sourcePod"] = fmt.Sprintf("source-%d", i)
							pod["targetPod"] = fmt.Sprintf("target-%d", i)
							pods[i] = pod
						}
						restoreMap(t, obj, "spec")["pods"] = pods
					},
					want: "must have at most 64 items",
				},
			}

			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					obj := restoreObject(file)
					tc.edit(obj)
					restoreAssertInvalidContains(t, validator, obj, nil, tc.want)
				})
			}

			updated := restoreObject(file)
			restoreMap(t, updated, "spec")["volumesReady"] = false
			restoreAssertInvalidContains(t, validator, updated, valid, "spec is immutable after creation")
		})
	}
}

func TestRestoreRequestAndPlanRequireClusters(t *testing.T) {
	requestValidator := restoreNewValidator(t, restoreLoadCRD(t, "restorerequests.yaml"))
	request := restoreObject("restorerequests.yaml")
	delete(restoreMap(t, request, "spec"), "sourceCluster")
	restoreAssertInvalidContains(t, requestValidator, request, nil, "sourceCluster: Required value")

	planValidator := restoreNewValidator(t, restoreLoadCRD(t, "restoreplans.yaml"))
	plan := restoreObject("restoreplans.yaml")
	delete(restoreMap(t, plan, "spec"), "sourceCluster")
	restoreAssertInvalidContains(t, planValidator, plan, nil, "sourceCluster: Required value")
}

func TestRestoreCRDPathHygieneAndPodSupport(t *testing.T) {
	for _, file := range []string{"restorerequests.yaml", "restoreplans.yaml"} {
		validator := restoreNewValidator(t, restoreLoadCRD(t, file))
		pod := restoreObject(file)
		ref := restoreMap(t, pod, "spec", "workloadRef")
		ref["apiVersion"], ref["kind"] = "v1", "Pod"
		restoreAssertValid(t, validator, pod, nil)
		for _, key := range []string{"sourcePath", "targetPath"} {
			for _, bad := range []string{"../escape.tar", "/var/lib/kubelet/checkpoints/../escape.tar", "/var/lib/kubelet/checkpoints/./a.tar", "/var/lib/kubelet/checkpoints//a.tar", "/var/lib/kubelet/checkpoints/a.tar/", "/var/lib/kubelet/checkpoints/a\x00.tar", "/var/lib/kubelet/checkpoints/a\n.tar", "/var/lib/kubelet/checkpoints/a\r.tar", "/var/lib/kubelet/checkpoints/a\\b.tar"} {
				obj := restoreObject(file)
				restoreMap(t, obj, "spec", "pods", "archives")[key] = bad
				restoreAssertInvalidContains(t, validator, obj, nil, key)
			}
		}
	}
}

func TestRestoreCRDKustomizationIncludesAllCRDs(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "config", "crd", "kustomization.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var kustomization struct {
		Resources []string `json:"resources"`
	}
	if err := yaml.Unmarshal(data, &kustomization); err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{"fluidcrmigrations.yaml": false, "restorerequests.yaml": false, "restoreplans.yaml": false}
	for _, resource := range kustomization.Resources {
		if _, ok := want[resource]; ok {
			want[resource] = true
		}
	}
	for resource, found := range want {
		if !found {
			t.Fatalf("kustomization.yaml missing %s; resources=%v", resource, kustomization.Resources)
		}
	}
}

func restoreLoadCRD(t *testing.T, file string) *apiextensions.CustomResourceDefinition {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "config", "crd", file))
	if err != nil {
		t.Fatal(err)
	}
	var external apiextensionsv1.CustomResourceDefinition
	if err := yaml.Unmarshal(data, &external); err != nil {
		t.Fatal(err)
	}
	scheme := runtime.NewScheme()
	if err := apiextensions.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := apiextensionsv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	scheme.Default(&external)
	for _, version := range external.Spec.Versions {
		if version.Storage {
			external.Status.StoredVersions = []string{version.Name}
		}
	}
	internal := &apiextensions.CustomResourceDefinition{}
	if err := scheme.Convert(&external, internal, nil); err != nil {
		t.Fatal(err)
	}
	return internal
}

func restoreNewValidator(t *testing.T, crd *apiextensions.CustomResourceDefinition) restoreCRDValidator {
	t.Helper()
	schema := restoreSchema(t, crd)
	openAPI, _, err := apiservervalidation.NewSchemaValidator(schema)
	if err != nil {
		t.Fatal(err)
	}
	structural, err := structuralschema.NewStructural(schema)
	if err != nil {
		t.Fatal(err)
	}
	return restoreCRDValidator{structural: structural, openAPI: openAPI, celValidator: cel.NewValidator(structural, true, celconfig.PerCallLimit)}
}

func restoreSchema(t *testing.T, crd *apiextensions.CustomResourceDefinition) *apiextensions.JSONSchemaProps {
	t.Helper()
	for i := range crd.Spec.Versions {
		if crd.Spec.Versions[i].Schema != nil && crd.Spec.Versions[i].Schema.OpenAPIV3Schema != nil {
			return crd.Spec.Versions[i].Schema.OpenAPIV3Schema
		}
	}
	if crd.Spec.Validation != nil && crd.Spec.Validation.OpenAPIV3Schema != nil {
		return crd.Spec.Validation.OpenAPIV3Schema
	}
	t.Fatalf("CRD %s has no OpenAPI v3 schema", crd.Name)
	return nil
}

func restoreAssertValid(t *testing.T, validator restoreCRDValidator, obj, old any) {
	t.Helper()
	if errs := restoreValidate(validator, obj, old); len(errs) > 0 {
		t.Fatalf("unexpected validation errors: %v", errs)
	}
}

func restoreAssertInvalidContains(t *testing.T, validator restoreCRDValidator, obj, old any, want string) {
	t.Helper()
	errs := restoreValidate(validator, obj, old)
	for _, err := range errs {
		if strings.Contains(err, want) {
			return
		}
	}
	t.Fatalf("expected validation error containing %q, got %v", want, errs)
}

func restoreValidate(validator restoreCRDValidator, obj, old any) []string {
	var out []string
	for _, err := range apiservervalidation.ValidateCustomResource(nil, obj, validator.openAPI) {
		out = append(out, err.Error())
	}
	if object, ok := obj.(map[string]any); ok {
		for _, err := range listtype.ValidateListSetsAndMaps(nil, validator.structural, object) {
			out = append(out, err.Error())
		}
	}
	errs, _ := validator.celValidator.Validate(context.Background(), nil, validator.structural, obj, old, celconfig.RuntimeCELCostBudget)
	for _, err := range errs {
		out = append(out, err.Error())
	}
	return out
}

func restoreObject(file string) map[string]any {
	kind := "RestoreRequest"
	name := "restore-request"
	spec := map[string]any{}
	if file == "restoreplans.yaml" {
		kind = "RestorePlan"
		name = "restore-plan"
		spec["requestUID"] = "request-uid"
	}
	spec["checkpointRef"] = map[string]any{"name": "checkpoint-a", "uid": "checkpoint-uid", "generation": int64(1)}
	spec["workloadRef"] = map[string]any{"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "database"}
	spec["sourceCluster"] = "source"
	spec["targetCluster"] = "target"
	spec["sourceFenced"] = true
	spec["volumesReady"] = true
	spec["pods"] = []any{map[string]any{"sourcePod": "database-0", "targetPod": "database-0-restore", "targetNode": "node-a", "archives": []any{map[string]any{"containerName": "app", "sourcePath": "/var/lib/checkpoints/checkpoint.tar", "targetPath": "/var/lib/kubelet/checkpoints/checkpoint.tar", "sha256": strings.Repeat("a", 64)}}}}
	return map[string]any{"apiVersion": "migration.dcnlab.com/v1alpha1", "kind": kind, "metadata": map[string]any{"name": name, "namespace": "default"}, "spec": spec, "status": restoreStatus()}
}

func restoreRICClusterStatusObject(file string) map[string]any {
	obj := restoreObject(file)
	obj["status"] = map[string]any{"phase": "Aggregated", "observedGeneration": int64(1), "clusters": []any{map[string]any{"clusterName": "source", "observedGeneration": int64(1), "phase": "Completed", "message": "source report", "pods": []any{map[string]any{"name": "database-0", "uid": "source-uid", "phase": "Completed", "message": "ok"}}}, map[string]any{"clusterName": "target", "observedGeneration": int64(1), "phase": "Running", "message": "target report", "pods": []any{map[string]any{"name": "database-0-restore", "uid": "target-uid", "phase": "Running", "message": "starting"}}}}}
	return obj
}

func restoreRealKubeletPathObject(file string) map[string]any {
	obj := restoreObject(file)
	archive := obj["spec"].(map[string]any)["pods"].([]any)[0].(map[string]any)["archives"].([]any)[0].(map[string]any)
	path := "/var/lib/kubelet/checkpoints/checkpoint-fluidcr-train-pip_fluidcr-demo-trainer-2026-08-01T15:41:54+09:00.tar"
	archive["sourcePath"] = path
	archive["targetPath"] = path
	return obj
}

func restoreStatus() map[string]any {
	return map[string]any{"observedGeneration": int64(1), "phase": "Ready", "message": "ok", "planName": "restore-plan", "artifacts": []any{map[string]any{"nodeName": "node-a", "observedGeneration": int64(1), "verified": true, "message": "ok", "checkedAt": "2026-09-25T00:00:00Z"}}, "pods": []any{map[string]any{"name": "database-0-restore", "uid": "pod-uid", "phase": "Ready", "message": "ok"}}, "clusters": []any{map[string]any{"clusterName": "target", "observedGeneration": int64(1), "phase": "Ready", "message": "ok", "pods": []any{map[string]any{"name": "database-0-restore", "uid": "pod-uid", "phase": "Ready", "message": "ok"}}}}}
}

func restoreMap(t *testing.T, root any, path ...string) map[string]any {
	t.Helper()
	cur := root
	for _, segment := range path {
		switch typed := cur.(type) {
		case map[string]any:
			cur = typed[segment]
		case []any:
			if len(typed) == 0 {
				t.Fatalf("empty slice while walking %v", path)
			}
			cur = typed[0].(map[string]any)[segment]
		default:
			t.Fatalf("path %v hit %T", path, cur)
		}
	}
	if items, ok := cur.([]any); ok && len(items) > 0 {
		cur = items[0]
	}
	out, ok := cur.(map[string]any)
	if !ok {
		t.Fatalf("path %v is %T, not map", path, cur)
	}
	return out
}

func restoreSlice(t *testing.T, root any, path ...string) []any {
	t.Helper()
	cur := root
	for _, segment := range path {
		switch typed := cur.(type) {
		case map[string]any:
			cur = typed[segment]
		case []any:
			if len(typed) == 0 {
				t.Fatalf("empty slice while walking %v", path)
			}
			cur = typed[0].(map[string]any)[segment]
		default:
			t.Fatalf("path %v hit %T", path, cur)
		}
	}
	out, ok := cur.([]any)
	if !ok {
		t.Fatalf("path %v is %T, not slice", path, cur)
	}
	return out
}

func restoreCloneMap(t *testing.T, in map[string]any) map[string]any {
	t.Helper()
	return runtime.DeepCopyJSONValue(in).(map[string]any)
}
