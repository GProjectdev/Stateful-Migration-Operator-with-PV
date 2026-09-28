package server

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	spec "github.com/opencontainers/runtime-spec/specs-go"
	types "k8s.io/cri-api/pkg/apis/runtime/v1"
	"tags.cncf.io/container-device-interface/pkg/cdi"
)

type fakeRestoreCDI struct {
	names      []string
	refreshErr error
	injectErr  error
}

func (f *fakeRestoreCDI) Refresh() error { return f.refreshErr }
func (f *fakeRestoreCDI) InjectDevices(s *spec.Spec, names ...string) ([]string, error) {
	f.names = names
	s.Mounts = []spec.Mount{{Destination: "/usr/lib/libcuda.so.1", Source: "/trusted/libcuda.so.1", Options: []string{"ro", "bind"}}}
	return nil, f.injectErr
}
func TestRestoreCDIAllocation(t *testing.T) {
	for _, annotationOnly := range []bool{false, true} {
		request := &types.ContainerConfig{Annotations: map[string]string{"cdi.k8s.io/device": "test.example/gpu=new"}}
		if !annotationOnly {
			request.CDIDevices = []*types.CDIDevice{{Name: "test.example/gpu=new"}}
		}
		original := map[string]string{"cdi.k8s.io/stale": "test.example/gpu=old", "keep": "value"}
		restored := &types.ContainerConfig{Annotations: original}
		registry := &fakeRestoreCDI{}
		mounts, err := prepareRestoreCDIWithRegistry(request, restored, registry)
		if err != nil {
			t.Fatal(err)
		}
		if !mounts["/usr/lib/libcuda.so.1"] || mounts["/etc/shadow"] {
			t.Fatal(mounts)
		}
		if !reflect.DeepEqual(registry.names, []string{"test.example/gpu=new"}) {
			t.Fatal(registry.names)
		}
		if len(restored.CDIDevices) != 1 || restored.CDIDevices[0].Name != "test.example/gpu=new" {
			t.Fatal(restored)
		}
		if restored.Annotations["cdi.k8s.io/stale"] != "" || restored.Annotations["keep"] != "value" {
			t.Fatal(restored.Annotations)
		}
		if original["cdi.k8s.io/stale"] == "" {
			t.Fatal("mutated input annotations")
		}
	}
}
func TestRestoreCDIRejectsUnsafeInputs(t *testing.T) {
	for _, name := range []string{"unresolved", "refresh", "conflict", "empty", "malformed"} {
		t.Run(name, func(t *testing.T) {
			request := &types.ContainerConfig{CDIDevices: []*types.CDIDevice{{Name: "test.example/gpu=new"}}}
			registry := &fakeRestoreCDI{}
			switch name {
			case "unresolved":
				registry.injectErr = errors.New("not allocated")
			case "refresh":
				registry.refreshErr = errors.New("invalid spec")
			case "conflict":
				request.Mounts = []*types.Mount{{ContainerPath: "/usr/lib/libcuda.so.1", HostPath: "/etc/shadow"}}
			case "empty":
				request.CDIDevices = []*types.CDIDevice{nil}
			case "malformed":
				request.Annotations = map[string]string{"cdi.k8s.io/device": "not-qualified"}
			}
			if _, err := prepareRestoreCDIWithRegistry(request, &types.ContainerConfig{}, registry); err == nil {
				t.Fatal("accepted unsafe request")
			}
		})
	}
}
func TestRestoreCDINoArchiveAllocation(t *testing.T) {
	restored := &types.ContainerConfig{Annotations: map[string]string{"cdi.k8s.io/device": "test.example/gpu=old"}}
	registry := &fakeRestoreCDI{}
	mounts, err := prepareRestoreCDIWithRegistry(&types.ContainerConfig{}, restored, registry)
	if err != nil || len(mounts) != 0 || len(restored.CDIDevices) != 0 || len(registry.names) != 0 || len(restored.Annotations) != 0 {
		t.Fatalf("%v %v", mounts, err)
	}
}

// Exercise the actual CDI resolver without a GPU, driver or privileged hook.
func TestRestoreCDIRealRegistry(t *testing.T) {
	dir := t.TempDir()
	data := `{"cdiVersion":"0.6.0","kind":"test.example/gpu","containerEdits":{"env":["CDI_TEST=1"],"mounts":[{"hostPath":"/trusted/libcuda.so.1","containerPath":"/usr/lib/libcuda.so.1","options":["ro","bind"]}],"hooks":[{"hookName":"createContainer","path":"/trusted/hook","args":["hook","restore"]}]},"devices":[{"name":"new","containerEdits":{"env":["GPU_TEST=new"]}},{"name":"other","containerEdits":{"mounts":[{"hostPath":"/trusted/other","containerPath":"/other-device","options":["ro","bind"]}]}}]}`
	if err := os.WriteFile(filepath.Join(dir, "gpu.json"), []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	registry, err := cdi.NewCache(cdi.WithSpecDirs(dir), cdi.WithAutoRefresh(false))
	if err != nil {
		t.Fatal(err)
	}
	request := &types.ContainerConfig{CDIDevices: []*types.CDIDevice{{Name: "test.example/gpu=new"}}}
	restored := &types.ContainerConfig{}
	allowed, err := prepareRestoreCDIWithRegistry(request, restored, registry)
	if err != nil {
		t.Fatal(err)
	}
	if !allowed["/usr/lib/libcuda.so.1"] || allowed["/other-device"] || allowed["/etc/shadow"] {
		t.Fatal(allowed)
	}
	// Simulate the later normal creation-stage injection from the copied CRI field.
	actual := &spec.Spec{Process: &spec.Process{}, Linux: &spec.Linux{}}
	if _, err = registry.InjectDevices(actual, restored.CDIDevices[0].Name); err != nil {
		t.Fatal(err)
	}
	if len(actual.Mounts) != 1 || actual.Mounts[0].Source != "/trusted/libcuda.so.1" || !reflect.DeepEqual(actual.Mounts[0].Options, []string{"ro", "bind"}) {
		t.Fatal(actual.Mounts)
	}
	if actual.Hooks == nil || len(actual.Hooks.CreateContainer) != 1 || !strings.Contains(strings.Join(actual.Process.Env, ","), "CDI_TEST=1") {
		t.Fatal("CDI edits lost")
	}
	request.CDIDevices[0].Name = "test.example/gpu=missing"
	if _, err = prepareRestoreCDIWithRegistry(request, restored, registry); err == nil {
		t.Fatal("unresolved device accepted")
	}
}
