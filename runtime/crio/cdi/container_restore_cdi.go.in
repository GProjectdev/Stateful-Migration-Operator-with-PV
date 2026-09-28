package server

import (
	"fmt"
	"path"
	"strings"

	spec "github.com/opencontainers/runtime-spec/specs-go"
	types "k8s.io/cri-api/pkg/apis/runtime/v1"
	"tags.cncf.io/container-device-interface/pkg/cdi"
)

type restoreCDIRegistry interface {
	Refresh() error
	InjectDevices(*spec.Spec, ...string) ([]string, error)
}

// Resolve only the new CRI allocation. Archive annotations never authorize a device.
// The real OCI edits (including hooks/options/devices) are applied later by
// createSandboxContainer's existing SpecInjectCDIDevices call.
func prepareRestoreCDI(request, restored *types.ContainerConfig) (map[string]bool, error) {
	return prepareRestoreCDIWithRegistry(request, restored, cdi.GetDefaultCache())
}

func prepareRestoreCDIWithRegistry(request, restored *types.ContainerConfig, registry restoreCDIRegistry) (map[string]bool, error) {
	_, annotated, err := cdi.ParseAnnotations(request.GetAnnotations())
	if err != nil {
		return nil, fmt.Errorf("restore CDI annotations: %w", err)
	}
	names := []string{}
	seen := map[string]bool{}
	add := func(name string) {
		if !seen[name] {
			names = append(names, name)
			seen[name] = true
		}
	}
	for _, device := range request.GetCDIDevices() {
		if device == nil || device.GetName() == "" {
			return nil, fmt.Errorf("empty restore CDI device")
		}
		add(device.GetName())
	}
	for _, name := range annotated {
		add(name)
	}
	// Do not mutate the checkpoint metadata map or alias the request.
	annotations := map[string]string{}
	for key, value := range restored.GetAnnotations() {
		if !strings.HasPrefix(key, "cdi.k8s.io/") {
			annotations[key] = value
		}
	}
	restored.Annotations = annotations
	restored.CDIDevices = nil
	allowed := map[string]bool{}
	if len(names) == 0 {
		return allowed, nil
	}
	if err := registry.Refresh(); err != nil {
		return nil, fmt.Errorf("restore CDI registry refresh: %w", err)
	}
	preview := &spec.Spec{Process: &spec.Process{}, Linux: &spec.Linux{}}
	unresolved, err := registry.InjectDevices(preview, names...)
	if err != nil || len(unresolved) != 0 {
		return nil, fmt.Errorf("restore CDI resolution failed (unresolved=%v): %v", unresolved, err)
	}
	for _, mount := range preview.Mounts {
		if !path.IsAbs(mount.Destination) || path.Clean(mount.Destination) != mount.Destination || mount.Destination == "/" {
			return nil, fmt.Errorf("invalid CDI mount destination %q", mount.Destination)
		}
		// CDI owns these destinations. Reject ambiguous overrides instead of
		// silently letting a CDI edit replace an explicitly requested volume.
		for _, requested := range request.GetMounts() {
			if requested.GetContainerPath() == mount.Destination {
				return nil, fmt.Errorf("restore CDI mount conflicts with CRI volume %q", mount.Destination)
			}
		}
		allowed[mount.Destination] = true
	}
	for _, name := range names {
		restored.CDIDevices = append(restored.CDIDevices, &types.CDIDevice{Name: name})
	}
	return allowed, nil
}
