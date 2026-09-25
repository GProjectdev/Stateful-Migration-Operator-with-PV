/*
Copyright 2026 Leehun.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package checkpoint

import (
	"fmt"
	"strconv"

	corev1 "k8s.io/api/core/v1"

	"github.com/GProjectdev/Stateful-Migration-Operator-with-PV/internal/ctrlapi"
)

// FluidCR pod conventions written by the admission webhook. These mirror the
// constants in the webhook's internal/inject package, which cannot be imported
// across modules.
const (
	// AnnotationInjected marks a pod the FluidCR admission webhook has wired.
	AnnotationInjected = "fluidcr.dcnlab.com/injected"
	// AnnotationContainer names the FluidCR-wrapped target container.
	AnnotationContainer = "fluidcr.dcnlab.com/container"
	// EnvCtrlPort is the in-pod control-API port environment variable.
	EnvCtrlPort = "FLUIDCR_CTRL_PORT"
)

// isInjected reports whether the pod was wired by the FluidCR webhook.
func isInjected(pod *corev1.Pod) bool {
	return pod.Annotations[AnnotationInjected] == "true"
}

// resolveContainerName picks the container to CRIU-checkpoint. Precedence:
// explicit spec override, the fluidcr.dcnlab.com/container annotation, then the
// pod's sole container.
func resolveContainerName(pod *corev1.Pod, override string) (string, error) {
	if override != "" {
		if hasContainer(pod, override) {
			return override, nil
		}
		return "", fmt.Errorf("spec.container %q not found in pod %s", override, pod.Name)
	}
	if name := pod.Annotations[AnnotationContainer]; name != "" {
		if hasContainer(pod, name) {
			return name, nil
		}
		return "", fmt.Errorf("annotated container %q not found in pod %s", name, pod.Name)
	}
	if len(pod.Spec.Containers) == 1 {
		return pod.Spec.Containers[0].Name, nil
	}
	return "", fmt.Errorf("pod %s has %d containers and no %s annotation or spec.container override",
		pod.Name, len(pod.Spec.Containers), AnnotationContainer)
}

func hasContainer(pod *corev1.Pod, name string) bool {
	for i := range pod.Spec.Containers {
		if pod.Spec.Containers[i].Name == name {
			return true
		}
	}
	return false
}

// resolveCtrlPort picks the in-pod control-API port. Precedence: explicit spec
// override, the target container's FLUIDCR_CTRL_PORT env, then the default.
func resolveCtrlPort(pod *corev1.Pod, containerName string, override int32) int {
	if override > 0 {
		return int(override)
	}
	for i := range pod.Spec.Containers {
		c := &pod.Spec.Containers[i]
		if c.Name != containerName {
			continue
		}
		for _, e := range c.Env {
			if e.Name == EnvCtrlPort {
				if p, err := strconv.Atoi(e.Value); err == nil && p > 0 {
					return p
				}
			}
		}
	}
	return ctrlapi.DefaultCtrlPort
}
