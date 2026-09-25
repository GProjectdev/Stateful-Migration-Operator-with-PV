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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/GProjectdev/Stateful-Migration-Operator-with-PV/internal/ctrlapi"
)

func podWith(containers []corev1.Container, annotations map[string]string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pod", Annotations: annotations},
		Spec:       corev1.PodSpec{Containers: containers},
	}
}

func TestResolveContainerName(t *testing.T) {
	tests := []struct {
		name     string
		pod      *corev1.Pod
		override string
		want     string
		wantErr  bool
	}{
		{
			name: "single container",
			pod:  podWith([]corev1.Container{{Name: "trainer"}}, nil),
			want: "trainer",
		},
		{
			name:     "override wins",
			pod:      podWith([]corev1.Container{{Name: "a"}, {Name: "b"}}, nil),
			override: "b",
			want:     "b",
		},
		{
			name:     "override not found",
			pod:      podWith([]corev1.Container{{Name: "a"}}, nil),
			override: "missing",
			wantErr:  true,
		},
		{
			name: "annotation selects container",
			pod:  podWith([]corev1.Container{{Name: "a"}, {Name: "trainer"}}, map[string]string{AnnotationContainer: "trainer"}),
			want: "trainer",
		},
		{
			name:    "multiple containers no hint",
			pod:     podWith([]corev1.Container{{Name: "a"}, {Name: "b"}}, nil),
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveContainerName(tt.pod, tt.override)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got %q", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestResolveCtrlPort(t *testing.T) {
	envPod := podWith([]corev1.Container{{
		Name: "trainer",
		Env:  []corev1.EnvVar{{Name: EnvCtrlPort, Value: "9000"}},
	}}, nil)
	noEnvPod := podWith([]corev1.Container{{Name: "trainer"}}, nil)

	if got := resolveCtrlPort(envPod, "trainer", 0); got != 9000 {
		t.Errorf("env port: got %d, want 9000", got)
	}
	if got := resolveCtrlPort(envPod, "trainer", 7000); got != 7000 {
		t.Errorf("override: got %d, want 7000", got)
	}
	if got := resolveCtrlPort(noEnvPod, "trainer", 0); got != ctrlapi.DefaultCtrlPort {
		t.Errorf("default: got %d, want %d", got, ctrlapi.DefaultCtrlPort)
	}
}

func TestIsInjected(t *testing.T) {
	if !isInjected(podWith(nil, map[string]string{AnnotationInjected: "true"})) {
		t.Error("expected injected pod to be detected")
	}
	if isInjected(podWith(nil, nil)) {
		t.Error("expected non-injected pod to be rejected")
	}
}
