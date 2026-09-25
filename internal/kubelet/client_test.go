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

package kubelet

import "testing"

func TestParseCheckpointPath(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		want    string
		wantErr bool
	}{
		{
			name: "items present",
			body: `{"items":["/var/lib/kubelet/checkpoints/checkpoint-default_pod-c-2026.tar"]}`,
			want: "/var/lib/kubelet/checkpoints/checkpoint-default_pod-c-2026.tar",
		},
		{
			name:    "empty items",
			body:    `{"items":[]}`,
			wantErr: true,
		},
		{
			name:    "not json",
			body:    `boom`,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseCheckpointPath([]byte(tt.body))
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
