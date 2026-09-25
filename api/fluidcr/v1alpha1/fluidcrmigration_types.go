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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// MigrationPhase is the overall phase of a FluidCRMigration workflow.
type MigrationPhase string

const (
	// PhasePending is the initial phase before any work has started.
	PhasePending MigrationPhase = "Pending"
	// PhaseAppCheckpointing means the controller is signalling the in-pod
	// FluidCR control API to create application-level checkpoints.
	PhaseAppCheckpointing MigrationPhase = "AppCheckpointing"
	// PhaseContainerCheckpointing means the controller is invoking the kubelet
	// CRIU checkpoint API for the target containers.
	PhaseContainerCheckpointing MigrationPhase = "ContainerCheckpointing"
	// PhaseResuming means the controller is releasing the application
	// checkpoint locks so the workload resumes in place.
	PhaseResuming MigrationPhase = "Resuming"
	// PhaseCompleted is a terminal phase indicating the workflow succeeded.
	PhaseCompleted MigrationPhase = "Completed"
	// PhaseFailed is a terminal phase indicating the workflow failed.
	PhaseFailed MigrationPhase = "Failed"
)

// PodPhase is the per-pod phase within a FluidCRMigration workflow.
type PodPhase string

const (
	// PodPhasePending means no step has completed for this pod yet.
	PodPhasePending PodPhase = "Pending"
	// PodPhaseAppCheckpointed means the application checkpoint succeeded.
	PodPhaseAppCheckpointed PodPhase = "AppCheckpointed"
	// PodPhaseContainerCheckpointed means the CRIU container checkpoint succeeded.
	PodPhaseContainerCheckpointed PodPhase = "ContainerCheckpointed"
	// PodPhaseResumed means the workload was resumed in place.
	PodPhaseResumed PodPhase = "Resumed"
	// PodPhaseFailed means a step failed for this pod.
	PodPhaseFailed PodPhase = "Failed"
)

// WorkloadReference identifies the workload whose pods will be checkpointed.
type WorkloadReference struct {
	// APIVersion of the workload, e.g. "apps/v1", "batch/v1" or "v1" (Pod).
	// +kubebuilder:validation:MinLength=1
	// +required
	APIVersion string `json:"apiVersion"`

	// Kind of the workload. A Pod reference targets that single pod directly;
	// the other kinds fan out to every pod the workload owns.
	// +kubebuilder:validation:Enum=Deployment;StatefulSet;Job;Pod
	// +required
	Kind string `json:"kind"`

	// Name of the workload.
	// +kubebuilder:validation:MinLength=1
	// +required
	Name string `json:"name"`

	// Namespace of the workload. Defaults to the FluidCRMigration's own
	// namespace when empty.
	// +optional
	Namespace string `json:"namespace,omitempty"`
}

// FluidCRMigrationSpec defines the desired state of FluidCRMigration.
// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="spec is immutable; create a new checkpoint CR"
type FluidCRMigrationSpec struct {
	// WorkloadRef references the workload whose pods will be checkpointed.
	// The controller resolves the workload's pod selector and fans out the
	// checkpoint workflow to every Running, FluidCR-injected pod it owns.
	// +required
	WorkloadRef WorkloadReference `json:"workloadRef"`

	// Resume controls whether the controller releases the application
	// checkpoint locks (calls the in-pod /resume API) after the container
	// checkpoints are taken, so the workload continues in place. Defaults to
	// true.
	// +optional
	Resume *bool `json:"resume,omitempty"`

	// Container overrides which container is checkpointed via the kubelet CRIU
	// API. When empty, the controller resolves it from the
	// "fluidcr.dcnlab.com/container" annotation, or the pod's sole container.
	// +optional
	Container string `json:"container,omitempty"`

	// CtrlPort overrides the in-pod FluidCR control-API port. When zero, the
	// controller reads the target container's FLUIDCR_CTRL_PORT environment
	// variable, falling back to 8298.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=65535
	// +optional
	CtrlPort int32 `json:"ctrlPort,omitempty"`

	// AppCheckpointTimeoutSeconds bounds each in-pod /checkpoint REST call.
	// Defaults to 300.
	// +kubebuilder:validation:Minimum=1
	// +optional
	AppCheckpointTimeoutSeconds int32 `json:"appCheckpointTimeoutSeconds,omitempty"`

	// KubeletTimeoutSeconds bounds each kubelet CRIU checkpoint call.
	// Defaults to 300.
	// +kubebuilder:validation:Minimum=1
	// +optional
	KubeletTimeoutSeconds int32 `json:"kubeletTimeoutSeconds,omitempty"`
}

// CheckpointFile records a CRIU checkpoint archive produced for a container.
type CheckpointFile struct {
	// ContainerName is the container that was checkpointed.
	// +required
	ContainerName string `json:"containerName"`

	// FilePath is the absolute path of the checkpoint archive on the host
	// node, e.g. /var/lib/kubelet/checkpoints/checkpoint-<ns>_<pod>-<container>-<ts>.tar.
	// +required
	FilePath string `json:"filePath"`

	// CheckpointTime is when the checkpoint archive was created.
	// +optional
	CheckpointTime *metav1.Time `json:"checkpointTime,omitempty"`
}

// PodMigrationStatus tracks the per-pod progress of the workflow.
type PodMigrationStatus struct {
	// PodUID binds persisted work to a specific pod incarnation.
	// +optional
	PodUID string `json:"podUID,omitempty"`

	// PodName is the target pod's name.
	// +required
	PodName string `json:"podName"`

	// NodeName is the node the pod runs on.
	// +optional
	NodeName string `json:"nodeName,omitempty"`

	// PodIP is the pod IP used for the in-pod control-API calls.
	// +optional
	PodIP string `json:"podIP,omitempty"`

	// Phase is the per-pod phase.
	// +optional
	Phase PodPhase `json:"phase,omitempty"`

	// AppCheckpointResult summarizes the in-pod /checkpoint REST response.
	// +optional
	AppCheckpointResult string `json:"appCheckpointResult,omitempty"`

	// CheckpointFiles lists the CRIU archives produced for this pod.
	// +optional
	CheckpointFiles []CheckpointFile `json:"checkpointFiles,omitempty"`

	// Message provides per-pod detail, including the error on failure.
	// +optional
	Message string `json:"message,omitempty"`
}

// FluidCRMigrationStatus defines the observed state of FluidCRMigration.
type FluidCRMigrationStatus struct {
	// Clusters is populated by Karmada aggregation, never by member HTTP work.
	// +optional
	// +listType=map
	// +listMapKey=clusterName
	Clusters []ClusterMigrationStatus `json:"clusters,omitempty"`
	// Phase is the overall phase of the migration workflow.
	// +optional
	Phase MigrationPhase `json:"phase,omitempty"`

	// Message provides human-readable detail about the current state.
	// +optional
	Message string `json:"message,omitempty"`

	// ObservedGeneration is the spec generation last reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// StartTime is when the workflow started.
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// CompletionTime is when the workflow reached a terminal phase.
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// Pods tracks the per-pod progress of the workflow.
	// +optional
	Pods []PodMigrationStatus `json:"pods,omitempty"`

	// Conditions represent the latest available observations of the
	// FluidCRMigration's current state.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// ClusterMigrationStatus retains the member status needed by orchestration.
type ClusterMigrationStatus struct {
	StartTime          *metav1.Time   `json:"startTime,omitempty"`
	CompletionTime     *metav1.Time   `json:"completionTime,omitempty"`
	ClusterName        string         `json:"clusterName"`
	Phase              MigrationPhase `json:"phase,omitempty"`
	ObservedGeneration int64          `json:"observedGeneration,omitempty"`
	Message            string         `json:"message,omitempty"`
	// +kubebuilder:pruning:PreserveUnknownFields
	Pods       []runtime.RawExtension `json:"pods,omitempty"`
	Conditions []metav1.Condition     `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Workload",type=string,JSONPath=`.spec.workloadRef.name`
// +kubebuilder:printcolumn:name="Message",type=string,JSONPath=`.status.message`,priority=1
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// FluidCRMigration is the Schema for the fluidcrmigrations API.
type FluidCRMigration struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of FluidCRMigration
	// +required
	Spec FluidCRMigrationSpec `json:"spec"`

	// status defines the observed state of FluidCRMigration
	// +optional
	Status FluidCRMigrationStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// FluidCRMigrationList contains a list of FluidCRMigration.
type FluidCRMigrationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []FluidCRMigration `json:"items"`
}

func init() {
	SchemeBuilder.Register(&FluidCRMigration{}, &FluidCRMigrationList{})
}
