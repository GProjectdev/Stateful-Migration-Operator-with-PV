package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var GroupVersion = schema.GroupVersion{Group: "migration.dcnlab.com", Version: "v1alpha1"}

func AddToScheme(s *runtime.Scheme) error {
	s.AddKnownTypes(GroupVersion, &RestoreRequest{}, &RestoreRequestList{}, &RestorePlan{}, &RestorePlanList{})
	metav1.AddToGroupVersion(s, GroupVersion)
	return nil
}

const PlanLabel = "migration.dcnlab.com/restore-plan"
const RestoreAnnotationPrefix = "checkpoint-restore.crio.io/"

type WorkloadReference struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Name       string `json:"name"`
	UID        string `json:"uid"`
}
type CheckpointReference struct {
	Name         string `json:"name"`
	UID          string `json:"uid"`
	Generation   int64  `json:"generation"`
	CheckpointID string `json:"checkpointID"`
}
type Archive struct {
	ContainerName string `json:"containerName"`
	SourcePath    string `json:"sourcePath"`
	TargetPath    string `json:"targetPath"`
	SHA256        string `json:"sha256"`
}
type RestorePod struct {
	SourcePod  string    `json:"sourcePod"`
	SourceNode string    `json:"sourceNode"`
	TargetPod  string    `json:"targetPod"`
	TargetNode string    `json:"targetNode"`
	Archives   []Archive `json:"archives"`
}
type RestoreRequestSpec struct {
	CheckpointRef      CheckpointReference `json:"checkpointRef"`
	WorkloadRef        WorkloadReference   `json:"workloadRef"`
	TrainingRuntimeRef RuntimeReference    `json:"trainingRuntimeRef"`
	SourceCluster      string              `json:"sourceCluster"`
	TargetCluster      string              `json:"targetCluster"`
	SourceFenced       bool                `json:"sourceFenced"`
	VolumesReady       bool                `json:"volumesReady"`
	Pods               []RestorePod        `json:"pods"`
}
type RestorePlanSpec struct {
	RequestUID         string              `json:"requestUID"`
	CheckpointRef      CheckpointReference `json:"checkpointRef"`
	WorkloadRef        WorkloadReference   `json:"workloadRef"`
	TrainingRuntimeRef RuntimeReference    `json:"trainingRuntimeRef"`
	SourceCluster      string              `json:"sourceCluster"`
	TargetCluster      string              `json:"targetCluster"`
	SourceFenced       bool                `json:"sourceFenced"`
	VolumesReady       bool                `json:"volumesReady"`
	Pods               []RestorePod        `json:"pods"`
}
type ArtifactStatus struct {
	NodeName           string      `json:"nodeName"`
	ObservedGeneration int64       `json:"observedGeneration"`
	Verified           bool        `json:"verified"`
	Message            string      `json:"message,omitempty"`
	DurableRef         string      `json:"durableRef,omitempty"`
	CheckedAt          metav1.Time `json:"checkedAt"`
}
type PodStatus struct {
	Name    string `json:"name"`
	UID     string `json:"uid,omitempty"`
	Phase   string `json:"phase"`
	Message string `json:"message,omitempty"`
}
type ClusterStatus struct {
	ClusterName        string      `json:"clusterName"`
	ObservedGeneration int64       `json:"observedGeneration,omitempty"`
	Phase              string      `json:"phase,omitempty"`
	Message            string      `json:"message,omitempty"`
	Pods               []PodStatus `json:"pods,omitempty"`
}
type RuntimeReference struct {
	Name string `json:"name"`
	UID  string `json:"uid,omitempty"`
}
type RestoreVerification struct {
	RequestUID         string           `json:"requestUID"`
	CheckpointID       string           `json:"checkpointID"`
	VerifiedAt         metav1.Time      `json:"verifiedAt"`
	TrainingRuntimeRef RuntimeReference `json:"trainingRuntimeRef"`
	SourceCluster      string           `json:"sourceCluster"`
	TargetCluster      string           `json:"targetCluster"`
}
type RestoreStatus struct {
	ObservedGeneration int64                `json:"observedGeneration,omitempty"`
	Phase              string               `json:"phase,omitempty"`
	Message            string               `json:"message,omitempty"`
	PlanName           string               `json:"planName,omitempty"`
	Artifacts          []ArtifactStatus     `json:"artifacts,omitempty"`
	Pods               []PodStatus          `json:"pods,omitempty"`
	Clusters           []ClusterStatus      `json:"clusters,omitempty"`
	Verification       *RestoreVerification `json:"verification,omitempty"`
}
type RestoreRequest struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              RestoreRequestSpec `json:"spec"`
	Status            RestoreStatus      `json:"status,omitempty"`
}
type RestoreRequestList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []RestoreRequest `json:"items"`
}
type RestorePlan struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              RestorePlanSpec `json:"spec"`
	Status            RestoreStatus   `json:"status,omitempty"`
}
type RestorePlanList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []RestorePlan `json:"items"`
}

func copyPods(in []RestorePod) []RestorePod {
	if in == nil {
		return nil
	}
	out := append([]RestorePod{}, in...)
	for i := range out {
		if in[i].Archives != nil {
			out[i].Archives = append([]Archive{}, in[i].Archives...)
		}
	}
	return out
}
func copyStatus(in RestoreStatus) RestoreStatus {
	out := in
	if in.Verification != nil {
		verification := *in.Verification
		out.Verification = &verification
	}
	if in.Artifacts != nil {
		out.Artifacts = append([]ArtifactStatus{}, in.Artifacts...)
	}
	if in.Pods != nil {
		out.Pods = append([]PodStatus{}, in.Pods...)
	}
	if in.Clusters != nil {
		out.Clusters = append([]ClusterStatus{}, in.Clusters...)
		for i := range out.Clusters {
			if in.Clusters[i].Pods != nil {
				out.Clusters[i].Pods = append([]PodStatus{}, in.Clusters[i].Pods...)
			}
		}
	}
	return out
}
func (x *RestoreRequest) DeepCopy() *RestoreRequest {
	if x == nil {
		return nil
	}
	out := new(RestoreRequest)
	*out = *x
	out.ObjectMeta = *x.ObjectMeta.DeepCopy()
	out.Spec.Pods = copyPods(x.Spec.Pods)
	out.Status = copyStatus(x.Status)
	return out
}
func (x *RestoreRequest) DeepCopyObject() runtime.Object {
	if x == nil {
		return nil
	}
	return x.DeepCopy()
}
func (x *RestoreRequestList) DeepCopyObject() runtime.Object {
	if x == nil {
		return nil
	}
	out := new(RestoreRequestList)
	*out = *x
	out.ListMeta = *x.ListMeta.DeepCopy()
	if x.Items != nil {
		out.Items = make([]RestoreRequest, len(x.Items))
		for i := range x.Items {
			out.Items[i] = *x.Items[i].DeepCopy()
		}
	}
	return out
}
func (x *RestorePlan) DeepCopy() *RestorePlan {
	if x == nil {
		return nil
	}
	out := new(RestorePlan)
	*out = *x
	out.ObjectMeta = *x.ObjectMeta.DeepCopy()
	out.Spec.Pods = copyPods(x.Spec.Pods)
	out.Status = copyStatus(x.Status)
	return out
}
func (x *RestorePlan) DeepCopyObject() runtime.Object {
	if x == nil {
		return nil
	}
	return x.DeepCopy()
}
func (x *RestorePlanList) DeepCopyObject() runtime.Object {
	if x == nil {
		return nil
	}
	out := new(RestorePlanList)
	*out = *x
	out.ListMeta = *x.ListMeta.DeepCopy()
	if x.Items != nil {
		out.Items = make([]RestorePlan, len(x.Items))
		for i := range x.Items {
			out.Items[i] = *x.Items[i].DeepCopy()
		}
	}
	return out
}
