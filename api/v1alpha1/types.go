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
	ArchiveEvidenceID string `json:"archiveEvidenceID,omitempty"`
	DurableRef        string `json:"durableRef,omitempty"`
	ContainerName     string `json:"containerName,omitempty"`
	SourcePath        string `json:"sourcePath,omitempty"`
	TargetPath        string `json:"targetPath,omitempty"`
	SHA256            string `json:"sha256"`
}
type RestorePod struct {
	Rank         int64     `json:"rank,omitempty"`
	SourcePod    string    `json:"sourcePod"`
	SourcePodUID string    `json:"sourcePodUID,omitempty"`
	SourceNode   string    `json:"sourceNode"`
	TargetPod    string    `json:"targetPod"`
	TargetNode   string    `json:"targetNode"`
	Archives     []Archive `json:"archives"`
}
type SurvivorEvidence struct {
	Rank          int64  `json:"rank"`
	PodName       string `json:"podName"`
	PodUID        string `json:"podUID"`
	NodeName      string `json:"nodeName"`
	Generation    int64  `json:"generation"`
	PauseLockPath string `json:"pauseLockPath"`
	ObservedAt    string `json:"observedAt,omitempty"`
}
type PartialRestoreSpec struct {
	TargetRanks           []int64            `json:"targetRanks"`
	PreservedSurvivors    []SurvivorEvidence `json:"preservedSurvivors"`
	PreventPeriodicResume bool               `json:"preventPeriodicResume"`
}
type RestoreRequestSpec struct {
	GroupRestore       *GroupRestoreSpec   `json:"groupRestore,omitempty"`
	CheckpointRef      CheckpointReference `json:"checkpointRef"`
	WorkloadRef        WorkloadReference   `json:"workloadRef"`
	TrainingRuntimeRef RuntimeReference    `json:"trainingRuntimeRef,omitempty"`
	SourceCluster      string              `json:"sourceCluster"`
	TargetCluster      string              `json:"targetCluster"`
	SourceFenced       bool                `json:"sourceFenced"`
	VolumesReady       bool                `json:"volumesReady"`
	Pods               []RestorePod        `json:"pods"`
	PartialRestore     *PartialRestoreSpec `json:"partialRestore,omitempty"`
}
type RestorePlanSpec struct {
	GroupRestore *GroupRestoreSpec `json:"groupRestore,omitempty"`
	// LocalPodRestore opts into a member-local, UID-fenced standalone Pod restore.
	LocalPodRestore    bool                `json:"localPodRestore,omitempty"`
	RequestUID         string              `json:"requestUID"`
	CheckpointRef      CheckpointReference `json:"checkpointRef"`
	WorkloadRef        WorkloadReference   `json:"workloadRef"`
	TrainingRuntimeRef RuntimeReference    `json:"trainingRuntimeRef,omitempty"`
	SourceCluster      string              `json:"sourceCluster"`
	TargetCluster      string              `json:"targetCluster"`
	SourceFenced       bool                `json:"sourceFenced"`
	VolumesReady       bool                `json:"volumesReady"`
	Pods               []RestorePod        `json:"pods"`
	PartialRestore     *PartialRestoreSpec `json:"partialRestore,omitempty"`
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
	GroupControl       *GroupControlStatus    `json:"groupControl,omitempty"`
	ClusterName        string                 `json:"clusterName"`
	ObservedGeneration int64                  `json:"observedGeneration,omitempty"`
	Phase              string                 `json:"phase,omitempty"`
	Message            string                 `json:"message,omitempty"`
	Pods               []PodStatus            `json:"pods,omitempty"`
	SourceFences       []SourcePodFenceStatus `json:"sourceFences,omitempty"`
}
type SourcePodFenceStatus struct {
	PodName            string       `json:"podName"`
	SourcePodUID       string       `json:"sourcePodUID"`
	ObservedGeneration int64        `json:"observedGeneration"`
	Phase              string       `json:"phase"`
	Message            string       `json:"message,omitempty"`
	DeleteRequestedAt  *metav1.Time `json:"deleteRequestedAt,omitempty"`
	GoneObservedAt     *metav1.Time `json:"goneObservedAt,omitempty"`
}
type SourceFenceEvidence struct {
	Fenced     bool   `json:"fenced"`
	Operation  string `json:"operation,omitempty"`
	EvidenceID string `json:"evidenceID,omitempty"`
	ObservedAt string `json:"observedAt,omitempty"`
}
type RuntimeReference struct {
	Name string `json:"name"`
	UID  string `json:"uid,omitempty"`
}
type PartialRestoreTargetEvidence struct {
	Rank              int64  `json:"rank"`
	TargetPodUID      string `json:"targetPodUID"`
	CheckpointID      string `json:"checkpointID"`
	ArchiveEvidenceID string `json:"archiveEvidenceID"`
}
type StateEvidence struct {
	Kind       string `json:"kind"`
	ObservedAt string `json:"observedAt"`
}
type SurvivorStateEvidence struct {
	Rank          int64         `json:"rank"`
	PodUID        string        `json:"podUID"`
	StateEvidence StateEvidence `json:"stateEvidence"`
}
type PartialRestoreVerification struct {
	PreventPeriodicResume bool                           `json:"preventPeriodicResume"`
	TargetRanks           []PartialRestoreTargetEvidence `json:"targetRanks"`
}
type RestoreVerification struct {
	RequestUID         string                      `json:"requestUID"`
	Operation          string                      `json:"operation,omitempty"`
	CheckpointID       string                      `json:"checkpointID"`
	VerifiedAt         metav1.Time                 `json:"verifiedAt"`
	TrainingRuntimeRef RuntimeReference            `json:"trainingRuntimeRef"`
	SourceCluster      string                      `json:"sourceCluster"`
	TargetCluster      string                      `json:"targetCluster"`
	SourceFenced       bool                        `json:"sourceFenced"`
	SourceFence        SourceFenceEvidence         `json:"sourceFence,omitempty"`
	PartialRestore     *PartialRestoreVerification `json:"partialRestore,omitempty"`
	PreservedSurvivors []SurvivorEvidence          `json:"preservedSurvivors,omitempty"`
	Survivors          []SurvivorStateEvidence     `json:"survivors,omitempty"`
}
type RestoreStatus struct {
	GroupControl       *GroupControlStatus    `json:"groupControl,omitempty"`
	ObservedGeneration int64                  `json:"observedGeneration,omitempty"`
	Phase              string                 `json:"phase,omitempty"`
	Message            string                 `json:"message,omitempty"`
	PlanName           string                 `json:"planName,omitempty"`
	Artifacts          []ArtifactStatus       `json:"artifacts,omitempty"`
	Pods               []PodStatus            `json:"pods,omitempty"`
	SourceFences       []SourcePodFenceStatus `json:"sourceFences,omitempty"`
	Clusters           []ClusterStatus        `json:"clusters,omitempty"`
	Verification       *RestoreVerification   `json:"verification,omitempty"`
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
func copyPartial(in *PartialRestoreSpec) *PartialRestoreSpec {
	if in == nil {
		return nil
	}
	out := *in
	if in.TargetRanks != nil {
		out.TargetRanks = append([]int64{}, in.TargetRanks...)
	}
	if in.PreservedSurvivors != nil {
		out.PreservedSurvivors = append([]SurvivorEvidence{}, in.PreservedSurvivors...)
	}
	return &out
}

func copyPartialVerification(in *PartialRestoreVerification) *PartialRestoreVerification {
	if in == nil {
		return nil
	}
	out := *in
	if in.TargetRanks != nil {
		out.TargetRanks = append([]PartialRestoreTargetEvidence{}, in.TargetRanks...)
	}
	return &out
}

func CopyPartialRestoreForStatus(in *PartialRestoreSpec) *PartialRestoreSpec {
	return copyPartial(in)
}

func copyStatus(in RestoreStatus) RestoreStatus {
	out := in
	out.GroupControl = copyGroupControl(in.GroupControl)
	if in.Verification != nil {
		verification := *in.Verification
		verification.PartialRestore = copyPartialVerification(in.Verification.PartialRestore)
		if in.Verification.PreservedSurvivors != nil {
			verification.PreservedSurvivors = append([]SurvivorEvidence{}, in.Verification.PreservedSurvivors...)
		}
		if in.Verification.Survivors != nil {
			verification.Survivors = append([]SurvivorStateEvidence{}, in.Verification.Survivors...)
		}
		out.Verification = &verification
	}
	if in.Artifacts != nil {
		out.Artifacts = append([]ArtifactStatus{}, in.Artifacts...)
	}
	if in.Pods != nil {
		out.Pods = append([]PodStatus{}, in.Pods...)
	}
	if in.SourceFences != nil {
		out.SourceFences = append([]SourcePodFenceStatus{}, in.SourceFences...)
	}
	if in.Clusters != nil {
		out.Clusters = append([]ClusterStatus{}, in.Clusters...)
		for i := range out.Clusters {
			out.Clusters[i].GroupControl = copyGroupControl(in.Clusters[i].GroupControl)
			if in.Clusters[i].Pods != nil {
				out.Clusters[i].Pods = append([]PodStatus{}, in.Clusters[i].Pods...)
			}
			if in.Clusters[i].SourceFences != nil {
				out.Clusters[i].SourceFences = append([]SourcePodFenceStatus{}, in.Clusters[i].SourceFences...)
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
	out.Spec.PartialRestore = copyPartial(x.Spec.PartialRestore)
	out.Spec.GroupRestore = CopyGroupRestore(x.Spec.GroupRestore)
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
	out.Spec.PartialRestore = copyPartial(x.Spec.PartialRestore)
	out.Spec.GroupRestore = CopyGroupRestore(x.Spec.GroupRestore)
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
