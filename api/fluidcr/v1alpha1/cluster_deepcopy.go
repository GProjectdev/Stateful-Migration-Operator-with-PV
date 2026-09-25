package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

func (in *ClusterMigrationStatus) DeepCopyInto(out *ClusterMigrationStatus) {
	*out = *in
	if in.StartTime != nil {
		out.StartTime = in.StartTime.DeepCopy()
	}
	if in.CompletionTime != nil {
		out.CompletionTime = in.CompletionTime.DeepCopy()
	}
	if in.Pods != nil {
		out.Pods = make([]runtime.RawExtension, len(in.Pods))
		for i := range in.Pods {
			in.Pods[i].DeepCopyInto(&out.Pods[i])
		}
	}
	if in.Conditions != nil {
		out.Conditions = make([]metav1.Condition, len(in.Conditions))
		copy(out.Conditions, in.Conditions)
	}
}
