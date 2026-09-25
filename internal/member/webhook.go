package member

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	api "github.com/GProjectdev/Stateful-Migration-Operator-with-PV/api/v1alpha1"
	"github.com/GProjectdev/Stateful-Migration-Operator-with-PV/internal/artifact"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"regexp"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
	"strconv"
	"strings"
	"time"
)

const PlanUIDAnnotation = "migration.dcnlab.com/restore-plan-uid"
const PlanGenerationAnnotation = "migration.dcnlab.com/restore-plan-generation"
const RuntimeCapabilityLabel = "migration.dcnlab.com/restore-from-file"
const InjectAnnotation = "fluidcr.dcnlab.com/inject"
const ContainerAnnotation = "fluidcr.dcnlab.com/container"
const InjectedAnnotation = "fluidcr.dcnlab.com/injected"

// Reader must be mgr.GetAPIReader(), not the informer cache.
type Webhook struct {
	Reader       client.Reader
	ClusterName  string
	ValidateOnly bool
}

func NewWebhook(reader client.Reader, clusterName string) *Webhook {
	return &Webhook{Reader: reader, ClusterName: clusterName}
}
func NewValidator(reader client.Reader, clusterName string) *Webhook {
	return &Webhook{Reader: reader, ClusterName: clusterName, ValidateOnly: true}
}

func (w *Webhook) Handle(ctx context.Context, req admission.Request) admission.Response {
	if req.Operation != admissionv1.Create || req.SubResource != "" {
		return admission.Allowed("not a Pod CREATE")
	}
	if req.Kind.Kind != "Pod" || req.Kind.Group != "" {
		return admission.Denied("only core Pod requests supported")
	}
	var pod corev1.Pod
	if err := json.Unmarshal(req.Object.Raw, &pod); err != nil {
		return admission.Denied("invalid Pod")
	}
	if _, opted := pod.Labels[api.PlanLabel]; !opted {
		return admission.Allowed("not opted in")
	}
	if pod.Namespace != "" && pod.Namespace != req.Namespace {
		return admission.Denied("namespace mismatch")
	}
	pod.Namespace = req.Namespace
	if err := w.Apply(ctx, &pod); err != nil {
		return admission.Denied(err.Error())
	}
	if w.ValidateOnly {
		return admission.Allowed("restore prerequisites verified; runtime restore not attested")
	}
	encoded, err := json.Marshal(&pod)
	if err != nil {
		return admission.Errored(500, err)
	}
	return admission.PatchResponseFromRaw(req.Object.Raw, encoded)
}

func validatePlan(p *api.RestorePlan, cluster string) error {
	if cluster == "" || p.Spec.TargetCluster != cluster || p.Namespace == "" || p.UID == "" || p.Generation < 1 || !p.DeletionTimestamp.IsZero() {
		return fmt.Errorf("invalid, deleting or wrong-cluster plan")
	}
	if !p.Spec.SourceFenced || !p.Spec.VolumesReady {
		return fmt.Errorf("sourceFenced and volumesReady must both be true")
	}
	if p.Spec.SourceCluster == p.Spec.TargetCluster {
		return fmt.Errorf("source and target clusters must differ")
	}
	if p.Spec.RequestUID == "" || p.Spec.CheckpointRef.UID == "" || p.Spec.CheckpointRef.Name == "" || p.Spec.CheckpointRef.Generation < 1 || p.Spec.SourceCluster == "" {
		return fmt.Errorf("missing immutable request/checkpoint provenance")
	}
	ref := p.Spec.WorkloadRef
	if ref.Name == "" || !((ref.Kind == "StatefulSet" && ref.APIVersion == "apps/v1") || (ref.Kind == "Pod" && ref.APIVersion == "v1")) {
		return fmt.Errorf("only StatefulSet or standalone Pod workloads supported")
	}
	if len(p.Spec.Pods) == 0 || (ref.Kind == "Pod" && len(p.Spec.Pods) != 1) {
		return fmt.Errorf("invalid pod mapping")
	}
	names, paths := map[string]bool{}, map[string]bool{}
	ordinal := regexp.MustCompile(`^` + regexp.QuoteMeta(ref.Name) + `-(0|[1-9][0-9]*)$`)
	for _, mapping := range p.Spec.Pods {
		if mapping.SourcePod != mapping.TargetPod {
			return fmt.Errorf("source and target Pod identity must match")
		}
		if (ref.Kind == "Pod" && mapping.TargetPod != ref.Name) || (ref.Kind == "StatefulSet" && !ordinal.MatchString(mapping.TargetPod)) {
			return fmt.Errorf("mapped Pod identity does not belong to workload")
		}
		if mapping.TargetPod == "" || mapping.SourcePod == "" || mapping.TargetNode == "" || names[mapping.TargetPod] {
			return fmt.Errorf("missing or duplicate pod mapping")
		}
		names[mapping.TargetPod] = true
		// The installed FluidCR injector wraps exactly one container per Pod.
		if len(mapping.Archives) != 1 {
			return fmt.Errorf("FluidCR requires exactly one restore container per Pod")
		}
		a := mapping.Archives[0]
		if a.ContainerName == "" {
			return fmt.Errorf("missing restore container")
		}
		if _, err := artifact.RelativePath(a.TargetPath); err != nil {
			return err
		}
		digest, err := hex.DecodeString(a.SHA256)
		if err != nil || len(digest) != 32 {
			return fmt.Errorf("invalid archive SHA256")
		}
		key := mapping.TargetNode + "/" + a.TargetPath
		if paths[key] {
			return fmt.Errorf("archive target collision")
		}
		paths[key] = true
	}
	return nil
}

func mappingFor(p *api.RestorePlan, pod *corev1.Pod) (*api.RestorePod, error) {
	if pod.Namespace != p.Namespace || pod.Name == "" {
		return nil, fmt.Errorf("exact mapped Pod name and namespace required")
	}
	for i := range p.Spec.Pods {
		if p.Spec.Pods[i].TargetPod == pod.Name {
			return &p.Spec.Pods[i], nil
		}
	}
	return nil, fmt.Errorf("unplanned Pod")
}

func checkOwner(p *api.RestorePlan, pod *corev1.Pod) error {
	if p.Spec.WorkloadRef.Kind == "Pod" {
		if len(pod.OwnerReferences) != 0 {
			return fmt.Errorf("standalone Pod must not have owners")
		}
		return nil
	}
	owner := metav1.GetControllerOf(pod)
	if owner == nil || owner.Kind != "StatefulSet" || owner.APIVersion != "apps/v1" || owner.Name != p.Spec.WorkloadRef.Name || owner.UID == "" {
		return fmt.Errorf("StatefulSet controller owner mismatch")
	}
	return nil
}

func (w *Webhook) Apply(ctx context.Context, pod *corev1.Pod) error {
	if w.Reader == nil {
		return fmt.Errorf("uncached API reader required")
	}
	name := pod.Labels[api.PlanLabel]
	if name == "" || pod.Namespace == "" {
		return fmt.Errorf("restore plan label and namespace required")
	}
	var plan api.RestorePlan
	if err := w.Reader.Get(ctx, client.ObjectKey{Namespace: pod.Namespace, Name: name}, &plan); err != nil {
		return fmt.Errorf("restore plan unavailable: %w", err)
	}
	if err := validatePlan(&plan, w.ClusterName); err != nil {
		return err
	}
	mapping, err := mappingFor(&plan, pod)
	if err != nil {
		return err
	}
	if err = checkOwner(&plan, pod); err != nil {
		return err
	}
	if !artifact.Fresh(&plan, mapping.TargetNode, time.Now()) {
		return fmt.Errorf("current-generation artifact verification missing or older than two minutes")
	}
	var node corev1.Node
	if err = w.Reader.Get(ctx, client.ObjectKey{Name: mapping.TargetNode}, &node); err != nil {
		return fmt.Errorf("target node unavailable: %w", err)
	}
	if node.Labels[RuntimeCapabilityLabel] != "true" {
		return fmt.Errorf("target node lacks admin-certified restore-from-file capability")
	}
	var existing corev1.Pod
	if err = w.Reader.Get(ctx, client.ObjectKeyFromObject(pod), &existing); err == nil {
		return fmt.Errorf("mapped Pod already exists; automatic adoption is forbidden")
	} else if !apierrors.IsNotFound(err) {
		return err
	}
	// The API server assigns system metadata before validating admission.
	// Persisted Pods are rejected by the uncached existence check above.
	if !w.ValidateOnly && (pod.UID != "" || !pod.CreationTimestamp.IsZero()) {
		return fmt.Errorf("already-created Pod cannot be admitted for restore")
	}
	if pod.Spec.NodeName != "" {
		return fmt.Errorf("spec.nodeName bypasses scheduling; use required node affinity")
	}
	if pod.Annotations[InjectAnnotation] != "true" {
		return fmt.Errorf("template must explicitly set fluidcr.dcnlab.com/inject=true before creation")
	}
	target := mapping.Archives[0].ContainerName
	if len(pod.Spec.Containers) > 1 && pod.Annotations[ContainerAnnotation] != target {
		return fmt.Errorf("multi-container template must preselect the FluidCR restore container")
	}
	if value := pod.Annotations[ContainerAnnotation]; value != "" && value != target {
		return fmt.Errorf("FluidCR container conflicts with restore plan")
	}
	if err = checkRestoreAnnotations(pod, mapping, false); err != nil {
		return err
	}
	for key, expected := range map[string]string{PlanUIDAnnotation: string(plan.UID), PlanGenerationAnnotation: strconv.FormatInt(plan.Generation, 10)} {
		if value, present := pod.Annotations[key]; present && value != expected {
			return fmt.Errorf("stale or conflicting %s", key)
		}
	}
	if w.ValidateOnly {
		return verifyBoundPod(&plan, mapping, pod, false)
	}
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	pod.Annotations[PlanUIDAnnotation] = string(plan.UID)
	pod.Annotations[PlanGenerationAnnotation] = strconv.FormatInt(plan.Generation, 10)
	pod.Annotations[ContainerAnnotation] = target
	for _, archive := range mapping.Archives {
		pod.Annotations[api.RestoreAnnotationPrefix+archive.ContainerName] = archive.TargetPath
	}
	pinNode(pod, mapping.TargetNode)
	return nil
}

func checkRestoreAnnotations(pod *corev1.Pod, mapping *api.RestorePod, required bool) error {
	expected := map[string]string{}
	for _, a := range mapping.Archives {
		count := 0
		for _, c := range pod.Spec.Containers {
			if c.Name == a.ContainerName {
				count++
			}
		}
		if count != 1 {
			return fmt.Errorf("restore container missing or duplicated")
		}
		expected[api.RestoreAnnotationPrefix+a.ContainerName] = a.TargetPath
	}
	for key, value := range pod.Annotations {
		if strings.HasPrefix(key, api.RestoreAnnotationPrefix) {
			if target, ok := expected[key]; !ok || target != value {
				return fmt.Errorf("conflicting restore annotation %s", key)
			}
		}
	}
	if required {
		for key, value := range expected {
			if pod.Annotations[key] != value {
				return fmt.Errorf("missing restore annotation %s", key)
			}
		}
	}
	return nil
}

func pinNode(pod *corev1.Pod, node string) {
	if pod.Spec.Affinity == nil {
		pod.Spec.Affinity = &corev1.Affinity{}
	}
	if pod.Spec.Affinity.NodeAffinity == nil {
		pod.Spec.Affinity.NodeAffinity = &corev1.NodeAffinity{}
	}
	a := pod.Spec.Affinity.NodeAffinity
	if a.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		a.RequiredDuringSchedulingIgnoredDuringExecution = &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchFields: []corev1.NodeSelectorRequirement{{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{node}}}}}}
		return
	}
	for i := range a.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
		term := &a.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[i]
		// Empty terms match no nodes. Preserve that existing restriction.
		if len(term.MatchFields)+len(term.MatchExpressions) == 0 {
			continue
		}
		pinned := false
		for _, field := range term.MatchFields {
			if field.Key == "metadata.name" && field.Operator == corev1.NodeSelectorOpIn && len(field.Values) == 1 && field.Values[0] == node {
				pinned = true
			}
		}
		if !pinned {
			term.MatchFields = append(term.MatchFields, corev1.NodeSelectorRequirement{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{node}})
		}
	}
}

func nodePinned(pod *corev1.Pod, node string) bool {
	if pod.Spec.Affinity == nil || pod.Spec.Affinity.NodeAffinity == nil || pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		return false
	}
	terms := pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	if len(terms) == 0 {
		return false
	}
	for _, term := range terms {
		pinned := false
		for _, field := range term.MatchFields {
			if field.Key == "metadata.name" && field.Operator == corev1.NodeSelectorOpIn && len(field.Values) == 1 && field.Values[0] == node {
				pinned = true
			}
		}
		if !pinned {
			return false
		}
	}
	return true
}

func verifyBoundPod(plan *api.RestorePlan, mapping *api.RestorePod, pod *corev1.Pod, existing bool) error {
	if pod.Labels[api.PlanLabel] != plan.Name || pod.Annotations[PlanUIDAnnotation] != string(plan.UID) || pod.Annotations[PlanGenerationAnnotation] != strconv.FormatInt(plan.Generation, 10) {
		return fmt.Errorf("Pod is not bound to this plan UID and generation")
	}
	if err := checkOwner(plan, pod); err != nil {
		return err
	}
	if err := checkRestoreAnnotations(pod, mapping, true); err != nil {
		return err
	}
	if !nodePinned(pod, mapping.TargetNode) || (pod.Spec.NodeName != "" && (!existing || pod.Spec.NodeName != mapping.TargetNode)) {
		return fmt.Errorf("Pod node does not match plan")
	}
	if pod.Annotations[InjectAnnotation] != "true" || pod.Annotations[InjectedAnnotation] != "true" || pod.Annotations[ContainerAnnotation] != mapping.Archives[0].ContainerName {
		return fmt.Errorf("FluidCR injection missing or mismatched")
	}
	return checkMounts(pod, mapping.Archives[0].ContainerName)
}

func checkMounts(pod *corev1.Pod, target string) error {
	volumes := map[string]corev1.Volume{}
	for _, volume := range pod.Spec.Volumes {
		volumes[volume.Name] = volume
	}
	payload, checkpoint := false, false
	for _, container := range pod.Spec.Containers {
		if container.Name == target {
			for _, mount := range container.VolumeMounts {
				if mount.MountPath == "/opt/fluidcr" && mount.Name == "fluidcr-payload" && mount.SubPath == "" && mount.SubPathExpr == "" && mount.ReadOnly {
					if volume, ok := volumes[mount.Name]; ok && volume.EmptyDir != nil {
						payload = true
					}
				}
				if mount.MountPath == "/checkpoint" && !mount.ReadOnly {
					_, checkpoint = volumes[mount.Name]
				}
			}
		}
	}
	init := false
	for _, container := range pod.Spec.InitContainers {
		if container.Name == "fluidcr-inject" && container.Image != "" && container.RestartPolicy == nil {
			for _, mount := range container.VolumeMounts {
				if mount.Name == "fluidcr-payload" && mount.MountPath == "/fluidcr" && !mount.ReadOnly && mount.SubPath == "" && mount.SubPathExpr == "" {
					init = true
				}
			}
		}
	}
	if !payload || !checkpoint || !init {
		return fmt.Errorf("required FluidCR payload/init or target /opt/fluidcr and /checkpoint mounts missing")
	}
	return nil
}
