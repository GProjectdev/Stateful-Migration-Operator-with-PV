package member

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	api "github.com/GProjectdev/Stateful-Migration-Operator-with-PV/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"reflect"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// The wrapper delegates to the independent CLI, validates the entire result, and
// emits a bounded receipt through the kubelet-owned termination message.
const groupJobScript = `import contextlib,io,json,os,sys
from fluidcr.group_control import main
request=json.loads(os.environ["GROUP_REQUEST"])
sys.stdin=io.StringIO(json.dumps(request))
out=io.StringIO()
with contextlib.redirect_stdout(out):
    code=main([sys.argv[1]])
if code:
    print(out.getvalue(),file=sys.stderr)
    sys.exit(code)
result=json.loads(out.getvalue())
world=int(os.environ["WORLD_SIZE"])
assert result["operationUID"]==request["operationUID"]
assert result["checkpointID"]==request["checkpointID"]
assert result["prepared"] is True
assert type(result["generation"]) is int and result["generation"]>0
assert result["state"] in (("prepared","completed") if sys.argv[1]=="prepare" else ("completed",))
pointers=result["checkpointPointers"]
assert set(pointers)=={str(i) for i in range(world)}
assert all(isinstance(p,str) and p.startswith("/checkpoint/") for p in pointers.values())
receipt={k:result[k] for k in ("operationUID","checkpointID","generation","prepared","state")}
with open("/dev/termination-log","w") as f:
    json.dump(receipt,f)
print(json.dumps(receipt),flush=True)
`

type groupJobResult struct {
	OperationUID string `json:"operationUID"`
	CheckpointID string `json:"checkpointID"`
	Generation   int64  `json:"generation"`
	Prepared     bool   `json:"prepared"`
	State        string `json:"state"`
}

func groupJobName(plan *api.RestorePlan, action string) string {
	digest := sha256.Sum256([]byte(string(plan.UID) + "/" + fmt.Sprint(plan.Generation) + "/" + action))
	return fmt.Sprintf("group-%s-%x", action, digest[:12])
}
func desiredGroupJob(plan *api.RestorePlan, image, action string) *batchv1.Job {
	g := plan.Spec.GroupRestore
	payload, _ := json.Marshal(map[string]interface{}{
		"all": true, "operationUID": g.OperationUID, "checkpointID": plan.Spec.CheckpointRef.CheckpointID,
		"sourceFenceProof": map[string]interface{}{"allRanksFenced": true, "operationUID": g.OperationUID, "checkpointID": plan.Spec.CheckpointRef.CheckpointID, "sourceWorldUID": g.SourceWorldUID, "evidenceRef": plan.Spec.RequestUID + "/" + g.OperationUID},
	})
	no, yes := false, true
	retries := int32(0)
	deadline := int64(600)
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: groupJobName(plan, action), Namespace: plan.Namespace,
			OwnerReferences: []metav1.OwnerReference{{APIVersion: api.GroupVersion.String(), Kind: "RestorePlan", Name: plan.Name, UID: plan.UID, Controller: &yes}}},
		Spec: batchv1.JobSpec{BackoffLimit: &retries, ActiveDeadlineSeconds: &deadline, Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"migration.dcnlab.com/group-control": action}},
			Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever, AutomountServiceAccountToken: &no,
				NodeSelector: map[string]string{"kubernetes.io/hostname": plan.Spec.Pods[0].TargetNode},
				Tolerations:  []corev1.Toleration{{Key: "node.kubernetes.io/unschedulable", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule}, {Key: "restore-validation", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule}},
				Containers: []corev1.Container{{Name: "control", Image: image, ImagePullPolicy: corev1.PullIfNotPresent,
					Command:                []string{"python", "-c", groupJobScript, action},
					Env:                    []corev1.EnvVar{{Name: "GROUP_REQUEST", Value: string(payload)}, {Name: "WORLD_SIZE", Value: fmt.Sprint(g.WorldSize)}, {Name: "RANK", Value: "0"}, {Name: "FLUIDCR_SOURCE_WORLD_UID", Value: g.SourceWorldUID}, {Name: "FLUIDCR_CHECKPOINT_DIR", Value: g.CheckpointRoot}},
					VolumeMounts:           []corev1.VolumeMount{{Name: "checkpoint", MountPath: g.CheckpointRoot}},
					SecurityContext:        &corev1.SecurityContext{AllowPrivilegeEscalation: &no, ReadOnlyRootFilesystem: &yes, Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}},
					TerminationMessagePath: "/dev/termination-log", TerminationMessagePolicy: corev1.TerminationMessageReadFile}},
				Volumes: []corev1.Volume{{Name: "checkpoint", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: g.SharedPVC}}}},
			},
		}},
	}
}
func sameGroupJob(actual, want *batchv1.Job) bool {
	a, b := actual.Spec.Template.Spec, want.Spec.Template.Spec
	owner := metav1.GetControllerOf(actual)
	expected := metav1.GetControllerOf(want)
	if owner == nil || expected == nil || owner.UID != expected.UID || owner.Kind != expected.Kind || owner.Name != expected.Name || len(a.Containers) != 1 || len(a.InitContainers) != 0 || len(a.EphemeralContainers) != 0 {
		return false
	}
	x, y := a.Containers[0], b.Containers[0]
	return reflect.DeepEqual(x.Command, y.Command) && len(x.Args) == 0 && x.Image == y.Image && reflect.DeepEqual(x.Env, y.Env) && len(x.EnvFrom) == 0 &&
		reflect.DeepEqual(x.VolumeMounts, y.VolumeMounts) && reflect.DeepEqual(a.Volumes, b.Volumes) && reflect.DeepEqual(a.NodeSelector, b.NodeSelector) &&
		a.AutomountServiceAccountToken != nil && !*a.AutomountServiceAccountToken && a.RestartPolicy == b.RestartPolicy &&
		!a.HostNetwork && !a.HostPID && !a.HostIPC && reflect.DeepEqual(x.SecurityContext, y.SecurityContext) &&
		x.TerminationMessagePath == y.TerminationMessagePath && x.TerminationMessagePolicy == y.TerminationMessagePolicy
}
func (r *Reconciler) groupJob(ctx context.Context, plan *api.RestorePlan, action string) (groupJobResult, string, bool, error) {
	empty := groupJobResult{}
	var pvc corev1.PersistentVolumeClaim
	if err := r.Reader.Get(ctx, client.ObjectKey{Namespace: plan.Namespace, Name: plan.Spec.GroupRestore.SharedPVC}, &pvc); err != nil {
		return empty, "", false, err
	}
	if pvc.Status.Phase != corev1.ClaimBound || !pvc.DeletionTimestamp.IsZero() {
		return empty, "", false, fmt.Errorf("group shared PVC is not Bound")
	}
	want := desiredGroupJob(plan, r.GroupControlImage, action)
	var job batchv1.Job
	err := r.Reader.Get(ctx, client.ObjectKeyFromObject(want), &job)
	if apierrors.IsNotFound(err) {
		if err = r.Client.Create(ctx, want); err != nil && !apierrors.IsAlreadyExists(err) {
			return empty, "", false, err
		}
		return empty, "", false, nil
	}
	if err != nil {
		return empty, "", false, err
	}
	if !sameGroupJob(&job, want) || !job.DeletionTimestamp.IsZero() {
		return empty, "", false, fmt.Errorf("group control Job ownership/spec conflict")
	}
	for _, condition := range job.Status.Conditions {
		if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
			return empty, "", false, fmt.Errorf("group %s Job failed: %s", action, condition.Message)
		}
	}
	complete := false
	for _, condition := range job.Status.Conditions {
		if condition.Type == batchv1.JobComplete && condition.Status == corev1.ConditionTrue {
			complete = true
		}
	}
	if !complete {
		return empty, "", false, nil
	}
	var pods corev1.PodList
	if err := r.Reader.List(ctx, &pods, client.InNamespace(plan.Namespace), client.MatchingLabels{"batch.kubernetes.io/job-name": job.Name}); err != nil {
		return empty, "", false, err
	}
	var result *groupJobResult
	for _, pod := range pods.Items {
		owner := metav1.GetControllerOf(&pod)
		if owner == nil || owner.UID != job.UID || owner.Kind != "Job" {
			continue
		}
		podJob := want.DeepCopy()
		podJob.Spec.Template.Spec = pod.Spec
		if !sameGroupJob(podJob, want) {
			return empty, "", false, fmt.Errorf("group Job Pod spec mismatch")
		}
		for _, cs := range pod.Status.ContainerStatuses {
			if cs.Name != "control" || cs.State.Terminated == nil || cs.State.Terminated.ExitCode != 0 {
				continue
			}
			if result != nil {
				return empty, "", false, fmt.Errorf("duplicate group Job success receipts")
			}
			var got groupJobResult
			if err := json.Unmarshal([]byte(cs.State.Terminated.Message), &got); err != nil {
				return empty, "", false, fmt.Errorf("invalid group Job receipt: %w", err)
			}
			if got.OperationUID != plan.Spec.GroupRestore.OperationUID || got.CheckpointID != plan.Spec.CheckpointRef.CheckpointID || !got.Prepared || got.Generation <= 0 || (action == "resume" && got.State != "completed") || (action == "prepare" && got.State != "prepared" && got.State != "completed") {
				return empty, "", false, fmt.Errorf("group Job receipt identity/state mismatch")
			}
			result = &got
		}
	}
	if result == nil {
		return empty, "", false, fmt.Errorf("group Job completion lacks a verified termination receipt")
	}
	return *result, string(job.UID), true, nil
}
