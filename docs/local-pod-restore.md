# Member-local standalone Pod restore

This is an explicit, isolated runtime test, not a management RestoreRequest or
the production partial-rank SpotReplacement workflow. Existing partial and
cross-cluster contracts are unchanged. GPU/CRI-O restore has NOT been verified
by the unit tests. Running/Ready is not native restore attestation.

`RestorePlan.spec.localPodRestore: true` permits a single rank-zero standalone
Pod in one member cluster. The plan must reference a real member-local completed
FluidCRMigration with resume=false, exact source Pod UID/node and exported
archive SHA256/durableRef. requestUID must equal that checkpoint's UID.
sourceFenced must be false: the member controller records deletion intent,
gracefully deletes only the UID-bound source, and observes its absence. There is
no force deletion and no fabricated survivor or cluster identity. Applying this
plan authorizes deletion of the named TEST POD once staging checks pass.

VolumesReady is an operator assertion: preserve the test PVC and original
volume mounts/image/payload. Do not use this mode for controller-owned Pods.
The target must have the existing runtime capability label. For initial runtime
certification, use that label ONLY temporarily on the cordoned isolated test
node. It is authorization for the experiment, not evidence of restore success.

## Upgrade (MGMT)

Build/push the reviewed commit to a new immutable image tag, then update only
stateful-member; no CRI-O, FluidCR payload or checkpoint-controller rebuild is
needed for this change. Keep current cluster-name, TLS mounts and arguments.

```bash
export ST=/root/hybridspot-validation/stateful
export ST_IMAGE=docker.io/jeongseungjun/stateful-migration-operator:local-pod-restore-20260927-01
docker build -t "$ST_IMAGE" "$ST"
docker push "$ST_IMAGE"
kubectl --kubeconfig="$AWS_KUBECONFIG" apply -f "$ST/config/crd/restoreplans.yaml"
kubectl --kubeconfig="$AWS_KUBECONFIG" apply -f "$ST/config/member/role.yaml"
kubectl --kubeconfig="$AWS_KUBECONFIG" -n stateful-migration-system set image deployment/stateful-member manager="$ST_IMAGE"
kubectl --kubeconfig="$AWS_KUBECONFIG" -n stateful-migration-system rollout status deployment/stateful-member --timeout=180s
```

## Export and preserve the source

Do not delete the Pod yourself. Do not change the checkpoint spec or status.
Run in Bash on MGMT with the AWS member kubeconfig. The namespace, Pod and node
below are the isolated test, not trainer-0/trainer-1.

```bash
set -euo pipefail
export ROOT=/root/hybridspot-validation NS=fluidcr-demo
export TEST_NODE=restore-test-ondemand-01
export MIG=restore-smoke-ckpt-20260927141900
export PLAN=restore-smoke-local-01
mkdir -p "$ROOT/evidence" "$ROOT/rendered"
kubectl --kubeconfig="$AWS_KUBECONFIG" cordon "$TEST_NODE"
kubectl --kubeconfig="$AWS_KUBECONFIG" label node "$TEST_NODE" migration.dcnlab.com/artifact-node=true --overwrite
kubectl --kubeconfig="$AWS_KUBECONFIG" -n stateful-migration-system get pods --field-selector="spec.nodeName=$TEST_NODE" -o wide
```

Wait for stateful-artifact on this node to be Ready. It uses the existing
stateful-migration-artifacts PVC; never substitute the training checkpoint PVC.
Then capture checkpoint evidence (retry capture if export has not finished):

```bash
kubectl --kubeconfig="$AWS_KUBECONFIG" -n "$NS" get fluidcrmigration "$MIG" -o json > "$ROOT/evidence/$MIG.json"
jq -e '.status.phase=="Completed" and (.status.pods|length)==1 and
  .status.pods[0].phase=="ContainerCheckpointed" and
  (.status.pods[0].checkpointFiles|length)==1 and
  (.status.pods[0].checkpointFiles[0].sha256|test("^[0-9a-f]{64}$")) and
  (.status.pods[0].checkpointFiles[0].durableRef|startswith("file-store:"))' "$ROOT/evidence/$MIG.json"
kubectl --kubeconfig="$AWS_KUBECONFIG" -n "$NS" get pod restore-smoke -o json > "$ROOT/evidence/$PLAN-source.json"
jq -e --slurpfile cp "$ROOT/evidence/$MIG.json" '.metadata.uid==$cp[0].status.pods[0].podUID and (.metadata.ownerReferences // [] | length)==0' "$ROOT/evidence/$PLAN-source.json"
```

Preserve the ORIGINAL pre-injection manifest `$ROOT/rendered/restore-smoke.yaml`.
Do not recreate directly from get pod output (it contains admission-injected
payload mounts, nodeName, old UID and projected service-account token volume).

## Prepare and apply the plan

```bash
jq --arg plan "$PLAN" --arg node "$TEST_NODE" '
  . as $cp | .status.pods[0] as $pod | $pod.checkpointFiles[0] as $a |
  {apiVersion:"migration.dcnlab.com/v1alpha1",kind:"RestorePlan",
   metadata:{name:$plan,namespace:$cp.metadata.namespace},
   spec:{localPodRestore:true,requestUID:$cp.metadata.uid,
     checkpointRef:{name:$cp.metadata.name,uid:$cp.metadata.uid,
       generation:$cp.metadata.generation,checkpointID:$cp.metadata.annotations["training.dcnlab.com/checkpoint-id"]},
     workloadRef:$cp.spec.workloadRef,sourceCluster:"aws",targetCluster:"aws",
     sourceFenced:false,volumesReady:true,
     pods:[{rank:0,sourcePod:$pod.podName,sourcePodUID:$pod.podUID,
       sourceNode:$pod.nodeName,targetPod:$pod.podName,targetNode:$node,
       archives:[{containerName:$a.containerName,sourcePath:$a.filePath,
         targetPath:("/var/lib/kubelet/checkpoints/"+$a.sha256+".tar"),
         sha256:$a.sha256,durableRef:$a.durableRef}]}]}}
' "$ROOT/evidence/$MIG.json" > "$ROOT/rendered/$PLAN.json"
kubectl --kubeconfig="$AWS_KUBECONFIG" apply --dry-run=server -f "$ROOT/rendered/$PLAN.json"
# Temporary permission ONLY for this isolated test node; not certification.
kubectl --kubeconfig="$AWS_KUBECONFIG" label node "$TEST_NODE" migration.dcnlab.com/restore-from-file=true --overwrite
# SIDE EFFECT: after artifact checks the controller deletes restore-smoke.
kubectl --kubeconfig="$AWS_KUBECONFIG" apply -f "$ROOT/rendered/$PLAN.json"
kubectl --kubeconfig="$AWS_KUBECONFIG" -n "$NS" wait restoreplan/"$PLAN" --for=jsonpath='{.status.sourceFences[0].phase}'=SourceGone --timeout=300s
kubectl --kubeconfig="$AWS_KUBECONFIG" -n "$NS" get restoreplan "$PLAN" -o json | jq '.status'
```

If waiting fails, inspect plan status and stateful-member logs. Do not delete
finalizers, fake status, remove admission checks, or resume the source to bypass
an error. A Failed local plan still blocks an unlabelled cold-start recreation.

## Recreate, verify native import, then resume

Only after SourceGone, recreate from the saved original manifest. Admission
finds the plan by name/namespace even without an explicit plan label. It requires
current-generation artifact verification younger than two minutes and adds the
native archive annotation, plan UID/generation and target node affinity.

```bash
kubectl --kubeconfig="$AWS_KUBECONFIG" apply -f "$ROOT/rendered/restore-smoke.yaml"
kubectl --kubeconfig="$AWS_KUBECONFIG" -n "$NS" wait pod/restore-smoke --for=condition=Ready --timeout=600s
kubectl --kubeconfig="$AWS_KUBECONFIG" -n "$NS" get pod restore-smoke -o json > "$ROOT/evidence/$PLAN-restored.json"
jq '{uid:.metadata.uid,node:.spec.nodeName,annotations:.metadata.annotations,containers:.status.containerStatuses}' "$ROOT/evidence/$PLAN-restored.json"
```

Verify the new Pod UID differs, the correct archive annotation/plan binding are
present and no startup error occurred. On the test Worker preserve
`sudo journalctl -u crio --since '15 minutes ago' --no-pager` and the corresponding
CRIU restore log. Require positive archive import/restore evidence for this
container, not only absence of errors. If evidence is unclear, STOP here.

After native restoration is confirmed, resume ONLY the standalone test via its
own control endpoint, then query runtime twice to prove learning progresses:

```bash
kubectl --kubeconfig="$AWS_KUBECONFIG" -n "$NS" exec restore-smoke -c trainer -- python3 -c 'import urllib.request; r=urllib.request.Request("http://127.0.0.1:8298/resume",data=b"{}",headers={"Content-Type":"application/json"}); print(urllib.request.urlopen(r,timeout=60).read().decode())'
for i in 1 2; do
  kubectl --kubeconfig="$AWS_KUBECONFIG" -n "$NS" exec restore-smoke -c trainer -- python3 -c 'import urllib.request; print(urllib.request.urlopen("http://127.0.0.1:8298/runtime",timeout=10).read().decode())'
  sleep 10
done
```

Keep the test PVC, checkpoint CR, archives, source/restored Pod JSON, plan and
runtime logs as evidence. Remove the temporary restore-from-file label if the
test fails; do not treat this single-rank result as distributed NCCL restore
certification. Keep the plan until the test Pod is deliberately retired; reusing
the same name while the plan exists remains restore-bound, not a cold start.
