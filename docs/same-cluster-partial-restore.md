# Same-Cluster Partial-Rank Restore Contract

This path exists for Spot to On-Demand replacement while preserving FluidCR survivor ranks in the same Karmada member cluster. It is intentionally fail-closed: the old cross-cluster restore contract is unchanged, and a same-cluster `RestoreRequest` is rejected unless every partial-restore field and every FluidCR evidence field below is present.

## RestoreRequest / RestorePlan

Same-cluster partial restore requires:

- `spec.sourceCluster == spec.targetCluster`
- `spec.sourceFenced: false`
- `spec.volumesReady: true`
- `spec.partialRestore.preventPeriodicResume: true`
- `spec.partialRestore.targetRanks`: the exact ranks to restore onto replacement pods
- `spec.partialRestore.preservedSurvivors`: one UID-bound entry for every survivor rank
- each restored `spec.pods[]` mapping sets `rank` and `sourcePodUID`

`sourcePodUID` is the precondition for the source pod incarnation that actually stopped and produced the container checkpoint. It is not an operator Boolean attestation.

## FluidCR Checkpoint Evidence

The referenced `FluidCRMigration` must have `spec.resume: false` for same-cluster partial restore. Stateful rejects auto-resumed checkpoints because they can interfere with survivor parking and target replacement.

The exact partial checkpoint schema is:

```yaml
spec:
  resume: false
  partialCheckpoint:
    targetRanks: [1]
```

The checkpoint controller calls the FluidCR control API once with `POST /checkpoint` and a `ranks` list, then calls kubelet CRIU checkpoint only for those target ranks. There is no full-round fallback for partial checkpoint. Application `.pt` artifacts do not count as `ContainerCheckpointed` proof; only the kubelet archive plus exporter-populated `sha256` and `durableRef` satisfy target checkpoint evidence.

For each target rank, FluidCR status must report:

- `podName`, `podUID`, `nodeName`, `rank`
- `phase: ContainerCheckpointed`
- exactly one `checkpointFiles[]` entry matching the requested container, source path, SHA256, and durable ref
- restore archive wiring uses `sourcePath` from the original checkpoint `filePath` and a deterministic `targetPath` of `/var/lib/kubelet/checkpoints/<sha256>.tar`; exporter `archiveEvidenceID` is optional, with verification identity derived from the durable ref or SHA256 when it is absent

For each survivor rank, FluidCR status must report:

- `podName`, `podUID`, `nodeName`, `rank`
- `phase: SurvivorPaused`
- `survivorEvidence.generation`
- `survivorEvidence.pauseLockPath`
- positive `survivorEvidence.pauseLockPID`
- nonempty `survivorEvidence.observedAt`

The survivor evidence must match `spec.partialRestore.preservedSurvivors` by pod name, pod UID, node, rank, generation, and pause-lock path. A Boolean such as `paused: true` is not accepted.

## Runtime Verification

This flow trusts the administrator-certified, fail-closed CRI-O restore adapter.
Archive hashes, admission bindings, and Running/Ready Pods are not independent
CRIU success attestations. Native two-GPU restore must be validated separately
before certifying a node; never derive that certification from NodeReady alone.

Target runtime verification expects the full DDP world to be running after restore:

`worldSize == len(spec.pods) + len(spec.partialRestore.preservedSurvivors)`

Replacement target pods are checked against the member `RestorePlan` pod status. Survivor pods are checked against the preserved survivor UID/rank evidence, so a recreated pod with the same name cannot impersonate a survivor.

## Member Actuation

Same-cluster partial restore cannot rely on a template restore-plan label, because changing the StatefulSet template would roll survivor pods. The member admission webhook therefore handles ordinary Pod CREATE requests, leaves unrelated Pods alone, and binds a recreated target Pod only when exactly one active same-cluster partial `RestorePlan` matches the Pod name and workload identity. Ambiguous active plans and replayed plan UID/generation annotations are denied.

The member actuator does not force-delete or infer fencing from `sourceFenced=false`. After the target archive is durable and current-generation, the target node is admin-certified, and admission is ready to bind the replacement Pod, it issues a graceful delete for only the mapped target/source Pod name with a Kubernetes UID precondition matching `spec.pods[].sourcePodUID`. `status.sourceFences[]` records `DeleteRequested` and later `SourceGone` evidence for that old UID. If the current Pod at that name has any other UID and is not bound to the current plan UID/generation, the plan fails closed before any delete request.

Survivor ranks remain parked until the target native restore and source-fence evidence are verified. Management then authorizes release by patching the referenced `FluidCRMigration` with `training.dcnlab.com/restore-owned-resume=true`. The member checkpoint controller consumes that annotation even after a terminal partial checkpoint, re-reads live survivor `/runtime` pause-lock evidence for the current pod UID, and calls the scoped FluidCR endpoint with exactly:

```json
{"all":true,"checkpointID":"<checkpointID>","generation":123,"restoreOwnedResume":true}
```

Release failures are retryable and idempotent for the same checkpoint ID and generation. Generic `/resume` or plain `{"all":true}` is outside this contract while a partial restore-owned manifest is active. Full-world `TrainingRuntime` readiness, readyRanks, and global-step progress are checked only after this scoped release, so verification does not wait on a condition that requires the survivor to be released first.

The old target Pod must be removed by graceful UID-precondition deletion only after durable archive evidence and admission binding readiness exist. Kubelet archive existence alone does not prove the old Pod is gone; restore verification carries typed partial/source-fence fields so that actuator evidence can be reported explicitly.

## Payload Overlay

`Dockerfile.payload-overlay` copies `runtime/fluidcr/` over the base FluidCR payload. Do not build or deploy this overlay unless its FluidCR Python files support the same manifest-driven `checkpoint --rank`, `survivor_pause_and_rebuild`, `pause-lock`, generation, and status evidence contract described here.

Compatibility is enforced by source and tests: the overlay `/runtime` endpoint must expose `survivorEvidence`, and the checkpoint controller must use the rank-aware control API plus target-only kubelet checkpoints. A build argument is not compatibility proof.
