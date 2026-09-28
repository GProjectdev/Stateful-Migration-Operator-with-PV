# Full-world Restore Execution

This contract restores every rank of a StatefulSet from one exported checkpoint.
It is distinct from partial restore. RestoreRequest lives only in Karmada;
management creates one immutable RestorePlan and propagates it to source and
target members (one member for same-cluster recovery).

## Request Contract

```yaml
groupRestore:
  operationUID: operation-unique-id
  sourceWorldUID: origin-statefulset-uid
  worldSize: 2
  sharedPVC: fluidcr-checkpoint-shared
  checkpointRoot: /checkpoint
  sourcePods:
  - rank: 0
    podName: trainer-0
    podUID: current-source-pod-uid
    nodeName: worker-0
    nodeProvisionRef:
      name: worker-0
      uid: member-nodeprovision-uid
      instanceID: i-example
  - rank: 1
    podName: trainer-1
    podUID: current-source-pod-uid-1
    nodeName: worker-1
```

`spec.pods` still contains the full archive mapping, including the historical
archive Pod UID, target Pod ordinal, target node and exported archive SHA256 and
durableRef. `groupRestore.sourcePods` contains the CURRENT source world, never
copied blindly from a historical archive. The checkpoint must be Completed and
every mapped archive exported. Historical `resume:true` checkpoints are allowed.
`trainingRuntimeRef` is required. `partialRestore` is mutually exclusive.
Operation/world identifiers are 1-128 ASCII letters/digits/dot/underscore/hyphen,
starting with a letter or digit. World size is 1-64 with all ordinals present.

There is no caller-supplied checkpoint generation or control image. The independent
group-control CLI reads and validates producer metadata and returns the generation.
Configure the operator with `--group-control-image=<immutable image digest>`.

## Execution And Safety Gates

1. Source member validates the bound writable NFS PVC/PV identity. It rejects
   unlisted or changed current source Pod identities. Admission blocks recreation
   of the entire named StatefulSet, including unmapped ordinals.
2. Source member persists UID-bound deletion intent before deleting. A freshly
   Ready node (heartbeat or NodeLease) permits graceful UID-precondition deletion.
   An absent or unreachable source requires an exact member NodeProvision UID,
   instance and operation match with current-generation `status.fence.phase=Fenced`
   and matching `spec.fence`. Merely deleting an API Pod is not lost-node fencing.
3. All source receipts become `SourceGone`; cross-cluster source phase becomes
   `SourceFenced`. Management validates all ranks and publishes
   `migration.dcnlab.com/group-source-fence` on the Plan. The target compares source
   and target NFS server/path. Matching PVC names alone are insufficient.
4. Target creates an operation-owned prepare Job using the independent My_FluidCR
   group-control image, shared PVC and stdin JSON CLI. Jobs require no Torch, GPU
   allocation or Kubernetes token. Job owner, template and termination receipt
   are checked. Caller booleans never authorize fencing.
5. `Prepared` means prepare succeeded and target archive/node evidence is ready.
   Only then can admission bind target Pods to this Plan UID/generation and exact
   target mappings. The StatefulSet controller recreates Pods; this operator does
   not mutate replicas. Same-cluster attempts before prepare are denied and retried.
6. Once every bound target Pod is Running/Ready, an operation-owned resume Job
   runs. Its generation must match prepare. Member reports Running after resume.
7. Management verifies a fresh complete TrainingRuntime world with exact target
   Pod UIDs/checkpoint and two increasing samples, BOTH observed after resume.
   Only then does RestoreRequest become Verified.

Source receipts: `sourceFences[]` contains `podName`, `sourcePodUID`,
`observedGeneration` (Plan generation), `phase=SourceGone`, `deleteRequestedAt`,
and `goneObservedAt`. Group receipt: `groupControl` contains `operationUID`,
`checkpointID`, `checkpointGeneration`, `prepareJobUID`, `preparedAt`,
`resumeJobUID`, `resumedAt`, `volumeServer`, `volumePath`, `volumePVCUID`,
and `volumePVUID`. These fields are preserved in Karmada cluster aggregation.

System's cross-cluster placement release must require the exact request-owned Plan,
current target generation, target phase Prepared, matching group prepare receipt
and complete source fencing. Running alone is not permission to release placement.
Target runtime collection may be provisioned before the StatefulSet arrives.

## Completion And Subsequent Operations

Management adds `migration.dcnlab.com/group-target-verified=<requestUID>` only
after verification. Target automatic enrollment then retires; an explicit label
pointing to that retired Plan is rejected to prevent archive replay. Producers
must clear/update an old restore-plan template label for a subsequent operation.
Verified request evidence is terminal, not a perpetual workload health probe.
Cross-cluster SOURCE admission remains blocked even after target verification.
The placement owner removes that old Plan only after target verification AND
source placement removal. Do not remove finalizers or barriers as a retry shortcut.

## Deployment And Validation

Update CRDs on Karmada and members, the RestorePlan resource interpreter on
Karmada, management/member images, member RBAC, and the group-control image flag.
`config/member/deployment.yaml` contains a development placeholder image; replace
it with the independently built trusted group-control image digest. Source and
target shared PVCs must already point at the same NFS export path. Member control
Jobs need scheduling access to the first target node and NFS access there.
Keep admission webhooks failurePolicy=Fail and restrict Plan/status/receipt writes
to controllers; the receipt is an RBAC-bound control-plane proof, not a signature.

Run local tests before rollout:

```sh
go test -mod=readonly ./...
go vet -mod=readonly ./...
```

Then validate same-cluster full-world replacement, cross-cluster held-placement
migration, and interrupted-node recovery separately. Record source intents and
receipts, target prepare/resume Job UIDs and logs, actual runtime restore logs,
new Pod UIDs, and two post-resume rank samples. Confirm no target Pod is admitted
before Prepared and no old source world can recreate during the barrier.
Automated local tests do not establish GPU/CRIU or real-cloud end-to-end success.
