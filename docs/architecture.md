# Architecture

## Control boundary

Only Karmada agents/controllers perform inter-cluster propagation and status collection.
The management process has one client, pointed explicitly at the Karmada API.
It cannot read member Secrets, Pods, Nodes, or cluster proxy subresources.
The checkpoint process, admission server and artifact verifier run in members using
their own in-cluster service accounts. Management never invokes kubelet or FluidCR HTTP.

```text
Karmada FluidCRMigration --source-only PP--> member FluidCR checkpoint controller
                        <--RIC----------- .status.pods[].checkpointFiles[]
Karmada RestoreRequest --> management Restore Controller
                       --> RestorePlan --source+target PP--> member artifact exporter/verifier
                                                        --> restore admission
                       <--RIC------------- .status Prepared / Running
```

## Contracts

- FluidCRMigration retains FluidCR's existing group, kind and spec.
  Its status has additive per-pod identity and Karmada clusters fields.
- RestoreRequest is a management-only, immutable, explicit migration request.
  Do not propagate it to members.
- RestorePlan is a generated immutable target plan with exact Pod, container,
  node, archive path and SHA-256 mappings.
- Each checkpoint operation uses a NEW CR name. Do not re-trigger by spec updates,
  repoint the checkpoint propagation policy or recreate the same operation name.
- ResourceInterpreterCustomization reflects local status and aggregates it by cluster.
  It does not call kubelet, transfer files, mutate Pods, or restore processes.

## Preparation versus execution

Prepared means target archives were downloaded from the durable file-store, verified, and the plan is armed. It must be observable
BEFORE releasing suspended workload dispatch. Waiting for Running before release deadlocks.
Running means mapped Pods are Running and Ready with the expected restore configuration.
It is NOT proof of CRIU success, correct training rank state, restored iteration, or data integrity.
Inspect CRI-O/CRIU logs and application-level progress to establish semantic recovery.

The opt-in Suspension Controller releases StatefulSet ResourceBinding dispatch only after
current UID-bound PVMigration and RestoreRequest/RestorePlan gates pass.
It removes the dispatching field rather than assigning false (Karmada accepts true only).
See [suspension contract](suspension.md).
No controller changes workload placement, deletes source Pods, copies archives or opens
traffic. PV-Migration-System separately stages retained NFS PVs.
SourceFenced and VolumesReady are operator attestations, not independently verified claims.

## Why a validating webhook too?

The supplied FluidCR injection webhook is fail-open and not automatically reinvoked.
Opt in on the workload template BEFORE admission. The restore mutator sets the CRI-O
annotations and pins the exact target node without bypassing the scheduler. A final
validating webhook checks that the original FluidCR payload mounts really exist.
A failed injector must not turn restore into a normal cold start.

## Legacy behavior intentionally not retained

The previous operator read and modified members through the Karmada cluster proxy,
packaged checkpoints into registry images, and resumed dispatch when Restore CR placement
matched. This implementation uses FluidCR archive paths and separates Prepared from Running.
Legacy StatefulMigration, CheckpointBackup and CheckpointRestore resources are not converted.
Legacy cron checkpoint scheduling and registry image building are not included.
Legacy annotation-only suspension release is replaced with UID-bound CR validation.

## Runtime dependency found during inspection

FluidCR's examples/prod/restore-pod.yaml uses checkpoint-restore.crio.io/<container>.
The inspected local leehun-cri-o-main server/container_create.go recognizes checkpoint
archives via the image field, but a repository search did not find this annotation handler.
The [runtime installation guide](runtime-installation.md) includes a small explicit adapter
patch to bridge that annotation to the existing CRI-O checkpoint import path.
Do not assume that the unpatched source tree implements the example's contract.
The target must run a tested runtime build that honors this annotation and fails if restore
fails. The restore webhook requires the admin-certified node label
migration.dcnlab.com/restore-from-file=true. The label is an attestation, not capability detection.

References:
- https://karmada.io/docs/userguide/globalview/customizing-resource-interpreter/
- https://kubernetes.io/docs/reference/access-authn-authz/extensible-admission-controllers/
