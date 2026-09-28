# Partial checkpoint readiness recovery

## Fixed contract

Partial checkpoint calls now request `wait=true` and a bounded timeout on each
target Pod. Only `checkpoint-ready` permits the container checkpoint stage.
`checkpoint-signalled`, survivor-only responses, empty results and errors fail
closed. Survivor evidence remains a separate prerequisite. The runtime's shared
checkpointID marker deduplicates signalling across target Pods.

Deploy the updated `stateful-checkpoint` manager in the AWS member cluster.
This change does not require a CRD or payload change if the deployed payload
already implements `checkpoint_ranks_and_wait` for partial requests. An older
payload must be upgraded before retrying; do not relax response validation.

## Existing operations

This change does not certify archives already marked ContainerCheckpointed,
nor retroactively validate persisted AppCheckpointed statuses. Do not reuse an
old operation or archive produced before target readiness was confirmed.

1. Preserve the old migration, replacement, restore request/plan YAML and runtime
   error logs. Keep the failed archive for diagnosis; do not edit its CRIU images.
2. Set the risk profile lambda to zero and confirm its observed generation.
   This does not cancel an existing operation. Identify the active replacement
   and any group-restore intent before cleaning up owned requests and plans.
3. Retire the failed control objects and confirm deletion has propagated to the
   AWS member. Do not remove finalizers to force this. Do not delete the source
   or replacement node while it still hosts a needed Pod or the only archive.
4. A fresh training start after a source Pod has been fenced is NOT successful
   native restore. Agree on the application checkpoint/restart baseline first;
   recreating Pods can lose progress after that checkpoint.
5. After restore admission no longer owns the workload, recover both training
   ranks using the intended payload and application checkpoint. Confirm runtime
   Running, readyRanks=worldSize=2, and increasing training steps. Pod Ready alone
   is insufficient. Record current Pod UIDs and nodes; ordinal placement can swap.
6. Before re-enabling replacement, verify target runtime prerequisites, driver
   library resolution and real restore capability. Do not certify a node solely
   because cuda-checkpoint --help succeeds. Check projected serviceaccount mount
   handling independently of this readiness fix.
7. Start a NEW operation with a NEW checkpointID and current Pod identities. Do
   not replay an old rendered retry JSON with stale UIDs or node bindings.
8. Require target appCheckpointResult to contain checkpoint-ready, then verify
   ContainerCheckpointed, matching checkpointIDs and exported SHA/durableRef.
   On failure, collect the target's application response; do not bypass the gate.
9. Success requires the target Pod restored on the replacement, survivor identity
   preserved, RestoreRequest verification successful and training steps advancing.
   Prepared, archive export, or Pod Running alone is not end-to-end success.

## Validation boundary

Unit tests cover bounded wait payloads, rejection of unready responses, each
target being contacted, and no kubelet checkpoint on unready target responses.
Real GPU cross-node restore, projected-volume remapping and remote deployed
payload compatibility must still be verified in the cluster.
