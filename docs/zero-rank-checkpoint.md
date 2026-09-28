# Preserve rank zero in checkpoint evidence

PodMigrationStatus must emit rank zero. Go's omitempty on an
integer dropped the field, while restore admission correctly rejects missing
survivor ranks. The validators remain strict; missing rank is not proof of zero.

## Deployment

Build this repository's Dockerfile, not the HybridSpot System Dockerfile.
Update stateful-checkpoint on AWS for the checkpoint writer. Coordinate other
writers of typed FluidCRMigration status on the same release to avoid mixed
serialization behavior. RestoreRequest/RestorePlan types are unchanged.
The CRD already allows the integer rank field; no schema relaxation is needed.

## Existing completed checkpoint

Image rollout does not repair a persisted completed checkpoint. Do not delete
the checkpoint, resume survivors, or recreate the restore request just to
populate rank.

Before any targeted status repair, save the AWS checkpoint, Karmada aggregated
checkpoint, RestoreRequest, SpotReplacement, and current Pod JSON. Require:

- The request checkpoint reference UID and generation match its source object.
- The paused survivor name, Pod UID, and node agree across the operation,
  request, checkpoint status, and current Pod.
- The current Pod's pod-index label and explicit preservedSurvivors rank agree.
- Local runtime evidence agrees on rank, checkpoint ID, and survivor pause-lock
  generation, path, and PID. A missing rank alone is insufficient.
- No source fencing or restore execution has started.

If these checks hold, repair only the missing AWS status pod rank with a JSON
Patch against the status subresource. Test metadata UID and resourceVersion,
the indexed pod UID, and its SurvivorPaused phase in the same patch. Do not
replace the entire status or write a blanket zero to all pods. Never perform
a typed read/write of legacy missing ranks as a substitute for these checks.
Let normal aggregation carry the evidence to Karmada and confirm its contents.

RestoreReconciler requeues failed validation every poll interval (default five
seconds), so the existing request can be revalidated after evidence propagation.
An annotation-only retry is not needed; its watch filters generation changes.
If any identity or live pause evidence differs, stop rather than invent evidence.

The live recovery procedure is not validated by unit tests and requires the
current cluster evidence before choosing a patch.
