# Stateful member v5.0.1

## Scope

Wake the target StatefulSet once after a persisted group prepare receipt.
This is a liveness safeguard based on the observed manual-annotation recovery,
not proof of the underlying Kubernetes retry failure. No TrainingPolicy to
ConfigMap conversion, VM cleanup automation, checkpoint period, payload, or
group-control changes are included.

The member reads uncached persisted Prepared status, validates source fencing
and workload origin UID, and patches only top-level metadata with an optimistic
resourceVersion lock. A token bound to plan UID/generation and prepare receipt
survives process restarts. Failed patches retry through reconciliation. Missing
StatefulSets are revisited by the existing 30-second requeue. Pod templates and
replicas are unchanged. Member RBAC now requires StatefulSet patch.

## Build and publish on mgmt

Finish/archive the current manually assisted run and perform scoped cleanup first.
Do not reuse that run as an unassisted performance measurement.

```bash
cd /root/hybridspot-validation/stateful
git status --short
git pull --ff-only
bash scripts/release-member-v5.0.1.sh build
```

Requires Go matching go.mod, Buildah, and registry login. The build refuses a
dirty checkout. The v5.0.1 tag must not be overwritten with different source.
Only stateful-member needs deployment; the shared image also contains other
modes, but do not update unrelated deployments for this fix.

## Deploy to AWS

```bash
bash scripts/release-member-v5.0.1.sh deploy
```

The script preserves existing deployment args, payload images, certificates and
live role rules. It backs up the deployment/role and adds only patch permission.
Deploy between experiments: admission availability may briefly change during
rollout. No webhook bypass or failurePolicy change is needed. Apply the same
member update to another target cluster only in a separate planned rollout.

## Validate

Run a new experiment. After prepare completes, inspect the AWS StatefulSet's
`migration.dcnlab.com/group-prepare-reconcile` annotation. Confirm both new Pod
UIDs, Ready status, restore completion and increasing steps on both ranks without
manual annotation. Unit tests cannot prove live Kubernetes scheduling/retry
behavior. Preserve controller logs and timestamps if creation still stalls.

## Rollback

Use the saved deployment.json to read the previous manager image, then run
`kubectl --kubeconfig="$AWS_KUBECONFIG" -n stateful-migration-system set image
deployment/stateful-member manager=PREVIOUS_IMAGE` and check rollout status.
The added patch permission can remain temporarily; restore RBAC from the backup
only after comparing current rules for unrelated changes. Do not delete storage,
restore receipts, or patch success statuses to force progress.
