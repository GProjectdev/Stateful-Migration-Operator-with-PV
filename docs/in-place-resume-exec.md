# Resume after the launcher closes its control API

The launcher intentionally closes its HTTP listener before CRIU takes the
snapshot. A periodic checkpoint must therefore not depend exclusively on HTTP
`/resume` to release the launcher lock.

The checkpoint controller now falls back to Pod exec only on TCP connection
refusal. Other HTTP failures remain failures. The fallback requires resume=true
(including the default), no partialCheckpoint, persisted Pod UID/checkpoint ID
evidence, and a fresh uncached Pod identity check. Inside the container it checks
the matching full-world manifest and local CheckpointReady evidence under the
runtime control lock. It releases only the current rank's lock, retaining the
manifest until the final rank is released. It never removes survivor pause-locks,
checkpoint data, archived workspace data, or the generation counter.

The controller waits for the same Pod's runtime API to return Running before
reporting exec recovery success. This proves API recovery, not increasing
training steps or end-to-end GPU restore correctness. Pod exec is addressed by
name; Kubernetes does not provide an atomic Pod UID precondition for this call.
Fresh checks plus in-container round identity checks reduce, but do not eliminate,
the replacement race. Keep operation IDs unique and do not reuse old manifests.

## Deployment

Build and push the updated stateful operator image from this checkout, pin its
registry digest, then run on mgmt:

```bash
kubectl --kubeconfig="$AWS_KUBECONFIG" apply -f config/checkpoint/role.yaml
kubectl --kubeconfig="$AWS_KUBECONFIG" auth can-i create pods/exec \
  --as=system:serviceaccount:stateful-migration-system:stateful-checkpoint \
  -n fluidcr-demo
kubectl --kubeconfig="$AWS_KUBECONFIG" -n stateful-migration-system \
  set image deployment/stateful-checkpoint manager="$STATEFUL_PINNED"
kubectl --kubeconfig="$AWS_KUBECONFIG" -n stateful-migration-system \
  rollout status deployment/stateful-checkpoint --timeout=300s
```

Only stateful-checkpoint and its RBAC need this change. The deployed payload must
provide the existing fluidcr ctrl/distributed/group_restore modules. No new
launcher or application image is needed for this recovery transport.

Already Failed migrations are terminal: an image rollout or annotation does not
automatically retry them. Preserve evidence and separately recover the paused
workload under its existing identity before starting a new checkpoint. Do not
delete Pods, recreate an old checkpoint ID, or blindly remove all locks.

## Validation

Run Go checkpoint tests and Python tests in internal/checkpoint. In the cluster,
keep replacement risk disabled, test one new periodic checkpoint, require both
Pods to retain their UIDs, checkpoint Completed with successful resume, both
runtime ranks Running, and training steps increasing. Test partial migration
separately: it must remain paused until the restore-owned release path runs.
