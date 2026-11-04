# E2 checkpoint publication fix

Use a new run/PVC after the failed `e2-1104021053` experiment. Preserve its
diagnostics. Do not remove its stale control lock to retry FIS. Previous verified
archives remain governed by the existing ownership/full-world validation; failed
periodic checkpoints do not authorize cross-run rollback.

## Deployment

Only the FluidCR payload overlay changes. Karmada, FIS gates and controller images
remain unchanged. Build using Buildah, not Docker. On mgmt:

```bash
cd /root/hybridspot-validation/stateful
git fetch origin restore-automation-20260928
git merge --ff-only origin/restore-automation-20260928
kubectl --kubeconfig="${AWS_KUBECONFIG:?}" -n fluidcr-system \
  get deployment fluidcr-webhook -o json |
  jq '.spec.template.spec.containers[] | {name,args}'
```

Set `BASE_IMAGE` to the currently deployed `--payload-image` argument (not the
webhook or trainer image). Keep the original argument for rollback.

```bash
export IMAGE=docker.io/jeongseungjun/myfluidcr-operator:payload-e2-publication-20261104
buildah bud --arch amd64 -f Dockerfile.payload-overlay \
  --build-arg "FLUIDCR_PAYLOAD_IMAGE=${BASE_IMAGE:?}" -t "$IMAGE" .
buildah push --digestfile /root/e2-publication.digest "$IMAGE" "docker://$IMAGE"
export PAYLOAD_IMAGE="docker.io/jeongseungjun/myfluidcr-operator@$(cat /root/e2-publication.digest)"
```

Stop on errors. Apply only the webhook payload argument using the guarded JSON
patch procedure in System/docs/partial-resume-release-20260930.md section 5.
Preserve other arguments and retain its resourceVersion check. Wait for webhook
rollout. Existing workload Pods do not get new injected files: create a new run.
For AWS-only E2 update AWS; before cross-cluster tests deploy the identical payload
to both members. Verify new Pod initContainer images match the pushed digest.

## Validation

The overlay copies, syncs and hashes each rank artifact outside the shared control
lock, then revalidates the round/generation under the lock before publishing.
Watchdog and save errors use a failure exit code instead of successful snapshot
code 99. The launcher does not enter checkpoint-ready handling on that failure.
Publication diagnostics identify copy, fsync, hash and commit stages.

The short metadata commit still uses an exclusive-create shared lock. A process
or host crash inside that short critical section can leave a lock; automatic
lock stealing is deliberately not implemented. This change reduces the long
critical section, not all possible distributed-storage failure modes. The 60s
watchdog is not increased: slow individual storage can still fail safely.

Render/prepare/start a new E2 run with the existing scripts. Use new instance IDs,
an instance-scoped FIS role, and an alarm for the new On-Demand instance. Never
reuse the old FIS schedule. Require a Completed full-world checkpoint, both current
Pod UIDs and exported archives before injection. Check training resumes after each
periodic checkpoint. Preserve the 20-minute scheduler's fail-closed behavior.

Local unit tests do not validate live CUDA/NFS behavior or end-to-end archive
recovery. Run those checks before claiming the experiment succeeds.
