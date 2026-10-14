# v3.1 Partial Restore release

This release is paired with HybridSpotVM_ManagementSystem v3.1.

## Scope

- Preserve one checkpoint entry per live model identity across survivor DDP rebuilds.
- Preserve completed partial restore operation evidence when later replacements change the workload.
- Keep staged-ready separate from verified runtime progress. Completion does not bypass initial source fencing or target verification.
- Use a fresh evaluation run and the updated injected payload on every rank. Do not resume the failed costb08 measurement or rewrite its checkpoint files.

## Images

- Controller (all modes): `docker.io/jeongseungjun/stateful-migration-system:v3.1`
- Injected runtime: `docker.io/jeongseungjun/myfluidcr-operator:payload-stateful-v3.1`

The training image is not the injected runtime image. Update the FluidCR webhook `--payload-image` argument and verify fresh Pods' initContainer image IDs.

See the [coordinated Buildah and deployment guide](https://github.com/GProjectdev/HybridSpotVM_ManagementSystem/blob/restore-automation-20260928/docs/evaluation-v3.1-release-20261014.md) for image build order, controller rollout, fresh-run execution and failed-run abort.

## Validation Boundary

Local contract/unit tests do not prove GPU/NCCL/CRIU recovery. Validate two successive partial replacements, continued progress on both ranks, and stable prior-operation receipts on the deployed cluster before accepting performance measurements.

