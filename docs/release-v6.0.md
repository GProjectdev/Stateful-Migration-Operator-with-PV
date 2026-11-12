# v6.0 architecture release

Multi-Cluster Restore Controller maps to existing `stateful-management`
(`--mode=management`). Member Checkpoint/Restore Controller spans existing
`stateful-member` and `stateful-checkpoint` processes; their modes remain
`member` and `checkpoint`. `stateful-artifact` remains the node-local verifier.

Release overlays expose presentation names through annotations. Existing names,
selectors, RBAC, CRDs, mode flags, leader-election identities and reconciliation
behavior are preserved.

Run from this repository on a Linux Buildah host:

```bash
bash scripts/build-v6.0.sh
kubectl kustomize release/v6.0/management > /tmp/stateful-migration-system-management-v6.0.yaml
kubectl kustomize release/v6.0/member > /tmp/stateful-migration-system-member-v6.0.yaml
kubectl kustomize release/v6.0/checkpoint > /tmp/stateful-migration-system-checkpoint-v6.0.yaml
```

Local image: `docker.io/jeongseungjun/stateful-migration-system:v6.0`.
Build scripts do not push or deploy. Review and commit source before producing
publishable images so revision labels identify reviewed content. These files
do not imply that v6.0 tags have been published. Publication and cluster changes
belong to a separate release step: record digests and back up live manifests
first, retain existing secrets/certificates/kubeconfigs/storage, and use the
prior manifests and image digests for rollback.

Build My_FluidCR-work's v6.0 images first. The member overlay uses
`docker.io/jeongseungjun/fluidcr-group-control:v6.0`. Then build the stateful
payload from this repository:

```bash
buildah bud --platform linux/amd64 -f Dockerfile.payload-overlay --build-arg FLUIDCR_PAYLOAD_IMAGE=docker.io/jeongseungjun/myfluidcr-operator:payload-base-v6.0 -t docker.io/jeongseungjun/myfluidcr-operator:payload-stateful-v6.0 .
```

During the separate deployment step, configure the injector payload-image with
the reviewed stateful payload digest. The base payload alone does not contain
this repository's stateful runtime overlay.
