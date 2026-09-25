# Validation Record

## Local checks

The following checks passed on 2026-09-25:

- Go 1.27.1: `go test -p 4 -mod=readonly -count=1 ./...`.
- Go: `go vet -p 4 -mod=readonly ./...`; `gofmt -l api cmd internal` returned no files.
- Race detector: `go test -race -p 4 -mod=readonly ./internal/management ./internal/member ./internal/artifact`.
- Python 3.13: `python -B -m unittest discover -s runtime/tests -v`, 13 tests passed.
- Linux/amd64 static manager build with `CGO_ENABLED=0`.
- Kustomize rendering for `config/crd`, `config/karmada`, `config/management`, `config/member`, and `config/checkpoint`.
- Bash syntax check for `scripts/create-karmada-kubeconfig.sh`.

Tests cover CRD structural schemas and CEL, actual RIC Lua execution and output schemas, stale/duplicate cluster reports, checkpoint identity and completion handling, restore mapping and ownership, admission rejection, artifact digests, and restricted deployment configuration.

The module and CI target Go 1.24; that exact toolchain was not executed locally. Linux-specific artifact tests were compiled, not executed. Windows symlink tests can skip when the host lacks symlink privileges. These results are not a live Kubernetes integration test.

## Not verified here

- Container image builds, image publication, and cert-manager installation.
- Live Karmada status aggregation, API-server admission ordering, controller restart recovery, and cluster RBAC authorization.
- GPU/CRIU restore, shared storage correctness, archive transport, and application state continuity.
- Global multi-Pod DDP checkpoint generation/rendezvous consistency. The overlay confirms local completion locks; the inherited shared-generation protocol is not transactional across Pods. See [FluidCR origin and limitations](fluidcr-origin.md).

The inspected local CRI-O source does not contain a handler for `checkpoint-restore.crio.io/<container>`. A separately tested runtime supporting that contract is required. Adding the capability label does not implement runtime support, and `Running`/`Ready` alone does not prove a restore occurred.

## Deployment acceptance checklist

1. Build and install the same payload overlay on source and target. Recreate source Pods before checkpointing; existing injected files do not change automatically.
2. Apply CRDs and perform server-side dry runs against the actual Kubernetes and Karmada versions. Verify RIC aggregated status generation and cluster identity.
3. Verify the management service account cannot read member credentials or access member APIs. Member agents use only their local in-cluster credentials.
4. Prove annotation-driven restore on one disposable Pod with the intended CRI-O, CRIU, GPU driver, mounts, and image before labelling any target node capable.
5. Run one checkpoint without overlapping rounds. Verify completion locks, Pod UIDs, archive paths and digests. Transfer archives independently and keep them immutable.
6. Fence the source and prepare volumes before setting the immutable request attestations. Confirm a wrong digest, wrong owner, missing payload, and unsupported node are rejected.
7. Wait for `Prepared`, then dispatch the labelled workload. Inspect runtime restore logs and application counters/tensors to distinguish recovery from a cold start.
8. Only after single-Pod acceptance, test every DDP rank, shared checkpoint generation, rendezvous, and resumed progress. Do not infer distributed consistency from per-Pod completion.
9. Exercise controller restarts, delayed status, unavailable nodes, expired credentials, Pod recreation, and the operator-controlled cleanup policy before production use.

Do not remove the source or recovery artifacts until application-level recovery has been proven. This implementation does not automate source fencing, volume migration, archive transfer, workload placement, or traffic cutover.
