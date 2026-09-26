# Validation Record

## Local checks

The following checks passed on 2026-09-25:

- Go 1.27.1: `go test -p 4 -mod=readonly -count=1 ./...`.
- Go: `go vet -p 4 -mod=readonly ./...`; `gofmt -l api cmd internal runtime/crio` returned no files.
- Race detector: `go test -race -p 4 -mod=readonly ./internal/management ./internal/member ./internal/artifact`.
- Python 3.13: `python -B -m unittest discover -s runtime/tests -v`, 13 tests passed.
- Linux/amd64 static manager build with `CGO_ENABLED=0`.
- Kustomize rendering for `config/crd`, `config/karmada`, `config/management`, `config/member`, and `config/checkpoint`.
- Bash syntax check for `scripts/create-karmada-kubeconfig.sh`.

Tests cover CRD structural schemas and CEL, actual RIC Lua execution and output schemas, stale/duplicate cluster reports, checkpoint identity and completion handling, restore mapping and ownership, admission rejection, artifact digests, and restricted deployment configuration.

The module and CI target Go 1.25; that exact toolchain was not executed locally. Linux-specific artifact tests were compiled, not executed. Windows symlink tests can skip when the host lacks symlink privileges. These results are not a live Kubernetes integration test.

## Extension Checks

The new suspension, management and manager packages passed their targeted tests.
Race tests passed for suspension, management and the standalone CRI-O adapter helper.
The full Stateful test suite was rerun after adding the release receipt and two-Pod samples.
The sample contract test checks replica count, retained claims, two ordinal mappings and
false-by-default fencing/readiness attestations. All 24 Bash blocks in the runtime and
combined installation guides passed syntax checks; relative documentation links resolved.
The adapter patch passed `git apply --check` against a temporary copy of the supplied
CRI-O `server/container_create.go`; the original runtime source was not modified.
This checks patch applicability, not compilation of the full CRI-O server.
PV-Migration-System passed its full test suite and vet, including a two-volume cleanup
regression proving that PV completion does not resume workload dispatch.

## Not verified here

- Container image builds, image publication, and cert-manager installation.
- Live Karmada status aggregation, API-server admission ordering, controller restart recovery, and cluster RBAC authorization.
- GPU/CRIU restore, shared storage correctness, archive transport, and application state continuity.
- Global multi-Pod DDP checkpoint generation/rendezvous consistency. The overlay confirms local completion locks; the inherited shared-generation protocol is not transactional across Pods. See [FluidCR origin and limitations](fluidcr-origin.md).

The inspected local CRI-O source does not contain a handler for `checkpoint-restore.crio.io/<container>`. The runtime installation guide now supplies an explicit adapter for that gap. Full CRI-O compilation, installation, and GPU recovery still require Linux integration validation. Adding the capability label does not implement runtime support, and `Running`/`Ready` alone does not prove a restore occurred.

## Deployment acceptance checklist

1. Build and install the same payload overlay on source and target. Recreate source Pods before checkpointing; existing injected files do not change automatically.
2. Apply CRDs and perform server-side dry runs against the actual Kubernetes and Karmada versions. Verify RIC aggregated status generation and cluster identity.
3. Verify the management service account cannot read member credentials or access member APIs. Member agents use only their local in-cluster credentials.
4. Prove annotation-driven restore on one disposable Pod with the intended CRI-O, CRIU, GPU driver, mounts, and image before labelling any target node capable.
5. Run one checkpoint without overlapping rounds. Verify completion locks, Pod UIDs, archive paths and digests. Transfer archives independently and keep them immutable.
6. Fence the source and prepare volumes before setting the immutable request attestations. Confirm a wrong digest, wrong owner, missing payload, and unsupported node are rejected.
7. Wait for `Prepared`, attach the UID-bound suspension gate, and select only the target cluster. Confirm the gate removes dispatch suspension only after PV completion and current restore readiness. Inspect runtime restore logs and application counters/tensors to distinguish recovery from a cold start; release FluidCR application locks only after all target containers have restored.
8. Only after single-Pod acceptance, test every DDP rank, shared checkpoint generation, rendezvous, and resumed progress. Do not infer distributed consistency from per-Pod completion.
9. Exercise controller restarts, delayed status, unavailable nodes, expired credentials, Pod recreation, and the operator-controlled cleanup policy before production use.

Do not remove the source or recovery artifacts until application-level recovery has been proven. This implementation does not automate source fencing, volume migration, archive transfer, workload placement, or traffic cutover.

## Managed restore identity contract

New managed RestoreRequests must set spec.workloadRef.uid to the MGMT workload UID and spec.trainingRuntimeRef.name explicitly. The referenced FluidCRMigration must carry the same spec.workloadRef.uid and the training.dcnlab.com/checkpoint-id annotation. Restore verification also requires exported checkpointFiles sha256 and durableRef fields from the shared file-store exporter before source loss.
