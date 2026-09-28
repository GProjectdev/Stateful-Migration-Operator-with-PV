# Partial restore staging before readiness

A native-restored FluidCR launcher can be Running while waiting on its rank lock,
with the control API deliberately stopped before the CRIU checkpoint. Waiting for
TCP readiness before authorizing the partial round release creates a cycle.

The member controller now reports `StagedReady` only after checking the current
plan binding, fresh target archive, controller-owned source deletion, replacement
UID, running container without restart, and preserved survivor identities. A
read-only `python3 -S` exec checks the restored PID 1 source identity, target rank,
checkpoint-ready status, launcher lock and operation-owned manifest generation.
It does not import the payload, delete locks, rewrite identity, or resume workers.
The member re-reads target and plan identities after the probe.

Management accepts that distinct staging state to authorize the existing
checkpoint controller's restore-owned resume. That controller validates live
survivor evidence and calls the existing checkpoint-ID/generation-bound API,
which releases the shared round. Readiness and actual per-rank progress are still
required for final `Verified`. Staging is not a native CRIU success attestation.

## Deployment

After updating this repository on MGMT, run `bash scripts/deploy-partial-staging.sh`
with `AWS_KUBECONFIG`, `MGMT_KUBECONFIG`, and `KARMADA_KUBECONFIG` exported.
This builds and pushes an image and updates the two controllers. It performs no
Pod/checkpoint deletion or manual lock release. The script uses its own repository
as the absolute build context and validates the Go module before building.

Build the Stateful repository, not System or the payload repository. Deploy the
same digest to AWS `stateful-member` and MGMT `stateful-management` in namespace
`stateful-migration-system`. Apply `config/member/role.yaml` and
`config/member/binding.yaml` to AWS. Verify permission with:

```bash
kubectl --kubeconfig="$AWS_KUBECONFIG" auth can-i create pods --subresource=exec \
  --as=system:serviceaccount:stateful-migration-system:stateful-member -n fluidcr-demo
```

Apply `config/karmada/ric/restoreplan_resource_interpreter.yaml` to Karmada.
Deploy management before member so staging reports have a consumer. Existing
plans are requeued; do not delete the active plan, checkpoint, target or survivor.
Do not manually release locks. No payload, archive or CRD rewrite is required.

Observe `StagedReady` -> request `RestoreReady` -> target Ready -> request
`Verified`, with both ranks progressing. Failure of the read-only probe leaves
the plan Prepared and records the reason in `status.pods[].message`.

The archived process environment retains the old source UID and node. This is
used as checkpoint provenance, not current Pod identity. TrainingRuntime binds
telemetry to the live Kubernetes Pod UID/node. This change does not refresh the
archived process environment; later checkpoint/survivor identity behavior still
needs live validation. A real GPU/CRIU restore cannot be certified by unit tests.
