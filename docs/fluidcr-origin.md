# FluidCR checkpoint reuse

## Origin and ownership

Copied from the local upstream snapshot
`../FluidCR-anhvt/FluidCR-anhvt/operator` on 2026-09-25:

- `api/v1alpha1` to `api/fluidcr/v1alpha1`.
- `internal/controller` to `internal/checkpoint`.
- `internal/ctrlapi` and `internal/kubelet` to their matching directories.
- `config/crd/bases/fluidcr.dcnlab.com_fluidcrmigrations.yaml` to
  `config/crd/fluidcrmigrations.yaml`.
- `../FluidCR-anhvt/FluidCR-anhvt/fluidcr/ctrl.py` to
  `runtime/fluidcr/ctrl.py`, retaining the Apache-2.0 upstream module and adding
  optional synchronous completion confirmation. Source SHA-256:
  `ECBD43BA7E1B40D75AC6ED3F1DACC06DCD5BFC3BC8FF2B561BEEDCF1D9279E1A`.

Copyright 2026 Leehun and the Apache-2.0 license headers are retained.
The source snapshot is untouched. No source Git commit was available in this
local snapshot. Imports now use
`github.com/GProjectdev/Stateful-Migration-Operator-with-PV`.
Upstream standard-testing unit tests are retained with realistic Pod UID,
ownership and response fixtures. No Ginkgo or new dependencies are required.

The API remains `fluidcr.dcnlab.com/v1alpha1`, kind `FluidCRMigration`;
all original spec fields and workload kinds are retained. Added status fields
are optional `pods[].podUID` and Karmada `clusters[]` reports. Each cluster
entry has a required clusterName, phase, observedGeneration, message, raw nested
pod objects and structurally defined conditions. The spec has the CEL transition
rule `self == oldSelf`. Each checkpoint needs a new generation-1 CR; changing
an existing spec cannot safely retrigger work across member clusters.

## Main wiring contract

```go
kc, err := kubelet.NewClientWithOptions(kubelet.Options{
    CAFile: "/etc/kubelet-ca/ca.crt",
    InsecureSkipVerify: false,
})
// Handle err before registering the reconciler.
r := &checkpoint.FluidCRMigrationReconciler{
    Client: mgr.GetClient(),
    APIReader: mgr.GetAPIReader(),
    Scheme: mgr.GetScheme(),
    CtrlClient: ctrlapi.NewClient(),
    KubeletClient: kc,
}
err = r.SetupWithManager(mgr)
```

Both `NewClient()` and `NewClientWithOptions(Options)` return `(*Client, error)`.
`Options` also exposes `TokenFile` and `Port`; their defaults are the projected
service-account token and port 10250. Empty CAFile uses the service-account CA.
TLS verification is enabled by default. Production deployment must mount a
kubelet-ca ConfigMap containing the CA that actually signed kubelet serving
certificates, with matching node-IP SANs. Explicit InsecureSkipVerify is a
lab-only option and is never the default. Tokens are reread for every request.
Redirects are refused, response bodies are capped at 1 MiB, and HTTP calls have
a maximum 300-second operation budget plus ten seconds of transport margin.

Register this reconciler only in the separate member-local `--mode checkpoint`
process. Never register it against the Karmada API or in management/member/
artifact modes. Pod and Node HTTP traffic stays in that member process.
Do not concurrently install the original FluidCR operator for these CRs.
The repository's `config/` manifests own Karmada CRD propagation, RIC, RBAC
and CA mounts; `cmd/manager` owns mode selection and main wiring.

## Payload completion protocol

Build the supplied overlay on the existing FluidCR payload image:

Configure the FluidCR injector to use that overlay image as described in the
repository README. The overlay copies this repository's `runtime/fluidcr/`
package files into `/opt/fluidcr/fluidcr/` in the payload image. That includes
the adapted control server, launcher, PyTorch backend step counter, and package
entrypoint; the original local FluidCR source tree remains reference-only and is
not modified. The Dockerfile removes inherited `__pycache__` directories at
build time, so copied bytecode cannot mask the new source; payload-image
environment variables are not relied on because the injector copies files, not
image environment.

Concrete install/build command:

```sh
docker build -f Dockerfile.payload-overlay \
  --build-arg FLUIDCR_PAYLOAD_IMAGE=<existing-fluidcr-payload-image> \
  -t <registry>/<fluidcr-payload-overlay>:<tag> .
```

The management/system coordinator must set
`training.dcnlab.com/checkpoint-id` on each `FluidCRMigration`. The member
checkpoint controller treats that annotation as the stable round ID and forwards
it unchanged as `checkpointID` in the in-pod `/checkpoint` request. Runtime code
does not generate substitute per-rank IDs. A confirmed checkpoint with a
`checkpointID` preserves the application checkpoint next to the rank checkpoint
path at `rounds/<checkpointID>/<checkpoint-file-name>` after the launcher lock is
observed. This prevents later `latest.pt` overwrites from being confused with the
application state that matched a historical CRIU archive.
The launcher keeps `FLUIDCR_CHECKPOINT_PATH` on the live `latest.pt` write path.
When the control server preserves a confirmed round artifact, it also pins that
artifact path and checkpoint ID in launcher-process memory before CRIU snapshots
the launcher. A restored launcher exports the captured values as
`FLUIDCR_RESTORE_CHECKPOINT_PATH` and `FLUIDCR_RESTORE_CHECKPOINT_ID` for the next
worker. The PyTorch backend loads from that restore path when present, so
restored historical CRIU archives resume from their matching model state even if
`latest.pt` and the mutable status sidecar have since advanced to a later round.
If the pinned artifact is missing, restore fails closed instead of falling back
to `latest.pt`.

For manual checkpoint requests, generate a unique stable ID before applying the
`FluidCRMigration` and place it in metadata, for example:

```yaml
metadata:
  annotations:
    training.dcnlab.com/checkpoint-id: trainer-checkpoint-20260926-001
```

The CRD cannot require a metadata annotation, so the member checkpoint
controller fails early with a clear missing/invalid annotation error when the ID
is absent or not in the runtime-safe character set.

The Go client sends
`{"all":true,"wait":true,"timeoutSeconds":300,"checkpointID":"..."}` on checkpoint
(using the requested timeout when lower and omitting `checkpointID` only when no
annotation was supplied by older callers). The copied endpoint takes one local
worker-to-launcher registry snapshot, rejects empty/ambiguous GPU-worker sets
and preexisting checkpoint locks before triggering, then invokes the existing
rank checkpoint generation/manifest/signal protocol. It waits for each signalled
worker's corresponding launcher lock using the existing parent-lock helper.
Only newly observed locks become `checkpoint-ready`; exited launchers, failed
signals and deadlines remain failure results. The client advances to CRIU only
for a nonempty result set where every worker is ready.

Requests omitting `wait` keep the original asynchronous endpoint behavior.
Confirmed requests support only all ranks, with a finite timeout up to 300
seconds. Partial rank requests, overlapping confirmed requests inside one
control process, stale locks, and bad timeouts fail before starting another
checkpoint. Tests stub FluidCR configuration/distributed helpers and OS signals;
no torch import is needed.

The same control server exposes `GET /runtime` for the TrainingRuntime member
collector. The JSON response is:

```json
{
  "globalStep": 42,
  "checkpointID": "mig-round-001",
  "rank": 0,
  "worldSize": 2,
  "observedAt": "2026-09-26T00:00:00Z",
  "state": "Running",
  "iterationTimeSeconds": 0.123
}
```

`globalStep` and `iterationTimeSeconds` are written by the patched PyTorch
optimizer-step wrapper in the worker process. Launcher states such as `Running`,
`CheckpointReady`, `Completed`, and `Failed` are written by the launcher process.
The control server reads the shared status file next to `FLUIDCR_CHECKPOINT_PATH`.
It prefers the worker's live `globalStep` over the saved checkpoint payload, uses
the worker-written `observedAt` instead of the HTTP request time, and returns 503
when rank/world environment or live worker-registry evidence is unavailable.
Checkpoint wait latency is reported as `checkpointDurationSeconds` and is not
mixed into `iterationTimeSeconds`. Optimizer-step telemetry is throttled per
process to at most once per second by default, configurable with
`FLUIDCR_RUNTIME_TELEMETRY_INTERVAL_SECONDS`; skipped telemetry does not refresh
`observedAt`. Checkpoint/request state transitions and durable round metadata
still bypass the throttle.

## Safety and limits

StatefulSet discovery requires a nonzero desired replica count, current
observed generation, matching replicas/current/ready/available/updated counts,
stable revision, and exactly that many eligible Running, injected, nonterminating
pods owned by the StatefulSet UID. Label-only foreign pods are excluded.
Deployment discovery verifies the ReplicaSet-to-Deployment UID chain, and Job
discovery verifies its controlling owner UID. Full replica/rollout consistency
gating is currently specific to StatefulSets.
When `spec.workloadRef.uid` is present, the member checkpoint controller treats
it as the management workload UID and requires the referenced member workload to
carry `training.dcnlab.com/workload-uid` with the same value before discovering
pods. This is the managed System path and prevents a recreated member workload
with the same name/selector from receiving a stale checkpoint request. Manual
legacy checkpoint CRs that omit `spec.workloadRef.uid` keep the older
selector/owner checks for compatibility.

Persisted pod work is tied to podUID. Missing legacy UIDs, replacements, and
changed pod sets fail closed instead of mixing checkpoint generations.
An uncached Pod identity/endpoint read precedes each HTTP operation. Every
status write verifies the current CR UID, generation and spec, including on
conflict retries. Kubernetes lookup/status errors do not count as success.
These checks cannot make a Kubernetes read and a remote HTTP effect atomic;
freeze rollout/scale operations during a checkpoint. A crash after a successful
remote effect but before status persistence may repeat that operation. Remote
checkpoint calls must be idempotent; exactly-once execution is not claimed.

The supplied payload overlay is required for confirmed operation. An unmodified
upstream endpoint returns `checkpoint-signalled`; the client deliberately
refuses it as completion evidence. Timeouts/partial application failures can
leave workers paused when completed locks were not reported; inspect and resume
those workers before creating another CR.

**Discovery and distributed generation risks:** `FLUIDCR_REGISTRY_DIR` must be
pod-local, including when checkpoint data uses shared storage. Every launcher
needs a distinct lock path (use distinct `FLUIDCR_CHECKPOINT_PATH` values per
rank when sharing a PVC). The overlay rejects duplicate local lock paths, but
cannot discover missing registrations or foreign processes hidden by an
incorrect shared registry. The readiness proof covers registered local GPU
workers, not an independently verified global rank inventory.

The existing `bump_generation()` computes `max(previous + 1, wallclock)` and
atomically replaces a file; it does not provide a cross-pod transaction or
compare-and-swap. Concurrent controller fan-out can bump a shared generation
multiple times and overwrite the shared manifest. The overlay retains this
protocol and only serializes confirmed requests in one local control process.
Lock confirmation therefore does not prove every distributed rank used the same
rendezvous generation. Avoid overlapping rounds, preserve the generation file
and live rendezvous-store relationship, and validate multi-pod GPU workloads
before treating their checkpoints as globally consistent or restore-compatible.
Resolving that protocol requires coordinated trigger ownership beyond this
bounded endpoint adaptation.

This is checkpoint and optional resume-in-place, not restore or full migration.
Archives remain under `/var/lib/kubelet/checkpoints` on their source nodes.
No transfer, OCI image conversion, destination restore, GPU/CRIU compatibility
validation, or filesystem/application consistency guarantee is supplied.
A successful archive path is not proof that it can be restored. resume=false
deliberately leaves application locks in place. Deletion cleanup is bounded and
retains the finalizer on lookup/resume errors; replacing or removing a workload
before cleanup can require operator intervention.

## Verification

Run the retained and added tests and vet without changing module dependencies:

```text
go test -mod=readonly -count=1 ./api/fluidcr/... ./internal/checkpoint ./internal/ctrlapi ./internal/kubelet
go vet -mod=readonly ./api/fluidcr/... ./internal/checkpoint ./internal/ctrlapi ./internal/kubelet
python -B -m unittest discover -s runtime/tests -p "test_*.py" -v
```

Coverage includes complete/partial StatefulSets, foreign owners, Pod replacement
and missing legacy UIDs, stale status writes across UID/spec/generation changes,
generation-2 rejection, application/container failure paths, TLS verification,
token rotation, redirects, cancellation, and malformed/error responses.
Runtime tests are in `runtime/tests/test_ctrl.py` (13 unittest cases), including
the HTTP handler's wait=true success, empty-worker error, legacy asynchronous
behavior, timeout, stale-lock rejection and multiple local launcher outcomes.
Real member/Karmada integration and CRIU/GPU restoration require cluster testing.
