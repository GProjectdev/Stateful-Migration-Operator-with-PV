# CDI-aware CRI-O checkpoint import

This patch fixes the import-time bind-mount rejection for mounts supplied by
the new container's CDI allocation. It is NOT proof of CUDA/CRIU restore support.
The Operator image does not need rebuilding for this patch.

## Contract

- Resolve only CDIDevices and CDI annotations from the CURRENT CRI request.
- Discard checkpoint-era CDI annotations; normalize the current allocation into
  CDIDevices on the reconstructed container configuration.
- Preview edits using CRI-O's existing CDI cache. Unknown devices, malformed
  annotations, invalid destinations, refresh failures and explicit CRI-volume
  collisions fail closed. No NVIDIA filename or directory wildcard is trusted.
- Only mount destinations resolved from that allocation bypass the CRI-volume
  lookup. Their archive host paths/options are never imported.
- The existing createSandboxContainer -> SpecInjectCDIDevices path applies the
  real CDI edits, including mount options, devices, hooks and environment.
- Ordinary mounts retain the existing check. No extra dependency is added to
  the Stateful operator; .go.in overlays compile against CRI-O's own vendor tree.

The CDI registry is administrator-controlled. It must remain stable during the
test; do not upgrade drivers or regenerate device definitions mid-restore.
A missing current CDI allocation intentionally does NOT authorize archive mounts.

## Apply and build on MGMT

Keep the existing annotation adapter installed. This is a separate additive
patch; do not reapply annotation-adapter.patch. Use a reviewed checkout of this
Stateful repository and preserve the existing CRI-O worktree changes.

```bash
(
  set -euo pipefail
  export ST=/root/hybridspot-validation/stateful
  export CRIO_SRC=/root/hybridspot-validation/runtime-src/leehun-cri-o
  cd "$CRIO_SRC"
  git diff -- server/container_create.go server/container_restore.go
  test -f server/container_restore_annotation.go
  git apply --check "$ST/runtime/crio/cdi-restore.patch"
  test ! -e server/container_restore_cdi.go
  test ! -e server/container_restore_cdi_test.go
  git apply "$ST/runtime/crio/cdi-restore.patch"
  cp "$ST/runtime/crio/cdi/container_restore_cdi.go.in" server/container_restore_cdi.go
  cp "$ST/runtime/crio/cdi/container_restore_cdi_test.go.in" server/container_restore_cdi_test.go
  gofmt -w server/container_restore_cdi.go server/container_restore_cdi_test.go
  go test -mod=vendor -v server/container_restore_cdi.go server/container_restore_cdi_test.go
  git diff --check
  # Use the same known-good build flags/dependencies as the current runtime.
  make -j2 BUILDTAGS="containers_image_openpgp containers_image_ostree_stub seccomp selinux" binaries
  ./bin/crio --version
  sha256sum bin/crio "$ST/runtime/crio/cdi-restore.patch" server/container_restore_cdi.go
)
```

Stop on any failed command. Never force a patch with rejected hunks.
For an already-applied patch, verify with git apply --reverse --check before
rerunning tests/build; do not duplicate the patch.
The full Linux build is mandatory even if isolated helper tests pass.

## Isolated Worker rollout only

Do not change worker-00 or worker-01. Keep restore-test-ondemand-01 cordoned.
Keep the current RestorePlan, failed-to-create restore-smoke Pod, PVC and archive.
Record the current Pod UID and current error before replacement.

Transfer the newly built bin/crio to /tmp/crio-cdi on the test Worker using the
existing approved SSH connection. Compare its SHA256 with the MGMT build output.
The commands below run ONLY on restore-test-ondemand-01 after that comparison:

```bash
(
  set -euo pipefail
  test "$(hostname)" = restore-test-ondemand-01
  test -s /tmp/crio-cdi
  /tmp/crio-cdi --version
  sha256sum /tmp/crio-cdi
  sudo systemctl show crio -p ExecStart
  # Confirm ExecStart uses /usr/local/bin/crio before continuing.
)
```

Then, during the isolated test node's maintenance window:

```bash
(
  set -euo pipefail
  test "$(hostname)" = restore-test-ondemand-01
  STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
  sudo cp -a /usr/local/bin/crio "/usr/local/bin/crio.before-cdi-$STAMP"
  sudo install -m 0755 /tmp/crio-cdi /usr/local/bin/crio.cdi-new
  sudo systemctl stop kubelet
  sudo systemctl stop crio
  sudo mv /usr/local/bin/crio.cdi-new /usr/local/bin/crio
  sudo systemctl start crio
  sudo systemctl is-active --quiet crio
  sudo systemctl start kubelet
  sudo sha256sum /usr/local/bin/crio
)
```

If startup fails, keep kubelet stopped, restore the recorded backup binary with
CRI-O stopped, and start CRI-O then kubelet. Do not delete containers or storage.
This manual override makes the installed runtime package manifest's old CRI-O
hash stale. Preserve that manifest and record the override hash; rebuild/version
the runtime package before using this fix for automatic provisioning.

## Evidence and stop conditions

Kubelet may retry container creation automatically; no Pod deletion is required
solely to install the new binary. Inspect events and CRI-O logs for the CURRENT
Pod UID and timestamp, not old aggregated failures.

```bash
# MGMT
kubectl --kubeconfig="$AWS_KUBECONFIG" -n fluidcr-demo describe pod restore-smoke
kubectl --kubeconfig="$AWS_KUBECONFIG" -n fluidcr-demo get restoreplan restore-smoke-local-01 -o json
# Test Worker
sudo journalctl -u crio --since '10 minutes ago' --no-pager
```

Require all of:
1. The prior missing NVIDIA mount error disappears for a fresh attempt.
2. Positive checkpoint import/CRIU restore evidence identifies this container.
3. The restore annotation still points at the SHA256-verified archive.
4. Only after native restore evidence, resume the single-rank test and prove
   its checkpoint identity and advancing globalStep.

Ready alone, a successful helper test, or app-level loading of latest.pt is not
native restore proof. Stop and capture logs on any new CRIU, mount, device or
driver error. Do not disable mount checking or broaden hostPath permissions.
Distributed NCCL restore remains a separate validation.
