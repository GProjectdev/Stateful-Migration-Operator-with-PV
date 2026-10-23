# Legacy CheckpointRestore webhook supplement

This package preserves the existing local source at:
`최종 주제 작업/stateful-migration/Stateful-Migration-Operator-with-PV-main/Stateful-Migration-Operator-with-PV-main/Mutation/webhook-server/`.

It supplements Stateful Old-Version `cf5431275263cd76fc1e63036f8f17b74a5801ef`, whose remote tree lacks Mutation/. The local README refers to GProjectdev/Mutating, but its remote main examined in this task implements older change=image behavior. Do not replace this CheckpointRestore-consuming source with that remote main.

main.go is unchanged, preserving copyright and LICENSE. SHA-256: `92f389ff98b4904f2f499c343b2c71f76786a7571bcdf11d7b6ce4012f7cd258`. The missing module directive is added to go.mod. go mod tidy completes the original incomplete transitive dependency/checksum metadata from the local Go cache, with networking disabled; Kubernetes dependency versions remain v0.33.0. Dockerfile and install.sh are new deployment assets; source repositories remain untouched.

## Linux build and installation

```bash
cd docs/legacy-demo-assets/webhook
export IMAGE=docker.io/YOUR_DOCKERHUB_ACCOUNT/legacy-checkpoint-restore-webhook:demo-001
# Use the same user for login, build and push.
umask 077
export REGISTRY_AUTH_FILE="$HOME/.config/containers/legacy-demo-auth.json"
mkdir -p "$(dirname "$REGISTRY_AUTH_FILE")"
buildah login --authfile "$REGISTRY_AUTH_FILE" docker.io
buildah bud --authfile "$REGISTRY_AUTH_FILE" --format docker -t "$IMAGE" . &&
buildah push --authfile "$REGISTRY_AUTH_FILE" "$IMAGE" "docker://$IMAGE"
export TARGET_KUBECONFIG=/root/.kube/onpre2-demo.config
export WORKLOAD_NAMESPACE=migration-demo
bash install.sh
```

Use an image accessible to onpre2 or configure image pull credentials. First install the old checkpointrestores.migration.dcnlab.com CRD on the target. The installer always passes TARGET_KUBECONFIG explicitly. It creates a 365-day CA and Service certificate, installs the server, waits for readiness, registers admission, and labels WORKLOAD_NAMESPACE legacy-restore=enabled. RBAC permits reading CheckpointRestore only in that namespace. Do not label unrelated namespaces.

Actual resources use namespace stateful-migration, Deployment/ServiceAccount checkpoint-restore-webhook, Service checkpoint-restore-webhook-svc:443, Secret checkpoint-restore-webhook-tls and /tls/tls.crt + /tls/tls.key. Admission endpoint is /mutate at 8443. SANs match the actual Service, unlike the old README. The malformed old MWC rules and placeholder certificates are corrected. No cert-manager is required.

Re-running rotates certificates and rolls the Deployment: do so only while target workload creation is suspended. Temporary private keys are removed on exit; the serving key remains in the Kubernetes Secret.

## Preserved behavior and acceptance

The server matches a CheckpointRestore in the same namespace by exact spec.podName, then replaces container images using spec.containers. Use a single-container, single-replica StatefulSet or fixed-name Pod. It does not implement Deployment restore-order handling, selects the first matching CR instead of rejecting duplicates, and can apply its first image to an unmatched container name. Keep one applicable CR, correct container names, and no sidecars for the demo.

The admission configuration uses failurePolicy: Fail for transport/server failures. **Application-level fail-open remains:** missing CRs/images and CR lookup errors return Allowed without mutation. This preserves legacy source behavior. Pod Ready and restore-phase=succeeded do not prove restoration.

Before releasing suspension, confirm the CheckpointRestore exists on onpre2. Afterwards inspect target Pod images and webhook logs, then CRI-O restore logs and application memory continuity. If the target retains the normal application image, mutation did not occur.

```bash
kubectl --kubeconfig="$TARGET_KUBECONFIG" -n "$WORKLOAD_NAMESPACE" get checkpointrestores -o yaml
kubectl --kubeconfig="$TARGET_KUBECONFIG" -n stateful-migration logs deployment/checkpoint-restore-webhook
kubectl --kubeconfig="$TARGET_KUBECONFIG" -n "$WORKLOAD_NAMESPACE" get pods -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.spec.containers[*].image}{"\n"}{end}'
```

Validation: source-copy SHA-256 comparison, Bash syntax check, parsing all six embedded Kubernetes manifests, and an offline Windows/amd64 build with Go 1.27.1 (-mod=readonly). Buildah image build, registry access, cluster deployment, TLS admission and CRI-O restoration have not been executed here. These remain required environment acceptance checks.
