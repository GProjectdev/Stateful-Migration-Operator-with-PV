#!/usr/bin/env bash
set -euo pipefail

: "${AWS_KUBECONFIG:?AWS_KUBECONFIG required}"
: "${MGMT_KUBECONFIG:?MGMT_KUBECONFIG required}"
: "${KARMADA_KUBECONFIG:?KARMADA_KUBECONFIG required}"
ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
grep -Fxq 'module github.com/GProjectdev/Stateful-Migration-Operator-with-PV' go.mod
for tool in git buildah kubectl; do command -v "$tool" >/dev/null; done

# Check both deployment contexts before building or changing either controller.
kubectl --kubeconfig="$MGMT_KUBECONFIG" -n stateful-migration-system \
  get deployment/stateful-management -o name
kubectl --kubeconfig="$AWS_KUBECONFIG" -n stateful-migration-system \
  get deployment/stateful-member -o name

REV="$(git rev-parse --short=12 HEAD)"
REPO=docker.io/jeongseungjun/stateful-migration-operator
IMAGE="$REPO:partial-staging-$REV"
DIGEST_FILE="$(mktemp /tmp/stateful-partial-staging.XXXXXX.digest)"
buildah bud --arch amd64 -f "$ROOT/Dockerfile" -t "$IMAGE" "$ROOT"
buildah push --digestfile "$DIGEST_FILE" "$IMAGE" "docker://$IMAGE"
DIGEST="$(cat "$DIGEST_FILE")"
[[ "$DIGEST" =~ ^sha256:[0-9a-f]{64}$ ]] || { echo 'Invalid image digest' >&2; exit 1; }
PINNED="$REPO@$DIGEST"
echo "PINNED=$PINNED"
echo "DIGEST_FILE=$DIGEST_FILE"

# Management must understand StagedReady before member can publish it.
kubectl --kubeconfig="$MGMT_KUBECONFIG" -n stateful-migration-system \
  set image deployment/stateful-management manager="$PINNED"
kubectl --kubeconfig="$MGMT_KUBECONFIG" -n stateful-migration-system \
  rollout status deployment/stateful-management --timeout=300s
kubectl --kubeconfig="$KARMADA_KUBECONFIG" \
  apply -f "$ROOT/config/karmada/ric/restoreplan_resource_interpreter.yaml"
kubectl --kubeconfig="$AWS_KUBECONFIG" \
  apply -f "$ROOT/config/member/role.yaml" -f "$ROOT/config/member/binding.yaml"
kubectl --kubeconfig="$AWS_KUBECONFIG" auth can-i create pods --subresource=exec \
  --as=system:serviceaccount:stateful-migration-system:stateful-member -n fluidcr-demo
kubectl --kubeconfig="$AWS_KUBECONFIG" -n stateful-migration-system \
  set image deployment/stateful-member manager="$PINNED"
kubectl --kubeconfig="$AWS_KUBECONFIG" -n stateful-migration-system \
  rollout status deployment/stateful-member --timeout=300s

kubectl --kubeconfig="$AWS_KUBECONFIG" -n fluidcr-demo get restoreplans \
  -o custom-columns='NAME:.metadata.name,PHASE:.status.phase,MESSAGE:.status.message'
kubectl --kubeconfig="$AWS_KUBECONFIG" -n fluidcr-demo get pod trainer-0 trainer-1 -o wide
