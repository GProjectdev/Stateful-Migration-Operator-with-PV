#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
IMAGE=docker.io/jeongseungjun/stateful-migration-system:v5.0.1
case "${1:-}" in
  build)
    test -z "$(git status --porcelain)" || { echo 'Commit/review local changes before building.' >&2; exit 1; }
    go test ./internal/member ./internal/groupcontract
    buildah bud --layers --label "org.opencontainers.image.revision=$(git rev-parse HEAD)" -f Dockerfile -t "$IMAGE" .
    buildah push "$IMAGE" "docker://$IMAGE"
    ;;
  deploy)
    : "${AWS_KUBECONFIG:?Set the AWS member kubeconfig}"
    BACKUP=$(mktemp -d /tmp/stateful-member-v5.0.1.XXXXXX)
    chmod 700 "$BACKUP"
    kubectl --kubeconfig="$AWS_KUBECONFIG" -n stateful-migration-system get deployment stateful-member -o json > "$BACKUP/deployment.json"
    kubectl --kubeconfig="$AWS_KUBECONFIG" get clusterrole stateful-member -o json > "$BACKUP/role.json"
    # Preserve live RBAC; add only the new permission rather than replacing rules.
    kubectl --kubeconfig="$AWS_KUBECONFIG" patch clusterrole stateful-member --type=json \
      -p='[{"op":"add","path":"/rules/-","value":{"apiGroups":["apps"],"resources":["statefulsets"],"verbs":["patch"]}}]'
    kubectl --kubeconfig="$AWS_KUBECONFIG" -n stateful-migration-system set image deployment/stateful-member manager="$IMAGE"
    kubectl --kubeconfig="$AWS_KUBECONFIG" -n stateful-migration-system rollout status deployment/stateful-member --timeout=180s
    echo "Backup: $BACKUP"
    kubectl --kubeconfig="$AWS_KUBECONFIG" -n stateful-migration-system get deployment stateful-member -o jsonpath='{.spec.template.spec.containers[*].image}{"\n"}'
    ;;
  *) echo "Usage: bash $0 build|deploy" >&2; exit 2 ;;
esac
