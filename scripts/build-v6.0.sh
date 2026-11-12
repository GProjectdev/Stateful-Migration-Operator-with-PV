#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
command -v buildah >/dev/null
IMAGE=docker.io/jeongseungjun/stateful-migration-system:v6.0
buildah bud --platform linux/amd64 --label "org.opencontainers.image.revision=$(git rev-parse HEAD)" --label org.opencontainers.image.version=v6.0 -f Dockerfile -t "$IMAGE" .
printf 'Built locally: %s\n' "$IMAGE"
