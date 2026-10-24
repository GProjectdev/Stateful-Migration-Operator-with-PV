#!/usr/bin/env bash
set -euo pipefail
export DEMO_ROOT="${DEMO_ROOT:-$HOME/legacy-demo}"
export PVFORK="$DEMO_ROOT/sources/karmada-old"
export ASSETS="$HOME/legacy-demo-guide-source/docs/legacy-demo-assets"
export AUTO_IMAGE="docker.io/${ACCOUNT:?ACCOUNT 설정 필요}/karmada-controller-manager:demo-old-auto-002"
(
  set -euo pipefail
  : "${REGISTRY_AUTH_FILE:?Buildah 로그인 파일 설정 필요}"
  test -s "$REGISTRY_AUTH_FILE"
  cd "$PVFORK"
  test "$(git rev-parse HEAD)" = 74c02e295fcba4fafbc4a79113c76988f87fed6c
  if git apply --reverse --check "$ASSETS/legacy-auto-failover.patch" 2>/dev/null; then
    echo '패치가 이미 적용되어 있습니다.'
  else
    git apply --check "$ASSETS/legacy-auto-failover.patch"
    git apply "$ASSETS/legacy-auto-failover.patch"
  fi
  go test -buildvcs=false -p 2 ./pkg/controllers/applicationfailover ./pkg/controllers/pvsync
  chmod u+x hack/build.sh
  make karmada-controller-manager GOOS=linux GOARCH=amd64
  test -x _output/bin/linux/amd64/karmada-controller-manager
  mkdir -p "$DEMO_ROOT/rendered" "$DEMO_ROOT/evidence"
  sed 's|^FROM alpine:|FROM docker.io/library/alpine:|' \
    cluster/images/Dockerfile > "$DEMO_ROOT/rendered/Containerfile.karmada-auto"
  buildah bud --authfile "$REGISTRY_AUTH_FILE" --format docker \
    --build-arg BINARY=karmada-controller-manager \
    -f "$DEMO_ROOT/rendered/Containerfile.karmada-auto" \
    -t "$AUTO_IMAGE" "$PVFORK/_output/bin/linux/amd64"
  (
    set -e
    ctr="$(buildah from "$AUTO_IMAGE")"
    trap 'buildah rm "$ctr" >/dev/null' EXIT
    buildah run "$ctr" -- /bin/karmada-controller-manager --help \
      > "$DEMO_ROOT/evidence/auto-manager-help.txt"
  )
  buildah push --authfile "$REGISTRY_AUTH_FILE" "$AUTO_IMAGE" "docker://$AUTO_IMAGE"
)
printf "%s\n" "자동 Failover manager 이미지 빌드/push 완료: $AUTO_IMAGE"
