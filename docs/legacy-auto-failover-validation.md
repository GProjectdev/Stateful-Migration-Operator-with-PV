# 11. 자동 Failover 호환 패치 및 촬영 전 검증

이 절은 기존 가이드 11절을 대체한다. 대상은 단일 replica, ordinal 0, NFS volumeClaimTemplates를 사용하는 demo StatefulSet이다. 기존 Old-Version의 체크포인트/PV Work 방식을 유지한다. 엄격한 검증은 strict-demo-restore annotation이 있는 RB에 적용된다. 패치의 25초 자동 해제 제거와 빈 PV Work 집합 대기는 manager의 다른 PV migration에도 적용되므로, 기존에 그 자동 해제에 의존하던 작업은 대기할 수 있다. Karmada API·scheduler·webhook은 교체하지 않는다. 실제 클러스터의 CRI-O 복원 성공은 아래 시험으로 별도 확인해야 한다.

현재 확인: source demo-0 Ready, PVC Bound, CheckpointBackup Completed, checkpoint image pull 성공. Gracefully 정책 server dry-run 성공. 실제 정책은 아직 onpre1 고정이다.

## 11.1 MGMT: 패치 적용과 새 이미지 빌드

동일 root 셸에서 실행한다. 이전 이미지 태그를 덮어쓰지 않는다. 소스는 고정 commit 74c02e2를 사용한다. 패치에는 회귀 테스트가 포함된다.

```bash
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
```

## 11.2 MGMT: manager만 교체

아래는 실제 변경이다. 11.1 성공 후 실행한다. 원래 v1.19 백업은 덮어쓰지 않고, 교체 직전 manager도 별도로 저장한다. 별도 suspension-controller는 자동 시험 내내 0으로 둔다. PVMigration manager가 배포 보류 해제의 단일 담당자다.

```bash
(
  set -euo pipefail
  : "${MGMT_KUBECONFIG:?}" "${AUTO_IMAGE:?}" "${KARMADA_KUBECONFIG:?}"
  kubectl --kubeconfig="$MGMT_KUBECONFIG" -n stateful-migration \
    scale deployment suspension-controller --replicas=0
  kubectl --kubeconfig="$MGMT_KUBECONFIG" -n stateful-migration \
    wait --for=delete pod -l app=suspension-controller --timeout=120s
  stamp="$(date -u +%Y%m%dT%H%M%SZ)"
  kubectl --kubeconfig="$MGMT_KUBECONFIG" -n karmada-system \
    get deployment karmada-controller-manager -o json \
    > "$DEMO_ROOT/evidence/manager-pre-auto-$stamp.json"
  python3 "$ASSETS/prepare-manager.py" \
    "$DEMO_ROOT/evidence/manager-pre-auto-$stamp.json" \
    "$DEMO_ROOT/rendered/manager-auto.json" --image "$AUTO_IMAGE"
  kubectl --kubeconfig="$MGMT_KUBECONFIG" -n karmada-system \
    patch deployment karmada-controller-manager --type=json \
    --patch-file "$DEMO_ROOT/rendered/manager-auto.json.patch.json"
  kubectl --kubeconfig="$MGMT_KUBECONFIG" -n karmada-system \
    rollout status deployment/karmada-controller-manager --timeout=180s
  kubectl --kubeconfig="$MGMT_KUBECONFIG" -n karmada-system \
    logs deployment/karmada-controller-manager --tail=100
)
```

rollout 실패 또는 unknown flag/API 오류가 있으면 정책 변경과 장애 주입으로 넘어가지 않는다. CRD 전체나 webhook을 구버전으로 덮어쓰지 않는다.

## 11.3 정책 적용 전 준비

onpre2의 provisioner는 실행해 둬도 된다. 실제 PV가 먼저 준비된 후 PVC가 생성되는 순서를 검증한다. onpre2에 기존 demo PVC/Pod가 있으면 자동 삭제하지 않고 원인을 조사한다.

```bash
kubectl --kubeconfig="$ONPRE2_KUBECONFIG" -n migration-demo get pods,pvc,checkpointrestores
kubectl --kubeconfig="$ONPRE2_KUBECONFIG" -n nfs-provisioner get deployment
kubectl --kubeconfig="$MGMT_KUBECONFIG" -n migration-demo get statefulmigrations -o yaml
kubectl --kubeconfig="$ONPRE1_KUBECONFIG" -n migration-demo get checkpointbackups -o yaml
kubectl --kubeconfig="$KARMADA_KUBECONFIG" get crd resourcebindings.work.karmada.io \
  -o jsonpath='{range .spec.versions[*]}{.name}{" task purge enum="}{.schema.openAPIV3Schema.properties.spec.properties.gracefulEvictionTasks.items.properties.purgeMode.enum}{"\n"}{end}'
```

기존 StatefulMigration이 demo를 가리키고 sourceClusters에 onpre1이 있어야 한다. RB의 task purge enum은 구버전 내부 값 Graciously를 허용해야 한다. 이 조건이 충족되지 않으면 중단한다. checkpoint 태그는 backup/controller를 중지한 상태에서 고정한다. source-memory.json은 체크포인트 당시 paused 상태를 기록한 파일이어야 한다. 지금 다시 읽은 재생 위치를 과거 체크포인트의 위치로 간주하지 않는다.

```bash
kubectl --kubeconfig="$KARMADA_KUBECONFIG" -n migration-demo \
  annotate resourcebinding demo-statefulset \
  migration.dcnlab.com/strict-demo-restore=true --overwrite
kubectl --kubeconfig="$MGMT_KUBECONFIG" -n stateful-migration \
  scale deployment migration-restore-controller --replicas=1
kubectl --kubeconfig="$MGMT_KUBECONFIG" -n stateful-migration \
  rollout status deployment/migration-restore-controller --timeout=180s
```

backup-controller와 suspension-controller는 0으로 유지한다. target restore webhook의 Deployment Ready, namespace label, mutation 설정은 기존 7절대로 유지되어 있어야 한다.

```bash
cat > "$DEMO_ROOT/rendered/demo-auto-policy.patch.json" <<'JSON'
{"spec":{"propagateDeps":true,"placement":{"clusterAffinity":null,"clusterAffinities":[{"affinityName":"primary","clusterNames":["onpre1"]},{"affinityName":"backup","clusterNames":["onpre2"]}],"replicaScheduling":{"replicaSchedulingType":"Duplicated"}},"failover":{"application":{"decisionConditions":{"tolerationSeconds":60},"purgeMode":"Gracefully","gracePeriodSeconds":600}}}}
JSON
kubectl --kubeconfig="$KARMADA_KUBECONFIG" -n migration-demo \
  patch propagationpolicy demo-placement --type=merge \
  --patch-file "$DEMO_ROOT/rendered/demo-auto-policy.patch.json" --dry-run=server -o yaml
```

dry-run 성공 후 실제 적용한다.

```bash
kubectl --kubeconfig="$KARMADA_KUBECONFIG" -n migration-demo \
  patch propagationpolicy demo-placement --type=merge \
  --patch-file "$DEMO_ROOT/rendered/demo-auto-policy.patch.json"
kubectl --kubeconfig="$KARMADA_KUBECONFIG" -n migration-demo \
  get resourcebinding demo-statefulset -o yaml
```

RB.spec.clusters가 onpre1 하나/replicas=1이고 health=Healthy, spec.failover.application이 반영됐는지 확인한다. 두 클러스터에 동시 배포하거나 이 시점에 suspension이 걸렸으면 장애를 주지 않는다.

## 11.4 촬영 전 실제 장애 시험

다음 명령은 실제 원본 Pod를 종료한다. 11.1~11.3 완료 후 실행한다. member1의 NFS 서버와 Kubernetes API는 유지한다. 전체 서버를 shutdown하지 않는다. 원본 프로세스는 마지막 체크포인트 시점으로 복원되며 체크포인트 이후 변경은 보존된다고 주장하지 않는다.

관찰용 터미널에서:

```bash
kubectl --kubeconfig="$KARMADA_KUBECONFIG" -n migration-demo \
  get resourcebinding demo-statefulset -w
```

다른 MGMT 터미널에서 기존 환경변수를 설정한 뒤:

```bash
kubectl --kubeconfig="$ONPRE1_KUBECONFIG" cordon member1
kubectl --kubeconfig="$ONPRE1_KUBECONFIG" -n migration-demo \
  delete pod demo-0 --wait=true --timeout=120s
```

강제 삭제는 쓰지 않는다. 삭제 대기가 실패하면 원본 종료 여부를 조사한다. source의 대체 Pod가 Pending이거나 제거되고, RB에서 Unhealthy가 관측된 후 toleration을 거쳐 재배치된다. cordon 자체가 즉시 Karmada failover를 유발하는 것은 아니다. 배치 정책을 수동으로 onpre2로 바꾸지 않는다.

```bash
kubectl --kubeconfig="$KARMADA_KUBECONFIG" -n migration-demo get resourcebinding demo-statefulset -o yaml
kubectl --kubeconfig="$MGMT_KUBECONFIG" -n karmada-system logs deployment/karmada-controller-manager --tail=150
kubectl --kubeconfig="$MGMT_KUBECONFIG" -n stateful-migration logs deployment/migration-restore-controller --tail=100
kubectl --kubeconfig="$ONPRE2_KUBECONFIG" -n migration-demo get checkpointrestores,pods,pvc -o wide
```

진행되지 않으면 위 출력을 저장해 원인을 조사한다. restore-phase를 수동 succeeded로 바꾸거나 suspension을 수동 해제하지 않는다. Pending/CrashLoop만으로 CRIU 복원 성공을 판단하지 않는다.

## 11.5 성공 판정

onpre2에서 demo-0이 Ready가 된 뒤:

```bash
kubectl --kubeconfig="$ONPRE2_KUBECONFIG" -n migration-demo \
  get pod demo-0 -o jsonpath='{.spec.containers[*].image}{"\n"}'
TARGET_PV="$(kubectl --kubeconfig="$ONPRE2_KUBECONFIG" -n migration-demo \
  get pvc data-demo-0 -o jsonpath='{.spec.volumeName}')"
kubectl --kubeconfig="$ONPRE2_KUBECONFIG" get pv "$TARGET_PV" \
  -o jsonpath='{.spec.nfs.server}{" "}{.spec.nfs.path}{"\n"}'
kubectl --kubeconfig="$ONPRE2_KUBECONFIG" -n migration-demo \
  exec demo-0 -c app -- python -c \
  'import urllib.request; print(urllib.request.urlopen("http://127.0.0.1:8080/state").read().decode())' \
  | tee "$DEMO_ROOT/evidence/target-memory.json"
kubectl --kubeconfig="$ONPRE1_KUBECONFIG" -n migration-demo get pods -o wide
```

- app 이미지가 체크포인트 이미지여야 한다.
- target PV의 NFS server/path가 원본 PV와 같아야 한다. PV 이름/UID는 달라도 된다.
- checkpoint 당시 source-memory.json과 boot_id, paused frame, marker를 비교한다. boot_id가 바뀌면 일반 재시작이다.
- target node의 CRI-O 로그에서 restore 성공을 확인한다.
- source에 실행 중인 영상 Pod가 없어야 한다. member1을 임의로 uncordon하지 않는다.
- NodePort demo-view가 양쪽에 설치된 경우 target URL http://192.168.30.102:30080 에 접속해 Reconnect/Play를 사용한다. 접속 주소 변경은 수동이며 무중단 엔드포인트 전환의 증거가 아니다.

이 패치는 API 호환 및 배포 순서 보호다. 서로 다른 CRI-O/crun/CRIU 버전 간 메모리 복원 성공까지 보장하지 않는다. 실제 시험에서 실패하면 target Pod events와 member2 CRI-O 로그로 분리 진단한다.

## 로컬 검증 기록

2026-10-24: applicationfailover 패키지 테스트 통과, 최종 pvsync 패키지 테스트 통과. 원본 고정 커밋에 대한 패치 적용/역적용 검사 및 수정 소스 일치 검사 통과. controller-manager Windows 빌드 통과. 가이드 Bash 구문 검사 통과. Linux Buildah 빌드/push, 실제 API admission 및 member2 CRI-O 복원은 이 문서의 현장 검증 대상이며 로컬에서 실행하지 않았다.
