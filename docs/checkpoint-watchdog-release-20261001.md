# Checkpoint watchdog 수정 배포 가이드 (2026-10-01)

## 범위와 한계

중복 SIGUSR1이 watchdog을 여러 개 생성하여 Survivor 취소 이후에도 이전 타이머가 프로세스를 종료할 수 있는 결함을 수정했다. 활성 타이머와 원래 제한시간을 유지하고, 취소 후 다음 라운드에서는 새 타이머를 생성한다.

이 결함은 로컬 테스트로 재현했지만, 12:22의 실제 장애에 중복 신호가 있었는지는 확인되지 않았다. 이번 배포만으로 장애 해결을 보장하지 않는다.

변경 대상은 payload-base와 payload-stateful이다. 이번 수정만 반영할 때는 컨트롤러 이미지, CRD, group-control, Provisioner, Node 라벨을 변경하지 않는다. namespace는 계속 fluidcr-realign-121040을 사용한다.

아래 명령은 MGMT의 Bash에서 한 줄씩 실행한다. 오류가 발생하면 다음 단계로 넘어가지 않는다. k()/a()/build_push 같은 축약 함수는 사용하지 않는다.

## 1. 소스 갱신

로컬 변경이 있으면 먼저 내용을 확인하고 보존한다. reset --hard는 사용하지 않는다.

```bash
git -C /root/hybridspot-validation/fluidcr status --short
git -C /root/hybridspot-validation/stateful status --short
git -C /root/hybridspot-validation/fluidcr switch restore-automation-20260928
git -C /root/hybridspot-validation/fluidcr pull --ff-only origin restore-automation-20260928
git -C /root/hybridspot-validation/stateful switch restore-automation-20260928
git -C /root/hybridspot-validation/stateful pull --ff-only origin restore-automation-20260928
git -C /root/hybridspot-validation/fluidcr log -1 --oneline
git -C /root/hybridspot-validation/stateful log -1 --oneline
python3 -m unittest discover -s /root/hybridspot-validation/stateful/runtime/tests -p test_checkpoint_watchdog.py
```

## 2. 빌드 결과 저장 경로

새 Kubernetes namespace를 만드는 단계가 아니다. 파일 저장 디렉토리만 추가한다. 기존 EVIDENCE를 덮어쓰지 않는다.

```bash
export FIX_RELEASE="watchdog-$(date -u +%Y%m%dT%H%M%SZ)"
export FIX_DIR="/root/hybridspot-validation/evidence/$FIX_RELEASE"
mkdir -p "$FIX_DIR/images"
printf '%s\n' "$FIX_DIR"
```

## 3. 이미지 빌드와 Push

기존에 사용한 buildah와 Docker Hub 계정을 사용한다. 인증이 필요하면 buildah login docker.io를 실행한다. 각 push 성공과 digest 파일 생성까지 확인한다.

```bash
buildah bud --arch amd64 -f /root/hybridspot-validation/fluidcr/Dockerfile.payload -t "docker.io/jeongseungjun/myfluidcr-operator:payload-base-$FIX_RELEASE" /root/hybridspot-validation/fluidcr
buildah push --digestfile "$FIX_DIR/images/payload-base.digest" "docker.io/jeongseungjun/myfluidcr-operator:payload-base-$FIX_RELEASE" "docker://docker.io/jeongseungjun/myfluidcr-operator:payload-base-$FIX_RELEASE"
cat "$FIX_DIR/images/payload-base.digest"
```

출력은 sha256:으로 시작해야 한다. 성공한 뒤 overlay를 빌드한다.

```bash
buildah bud --arch amd64 -f /root/hybridspot-validation/stateful/Dockerfile.payload-overlay --build-arg "FLUIDCR_PAYLOAD_IMAGE=docker.io/jeongseungjun/myfluidcr-operator@$(cat "$FIX_DIR/images/payload-base.digest")" -t "docker.io/jeongseungjun/myfluidcr-operator:payload-stateful-$FIX_RELEASE" /root/hybridspot-validation/stateful
buildah push --digestfile "$FIX_DIR/images/payload-stateful.digest" "docker.io/jeongseungjun/myfluidcr-operator:payload-stateful-$FIX_RELEASE" "docker://docker.io/jeongseungjun/myfluidcr-operator:payload-stateful-$FIX_RELEASE"
cat "$FIX_DIR/images/payload-stateful.digest"
export FIX_PAYLOAD="docker.io/jeongseungjun/myfluidcr-operator@$(cat "$FIX_DIR/images/payload-stateful.digest")"
printf '%s\n' "$FIX_PAYLOAD"
```

## 4. 현재 상태 보존 및 신규 정책 동작 정지

```bash
kubectl --kubeconfig="${KARMADA_KUBECONFIG:?}" --request-timeout=15s -n fluidcr-realign-121040 get trainingpolicy trainer-realign -o yaml > "$FIX_DIR/policy-before.yaml"
kubectl --kubeconfig="${KARMADA_KUBECONFIG:?}" --request-timeout=15s -n fluidcr-realign-121040 get spotreplacements,restorerequests -o yaml > "$FIX_DIR/recovery-before.yaml"
kubectl --kubeconfig="${AWS_KUBECONFIG:?}" --request-timeout=15s -n fluidcr-realign-121040 get fluidcrmigrations,restoreplans,pods -o yaml > "$FIX_DIR/member-before.yaml"
kubectl --kubeconfig="${AWS_KUBECONFIG:?}" --request-timeout=15s -n fluidcr-system get deployment fluidcr-webhook -o yaml > "$FIX_DIR/webhook-before.yaml"
kubectl --kubeconfig="${KARMADA_KUBECONFIG:?}" --request-timeout=15s -n fluidcr-realign-121040 annotate trainingpolicy trainer-realign training.dcnlab.com/suspend=true --overwrite
```

주의: suspend는 기존 SpotReplacement 작업까지 취소하는 기능이 아니다. 기존 작업은 계속 진행할 수 있다. Failed CR, pause-lock, manifest, PVC, PV, NodeProvision을 이 단계에서 삭제하지 않는다.

## 5. Webhook의 payload 이미지 변경

```bash
printf '%s\n' "$FIX_PAYLOAD"
kubectl --kubeconfig="${AWS_KUBECONFIG:?}" --request-timeout=15s -n fluidcr-system get deployment fluidcr-webhook -o json | jq '.spec.template.spec.containers[] | {name,args}'
kubectl --kubeconfig="${AWS_KUBECONFIG:?}" --request-timeout=15s -n fluidcr-system edit deployment fluidcr-webhook
```

편집기에서 webhook 컨테이너의 args 중 --payload-image 값만 위에서 출력한 FIX_PAYLOAD 전체 문자열로 변경한다. --payload-image=기존값 형식이면 = 뒤를, --payload-image와 값이 별도 항목이면 바로 다음 항목을 변경한다. Deployment의 image 자체를 payload 이미지로 바꾸지 않는다. 다른 인자는 유지한다.

```bash
kubectl --kubeconfig="${AWS_KUBECONFIG:?}" -n fluidcr-system rollout status deployment/fluidcr-webhook --timeout=300s
kubectl --kubeconfig="${AWS_KUBECONFIG:?}" --request-timeout=15s -n fluidcr-system get deployment fluidcr-webhook -o json | jq '.spec.template.spec.containers[] | {name,args}'
```

이 시점에는 앞으로 생성될 Pod의 기본 payload만 변경되었다. 기존 trainer Pod 파일은 바뀌지 않는다.

## 6. 기존 실패 작업과 Pod 재생성 구분

현재 보고된 trainer-0은 학습 자식 프로세스가 없고 partial Checkpoint는 Failed 상태다. 이미지 설정 변경만으로 학습이나 기존 Failed 작업이 자동 복구되지는 않는다.

현재 작업을 먼저 조회한다.

```bash
kubectl --kubeconfig="${KARMADA_KUBECONFIG:?}" --request-timeout=15s -n fluidcr-realign-121040 get spotreplacements,restorerequests -o yaml
kubectl --kubeconfig="${AWS_KUBECONFIG:?}" --request-timeout=15s -n fluidcr-realign-121040 get restoreplans,fluidcrmigrations -o yaml
```

진행 중이거나 실패한 교체 작업이 남아 있으면 여기서 무조건 rollout restart 또는 Pod 삭제를 하지 않는다. 기존 작업은 원본 Pod UID에 연결되어 있어 재생성하면 재사용할 수 없을 수 있다. 작업 상태에 따라 기존 복구를 마무리하거나, 증거를 보존하고 해당 작업을 정리한 뒤 새 Checkpoint와 새 교체 작업으로 시작해야 한다. 이 가이드는 기존 작업 취소나 데이터 삭제를 자동 수행하지 않는다.

활성 복구 작업이 없고, 두 Rank를 재시작해도 되는 학습 상태가 확보된 경우에만 Karmada StatefulSet의 OnDelete 여부와 replicas를 확인한다.

```bash
kubectl --kubeconfig="${KARMADA_KUBECONFIG:?}" --request-timeout=15s -n fluidcr-realign-121040 get statefulset trainer -o json | jq '{replicas:.spec.replicas,strategy:.spec.updateStrategy,annotations:.spec.template.metadata.annotations}'
```

기존 Pod/template에 payload 이미지 override가 있으면 새 digest로 갱신해야 한다. 현재 검증 workload가 OnDelete이고 replicas=2이며 위 조건을 만족할 때, 다음 명령은 두 학습 Pod를 재생성하므로 학습이 중단된다.

```bash
kubectl --kubeconfig="${AWS_KUBECONFIG:?}" --request-timeout=15s -n fluidcr-realign-121040 delete pod trainer-0 trainer-1 --wait=false
kubectl --kubeconfig="${AWS_KUBECONFIG:?}" --request-timeout=15s -n fluidcr-realign-121040 get pods -l app=trainer -o wide
```

## 7. 적용 확인과 재검증

새 Pod 생성 후 확인한다. 두 Pod의 payload initContainer 이미지가 FIX_PAYLOAD와 일치해야 한다.

```bash
kubectl --kubeconfig="${AWS_KUBECONFIG:?}" --request-timeout=15s -n fluidcr-realign-121040 get pod trainer-0 trainer-1 -o json | jq '.items[] | {name:.metadata.name,uid:.metadata.uid,initContainers:.spec.initContainers}'
sha256sum /root/hybridspot-validation/stateful/runtime/fluidcr/__init__.py
kubectl --kubeconfig="${AWS_KUBECONFIG:?}" -n fluidcr-realign-121040 exec trainer-0 -c trainer -- sha256sum /opt/fluidcr/fluidcr/__init__.py
kubectl --kubeconfig="${AWS_KUBECONFIG:?}" -n fluidcr-realign-121040 exec trainer-1 -c trainer -- sha256sum /opt/fluidcr/fluidcr/__init__.py
kubectl --kubeconfig="${AWS_KUBECONFIG:?}" -n fluidcr-realign-121040 logs trainer-0 -c trainer --tail=30 --timestamps
kubectl --kubeconfig="${AWS_KUBECONFIG:?}" -n fluidcr-realign-121040 logs trainer-1 -c trainer --tail=30 --timestamps
```

이전 hash 575076ca...와 달라지는 것이 정상이다. 현재 checkout 파일과 두 Pod의 hash가 같아야 한다. 두 Rank 모두 Step이 증가하는지 확인한다.

기존 실패 작업 정리, 새로운 학습 실행 준비, 위험률 설정 확인까지 끝난 뒤에만 suspend를 해제한다. 위험률 0.8과 replacement.enabled=true가 유지되면 바로 교체가 시작될 수 있다.

```bash
kubectl --kubeconfig="${KARMADA_KUBECONFIG:?}" --request-timeout=15s -n fluidcr-realign-121040 annotate trainingpolicy trainer-realign training.dcnlab.com/suspend-
```

재검증 성공 기준은 새 Partial Checkpoint에서 Survivor 증거 생성, 대상 Rank의 ContainerCheckpointed, Restore 완료 및 모든 Rank의 Step 증가다. Running만으로 성공 판정하지 않는다. 같은 실패가 반복되면 새 CR의 시작/종료 시각과 양쪽 학습 로그를 보존한다.

## 롤백

5절의 --payload-image를 webhook-before.yaml에 기록한 이전 값으로 되돌린다. 이미 새 payload를 복사한 학습 Pod에는 자동 반영되지 않으며, 재생성에는 6절과 동일한 복구 작업 확인이 필요하다.

