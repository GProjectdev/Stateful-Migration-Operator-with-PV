# Partial Checkpoint 역할 보호 및 진단 로그 배포 (2026-10-02)

## 변경 범위와 검증 한계

- 분산 optimizer-step 경계에서 모든 Rank가 동일한 명시적 manifest를 확인한 뒤 Target/Survivor 역할을 선택한다.
- manifest가 없거나 잘못되거나 Rank마다 다르면 전체 종료로 대체하지 않고 같은 경계에서 대기한다. 명시적인 targets=all은 계속 Full Checkpoint로 처리한다.
- 동기화 경계에 도달하면 신호 watchdog을 해제한다. Target 저장/종료에는 다시 watchdog을 적용한다.
- 합의 및 Survivor 대기 중 중복 SIGUSR1은 watchdog을 재시작하지 않는다.
- Survivor 증거는 역할 판정에 쓴 manifest의 ID/generation으로 기록한다.
- 기본 QUIET 설정에서도 신호 분기, 역할 판정, watchdog 및 자식 종료 로그가 출력된다.
- 기존 controller의 정기 Checkpoint 정지, restore-owned resume 거부, 복구 증거 검증은 유지한다. 이번 배포에는 payload-base와 payload-stateful만 새로 빌드한다.

2026-10-02 장애의 정확한 실행 분기는 과거 로그가 없어 확정하지 못했다. 이번 변경은 확인된 위험 경로를 보강하고 다음 실행의 원인을 관측 가능하게 한다. 로컬에서는 CPU mock/contract 및 Go controller 테스트를 수행했다. PyTorch/CUDA/NCCL이 없는 개발 환경이므로 실제 GPU 2-Rank 복구 성공은 별도 검증해야 한다.

manifest 합의 대기는 의도적인 안전 정지다. controller가 timeout으로 Failed를 기록해도 이를 성공으로 바꾸거나 수동으로 lock을 지우지 않는다. peer 소실/NCCL 자체 장애까지 이 변경으로 해결되지는 않는다. 구버전과 신버전 payload를 Rank별로 섞으면 collective 순서가 달라질 수 있으므로 두 Rank를 같은 버전으로 교체해야 한다.

## 1. MGMT에서 소스 갱신

명령은 Bash에서 한 줄씩 실행하며 실패하면 다음 단계로 넘어가지 않는다. namespace는 fluidcr-realign-121040으로 고정한다. 새 namespace는 만들지 않는다.

```bash
git -C /root/hybridspot-validation/fluidcr status --short
git -C /root/hybridspot-validation/stateful status --short
git -C /root/hybridspot-validation/fluidcr switch restore-automation-20260928
git -C /root/hybridspot-validation/stateful switch restore-automation-20260928
git -C /root/hybridspot-validation/fluidcr pull --ff-only origin restore-automation-20260928
git -C /root/hybridspot-validation/stateful pull --ff-only origin restore-automation-20260928
python3 -m unittest discover -s /root/hybridspot-validation/stateful/runtime/tests
```

로컬 변경으로 pull이 거부되면 덮어쓰지 말고 해당 변경을 보존한다.

## 2. 이미지 빌드 및 Push

기존 buildah 환경을 사용한다. Docker Hub 인증이 없으면 buildah login docker.io를 먼저 실행한다.

```bash
export FIX_RELEASE="partial-role-$(date -u +%Y%m%dT%H%M%SZ)"
export FIX_DIR="/root/hybridspot-validation/evidence/$FIX_RELEASE"
mkdir -p "$FIX_DIR/images"
printf '%s\n' "$FIX_DIR"
buildah bud --arch amd64 -f /root/hybridspot-validation/fluidcr/Dockerfile.payload -t "docker.io/jeongseungjun/myfluidcr-operator:payload-base-$FIX_RELEASE" /root/hybridspot-validation/fluidcr
buildah push --digestfile "$FIX_DIR/images/payload-base.digest" "docker.io/jeongseungjun/myfluidcr-operator:payload-base-$FIX_RELEASE" "docker://docker.io/jeongseungjun/myfluidcr-operator:payload-base-$FIX_RELEASE"
cat "$FIX_DIR/images/payload-base.digest"
buildah bud --arch amd64 -f /root/hybridspot-validation/stateful/Dockerfile.payload-overlay --build-arg "FLUIDCR_PAYLOAD_IMAGE=docker.io/jeongseungjun/myfluidcr-operator@$(cat "$FIX_DIR/images/payload-base.digest")" -t "docker.io/jeongseungjun/myfluidcr-operator:payload-stateful-$FIX_RELEASE" /root/hybridspot-validation/stateful
buildah push --digestfile "$FIX_DIR/images/payload-stateful.digest" "docker.io/jeongseungjun/myfluidcr-operator:payload-stateful-$FIX_RELEASE" "docker://docker.io/jeongseungjun/myfluidcr-operator:payload-stateful-$FIX_RELEASE"
cat "$FIX_DIR/images/payload-stateful.digest"
export FIX_PAYLOAD="docker.io/jeongseungjun/myfluidcr-operator@$(cat "$FIX_DIR/images/payload-stateful.digest")"
printf '%s\n' "$FIX_PAYLOAD"
```

digest 파일이 없거나 sha256: 형식이 아니면 배포하지 않는다. 과거 evidence의 digest를 대신 사용하지 않는다.

## 3. 상태 보존과 Webhook 설정 변경

```bash
kubectl --kubeconfig="${KARMADA_KUBECONFIG:?}" --request-timeout=15s -n fluidcr-realign-121040 get trainingpolicy,spotreplacements,restorerequests,fluidcrmigrations -o yaml > "$FIX_DIR/control-before.yaml"
kubectl --kubeconfig="${AWS_KUBECONFIG:?}" --request-timeout=15s -n fluidcr-realign-121040 get pods,restoreplans,fluidcrmigrations -o yaml > "$FIX_DIR/member-before.yaml"
kubectl --kubeconfig="${AWS_KUBECONFIG:?}" --request-timeout=15s -n fluidcr-system get deployment fluidcr-webhook -o yaml > "$FIX_DIR/webhook-before.yaml"
kubectl --kubeconfig="${KARMADA_KUBECONFIG:?}" --request-timeout=15s -n fluidcr-realign-121040 annotate trainingpolicy trainer-realign training.dcnlab.com/suspend=true --overwrite
printf '%s\n' "$FIX_PAYLOAD"
kubectl --kubeconfig="${AWS_KUBECONFIG:?}" --request-timeout=15s -n fluidcr-system edit deployment fluidcr-webhook
```

webhook 컨테이너의 args에서 --payload-image 값만 출력한 FIX_PAYLOAD 전체 문자열로 바꾼다. --payload-image=... 형식이면 = 뒤, 별도 항목이면 바로 다음 값이다. webhook의 컨테이너 image 자체는 바꾸지 않는다.

```bash
kubectl --kubeconfig="${AWS_KUBECONFIG:?}" -n fluidcr-system rollout status deployment/fluidcr-webhook --timeout=300s
kubectl --kubeconfig="${AWS_KUBECONFIG:?}" --request-timeout=15s -n fluidcr-system get deployment fluidcr-webhook -o json | jq '.spec.template.spec.containers[] | {name,args}'
```

suspend는 이미 생성된 SpotReplacement를 취소하지 않는다. 현재 실패 작업에 연결된 Pod를 이 단계에서 삭제하면 안 된다. 기존 trainer Pod는 initContainer가 복사한 구버전 코드를 계속 사용한다.

## 4. 실패한 실행과 새 검증 실행 분리

현재 Rank 0 학습 프로세스는 종료됐으므로 살아 있는 Survivor로 이어갈 수 없다. 기존 작업의 checkpointID/Pod UID를 새 Pod에 재사용하거나 status를 성공으로 patch하지 않는다.

현재 작업 상태를 확인한 뒤 해당 실행을 안전하게 종료하거나 검증된 전체 Checkpoint로 복구해야 한다. 이 문서는 PVC/PV/NodeProvision 삭제나 진행 중 작업 강제 취소를 자동으로 수행하지 않는다. 기존 증거와 Checkpoint 파일을 보존한다.

```bash
kubectl --kubeconfig="${KARMADA_KUBECONFIG:?}" --request-timeout=15s -n fluidcr-realign-121040 get spotreplacements,restorerequests -o yaml
kubectl --kubeconfig="${AWS_KUBECONFIG:?}" --request-timeout=15s -n fluidcr-realign-121040 get restoreplans -o yaml
```

활성 복구 작업이 없어지고 재시작할 학습 상태가 결정된 뒤 두 Rank를 모두 새 payload로 생성한다. OnDelete StatefulSet이라면 template 변경만으로 기존 Pod가 교체되지 않는다. Pod 삭제/재생성은 학습을 중단하므로 진행 중 복구가 없는 경우에만 수행한다.

```bash
kubectl --kubeconfig="${KARMADA_KUBECONFIG:?}" --request-timeout=15s -n fluidcr-realign-121040 get statefulset trainer -o json | jq '{uid:.metadata.uid,replicas:.spec.replicas,strategy:.spec.updateStrategy,annotations:.spec.template.metadata.annotations}'
```

현재 리소스를 모두 정리하고 처음부터 배포하는 경우 기존 System/docs/trainer-realign-recreate.md의 workload/PVC 절차를 사용하되 다음을 반영한다.

- namespace는 동일하게 유지한다.
- TrainingPolicy의 workloadRef.uid는 새 Karmada StatefulSet의 실제 UID로 채운다.
- 예전 Policy/Workload UID를 가진 TrainingRuntime 및 전용 placement가 남아 있지 않도록 확인한다.
- 두 학습 Rank가 필요한 실행 시 Karmada StatefulSet replicas=2, ResourceBinding clusters=[aws]를 확인한다.
- SpotRiskProfile 초기 위험률은 0.05, replacement.enabled=true로 설정하되, 새 학습 준비 전에 위험률을 올리지 않는다.
- 기존 NodeProvision이 모두 OnDemand라면 위험률만 올려도 Spot 교체 시험이 되지 않는다. 실제 marketType과 초기 구성을 확인한다.

## 5. 두 Rank의 새 payload 및 정상 학습 확인

```bash
kubectl --kubeconfig="${AWS_KUBECONFIG:?}" -n fluidcr-realign-121040 get pod trainer-0 trainer-1 -o json | jq '.items[] | {name:.metadata.name,uid:.metadata.uid,init:.spec.initContainers}'
sha256sum /root/hybridspot-validation/stateful/runtime/fluidcr/__init__.py /root/hybridspot-validation/stateful/runtime/fluidcr/backends/pytorch.py /root/hybridspot-validation/stateful/runtime/fluidcr/distributed.py
kubectl --kubeconfig="${AWS_KUBECONFIG:?}" -n fluidcr-realign-121040 exec trainer-0 -c trainer -- sha256sum /opt/fluidcr/fluidcr/__init__.py /opt/fluidcr/fluidcr/backends/pytorch.py /opt/fluidcr/fluidcr/distributed.py
kubectl --kubeconfig="${AWS_KUBECONFIG:?}" -n fluidcr-realign-121040 exec trainer-1 -c trainer -- sha256sum /opt/fluidcr/fluidcr/__init__.py /opt/fluidcr/fluidcr/backends/pytorch.py /opt/fluidcr/fluidcr/distributed.py
```

두 Pod의 init payload가 FIX_PAYLOAD와 같고 파일 hash도 checkout과 같아야 한다. 두 Rank의 Step 증가를 확인한 뒤 Policy suspend를 해제한다. 이전 실패 작업이 남아 있는 상태에서는 해제하지 않는다.

```bash
kubectl --kubeconfig="${KARMADA_KUBECONFIG:?}" -n fluidcr-realign-121040 annotate trainingpolicy trainer-realign training.dcnlab.com/suspend-
kubectl --kubeconfig="${AWS_KUBECONFIG:?}" -n fluidcr-realign-121040 get fluidcrmigrations
```

정기 Checkpoint Completed 후 양쪽 학습이 재개된 것을 확인한다. 그다음 위험률을 올린다.

```bash
kubectl --kubeconfig="${KARMADA_KUBECONFIG:?}" -n fluidcr-realign-121040 patch spotriskprofile trainer-risk --type=merge -p '{"spec":{"staticLambdaPerHour":0.8}}'
kubectl --kubeconfig="${KARMADA_KUBECONFIG:?}" -n fluidcr-realign-121040 get spotreplacements,restorerequests -o custom-columns='KIND:.kind,NAME:.metadata.name,PHASE:.status.phase,MESSAGE:.status.message'
kubectl --kubeconfig="${AWS_KUBECONFIG:?}" -n fluidcr-realign-121040 logs trainer-0 -c trainer --timestamps --since=10m --tail=200
kubectl --kubeconfig="${AWS_KUBECONFIG:?}" -n fluidcr-realign-121040 logs trainer-1 -c trainer --timestamps --since=10m --tail=200
```

## 6. 성공 조건과 실패 시 증거

Target이 Rank 1인 경우:
- Rank 0 로그: checkpoint role ... targets=[1] role=survivor.
- Rank 1 로그: checkpoint role ... targets=[1] role=target.
- Rank 0에는 launcher뿐 아니라 학습 프로세스가 계속 존재하고, 해당 ID의 survivor 증거가 생성되어야 한다.
- Rank 1은 ContainerCheckpointed 이후 새 노드에서 복구한다.
- RestoreRequest 검증 및 교체 완료, 양쪽 Step 증가까지 확인한다. Running만으로 성공 처리하지 않는다.

checkpoint manifest agreement pending이 반복되면 어느 Rank가 어떤 manifest/error를 보고 있는지 로그를 보존한다. 이때 all로 강제 변경하거나 pause-lock을 제거하지 않는다. checkpoint signal branch distributed=false, backend=none, watchdog fired 또는 예상과 다른 role=target이 찍히면 그 로그가 다음 원인 분석의 직접 증거다.

롤백은 webhook-before.yaml의 이전 --payload-image로 되돌린다. 실행 중인 두 Rank의 버전을 섞지 않으며, 실패 작업 정리와 재시작 확인은 롤백에도 동일하게 적용한다.

