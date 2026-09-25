# Stateful-Migration-System

FluidCR Checkpoint CR과 Karmada ResourceInterpreterCustomization을 사용하는 독립 Checkpoint/Restore 제어 시스템입니다.
Karmada 전체 소스, CRI-O, CRIU, FluidCR Python payload는 포함하지 않습니다.

```bash
git clone --single-branch --branch main https://github.com/GProjectdev/Stateful-Migration-Operator-with-PV.git Stateful-Migration-System
cd Stateful-Migration-System
```

기존 main 구현은 교체되며 Old-Version 브랜치는 보존됩니다. 일반 clone은 기존 브랜치 이력도 받을 수 있습니다.

## 확인 결과와 구현 범위

FluidCR에는 application checkpoint → kubelet CRIU checkpoint → 선택적 resume을 수행하는
`fluidcr.dcnlab.com/v1alpha1, FluidCRMigration`과 주입 웹훅이 이미 있습니다.
Restore는 Pod annotation 예제만 있으므로 RestorePlan용 Member 컨트롤러·생성 웹훅·검증 웹훅을 추가했습니다.

| 위치 | 구성 | 역할 |
|---|---|---|
| MGMT | Restore Controller | Karmada Checkpoint 집계 상태 검증, RestorePlan + 대상 PP 생성 |
| Source Member | FluidCR Checkpoint Controller | 기존 FluidCR 방식 실행, Pod별 결과를 CR status로 기록 |
| Target Member | Artifact verifier DaemonSet | 대상 노드 archive 경로·SHA-256 검증 |
| Target Member | Restore Controller / admission | 준비 상태, Pod 복원 annotation·node affinity, 최종 주입 검증 |
| Karmada | RIC 2개 | FluidCRMigration 및 RestorePlan 상태 반사·클러스터별 집계 |

MGMT는 Member API·클러스터 프록시·kubelet·Pod HTTP에 접근하지 않습니다.
`RestoreRequest`는 MGMT에만 두고, `FluidCRMigration`은 source 한 곳,
생성된 `RestorePlan`은 target 한 곳으로만 전파합니다.
[구조와 기존 시스템 차이](docs/architecture.md), [FluidCR 코드 출처](docs/fluidcr-origin.md).

지원 복원 대상은 **이름이 안정적인 StatefulSet 또는 단독 Pod**입니다.
StatefulSet은 Pod 이름/ordinal을 유지하며 모든 체크포인트 Pod를 빠짐없이 매핑해야 합니다.
Deployment/Job 복원, 다른 ordinal로 이동, cron 백업, OCI checkpoint image 빌드는 이번 범위가 아닙니다.

## 필수 환경

- Go 1.24 이상, Docker 또는 호환 빌더, kubectl/Kustomize.
- Karmada의 ResourceInterpreterCustomization 및 Lua status reflection/aggregation 지원.
- 각 Member에 FluidCR payload/injection webhook, GPU runtime/CRIU, kubelet Checkpoint API 준비.
- Target Member에 cert-manager. 다른 CA를 쓰려면 제공 Certificate/Issuer 대신 TLS Secret과 caBundle을 구성합니다.
- 소스와 대상의 컨테이너 이미지, GPU·driver·CUDA·CRIU·runtime 및 bind mount 호환성.
- 애플리케이션 `/checkpoint` 데이터와 PVC를 대상에서도 동일하게 접근할 수 있어야 합니다.
- CRIU archive는 소스 노드 로컬 파일입니다. **RIC는 경로를 전달할 뿐 파일을 복사하지 않습니다.**

**중요: 런타임 확인이 필요합니다.** FluidCR 예제는
`checkpoint-restore.crio.io/<container>`를 처리하는 수정 CRI-O를 전제합니다.
확인한 로컬 CRI-O 소스에는 이 annotation 처리 코드가 보이지 않았습니다.
해당 동작을 실제 검증한 runtime을 대상에 설치한 뒤에만 다음 capability label을 붙이세요.
표준 CRI-O/미지원 runtime에 label만 붙이면 복원 대신 새 프로세스가 시작될 위험이 있습니다.

```bash
kubectl --context aws label node <TARGET_NODE> migration.dcnlab.com/restore-from-file=true
```

이 명령은 설치 담당자의 Member 작업입니다. MGMT 컨트롤러가 실행하지 않습니다.
실제 runtime 지원을 확인하지 않았다면 label을 붙이지 않고 복원을 중단하세요.

## 빌드

```bash
go test ./...
go vet ./...
make docker-build IMG=<REGISTRY>/stateful-migration-system:<TAG>
make docker-push IMG=<REGISTRY>/stateful-migration-system:<TAG>
```

`config/{management,checkpoint,member}`의 image를 배포한 이미지로 변경합니다.
기본 이미지 이름은 빌드용 예시이며 이미 게시되어 있다는 의미가 아닙니다.

```bash
(cd config/management && kustomize edit set image ghcr.io/gprojectdev/stateful-migration-system=<REGISTRY>/stateful-migration-system:<TAG>)
(cd config/checkpoint && kustomize edit set image ghcr.io/gprojectdev/stateful-migration-system=<REGISTRY>/stateful-migration-system:<TAG>)
(cd config/member && kustomize edit set image ghcr.io/gprojectdev/stateful-migration-system=<REGISTRY>/stateful-migration-system:<TAG>)
```

## 설치

### FluidCR payload 호환 overlay

제공된 FluidCR의 rank checkpoint API는 신호 전송 직후 `checkpoint-signalled`를 반환할 수 있습니다.
이 상태를 완료로 취급하지 않도록, 이 저장소의 overlay는 `wait:true` 요청에 대해
애플리케이션 완료 잠금을 확인한 뒤 응답하도록 보완합니다. 원본 FluidCR 소스는 변경하지 않습니다.

```bash
docker build -f Dockerfile.payload-overlay \
  --build-arg FLUIDCR_PAYLOAD_IMAGE=<YOUR_EXISTING_FLUIDCR_PAYLOAD_IMAGE> \
  -t <REGISTRY>/fluidcr-payload:stateful-wait .
docker push <REGISTRY>/fluidcr-payload:stateful-wait
```

Source/Target FluidCR 주입 웹훅의 `--payload-image`를 이 이미지로 변경한 뒤 새로운 workload Pod를 배포하세요.
이미 주입된 실행 중 Pod의 파일은 웹훅 설정 변경만으로 갱신되지 않습니다.
Source와 Target은 동일 payload 이미지 및 Python/runtime 환경을 사용해야 합니다.
overlay 없이 `checkpoint-signalled`만 반환하는 환경은 안전하게 실패하며 CRIU 단계로 넘어가지 않습니다.

아래 context는 예시입니다: `host`=MGMT Pod 실행 클러스터,
`karmada`=Karmada API, `onprem`=source Member, `aws`=target Member.
Member 설치 권한은 초기 설치 담당자에게만 필요합니다.

### 1. CRD와 RIC

```bash
kubectl --context karmada apply -k config/crd
kubectl --context onprem apply -k config/crd
kubectl --context aws apply -k config/crd
kubectl --context karmada apply -k config/karmada
```

이미 FluidCR CRD가 있다면 먼저 백업하고 schema 확장을 검토하세요.
이 배포판은 같은 CRD에 clusters/pod identity를 추가하며 spec을 immutable로 제한합니다.
기존 CRD 설치 도구가 이후 원본 schema로 덮어쓰지 않도록 관리 주체를 하나로 정하세요.
Karmada agent의 Member RBAC에도 FluidCRMigration/RestorePlan get/list/watch/create/update/patch/delete와
관련 status 읽기가 허용되어야 합니다. 클러스터 전체 admin 권한을 추가할 필요는 없습니다.

### 2. Source Checkpoint

원본 FluidCR Operator와 이 Checkpoint Deployment를 **동시에 실행하지 마세요**.
같은 CR을 두 번 처리하여 checkpoint가 중복 실행될 수 있습니다.
원본 Python runtime과 injection webhook은 계속 사용합니다.

기존 Stateful Migration Operator의 백업·복원·자동 suspension 해제 프로세스도 전환 전에 중지하세요.
기존 CR 삭제는 finalizer나 정리 동작을 유발할 수 있으므로 데이터와 상태를 백업한 뒤 별도로 결정합니다.

```bash
kubectl --context onprem apply -f config/checkpoint/namespace.yaml
kubectl --context onprem -n stateful-migration-system create configmap kubelet-serving-ca --from-file=ca.crt=<KUBELET_SERVING_CA>
kubectl --context onprem apply -k config/checkpoint
```

kubelet 서버 인증서의 CA와 IP SAN이 올바라야 합니다. TLS 검증은 기본 활성화입니다.
실험용 insecure 플래그는 운영 환경에서 사용하지 마세요.
컨트롤러는 Member 내 Pod control API(기본 8298)와 kubelet(10250)에 연결합니다.
방화벽/NetworkPolicy와 kubelet webhook authorization의 nodes/checkpoint 권한을 확인하세요.

### 3. Target Restore

`config/member/deployment.yaml`과 `artifact-daemonset.yaml`의
`--cluster-name=aws`가 Karmada Member 이름과 일치해야 합니다.

```bash
kubectl --context aws apply -k config/member
kubectl --context aws -n stateful-migration-system wait --for=condition=Ready certificate/stateful-restore-cert --timeout=120s
kubectl --context aws -n stateful-migration-system rollout status deployment/stateful-member
kubectl --context aws -n stateful-migration-system rollout status daemonset/stateful-artifact
```

Artifact DaemonSet은 archive 디렉터리를 read-only hostPath로 읽습니다.
root 소유 archive를 읽기 위해 UID 0을 사용하지만 privileged는 아니며 capability를 제거합니다.
Pod Security 정책에 맞는 전용 namespace 예외를 승인받아 설치하세요.
archive와 SHA-256은 프로세스 메모리/비밀을 포함할 수 있으므로 암호화 전송 및 접근 제어가 필요합니다.

FluidCR injection webhook은 대상 namespace를 선택하도록 설정되어 있어야 합니다.
제공된 FluidCR 예제는 namespace 목록이 제한되어 있으므로 실제 workload namespace를 확인하세요.
원본/복원 Pod template에 inject=true, 올바른 container annotation, 동일 payload image와
/checkpoint PVC를 설정합니다. 복원 검증 웹훅은 주입 누락을 거부합니다.

### 4. MGMT Restore Controller

`config/karmada`에서 만든 `stateful-management` ServiceAccount 권한으로 Karmada kubeconfig를 발급합니다.
cluster-admin kubeconfig나 Member kubeconfig를 넣지 마세요.
서버 CA 검증을 유지하고 Secret 파일 이름을 `kubeconfig`로 지정합니다.

`bash scripts/create-karmada-kubeconfig.sh karmada /tmp/stateful-karmada.kubeconfig`로
위 ServiceAccount의 1시간 token을 사용하는 최소 kubeconfig를 만들 수 있습니다.
API 주소가 host Pod에서 접근할 수 없다면 실행 전에 `KARMADA_SERVER`에 올바른 주소를 지정하세요.
출력 파일은 비밀이며 Git에 커밋하지 않습니다. 장기 운영에서는 token 회전을 구성하세요.

```bash
kubectl --context host apply -f config/management/namespace.yaml
kubectl --context host -n stateful-migration-system create secret generic stateful-karmada-kubeconfig --from-file=kubeconfig=<LIMITED_KARMADA_KUBECONFIG>
kubectl --context host apply -k config/management
kubectl --context host -n stateful-migration-system rollout status deployment/stateful-management
```

짧은 수명의 token으로 만든 kubeconfig는 갱신 절차가 필요합니다.
이 스크립트는 inline token을 넣으므로 Secret 갱신 후 `deployment/stateful-management`도
`kubectl --context host -n stateful-migration-system rollout restart deployment/stateful-management`로 재시작합니다.
Secret 파일 변경만으로 실행 중인 client의 inline token이 갱신되지는 않습니다.
MGMT manager leader-election Lease는 Karmada의 stateful-migration-system namespace에 생성됩니다.

## 사용 순서

### 1. Checkpoint 요청

기존 workload가 source에서 실행되고 모든 Pod에 FluidCR이 주입되어 있어야 합니다.
`config/samples/checkpoint.yaml`의 namespace/workload/container/source 이름을 수정합니다.
애플리케이션 잠금을 유지하기 위해 **resume: false**를 명시합니다.

```bash
kubectl --context karmada apply -f config/samples/checkpoint.yaml
kubectl --context karmada -n fluidcr-demo get fluidcrmigration trainer-checkpoint-001 -o yaml
```

`status.clusters`의 source 항목에서 Completed 및 현재 observedGeneration을 확인합니다.
Pod별 podName/nodeName/podUID/checkpointFiles의 containerName/filePath가 복원 입력입니다.
새 백업은 새 CR 이름으로 요청하세요. 기존 CR spec/배치 정책을 변경하여 재실행하지 마세요.

### 2. Source fence, PV와 파일 준비

1. workload ResourceBinding 이름을 확인하고 dispatch를 중지합니다. 자동 failover/기존 suspension 자동 해제 컨트롤러도 중지합니다.
2. 원본 프로세스를 중지하거나 fence합니다. resume:false만으로 외부 쓰기와 split-brain이 완전히 차단된다고 가정하지 마세요.
3. PVC retention/reclaim policy를 확인하고 PV-Migration-System으로 대상 NFS PV를 준비합니다.
4. 애플리케이션 checkpoint 데이터와 모든 mount를 보존합니다.
5. 노드별 archive를 승인된 별도 전송 도구로 대상 노드에 복사하고 원본과 대상 SHA-256을 대조합니다.
6. sourceFenced와 volumesReady는 실제 완료 후에만 true로 설정합니다.

```bash
kubectl --context karmada -n fluidcr-demo get resourcebindings \
  -o custom-columns=NAME:.metadata.name,KIND:.spec.resource.kind,WORKLOAD:.spec.resource.name
kubectl --context karmada -n fluidcr-demo patch resourcebinding <RB_NAME> --type=merge \
  -p '{"spec":{"suspension":{"dispatching":true}}}'
```

PV-Migration-System의 사전 staging은 target이 기존 ResourceBinding clusters에 없을 때 수행합니다.
그 시스템의 README 순서를 먼저 따르세요. 여기서는 PV 생성/복사/정리를 중복 수행하지 않습니다.
노드 파일 전송은 이 컨트롤러에 포함되지 않으며, MGMT가 Member에 직접 접속하는 우회 로직도 없습니다.

### 3. RestoreRequest 생성

`config/samples/restore-request.yaml`의 모든 REPLACE 값을 실제 값으로 수정합니다.
UID는 Member CR의 UID가 아닌 **Karmada FluidCRMigration UID**입니다.
각 source Pod의 모든 checkpointFiles를 정확히 매핑해야 하며 targetPod는 sourcePod와 같습니다.
targetPath는 대상 노드 /var/lib/kubelet/checkpoints 아래 일반 파일이어야 합니다.
다른 경로·symlink·digest 불일치는 거부됩니다.

```bash
kubectl --context karmada -n fluidcr-demo get fluidcrmigration trainer-checkpoint-001 \
  -o jsonpath='{.metadata.uid}{"\n"}{.metadata.generation}{"\n"}'
# sample의 UID, 경로, node, SHA-256 및 완료 확인 값을 수정한 후:
kubectl --context karmada apply -f config/samples/restore-request.yaml
kubectl --context karmada -n fluidcr-demo get restorerequest trainer-to-aws-001 -o yaml
```

CR spec은 immutable입니다. 잘못된 요청은 새로운 이름으로 만들되,
기존 plan과 restore label이 남은 상태에서 중복 복원을 시작하지 마세요.
RestoreRequest를 Member로 보내는 wildcard PropagationPolicy를 만들지 마세요.

### 4. Prepared 확인 후 workload 전파

```bash
PLAN=$(kubectl --context karmada -n fluidcr-demo get restorerequest trainer-to-aws-001 -o jsonpath='{.status.planName}')
kubectl --context karmada -n fluidcr-demo get restoreplan "$PLAN" -o yaml
kubectl --context karmada -n fluidcr-demo get restorerequest trainer-to-aws-001 -w
```

`Prepared`가 되면 대상 파일 검증과 CR 전파가 준비된 상태입니다.
실행 중인 원본이 없고 dispatch가 중지된 상태에서 대상 workload template에 다음 값을 설정합니다.
(StatefulSet은 spec.template.metadata, 단독 Pod는 metadata)

```yaml
labels:
  migration.dcnlab.com/restore-plan: <PLAN>
annotations:
  fluidcr.dcnlab.com/inject: "true"
  fluidcr.dcnlab.com/container: trainer
  fluidcr.dcnlab.com/checkpoint-claim: <APPLICATION_CHECKPOINT_PVC>
```

기존 template labels/annotations를 보존하여 merge하세요. namespace/Pod 이름/컨테이너 이름과
mount, 이미지, runtimeClass, GPU claim, DDP rank 구성을 source와 일치시킵니다.
대상 Pod가 이미 생성되어 있으면 소급 복원하지 않습니다. 컨트롤러는 이를 임의 삭제하지 않습니다.

그 다음 workload PropagationPolicy를 target 한 곳으로 변경하고, PV 및 복원 준비를 재확인한 뒤
ResourceBinding dispatch를 수동 재개합니다. Checkpoint의 source 고정 PP는 변경하지 않습니다.

```bash
kubectl --context karmada -n fluidcr-demo patch propagationpolicy <WORKLOAD_PP> --type=merge \
  -p '{"spec":{"placement":{"clusterAffinity":{"clusterNames":["aws"]}}}}'
kubectl --context karmada -n fluidcr-demo patch resourcebinding <RB_NAME> --type=merge \
  -p '{"spec":{"suspension":{"dispatching":false}}}'
```

### 5. 결과 확인과 정리

`RestoreRequest.status.phase=Running`은 대상 Pod의 Running/Ready와 설정 일치를 뜻하며
**메모리 상태·학습 step 복원 성공을 보증하지 않습니다**.
CRI-O/CRIU 로그와 애플리케이션 iteration, tensor/model 상태, DDP 전체 rank 재결합을 확인하세요.
그 전에는 트래픽 전환, 원본/체크포인트 삭제를 하지 마세요.

Plan과 CR은 자동 삭제하지 않습니다. 운영자가 이력을 보관하고 재복원 정책을 결정해야 합니다.
복원 label이 유지된 StatefulSet은 Pod 재생성 시 같은 checkpoint를 다시 사용할 수 있으므로
검증 이후 정상 재시작 정책을 별도로 결정하세요. Plan 삭제 전 template의 restore label을 제거하되,
StatefulSet template 변경이 rollout을 일으킬 수 있으므로 OnDelete 등 운영 절차로 제어하세요.

## 보안 및 검증 한계

RestoreRequest/Plan 생성·수정 및 status 쓰기 권한은 신뢰할 수 있는 운영자/컨트롤러에만 부여하세요.
웹훅의 opt-in label은 멀티테넌트 보안 경계가 아닙니다. tenant가 직접 Pod/Node label/status를 조작할 수 있으면 우회할 수 있습니다.
파일 검증과 runtime 파일 읽기 사이 변경 위험을 줄이기 위해 archive를 배포 후 변경 불가능하게 운영하세요.
실제 검증 결과와 환경 의존 항목은 [검증 기록](docs/validation.md)을 참고하세요.

공식 참고: [Karmada RIC](https://karmada.io/docs/userguide/globalview/customizing-resource-interpreter/),
[Kubernetes admission](https://kubernetes.io/docs/reference/access-authn-authz/extensible-admission-controllers/).
