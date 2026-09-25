# StatefulSet 2개 Pod 통합 이전

하나의 StatefulSet `trainer`와 `trainer-0`, `trainer-1`을 사용합니다. MGMT 컨트롤러는 Karmada API만 사용합니다. Member 설치, source fencing, archive 전송은 운영자 작업입니다. 이 문서는 실제 GPU 복원 성공 기록이 아닙니다.

## 1. 준비와 이미지

Linux 관리 터미널에 git, Docker, kubectl, Kustomize, jq, envsubst를 준비합니다. Context는 `host`(관리 Pod), `karmada`(Karmada API), `onprem`(source), `aws`(target)입니다. Context와 Karmada member 이름이 다르면 각각 구분하여 변경하세요. 아래 YOUR 값은 직접 준비하는 값이며 게시된 이미지가 아닙니다.

```bash
set -euo pipefail
git clone --single-branch --branch main https://github.com/GProjectdev/Karmada_with_PVMigration.git PV-Migration-System
git clone --single-branch --branch main https://github.com/GProjectdev/Stateful-Migration-Operator-with-PV.git Stateful-Migration-System
export PV_ROOT="$PWD/PV-Migration-System"
export ST_ROOT="$PWD/Stateful-Migration-System"
export PV_IMAGE='YOUR_REGISTRY/pv-migration-system:YOUR_TAG'
export ST_IMAGE='YOUR_REGISTRY/stateful-migration-system:YOUR_TAG'
export PAYLOAD_BASE='YOUR_EXISTING_FLUIDCR_PAYLOAD_IMAGE'
export PAYLOAD_IMAGE='YOUR_REGISTRY/fluidcr-payload:stateful-wait'
(cd "$PV_ROOT" && make docker-build docker-push IMG="$PV_IMAGE")
(cd "$ST_ROOT" && make docker-build docker-push IMG="$ST_IMAGE")
(cd "$ST_ROOT" && docker build -f Dockerfile.payload-overlay --build-arg FLUIDCR_PAYLOAD_IMAGE="$PAYLOAD_BASE" -t "$PAYLOAD_IMAGE" .)
docker push "$PAYLOAD_IMAGE"
for d in management member; do
  (cd "$PV_ROOT/config/$d" && kustomize edit set image ghcr.io/gprojectdev/pv-migration-system="$PV_IMAGE")
done
for d in management checkpoint member; do
  (cd "$ST_ROOT/config/$d" && kustomize edit set image ghcr.io/gprojectdev/stateful-migration-system="$ST_IMAGE")
done
```

[런타임 설치 가이드](runtime-installation.md)를 먼저 수행하세요. 제공된 CRI-O fork만 설치했다고 restore annotation 지원이 보장되지 않습니다. 가이드의 adapter patch와 실제 annotation 복원 기능시험이 필요합니다. Pod image는 정상 컨테이너 이미지로 유지하며 archive 절대경로를 image에 넣지 않습니다. 검증한 target node에만 capability label을 붙입니다.

```bash
kubectl --context aws label node TARGET_NODE_0 TARGET_NODE_1 migration.dcnlab.com/restore-from-file=true
```

Source/target에 원본 FluidCR injection webhook을 설치하고 동일 overlay payload image를 사용합니다. `fluidcr-demo`가 webhook 대상 namespace인지 확인하세요. 기존 실행 Pod에는 새 payload가 자동 반영되지 않으므로 source workload는 설정 이후 생성합니다. 원본 FluidCR checkpoint operator와 새 checkpoint controller를 동시에 실행하지 않습니다. 이전 자동 suspension 해제 controller도 중지합니다. GPU device plugin/runtimeClass, target cert-manager, NFS 접근, kubelet serving CA/IP SAN을 준비합니다.

검토한 원본 FluidCR 소스 경로를 지정합니다. 다음은 cert-manager가 양쪽 Member에 준비된 경우의 webhook-only 설치입니다. 관리 노드 통합 bundle은 구 checkpoint operator까지 설치하므로 사용하지 않습니다. 기존 설치를 갱신한다면 먼저 진행 중인 migration과 Pod 생성을 중지하세요.

```bash
export FLUIDCR_ROOT=/YOUR_REVIEWED/FluidCR-anhvt
export INJECTOR_IMAGE=YOUR_REVIEWED_FLUIDCR_WEBHOOK_IMAGE
for ctx in onprem aws; do
  for file in namespace rbac cert-manager service; do
    kubectl --context "$ctx" apply -f "$FLUIDCR_ROOT/deploy/webhook/$file.yaml"
  done
  kubectl patch --local -f "$FLUIDCR_ROOT/deploy/webhook/deployment.yaml" --type=strategic \
    -p "$(jq -nc --arg image "$INJECTOR_IMAGE" --arg payload "$PAYLOAD_IMAGE" \
      '{spec:{template:{spec:{containers:[{name:"webhook",image:$image,args:[("--payload-image="+$payload),"--port=9443","--cert-dir=/tmp/k8s-webhook-server/serving-certs"]}]}}}}')" \
    -o yaml | kubectl --context "$ctx" apply -f -
  kubectl --context "$ctx" -n fluidcr-system wait --for=condition=Ready certificate/fluidcr-webhook-cert --timeout=180s
  kubectl --context "$ctx" -n fluidcr-system rollout status deployment/fluidcr-webhook --timeout=180s
  kubectl --context "$ctx" apply -f "$FLUIDCR_ROOT/deploy/webhook/mutatingwebhookconfiguration.yaml"
done
```

원본 webhook namespace selector에 `fluidcr-demo`가 포함되는지 확인하세요. 원본 설정은 failurePolicy=Ignore이므로 아래 source Pod의 injected 검증은 생략할 수 없습니다. Target에서는 새 restore webhook의 fail-closed 검증까지 통과해야 합니다.

## 2. 두 시스템 설치

```bash
for ctx in karmada onprem aws; do
  kubectl --context "$ctx" apply -k "$PV_ROOT/config/crd"
  kubectl --context "$ctx" apply -k "$ST_ROOT/config/crd"
done
kubectl --context karmada apply -k "$PV_ROOT/config/karmada/rbac"
kubectl --context karmada apply -k "$PV_ROOT/config/karmada/ric"
kubectl --context karmada apply -k "$ST_ROOT/config/karmada"
kubectl --context onprem apply -k "$PV_ROOT/config/member"
kubectl --context onprem -n pv-migration-system patch deployment pv-migration-member --type=json \
  -p='[{"op":"replace","path":"/spec/template/spec/containers/0/args/1","value":"--cluster-name=onprem"}]'
kubectl --context aws apply -k "$PV_ROOT/config/member"
kubectl --context onprem apply -f "$ST_ROOT/config/checkpoint/namespace.yaml"
kubectl --context onprem -n stateful-migration-system create configmap kubelet-serving-ca --from-file=ca.crt=YOUR_KUBELET_SERVING_CA
kubectl --context onprem apply -k "$ST_ROOT/config/checkpoint"
kubectl --context aws apply -k "$ST_ROOT/config/member"
kubectl --context aws -n stateful-migration-system wait --for=condition=Ready certificate/stateful-restore-cert --timeout=180s
kubectl --context aws -n stateful-migration-system rollout status deployment/stateful-member --timeout=180s
kubectl --context aws -n stateful-migration-system rollout status daemonset/stateful-artifact --timeout=180s
```

위 설치 순서는 아직 workload/PVMetadata가 없는 신규 환경을 기준으로 합니다. 기존 환경에서는 PVSync Deployment의 cluster-name을 실제 Member 이름으로 맞춘 manifest를 먼저 준비한 뒤 배포하세요.

MGMT에는 각 전용 ServiceAccount의 제한된 Karmada credential만 넣습니다. Admin flatten이나 Member kubeconfig는 사용하지 않습니다.

```bash
bash "$ST_ROOT/scripts/create-karmada-kubeconfig.sh" karmada /tmp/pv.kubeconfig pv-migration-system pv-migration-management
bash "$ST_ROOT/scripts/create-karmada-kubeconfig.sh" karmada /tmp/stateful.kubeconfig
kubectl --context host apply -f "$PV_ROOT/config/management/namespace.yaml"
kubectl --context host apply -f "$ST_ROOT/config/management/namespace.yaml"
kubectl --context host -n pv-migration-system create secret generic karmada-kubeconfig --from-file=kubeconfig=/tmp/pv.kubeconfig
kubectl --context host -n stateful-migration-system create secret generic stateful-karmada-kubeconfig --from-file=kubeconfig=/tmp/stateful.kubeconfig
kubectl --context host apply -k "$PV_ROOT/config/management"
kubectl --context host apply -k "$ST_ROOT/config/management"
kubectl --context host -n pv-migration-system rollout status deployment/pv-migration-management --timeout=180s
kubectl --context host -n stateful-migration-system rollout status deployment/stateful-management --timeout=180s
```

Secret은 Git에 넣지 않습니다. Inline token 만료 전에 갱신하고 해당 deployment를 rollout restart해야 합니다. Karmada endpoint가 host Pod에서 접근 가능해야 합니다. Karmada agent의 Member 권한에 새 CR 전파 및 status 읽기가 허용되어야 합니다.

## 3. 두 Pod와 NFS PV

하나의 `checkpoint` claim template이 `checkpoint-trainer-0`, `checkpoint-trainer-1`을 만듭니다. Source static PV 두 개는 RWX/Retain이고 각 ordinal PVC에 미리 연결됩니다. NFS 경로는 source/target 모두에서 접근 가능해야 합니다. Target PV는 PVMigration이 생성하므로 source PV manifest를 target에 적용하지 않습니다.

기본 샘플은 독립 GPU 작업 두 개입니다. 학습 이미지는 Python/PyTorch와 FluidCR 호환 hook을 사용하는 `/workspace/train.py`를 직접 포함해야 합니다. 임의의 Python 프로그램이 자동 checkpoint된다는 의미가 아닙니다. Webhook이 launcher를 감싸므로 command에서 launcher를 중복 실행하지 않습니다. `/checkpoint`는 claim template volume으로, `/opt/fluidcr`는 webhook의 read-only payload volume으로 사용합니다. Source/target 이미지, payload, GPU/runtime 및 mount를 동일하게 맞춥니다.

```bash
export SOURCE_CLUSTER=onprem TARGET_CLUSTER=aws
export NFS_SERVER=YOUR_NFS_SERVER
export NFS_EXPORT_0=/YOUR_EXPORT/trainer-0 NFS_EXPORT_1=/YOUR_EXPORT/trainer-1
export TRAINER_IMAGE=YOUR_REGISTRY/YOUR_FLUIDCR_COMPATIBLE_TRAINER:YOUR_TAG
export GPU_RUNTIME_CLASS=YOUR_GPU_RUNTIME_CLASS
export SAMPLES="$ST_ROOT/config/samples/two-replica"
mkdir -p /tmp/two-replica
for ctx in karmada onprem aws; do
  kubectl --context "$ctx" apply -f "$SAMPLES/namespace.yaml"
done
envsubst < "$SAMPLES/source-pvs.yaml" > /tmp/two-replica/source-pvs.yaml
envsubst < "$SAMPLES/workload.yaml" > /tmp/two-replica/workload.yaml
kubectl --context onprem apply -f /tmp/two-replica/source-pvs.yaml
kubectl --context karmada apply -f /tmp/two-replica/workload.yaml
kubectl --context onprem -n fluidcr-demo rollout status statefulset/trainer --timeout=600s
kubectl --context onprem -n fluidcr-demo get pods -l app=trainer -o json |
  jq -e '(.items|length)==2 and ([.items[].metadata.name]|sort)==["trainer-0","trainer-1"] and
    all(.items[]; .metadata.annotations["fluidcr.dcnlab.com/injected"]=="true" and
      .status.phase=="Running" and any(.status.conditions[]?; .type=="Ready" and .status=="True"))'
```

## 4. 메타데이터 확보와 checkpoint

PVMetadata는 source fencing 전에 확보합니다. `spec.workloadRef.uid`는 Karmada StatefulSet UID입니다. `status.clusters[].workloadUID`는 Member-local UID이므로 두 값을 서로 비교하지 않습니다.

```bash
RB_NAME=$(kubectl --context karmada -n fluidcr-demo get resourcebindings -o json |
  jq -er '[.items[]|select(.spec.resource.kind=="StatefulSet" and .spec.resource.name=="trainer")]|
    if length==1 then .[0].metadata.name else error("ambiguous RB") end')
export RB_NAME
PV_METADATA=$(kubectl --context karmada -n fluidcr-demo get pvmetadata -o json |
  jq -er --arg src "$SOURCE_CLUSTER" '[.items[]|select(.spec.sourceCluster==$src and .spec.workloadRef.name=="trainer")]|
    if length==1 then .[0].metadata.name else error("metadata not unique/ready") end')
export PV_METADATA
kubectl --context karmada -n fluidcr-demo get pvmetadata "$PV_METADATA" -o yaml
```

현재 generation의 source snapshot에 두 PVC/PV와 ordinal 정보가 있는지 확인합니다. 수집되지 않았다면 진행하지 않습니다. 자동 failover와 외부 placement 변경을 중지하고 RB에 target이 없는 상태에서 dispatch를 잠급니다. 이 RB는 workload만 잠그므로 별도 checkpoint CR은 source로 전파됩니다.

```bash
kubectl --context karmada -n fluidcr-demo patch resourcebinding "$RB_NAME" --type=merge \
  -p '{"spec":{"suspension":{"dispatching":true}}}'
envsubst < "$SAMPLES/checkpoint.yaml" > /tmp/two-replica/checkpoint.yaml
kubectl --context karmada apply -f /tmp/two-replica/checkpoint.yaml
for i in $(seq 1 120); do
  if kubectl --context karmada -n fluidcr-demo get fluidcrmigration trainer-checkpoint-001 -o json |
    jq -e --arg src "$SOURCE_CLUSTER" '.metadata.generation as $g |
      [.status.clusters[]?|select(.clusterName==$src and .observedGeneration==$g and .phase=="Completed")|
       select((.pods|length)==2 and ([.pods[].podName]|sort)==["trainer-0","trainer-1"])|
       select(all(.pods[]; (.podUID|length)>0 and (.checkpointFiles|length)==1))]|length==1' >/dev/null; then break; fi
  test "$i" -lt 120 || { echo 'Checkpoint timeout; do not continue'; exit 1; }
  sleep 5
done
kubectl --context karmada -n fluidcr-demo get fluidcrmigration trainer-checkpoint-001 -o json > /tmp/two-replica/checkpoint.json
CHECKPOINT_UID=$(jq -er .metadata.uid /tmp/two-replica/checkpoint.json)
export CHECKPOINT_UID
```

Checkpoint CR UID는 Karmada CR의 UID를 사용합니다. Source Pod/node/archive 경로와 application checkpoint를 보존합니다. `resume:false`는 외부 writer까지 차단하는 fencing이 아닙니다.

## 5. Source fencing과 두 PV 이전

운영자가 source fencing을 수행합니다. 예를 들어 RB dispatch가 잠긴 상태에서 source StatefulSet만 scale 0하고 두 Pod 종료 및 외부 writer 차단을 확인합니다. Karmada StatefulSet replicas는 2 그대로 둡니다. 먼저 PVC retention/Retain과 archive 보존을 확인하세요.

RB 변경 직후 바로 scale하지 마세요. Source 실행 namespace의 해당 workload Work까지 `spec.suspendDispatching: true`가 반영되어 execution controller가 다시 replicas=2로 덮어쓰지 않는지 확인합니다. Scale 0만으로 영구 fencing을 보장하지 않습니다. Source가 다시 쓰기를 시작할 수 없도록 운영 환경의 fencing을 유지하고, 이를 확인할 수 없다면 다음 단계로 진행하지 않습니다.

```bash
kubectl --context onprem -n fluidcr-demo scale statefulset trainer --replicas=0
kubectl --context onprem -n fluidcr-demo wait --for=delete pod/trainer-0 pod/trainer-1 --timeout=300s
```

실제 fencing 완료 후에만 아래 `sourceFenced:true` 요청을 생성합니다. CR spec은 immutable이므로 false로 생성했다가 나중에 바꾸는 절차가 아닙니다.

```bash
envsubst < "$SAMPLES/pvmigration.yaml" > /tmp/two-replica/pv-template.yaml
kubectl patch --local -f /tmp/two-replica/pv-template.yaml --type=merge \
  -p '{"spec":{"sourceFenced":true}}' -o yaml > /tmp/two-replica/pvmigration.yaml
kubectl --context karmada apply -f /tmp/two-replica/pvmigration.yaml
for i in $(seq 1 120); do
  if kubectl --context karmada -n fluidcr-demo get pvmigration trainer-pv-001 -o json |
    jq -e --arg target "$TARGET_CLUSTER" '.status.observedGeneration==.metadata.generation and
      .status.phase=="Completed" and (.status.planHash|length)>0 and (.status.works|length)==2 and
      all(.status.works[]; .applied==true and .detached==true and .namespace==("karmada-es-"+$target))' >/dev/null; then break; fi
  test "$i" -lt 120 || { echo 'PV timeout; remain suspended'; exit 1; }
  sleep 5
done
```

Completed는 두 PV Work 적용/detach의 이력 증거이지 현재 PV health, 데이터 복사 또는 복원 성공이 아닙니다. 운영자가 target PV 두 개와 NFS 접근을 확인합니다. 이 단계가 끝나기 전에 workload target placement를 선택하지 않습니다.

## 6. Archive 전송과 RestoreRequest

checkpoint.json의 source cluster에서 각 Pod의 nodeName과 checkpointFiles.filePath를 확인합니다. 승인된 SSH/파일전송 도구로 source node archive를 지정 target node의 `/var/lib/kubelet/checkpoints/` 아래에 복사합니다. RIC는 상태와 경로만 전달하며 파일을 전송하지 않습니다. Source/target SHA256이 같아야 합니다. 임시 파일로 전송하고 검증한 뒤 최종 경로로 옮깁니다. Archive에는 프로세스 메모리 비밀정보가 포함될 수 있으므로 암호화 전송과 제한된 접근 권한을 사용하세요.

```bash
export SOURCE_ARCHIVE_0=YOUR_REPORTED_ABSOLUTE_ARCHIVE_0
export SOURCE_ARCHIVE_1=YOUR_REPORTED_ABSOLUTE_ARCHIVE_1
export TARGET_ARCHIVE_0=/var/lib/kubelet/checkpoints/YOUR_ARCHIVE_0.tar
export TARGET_ARCHIVE_1=/var/lib/kubelet/checkpoints/YOUR_ARCHIVE_1.tar
export TARGET_NODE_0=YOUR_TARGET_NODE_0 TARGET_NODE_1=YOUR_TARGET_NODE_1
export SHA256_0=YOUR_64_LOWERCASE_HEX_DIGEST_0 SHA256_1=YOUR_64_LOWERCASE_HEX_DIGEST_1
envsubst < "$SAMPLES/restore-request.yaml" > /tmp/two-replica/restore-template.yaml
# Execute only after actual fencing, PV/NFS checks and archive transport.
kubectl patch --local -f /tmp/two-replica/restore-template.yaml --type=merge \
  -p '{"spec":{"sourceFenced":true,"volumesReady":true}}' -o yaml > /tmp/two-replica/restore-request.yaml
kubectl --context karmada apply -f /tmp/two-replica/restore-request.yaml
kubectl --context karmada -n fluidcr-demo wait --for=jsonpath='{.status.phase}'=Prepared restorerequest/trainer-restore-001 --timeout=600s
PLAN=$(kubectl --context karmada -n fluidcr-demo get restorerequest trainer-restore-001 -o jsonpath='{.status.planName}')
export PLAN
```

Prepared는 target 파일/노드/계획 준비이지 GPU 메모리 복원 성공이 아닙니다. Archive verifier의 관측은 신선해야 하므로 archive와 verifier를 유지하세요. RestoreRequest는 Member에 전파하지 않습니다. 잘못된 immutable 요청은 기존 operation을 점검한 후 새로운 이름으로 만듭니다.

## 7. Suspension gate와 target 전환

새 suspension controller 및 RBAC가 포함된 관리 이미지를 설치한 뒤 진행합니다. Gate는 같은 namespace의 RestoreRequest와 PVMigration을 name+UID로 고정합니다. Source/target/RB, workload identity, 볼륨 집합, 현재 generation, 두 PV Work의 applied/detached, RestorePlan Prepared 및 target-only RB를 확인한 뒤 dispatching 키를 제거합니다. PVMigration controller 자체는 RB를 수정하지 않습니다.

```bash
RESTORE_UID=$(kubectl --context karmada -n fluidcr-demo get restorerequest trainer-restore-001 -o jsonpath='{.metadata.uid}')
PV_UID=$(kubectl --context karmada -n fluidcr-demo get pvmigration trainer-pv-001 -o jsonpath='{.metadata.uid}')
kubectl --context karmada -n fluidcr-demo annotate resourcebinding "$RB_NAME" \
  migration.dcnlab.com/restore-request=trainer-restore-001 \
  migration.dcnlab.com/restore-request-uid="$RESTORE_UID" \
  migration.dcnlab.com/pv-migration=trainer-pv-001 \
  migration.dcnlab.com/pv-migration-uid="$PV_UID" --overwrite
kubectl --context karmada -n fluidcr-demo patch statefulset trainer --type=merge \
  -p "$(jq -nc --arg plan "$PLAN" '{spec:{template:{metadata:{labels:{"migration.dcnlab.com/restore-plan":$plan}}}}}')"
kubectl --context karmada -n fluidcr-demo patch propagationpolicy trainer-workload --type=merge \
  -p "$(jq -nc --arg t "$TARGET_CLUSTER" '{spec:{placement:{clusterAffinity:{clusterNames:[$t]}}}}')"
for i in $(seq 1 120); do
  if kubectl --context karmada -n fluidcr-demo get resourcebinding "$RB_NAME" -o json |
    jq -e --arg t "$TARGET_CLUSTER" '([.spec.clusters[].name]==[$t]) and
      ((.spec.suspension // {})|has("dispatching")|not)' >/dev/null; then break; fi
  test "$i" -lt 120 || { echo 'Gate timeout; inspect management logs; do not bypass'; exit 1; }
  sleep 5
done
kubectl --context karmada -n fluidcr-demo wait --for=jsonpath='{.status.phase}'=Running restorerequest/trainer-restore-001 --timeout=600s
kubectl --context aws -n fluidcr-demo get pods -l app=trainer -o json |
  jq -e '(.items|length)==2 and ([.items[].metadata.name]|sort)==["trainer-0","trainer-1"] and
    all(.items[]; .status.phase=="Running" and any(.status.conditions[]?; .type=="Ready" and .status=="True"))'
```

`dispatching:false`는 사용하지 않습니다. Gate가 멈췄을 때 수동으로 키를 지워 우회하지 마세요. Running/Ready 이후 CRI-O/CRIU restore 로그, 학습 step/model 상태와 실제 복원 증거를 확인합니다. 이후에만 application resume와 트래픽 전환을 결정합니다. 원본 데이터, PVMetadata/PVMigration 이력과 archive를 성급하게 삭제하지 마세요. Restore label이 남으면 다음 Pod 재생성도 오래된 checkpoint를 재사용할 수 있습니다. 검증 후 OnDelete 상태에서 restore label 정리와 정상 재시작 정책을 별도로 결정합니다.

Controller는 해제한 RestoreRequest UID를 `migration.dcnlab.com/dispatch-released-for`에 기록합니다. 같은 요청의 오래된 완료 상태로 이후의 수동 suspension을 다시 해제하지 않습니다. 다음 이전에는 새로운 checkpoint/restore operation을 만드세요.

기본 예제의 독립 작업 두 개는 두 Pod의 native restore 성공과 source fencing을 확인한 후 각각 resume합니다. 다음 명령은 DDP용 절차가 아닙니다.

```bash
for pod in trainer-0 trainer-1; do
  kubectl --context aws -n fluidcr-demo exec "$pod" -c trainer -- \
    env PYTHONPATH=/opt/fluidcr python -m fluidcr.ctrl resume --all
done
```

## 8. DDP 확장 조건

FluidCR `examples/statefulset/ddp-statefulset.yaml`은 rank별 StatefulSet과 공유 PVC를 사용합니다. 새 Pod별 restore mapping은 하나의 replicas=2 StatefulSet에도 서로 다른 archive와 node를 지정할 수 있게 합니다. 그러나 기본 샘플은 독립 작업이며 DDP 완료 검증을 대신하지 않습니다.

DDP에서는 WORLD_SIZE=2, ordinal에서 구한 RANK=0/1, LOCAL_RANK=0, MASTER_ADDR=trainer-0.trainer.fluidcr-demo.svc.cluster.local, MASTER_PORT=29500, FLUIDCR_DISTRIBUTED=1을 실제 프로세스에 전달해야 합니다. Launcher와 control API도 동일한 설정을 볼 수 있도록 이미지 진입점 및 FluidCR 주입 설정을 함께 검증하세요. 학습 프로세스 내부에서만 환경변수를 바꾸는 것으로 충분하다고 가정하지 않습니다.

두 PV가 동일한 NFS export root를 보게 하고 rank별 latest.pt 경로만 분리해야 shared manifest/generation/lock을 공유할 수 있습니다. 독립된 export를 쓰는 기본 샘플은 이 조건을 충족하지 않습니다. DRA를 쓰는 원본 예제와 달리 이 샘플은 GPU device plugin resource를 사용합니다. DRA 환경에서는 양쪽 resourceClaimTemplate과 GPU resource 선언을 함께 교체하고 scheduler를 우회하는 spec.nodeName을 사용하지 않습니다.

현재 payload overlay는 local lock 완료를 확인하지만 여러 Pod의 generation/rendezvous를 하나의 transaction으로 만들지는 않습니다. 겹치는 checkpoint round를 금지하고 두 rank의 동일 generation, checkpoint step 및 rendezvous 재결합을 실환경에서 검증하기 전에는 DDP 운영 이전을 진행하지 마세요. 모든 rank가 복원된 뒤 원본 FluidCR 절차에 따라 운영자가 한 번만 resume합니다. Source가 여전히 실행 중이거나 한 rank라도 준비되지 않았다면 resume하지 않습니다.

## 검증 범위

이 가이드는 parameterized 배포/운영 예시입니다. 서버측 schema dry-run, admission webhook 순서, NFS/PVC binding, patched CRI-O/CRIU, GPU 상태 및 두 Pod 동시 복원은 설치 환경에서 검증해야 합니다. 실패 시 source/target을 동시에 실행하지 말고 RB suspension과 fencing을 유지하며 원인을 확인합니다.
