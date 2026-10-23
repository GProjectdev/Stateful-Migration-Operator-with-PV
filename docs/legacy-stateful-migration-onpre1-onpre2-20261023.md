# 기존 Karmada v1.19에서 Old-Version Stateful Migration 시연하기

작성일: 2026-10-23. 대상: MGMT + Push member onpre1 → onpre2. **설치 절차와 보조 파일을 준비한 문서이며, 실제 MGMT에 배포하거나 복원을 검증한 결과는 아니다.**

## 1. 현재 환경에서 무엇을 바꾸는가

사용자가 제공한 Pod 목록에 따르면 karmada-controller-manager는 ReplicaSet 소유 Pod로 실행 중이며 이미지가 docker.io/karmada/karmada-controller-manager:v1.19.0이다. Deployment 이름은 아래 조회로 최종 확인한다. API 서버는 Kubernetes v1.36.2, scheduler/webhook/aggregated-apiserver는 Karmada v1.19.0이다.

**Karmada 전체를 재설치하지 않는다.** pvsync는 독립 PV operator가 아니라 커스텀 karmada-controller-manager에 등록된 컨트롤러다. 기존 controller-manager Deployment의 실행 이미지/일부 옵션을 바꾸고, Stateful Migration 컨트롤러들과 member 구성요소를 추가한다.

| 위치/API | 설치/변경 대상 | 역할 |
|---|---|---|
| MGMT Kubernetes, context kubernetes | 기존 karmada-controller-manager Deployment | 커스텀 pvSync/pvMigration 및 구버전 application failover |
| MGMT Kubernetes | MigrationBackup, MigrationRestore, suspension-controller | 백업 요청, 복원 요청, dispatch 재개 |
| MGMT Kubernetes | StatefulMigration CR | 어떤 워크로드를 어느 source에서 백업할지 지정 |
| Karmada API, context karmada-apiserver | 워크로드/PP, CheckpointBackup/Restore 및 해당 CRD | member로 전파할 선언과 Work/RB 관리 |
| onpre1 | CheckpointBackup CRD, checkpoint DaemonSet, registry Secret | kubelet checkpoint → OCI image → registry |
| onpre2 | CheckpointRestore CRD, 복원 mutating webhook | Pod 생성 때 checkpoint image로 치환 |
| 양쪽 실제 worker | checkpoint/restore 가능한 CRI-O + CRIU, NFS 접근 | 실제 프로세스 복원과 기존 파일 접근 |

기존 etcd, API 서버, scheduler, Karmada webhook, Cluster 등록을 그대로 사용한다. 본문의 복원 webhook은 기존 Karmada webhook과 별개다. karmada init / hack/local-up-karmada.sh / 구버전 전체 CRD 적용은 이 절차에 포함하지 않는다.

~~~mermaid
flowchart LR
  SM["MGMT: StatefulMigration"] --> MB["MigrationBackup"]
  MB --> CB["Karmada: CheckpointBackup + PP"]
  CB --> SRC["onpre1: checkpoint DaemonSet"]
  SRC --> REG["checkpoint OCI registry"]
  RB["Karmada: suspended StatefulSet RB"] --> PV["custom manager: pvMigration"]
  PV --> TPV["onpre2: 동일 NFS를 가리키는 PV"]
  RB --> MR["MGMT: MigrationRestore"]
  MR --> CR["onpre2: CheckpointRestore"]
  CR --> WH["target Pod mutation webhook"]
  REG --> RT["onpre2: CRI-O restore"]
  WH --> RT
  TPV --> RT
~~~

## 2. 사용할 소스와 적용 범위

| 소스 | 고정 commit | 용도 |
|---|---|---|
| Stateful-Migration-Operator-with-PV / Old-Version | cf5431275263cd76fc1e63036f8f17b74a5801ef | 백업/복원/suspension |
| Karmada_with_PVMigration / Old_and_have_karmada | 74c02e295fcba4fafbc4a79113c76988f87fed6c | pkg/controllers/pvsync 포함 manager |
| 이 문서의 legacy-demo-assets/webhook | 로컬 과거 Mutation 코드의 SHA-256을 README에 기록 | CheckpointRestore를 소비하는 복원 webhook |

중요한 차이: 공개 [Mutating main의 확인된 코드](https://github.com/GProjectdev/Mutating/blob/d50c6d1a7c43a95a8174e7d771d485b360bd0730/Mutation/webhook-server/main.go)는 change=image 라벨과 문자열 조합만 사용한다. **해당 저장소를 그대로 clone하는 것만으로 여기 필요한 CheckpointRestore 연동이 완성되지 않는다.** 제공한 보조 패키지는 로컬 과거 소스의 CheckpointRestore 연동 main.go를 그대로 복사하고 빌드/설치 파일을 정리한 것이다. 원본 소스 저장소는 수정하지 않았다.

구버전 PV 기능은 native spec.nfs.server/path를 대상으로 한다. **파일을 다른 디스크로 복사하는 시스템이 아니라, 같은 NFS 경로를 대상 클러스터에 다시 연결하는 방식**이다. CSI/local/hostPath PV를 같은 방식으로 처리한다고 가정하지 않는다.

현재 v1.19와 legacy manager의 혼합은 검증이 필요한 구성이다. legacy fork의 Go 의존성은 client-go v0.31.3/controller-runtime v0.19.6이며, 이 숫자가 현재 Karmada v1.19와의 호환성을 보증하지 않는다. 우선 동일 버전의 별도 실습 control plane에서 확인하는 것이 좋다. 기존 MGMT에서 진행할 경우 controller-manager 변경이 다른 namespace의 워크로드에도 영향을 준다는 점을 반영해 실습 시간을 확보한다.

## 3. MGMT 사전 준비와 API 구분

이후 명령은 Linux MGMT의 Bash 기준이다. 각 kubectl 명령에 --kubeconfig를 명시하므로 단축 함수를 등록할 필요가 없다. MGMT_KUBECONFIG는 MGMT Kubernetes, KARMADA_KUBECONFIG는 Karmada API, ONPRE1_KUBECONFIG와 ONPRE2_KUBECONFIG는 각각 member API를 가리킨다. 필요한 도구: kubectl, Git, Buildah, make, 소스 go.mod에 맞는 Go, Python3 + PyYAML, openssl. builder CPU 아키텍처와 worker 아키텍처를 일치시킨다. 아래 빌드는 amd64 예시다.

가이드와 보조 파일은 아래 명령으로 GitHub에서 직접 받는다. 이 checkout은 설치 자료를 받는 용도이며, 4절의 컨트롤러 소스 checkout과 분리한다. 컨트롤러는 원래의 고정 commit으로 빌드한다.

~~~bash
export GUIDE_REPO="$HOME/legacy-demo-guide-source"
git clone --branch Old-Version --single-branch \
  https://github.com/GProjectdev/Stateful-Migration-Operator-with-PV.git "$GUIDE_REPO"
export ASSETS="$GUIDE_REPO/docs/legacy-demo-assets"
test -f "$ASSETS/prepare-stateful-containerfiles.py"
~~~

이미 위 경로에 이 저장소를 clone했다면 clone 대신 git -C "$GUIDE_REPO" pull --ff-only origin Old-Version으로 업데이트한다. 앞서 ZIP을 풀어 사용하던 경우에도 이후 ASSETS는 위 Git checkout의 docs/legacy-demo-assets를 가리키게 한다. export는 파일을 내려받는 명령이 아니며, 새 터미널에서는 필요한 변수를 다시 설정한다.

 onpre1/onpre2 kubeconfig는 각 member의 실제 API에 연결되는 파일을 준비한다. Cluster 등록이 되어 있다는 사실만으로 kubectl context onpre1이 생기지는 않는다.

~~~bash
umask 077
mkdir -p /root/legacy-demo/{sources,rendered,evidence}
export DEMO_ROOT=/root/legacy-demo
export ASSETS="$HOME/legacy-demo-guide-source/docs/legacy-demo-assets"
export ACCOUNT=YOUR_DOCKERHUB_ACCOUNT
export TAG=demo-old-001
export ONPRE1_KUBECONFIG=/root/.kube/onpre1.config
export ONPRE2_KUBECONFIG=/root/.kube/onpre2.config

kubectl --context kubernetes config view --minify --flatten --raw > "$DEMO_ROOT/mgmt.config"
kubectl --context karmada-apiserver config view --minify --flatten --raw > "$DEMO_ROOT/karmada.config"
export MGMT_KUBECONFIG="$DEMO_ROOT/mgmt.config"
export KARMADA_KUBECONFIG="$DEMO_ROOT/karmada.config"

kubectl --kubeconfig="$MGMT_KUBECONFIG" -n karmada-system get deployment karmada-controller-manager
kubectl --kubeconfig="$KARMADA_KUBECONFIG" get clusters
kubectl --kubeconfig="$ONPRE1_KUBECONFIG" get nodes -o wide
kubectl --kubeconfig="$ONPRE2_KUBECONFIG" get nodes -o wide
kubectl --kubeconfig="$MGMT_KUBECONFIG" -n karmada-system get deployment karmada-controller-manager -o json \
  > "$DEMO_ROOT/evidence/manager-original.json"
kubectl --kubeconfig="$MGMT_KUBECONFIG" -n karmada-system get deployment karmada-controller-manager -o yaml \
  > "$DEMO_ROOT/evidence/manager-original.yaml"
kubectl --kubeconfig="$KARMADA_KUBECONFIG" get crd resourcebindings.work.karmada.io -o yaml \
  > "$DEMO_ROOT/evidence/rb-crd-original.yaml"
kubectl --kubeconfig="$KARMADA_KUBECONFIG" get resourcebindings -A -o yaml > "$DEMO_ROOT/evidence/rb-before.yaml"
~~~

파일의 server가 127.0.0.1이면 Pod 내부에서 사용할 수 없다. Karmada kubeconfig의 server는 MGMT controller Pod가 도달할 수 있어야 하며, 인증서 SAN도 일치해야 한다. kubeconfig는 인증정보이므로 영상에 내용을 출력하지 않는다. 위 파일은 API 설정/워크로드 조회 백업이며 etcd 전체 백업은 아니다.

worker에서 다음을 확인한다.

~~~bash
# 각 실제 worker에서
sudo crio --version
sudo crictl info
sudo criu --version
sudo criu check
~~~

Kubernetes 버전/Ready 상태만으로 checkpoint OCI restore가 가능하다고 판단할 수 없다. source의 kubelet checkpoint API, target의 checkpoint image restore를 실제 runtime에서 먼저 확인한다. containerd만 설치된 환경이라면 이 legacy 경로를 그대로 완주할 수 있다고 가정하지 않는다. 양쪽 CPU/kernel/runtime 호환성과 NFS 접근도 필요하다. checkpoint 메모리에는 프로세스 비밀값이 포함될 수 있으므로 이미지 저장소 접근을 제한한다.

## 4. 소스 고정과 controller 이미지 준비

~~~bash
cd "$DEMO_ROOT/sources"
git clone --branch Old-Version --single-branch \
  https://github.com/GProjectdev/Stateful-Migration-Operator-with-PV.git stateful-old
git -C stateful-old checkout --detach cf5431275263cd76fc1e63036f8f17b74a5801ef
git clone --branch Old_and_have_karmada --single-branch \
  https://github.com/GProjectdev/Karmada_with_PVMigration.git karmada-old
git -C karmada-old checkout --detach 74c02e295fcba4fafbc4a79113c76988f87fed6c
export STATEFUL="$DEMO_ROOT/sources/stateful-old"
export PVFORK="$DEMO_ROOT/sources/karmada-old"
# MGMT의 같은 사용자 세션에서 로그인·빌드·push 수행
buildah --version
export REGISTRY_AUTH_FILE="$DEMO_ROOT/registry-auth.json"
buildah login --authfile "$REGISTRY_AUTH_FILE" --username "$ACCOUNT" docker.io
~~~

이미지 준비는 MGMT에서 Buildah로 수행한다. 현재 root 세션을 유지하고 buildah와 sudo buildah를 섞지 않는다. 두 방식은 이미지 저장소와 인증 경로가 다를 수 있다. Buildah가 없다면 Ubuntu MGMT에서 먼저 sudo apt-get update와 sudo apt-get install -y buildah를 실행한다. worker의 커스텀 CRI-O를 변경할 필요는 없다.

Docker Hub는 이미지를 저장할 registry이므로 계속 사용한다. 빌드 도구만 Buildah로 바뀐다. 완전한 이미지 이름 docker.io/ACCOUNT/...와 명시적인 push 목적지 docker://docker.io/ACCOUNT/...를 사용한다. 여기서 docker://는 registry 전송 방식이며 Docker 데몬 호출이 아니다. 로그인 파일은 비밀정보이므로 영상에 출력하지 않는다. MGMT의 Buildah 로그인은 member의 checkpoint push Secret이나 imagePullSecret을 대신하지 않는다.

원본 build-and-push.sh는 Docker 명령을 직접 호출하므로 실행하지 않는다. 함께 제공한 prepare-stateful-containerfiles.py는 원본 스크립트를 실행하지 않고 두 Dockerfile 템플릿과 모드별 flags를 읽어 Containerfile을 생성한다. checkpoint 이미지의 Buildah 설치 및 세 controller의 ENTRYPOINT 설정은 유지한다. deploy.sh의 controller 이미지 계정만 본인 계정으로 변경한다.

~~~bash
cd "$STATEFUL"
python3 - <<'PY'
import os
from pathlib import Path
account = os.environ["ACCOUNT"]
assert account and account != "YOUR_DOCKERHUB_ACCOUNT"
p = Path("deploy.sh")
content = p.read_text()
old = 'DOCKERHUB_USERNAME="lehuannhatrang"'
new = 'DOCKERHUB_USERNAME="' + account + '"'
if old in content:
    assert content.count(old) == 1
    p.write_text(content.replace(old, new))
elif new not in content:
    raise SystemExit("deploy.sh의 DOCKERHUB_USERNAME을 확인하세요.")
PY
(
  set -e
  make manifests generate
  make test
  python3 "$ASSETS/prepare-stateful-containerfiles.py" "$STATEFUL" "$DEMO_ROOT/rendered"

  for mode in checkpoint migration restore; do
    case "$mode" in
      checkpoint) image_tag="checkpointBackup_$TAG" ;;
      migration) image_tag="migrationBackup_$TAG" ;;
      restore) image_tag="migrationRestore_$TAG" ;;
    esac
    image="docker.io/$ACCOUNT/stateful-migration-operator:$image_tag"
    buildah bud --authfile "$REGISTRY_AUTH_FILE" --format docker \
      --build-arg TARGETOS=linux --build-arg TARGETARCH=amd64 \
      -f "$DEMO_ROOT/rendered/Containerfile.$mode" -t "$image" "$STATEFUL"
    (
      set -e
      ctr="$(buildah from "$image")"
      trap 'buildah rm "$ctr" >/dev/null' EXIT
      buildah run "$ctr" -- /manager --help >/dev/null
    )
    buildah push --authfile "$REGISTRY_AUTH_FILE" "$image" "docker://$image"
  done
)
~~~

위 subshell이 오류 없이 끝난 뒤 다음 단계로 진행한다. 테스트·이미지 빌드·실행 점검이 실패한 경우 해당 이미지의 push를 진행하지 않는다. make test가 요구하는 envtest 의존성도 준비해야 한다.

생성되는 이미지는 다음과 같다. ACCOUNT와 TAG는 실제 변수 값으로 치환된다.

- docker.io/ACCOUNT/stateful-migration-operator:checkpointBackup_TAG
- docker.io/ACCOUNT/stateful-migration-operator:migrationBackup_TAG
- docker.io/ACCOUNT/stateful-migration-operator:migrationRestore_TAG

기존 manifest의 ACCOUNT/... 표기는 같은 Docker Hub repository를 가리키므로 renderer와 deploy.sh의 이미지 태그는 그대로 호환된다. 루트 Dockerfile만 사용하면 checkpoint controller에 필요한 Buildah 구성이 빠질 수 있으므로 위에서 생성한 Containerfile.checkpoint를 사용한다.

suspension의 dispatching=false 쓰기는 현재 API에서 거부될 수 있다. 현재 Karmada API의 재개 표현인 필드 제거로 설치용 checkout을 수정한다. 기능 흐름은 동일하다.

~~~bash
cd "$STATEFUL"
python3 - <<'PY'
from pathlib import Path
p = Path("suspension/internal/rb_unstructed.go")
text = p.read_text()
for tail in ('"spec", "suspension", "suspension", "dispatching"',
             '"spec", "suspension", "dispatching"'):
    old = '_ = unstructured.SetNestedField(after.Object, false, ' + tail + ')'
    new = 'unstructured.RemoveNestedField(after.Object, ' + tail + ')'
    assert text.count(old) == 1
    text = text.replace(old, new)
p.write_text(text)
PY
# Containerfile은 원본 suspension/Dockerfile의 builder registry만 명시한 파일
buildah bud --authfile "$REGISTRY_AUTH_FILE" --format docker \
  -f "$DEMO_ROOT/rendered/Containerfile.suspension" \
  -t "docker.io/$ACCOUNT/karmada-suspension:$TAG" "$STATEFUL" &&
buildah push --authfile "$REGISTRY_AUTH_FILE" \
  "docker.io/$ACCOUNT/karmada-suspension:$TAG" \
  "docker://docker.io/$ACCOUNT/karmada-suspension:$TAG"

cd "$PVFORK"
export LEGACY_IMAGE="docker.io/$ACCOUNT/karmada-controller-manager:$TAG"
(
  set -e
  # image-* target은 Docker를 호출하므로 바이너리 target만 사용
  make karmada-controller-manager GOOS=linux GOARCH=amd64
  test -x "$PVFORK/_output/bin/linux/amd64/karmada-controller-manager"
  sed 's|^FROM alpine:|FROM docker.io/library/alpine:|' \
    "$PVFORK/cluster/images/Dockerfile" > "$DEMO_ROOT/rendered/Containerfile.karmada"
  buildah bud --authfile "$REGISTRY_AUTH_FILE" --format docker \
    --build-arg BINARY=karmada-controller-manager \
    -f "$DEMO_ROOT/rendered/Containerfile.karmada" -t "$LEGACY_IMAGE" \
    "$PVFORK/_output/bin/linux/amd64"
  (
    set -e
    ctr="$(buildah from "$LEGACY_IMAGE")"
    trap 'buildah rm "$ctr" >/dev/null' EXIT
    buildah run "$ctr" -- /bin/karmada-controller-manager --help \
      > "$DEMO_ROOT/evidence/legacy-manager-help.txt"
  )
  buildah push --authfile "$REGISTRY_AUTH_FILE" "$LEGACY_IMAGE" "docker://$LEGACY_IMAGE"
)
~~~

## 5. v1.19 호환성 점검 후 기존 Deployment 교체

현재 command/args/feature-gates를 legacy-manager-help.txt와 대조한다. 지원하지 않는 옵션이 나오면 먼저 해당 차이를 해결한다. 새 manager의 image에는 ENTRYPOINT가 없으므로 기존 실행 command 보존이 중요하다. helper는 기존 volume, Secret, kubeconfig, leader election 설정, replica 수와 다른 container를 보존한다.

~~~bash
python3 - <<'PY'
import json, os
from pathlib import Path
d=json.loads((Path(os.environ["DEMO_ROOT"])/"evidence/manager-original.json").read_text())
for c in d["spec"]["template"]["spec"]["containers"]:
    print(c["name"], c.get("command", []), c.get("args", []))
PY
kubectl --kubeconfig="$KARMADA_KUBECONFIG" explain resourcebinding.spec.suspension --api-version=work.karmada.io/v1alpha2
python3 "$ASSETS/prepare-manager.py" \
  "$DEMO_ROOT/evidence/manager-original.json" "$DEMO_ROOT/rendered/manager-active.json" \
  --image "$LEGACY_IMAGE"
python3 "$ASSETS/prepare-manager.py" \
  "$DEMO_ROOT/evidence/manager-original.json" "$DEMO_ROOT/rendered/manager-pv-paused.json" \
  --image "$LEGACY_IMAGE" --pause-pv-migration
kubectl --kubeconfig="$MGMT_KUBECONFIG" -n karmada-system patch deployment karmada-controller-manager --type=json \
  --patch-file "$DEMO_ROOT/rendered/manager-active.json.patch.json" --dry-run=server -o yaml
~~~

helper는 기본 컨트롤러 목록에 pvSync/pvMigration을 활성화하고 pvCleanup을 비활성화한다. 기존 opt-in controller도 보존한다. 원본 cleanup은 target PV Work 및 source PVC/PV를 정리하므로 첫 실습에서는 사용하지 않는다. 서로 다른 manager가 동시에 전체 리소스를 처리하지 않도록 rollout 전략은 Recreate로 만든다. 교체 시 일시적으로 reconciliation이 멈춘다.

**중단 기준:** 기존 flag를 수용하지 못함, v1.19 API 필드 손실, Work/RB update 거절, 기존 workload reconciliation 이상. dry-run은 Deployment 문법만 검사하며 manager의 v1.19 동작을 보증하지 않는다. 호환성 검증이 끝난 실습 환경에서 적용한다.

~~~bash
kubectl --kubeconfig="$MGMT_KUBECONFIG" -n karmada-system patch deployment karmada-controller-manager --type=json \
  --patch-file "$DEMO_ROOT/rendered/manager-active.json.patch.json"
kubectl --kubeconfig="$MGMT_KUBECONFIG" -n karmada-system rollout status deployment/karmada-controller-manager --timeout=180s
kubectl --kubeconfig="$MGMT_KUBECONFIG" -n karmada-system logs deployment/karmada-controller-manager --tail=150
kubectl --kubeconfig="$KARMADA_KUBECONFIG" get clusters
kubectl --kubeconfig="$KARMADA_KUBECONFIG" get resourcebindings -A
~~~

unknown flag, forbidden, schema 오류가 있으면 다음 설치로 넘어가지 않고 12절의 원본 template 복구를 사용한다. old manager가 모르는 v1.19 필드가 typed Update 과정에서 소실될 수 있으므로 기존 RB/Work도 비교한다. 이 문제를 해결하기 위해 전체 CRD를 legacy로 덮어쓰지는 않는다.

## 6. Stateful 컨트롤러 설치

아래 설치는 demo 전용 namespace/CRD 이름이 다른 버전 설치와 충돌하지 않는 환경을 전제로 한다. 같은 migration.dcnlab.com CRD의 새 시스템이 이미 설치돼 있다면 이 legacy schema를 덮어쓰지 말고 별도 control plane을 사용한다.

~~~bash
# MGMT Kubernetes
kubectl --kubeconfig="$MGMT_KUBECONFIG" create namespace stateful-migration --dry-run=client -o yaml | kubectl --kubeconfig="$MGMT_KUBECONFIG" apply -f -
kubectl --kubeconfig="$MGMT_KUBECONFIG" create namespace migration-demo --dry-run=client -o yaml | kubectl --kubeconfig="$MGMT_KUBECONFIG" apply -f -

# Karmada API
kubectl --kubeconfig="$KARMADA_KUBECONFIG" create namespace stateful-migration --dry-run=client -o yaml | kubectl --kubeconfig="$KARMADA_KUBECONFIG" apply -f -
kubectl --kubeconfig="$KARMADA_KUBECONFIG" create namespace migration-demo --dry-run=client -o yaml | kubectl --kubeconfig="$KARMADA_KUBECONFIG" apply -f -

# onpre1
kubectl --kubeconfig="$ONPRE1_KUBECONFIG" create namespace stateful-migration --dry-run=client -o yaml | kubectl --kubeconfig="$ONPRE1_KUBECONFIG" apply -f -
kubectl --kubeconfig="$ONPRE1_KUBECONFIG" create namespace migration-demo --dry-run=client -o yaml | kubectl --kubeconfig="$ONPRE1_KUBECONFIG" apply -f -

# onpre2
kubectl --kubeconfig="$ONPRE2_KUBECONFIG" create namespace stateful-migration --dry-run=client -o yaml | kubectl --kubeconfig="$ONPRE2_KUBECONFIG" apply -f -
kubectl --kubeconfig="$ONPRE2_KUBECONFIG" create namespace migration-demo --dry-run=client -o yaml | kubectl --kubeconfig="$ONPRE2_KUBECONFIG" apply -f -
kubectl --kubeconfig="$MGMT_KUBECONFIG" apply -f "$STATEFUL/config/crd/bases/"
# Karmada API
kubectl --kubeconfig="$KARMADA_KUBECONFIG" apply -f "$STATEFUL/config/crd/bases/migration.dcnlab.com_checkpointbackups.yaml"
kubectl --kubeconfig="$KARMADA_KUBECONFIG" apply -f "$STATEFUL/config/crd/bases/migration.dcnlab.com_checkpointrestores.yaml"
kubectl --kubeconfig="$KARMADA_KUBECONFIG" wait --for=condition=Established crd/checkpointbackups.migration.dcnlab.com \
  crd/checkpointrestores.migration.dcnlab.com --timeout=60s

# onpre1
kubectl --kubeconfig="$ONPRE1_KUBECONFIG" apply -f "$STATEFUL/config/crd/bases/migration.dcnlab.com_checkpointbackups.yaml"
kubectl --kubeconfig="$ONPRE1_KUBECONFIG" apply -f "$STATEFUL/config/crd/bases/migration.dcnlab.com_checkpointrestores.yaml"
kubectl --kubeconfig="$ONPRE1_KUBECONFIG" wait --for=condition=Established crd/checkpointbackups.migration.dcnlab.com \
  crd/checkpointrestores.migration.dcnlab.com --timeout=60s

# onpre2
kubectl --kubeconfig="$ONPRE2_KUBECONFIG" apply -f "$STATEFUL/config/crd/bases/migration.dcnlab.com_checkpointbackups.yaml"
kubectl --kubeconfig="$ONPRE2_KUBECONFIG" apply -f "$STATEFUL/config/crd/bases/migration.dcnlab.com_checkpointrestores.yaml"
kubectl --kubeconfig="$ONPRE2_KUBECONFIG" wait --for=condition=Established crd/checkpointbackups.migration.dcnlab.com \
  crd/checkpointrestores.migration.dcnlab.com --timeout=60s
kubectl --kubeconfig="$MGMT_KUBECONFIG" -n stateful-migration create secret generic karmada-kubeconfig \
  --from-file=kubeconfig="$KARMADA_KUBECONFIG" --dry-run=client -o yaml | kubectl --kubeconfig="$MGMT_KUBECONFIG" apply -f -
python3 "$ASSETS/render-stateful.py" "$STATEFUL" "$DEMO_ROOT/rendered" \
  --account "$ACCOUNT" --tag "$TAG"
kubectl --kubeconfig="$MGMT_KUBECONFIG" apply -f "$DEMO_ROOT/rendered/backup.yaml"
kubectl --kubeconfig="$ONPRE1_KUBECONFIG" apply -f "$DEMO_ROOT/rendered/checkpoint.yaml"
kubectl --kubeconfig="$MGMT_KUBECONFIG" apply -f "$DEMO_ROOT/rendered/suspension.yaml"

cd "$STATEFUL"
bash deploy.sh --restore --mgmt-config "$MGMT_KUBECONFIG" --version "$TAG"
kubectl --kubeconfig="$MGMT_KUBECONFIG" -n stateful-migration scale deployment migration-restore-controller --replicas=0
kubectl --kubeconfig="$MGMT_KUBECONFIG" -n stateful-migration rollout status deployment/migration-backup-controller --timeout=180s
kubectl --kubeconfig="$ONPRE1_KUBECONFIG" -n stateful-migration rollout status daemonset/checkpoint-backup-controller --timeout=180s
~~~

suspension은 renderer에서 replicas=0으로 설정한다. 첫 수동 실습에서는 복원 요청과 dispatch 재개 순서를 확인한 뒤 실행한다.

즉, **suspension과 restore 모두 처음에는 중지 상태**로 둔다. 컨트롤러의 자동 실행을 멈추고 순서를 확인하는 실습이다.

MGMT의 backup manifest는 /manager command로 이미지 ENTRYPOINT의 모드 지정을 덮어쓰므로, renderer에서 backup-only 인수를 명시했다. 이를 생략하면 기본값인 restore도 실행되어 별도의 restore Deployment와 중복된다.

이 실습의 mounted kubeconfig는 현재 Karmada 관리자 권한을 사용한다. host의 ClusterRole만 추가해도 remote Karmada 권한이 생기는 것은 아니다. 실제 운영용 계정으로 바꿀 때는 Karmada의 CheckpointBackup/Restore·PP·Work·RB·clusters/proxy 접근권한을 별도로 검증해야 한다.

source member의 push 인증 Secret을 생성한다. 비밀번호를 커맨드라인이나 영상에 표시하지 않도록 임시 파일을 사용한다.

~~~bash
cred_dir="$(mktemp -d)"
read -r -p 'Registry username: ' REG_USER
read -r -s -p 'Registry password/token: ' REG_PASS
printf '\n'
printf '%s' "$REG_USER" > "$cred_dir/username"
printf '%s' "$REG_PASS" > "$cred_dir/password"
printf '%s' docker.io > "$cred_dir/registry"
kubectl --kubeconfig="$ONPRE1_KUBECONFIG" -n stateful-migration create secret generic registry-credentials \
  --from-file=username="$cred_dir/username" --from-file=password="$cred_dir/password" \
  --from-file=registry="$cred_dir/registry" --dry-run=client -o yaml | kubectl --kubeconfig="$ONPRE1_KUBECONFIG" apply -f -
unset REG_PASS
rm -rf -- "$cred_dir"
~~~

이는 Opaque push용 Secret이다. checkpoint repository가 private이면 별도 kubernetes.io/dockerconfigjson imagePullSecret을 **onpre2의 migration-demo namespace**에 만들어야 한다. demo StatefulSet의 imagePullSecrets 또는 해당 namespace ServiceAccount에 연결한다. 이 인증은 source push Secret과 별개다. source의 app 이미지가 private이면 source에도 pull 인증을 제공한다.

## 7. 대상 복원 webhook 설치

~~~bash
cd "$ASSETS/webhook"
export IMAGE="docker.io/$ACCOUNT/legacy-checkpoint-restore-webhook:$TAG"
buildah bud --authfile "$REGISTRY_AUTH_FILE" --format docker -t "$IMAGE" . &&
buildah push --authfile "$REGISTRY_AUTH_FILE" "$IMAGE" "docker://$IMAGE"
export TARGET_KUBECONFIG="$ONPRE2_KUBECONFIG"
export WORKLOAD_NAMESPACE=migration-demo
bash install.sh
kubectl --kubeconfig="$ONPRE2_KUBECONFIG" -n stateful-migration rollout status deployment/checkpoint-restore-webhook --timeout=180s
kubectl --kubeconfig="$ONPRE2_KUBECONFIG" -n stateful-migration get service checkpoint-restore-webhook-svc
~~~

보조 installer가 새 TLS 인증서/SAN, RBAC, Service, Deployment, 유효한 Pod CREATE rules를 만든다. migration-demo에만 legacy-restore=enabled 라벨을 붙여 적용한다. 소스 저장소의 인증서/개인키는 재사용하지 않는다.

이 webhook의 내부 동작은 구버전 그대로다. CR 조회 오류, CR 부재, 이미지 부재 때 정상 이미지로 통과할 수 있다. failurePolicy: Fail은 이 application-level fail-open을 막지 못한다. 따라서 **대상 CheckpointRestore가 실제 도착했는지 확인한 다음 Pod 생성/dispatch를 허용**한다.

## 8. 단일 StatefulSet + NFS 실습 워크로드

### 8.1 member1 NFS 서버와 양쪽 Provisioner 준비

참고: [Karmada_velero NFS 구성](https://github.com/GProjectdev/Karmada_velero#nfs-구성), [서버 스크립트](https://github.com/GProjectdev/Karmada_velero/blob/main/setup-nfs-server.sh), [Provisioner Helm values](https://github.com/kubernetes-sigs/nfs-subdir-external-provisioner/blob/master/charts/nfs-subdir-external-provisioner/values.yaml).

서버는 member1(192.168.30.101), export는 /mnt/nfs이다. 원본 서버 스크립트의 실제 허용 대역은 192.168.40.0/24이므로 그대로 실행하지 않는다. 아래는 현재 두 worker IP만 허용한다. root_squash를 유지하고 실습 디렉터리를 nobody 소유로 만든다. member1 서버 자체가 중단되면 onpre2도 이 스토리지를 사용할 수 없다. 이 구성은 member1이 살아 있는 상태의 워크로드 이동 시연용이며 스토리지 HA가 아니다.

**member1의 호스트 터미널에서 실행:** 기존 /mnt/nfs export가 있다면 먼저 `sudo exportfs -v`로 확인하고 중복 규칙을 만들지 않는다. 아래는 신규 설정 기준이다. 기존 데이터에 재귀 chown/chmod를 하지 않는다.

~~~bash
sudo apt-get update
sudo apt-get install -y nfs-kernel-server nfs-common
sudo mkdir -p /mnt/nfs/legacy-demo
sudo chown nobody:nogroup /mnt/nfs /mnt/nfs/legacy-demo
sudo chmod 0777 /mnt/nfs /mnt/nfs/legacy-demo
sudo mkdir -p /etc/exports.d
sudo tee /etc/exports.d/legacy-demo.exports >/dev/null <<'EOF'
/mnt/nfs 192.168.30.101(rw,sync,no_subtree_check,root_squash) 192.168.30.102(rw,sync,no_subtree_check,root_squash)
EOF
sudo systemctl enable --now nfs-kernel-server
sudo exportfs -ra
sudo exportfs -v
~~~

0777은 UID가 다른 실습 앱과 provisioner의 쓰기를 허용하기 위한 실습용 설정이다. export 접근은 위 두 IP로 제한한다. 서버 방화벽이 켜져 있다면 두 worker에서 TCP 2049에 접근 가능하게 허용한다. 아래는 NFSv4.1을 사용한다.

**member2의 호스트 터미널에서 실행:** 이후 worker를 추가하면 그 worker에도 nfs-common 및 export 접근 권한이 필요하다.

~~~bash
sudo apt-get update
sudo apt-get install -y nfs-common
~~~

**member1과 member2 각각의 호스트에서 실행:** 두 서버 모두 같은 marker 파일을 읽고 쓸 수 있어야 한다. 실패하면 Provisioner 설치보다 NFS 연결부터 해결한다.

~~~bash
sudo mkdir -p /mnt/legacy-nfs-check
sudo mount -t nfs -o vers=4.1 192.168.30.101:/mnt/nfs /mnt/legacy-nfs-check
printf 'NFS client: %s\n' "$(hostname)" | sudo tee "/mnt/legacy-nfs-check/legacy-demo/mount-check-$(hostname).txt"
ls -l /mnt/legacy-nfs-check/legacy-demo
sudo umount /mnt/legacy-nfs-check
~~~

**이후 MGMT의 기존 root 터미널에서 실행:** Helm이 없다면 참고 저장소와 동일한 Helm 3 설치 경로를 사용한다.

~~~bash
if ! command -v helm >/dev/null 2>&1; then
  curl -fsSL -o "$DEMO_ROOT/rendered/get-helm-3.sh" \
    https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3
  bash "$DEMO_ROOT/rendered/get-helm-3.sh"
fi
helm version --short
export NFS_SERVER=192.168.30.101
export NFS_PATH=/mnt/nfs/legacy-demo
helm repo add nfs-subdir-external-provisioner \
  https://kubernetes-sigs.github.io/nfs-subdir-external-provisioner/
helm repo update
cat > "$DEMO_ROOT/rendered/nfs-values.yaml" <<'EOF'
nfs:
  server: 192.168.30.101
  path: /mnt/nfs
  mountOptions:
    - vers=4.1
storageClass:
  name: legacy-nfs-client
  defaultClass: false
  reclaimPolicy: Retain
  onDelete: retain
  archiveOnDelete: true
  accessModes: ReadWriteMany
tolerations:
  - key: node-role.kubernetes.io/control-plane
    operator: Exists
    effect: NoSchedule
  - key: node-role.kubernetes.io/master
    operator: Exists
    effect: NoSchedule
EOF
helm upgrade --install legacy-nfs \
  nfs-subdir-external-provisioner/nfs-subdir-external-provisioner \
  --version 4.0.18 --kubeconfig "$ONPRE1_KUBECONFIG" \
  --namespace nfs-provisioner --create-namespace \
  -f "$DEMO_ROOT/rendered/nfs-values.yaml" \
  --set storageClass.provisionerName=legacy.demo/onpre1-nfs \
  --wait --timeout 5m
helm upgrade --install legacy-nfs \
  nfs-subdir-external-provisioner/nfs-subdir-external-provisioner \
  --version 4.0.18 --kubeconfig "$ONPRE2_KUBECONFIG" \
  --namespace nfs-provisioner --create-namespace \
  -f "$DEMO_ROOT/rendered/nfs-values.yaml" \
  --set storageClass.provisionerName=legacy.demo/onpre2-nfs \
  --wait --timeout 5m
kubectl --kubeconfig="$ONPRE1_KUBECONFIG" -n nfs-provisioner get deploy,pods
kubectl --kubeconfig="$ONPRE2_KUBECONFIG" -n nfs-provisioner get deploy,pods
kubectl --kubeconfig="$ONPRE1_KUBECONFIG" get storageclass legacy-nfs-client
kubectl --kubeconfig="$ONPRE2_KUBECONFIG" get storageclass legacy-nfs-client
~~~

각 Helm 설치가 실패하면 다음 단계로 넘어가지 않는다. Pending이면 Pod Events에서 taint/마운트 오류를, ImagePullBackOff이면 registry 접근을 확인한다. 두 클러스터가 같은 export를 사용하지만 provisionerName은 구분한다. Karmada API나 MGMT Kubernetes에 Provisioner를 설치하는 것이 아니다.

**Provisioner 동적 할당 확인 (MGMT):** 새 이름의 테스트 PVC만 사용하며 실습 StatefulSet PVC와 분리한다.

~~~bash
cat > "$DEMO_ROOT/rendered/nfs-smoke.yaml" <<'EOF'
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: legacy-nfs-smoke
  namespace: nfs-provisioner
spec:
  accessModes: [ReadWriteMany]
  storageClassName: legacy-nfs-client
  resources:
    requests:
      storage: 32Mi
EOF
kubectl --kubeconfig="$ONPRE1_KUBECONFIG" apply -f "$DEMO_ROOT/rendered/nfs-smoke.yaml"
kubectl --kubeconfig="$ONPRE2_KUBECONFIG" apply -f "$DEMO_ROOT/rendered/nfs-smoke.yaml"
kubectl --kubeconfig="$ONPRE1_KUBECONFIG" -n nfs-provisioner wait \
  --for=jsonpath='{.status.phase}'=Bound pvc/legacy-nfs-smoke --timeout=120s
kubectl --kubeconfig="$ONPRE2_KUBECONFIG" -n nfs-provisioner wait \
  --for=jsonpath='{.status.phase}'=Bound pvc/legacy-nfs-smoke --timeout=120s
kubectl --kubeconfig="$ONPRE1_KUBECONFIG" -n nfs-provisioner get pvc legacy-nfs-smoke -o wide
kubectl --kubeconfig="$ONPRE2_KUBECONFIG" -n nfs-provisioner get pvc legacy-nfs-smoke -o wide
~~~

테스트 PVC는 유지해도 된다. Retain 정책이므로 삭제해도 PV와 NFS 디렉터리가 자동 정리되지 않는다. PVC Bound는 동적 할당 확인이며 앱 Pod의 실제 I/O는 아래 demo 단계에서 검증한다.

**중요:** Provisioner는 설치하고 별도 PVC로 검증하지만, 아래 migration-demo StatefulSet은 기존 방식대로 정적 PV + `storageClassName: ""`를 사용한다. 이를 `legacy-nfs-client`로 바꾸지 않는다. 대상에서 동적 할당이 먼저 일어나 새 디렉터리를 연결하는 것을 피하고, 구버전 PV migration이 동일한 `/mnt/nfs/legacy-demo` 경로를 연결하도록 하기 위함이다. 동적 할당 PVC 자체의 migration 시연은 별도로 경합과 PV annotation 처리를 검증해야 한다.

### 8.2 영상 StatefulSet과 volumeClaimTemplates 준비


### 영상 데모의 이미지와 성공 기준

예시를 비교했다. [공식 nginx 이미지](https://hub.docker.com/_/nginx)는 MP4 파일 제공에 적합하지만 HTML video의 재생 시각이 브라우저 상태여서 서버 메모리 복원 증거로 부족하다. 이번에는 [공식 Python 이미지](https://hub.docker.com/_/python)의 `python:3.11-slim-bookworm`에 제공된 video-demo/app.py를 넣어 `docker.io/ACCOUNT/legacy-video-demo:TAG`를 빌드한다. 이 이미지는 아직 사전 게시된 제품 이미지가 아니라 이 가이드에서 직접 만드는 시연용 이미지다.

영상 예시는 [Big Buck Bunny](https://peach.blender.org/download/)이며 [CC BY 3.0, Blender Foundation 저작자 표시](https://peach.blender.org/about/)를 유지한다. ffmpeg로 원본의 처음 120초를 640px/5fps JPEG로 준비하며 무음이다. JPEG는 브라우저 표시 방식이고 프레임 위치는 서버 Python 프로세스의 RAM에만 저장된다. 브라우저 localStorage나 PV에서 진행 위치를 복구하지 않는다. GPU/실시간 디코더/오디오 복원 시연은 아니다.

- PV: 원본 MP4, 미리 추출한 영상 프레임, marker.txt를 저장한다.
- RAM: boot_id, 현재 frame/seconds, paused 상태를 저장한다.
- 동일 boot_id와 정지 프레임으로 복원 후 frame이 증가해야 메모리 복원의 증거가 된다. 같은 영상 파일이나 브라우저에 남은 화면만으로 성공으로 판정하지 않는다.
- 일반 신규 프로세스는 boot_id가 바뀌고 frame=0부터 시작한다.
- 체크포인트는 장애 직전 모든 상태를 자동 보존하지 않는다. 재생 중 갑작스러운 장애라면 마지막 백업 지점으로 되돌아간다. 정확히 정지한 장면의 재개를 촬영할 때는 pause -> checkpoint 완료 -> 서비스 중단 -> restore -> resume 순서를 명시한다. 이는 수동 복구 시연이며 무중단 서비스나 자동 장애 복구를 입증하지 않는다.

**member1에서 영상 파일 준비:** 8.1의 /mnt/nfs/legacy-demo 생성 후, 아직 demo StatefulSet 배포 전 실행한다. 이미 다른 영상 프레임이 있다면 섞지 말고 별도 디렉터리를 사용한다. 직접 촬영한 MP4를 아래 input.mp4로 복사해도 된다.

~~~bash
sudo apt-get update
sudo apt-get install -y ffmpeg curl
sudo mkdir -p /mnt/nfs/legacy-demo/video/frames
sudo curl -fL --retry 3 \
  https://download.blender.org/peach/bigbuckbunny_movies/BigBuckBunny_320x180.mp4 \
  -o /mnt/nfs/legacy-demo/video/input.mp4
sudo ffmpeg -nostdin -n -i /mnt/nfs/legacy-demo/video/input.mp4 \
  -t 120 -vf 'fps=5,scale=640:-2' -q:v 4 \
  /mnt/nfs/legacy-demo/video/frames/%06d.jpg
printf '%s\n' 'Big Buck Bunny (c) 2008 Blender Foundation / www.bigbuckbunny.org | CC BY 3.0 | resized, silent 5fps excerpt' \
  | sudo tee /mnt/nfs/legacy-demo/video/ATTRIBUTION.txt
sudo chmod -R a+rX /mnt/nfs/legacy-demo/video
find /mnt/nfs/legacy-demo/video/frames -name '*.jpg' | wc -l
du -sh /mnt/nfs/legacy-demo/video
~~~

영상 다운로드가 실패하면 공식 다운로드 페이지에서 파일을 받아 같은 input.mp4 경로로 복사한 뒤 ffmpeg부터 진행한다. 프레임은 600개를 예상하며 원본 길이가 120초 미만이면 더 적다. NFS의 PVC 요청 용량은 실제 사용량을 강제하는 quota가 아니므로 디스크 여유를 확인한다.

**MGMT에서 이미지 빌드:** 아래 블록과 이후 StatefulSet YAML을 사용한다. 다른 데모를 이미 demo 이름으로 배포했다면 무작정 덮어쓰지 않는다. 먼저 기존 migration을 마치고 CR 및 checkpoint 이미지의 대상이 이번 영상 Pod인지 구분한다. volumeClaimTemplates는 일반적인 업데이트로 변경할 수 없으므로 최초 배포 전에 manifest를 확정한다.


~~~bash
buildah bud --authfile "$REGISTRY_AUTH_FILE" --format docker \
  -t "docker.io/$ACCOUNT/legacy-video-demo:$TAG" "$ASSETS/video-demo" &&
buildah push --authfile "$REGISTRY_AUTH_FILE" \
  "docker.io/$ACCOUNT/legacy-video-demo:$TAG" \
  "docker://docker.io/$ACCOUNT/legacy-video-demo:$TAG"
export NFS_SERVER=192.168.30.101
export NFS_PATH=/mnt/nfs/legacy-demo
~~~

PV 자체는 클러스터 범위 리소스로 별도 생성하고, StatefulSet에는 volumeClaimTemplates로 PVC 템플릿을 넣는다. data 템플릿이 data-demo-0 PVC를 만들고, legacy-demo-source PV의 claimRef와 연결된다. 별도의 앱 PVC manifest를 만들지 않는다.

NFS export는 양쪽 worker에서 마운트 가능해야 한다. 실습 전용 디렉터리, 동일 UID 권한을 사용한다. storageClassName은 비워 두어 대상의 default dynamic provisioner가 새 빈 볼륨을 먼저 만들지 않게 한다.

~~~bash
cat > "$DEMO_ROOT/rendered/source-pv.yaml" <<EOF
apiVersion: v1
kind: PersistentVolume
metadata:
  name: legacy-demo-source
spec:
  mountOptions: [vers=4.1]
  capacity:
    storage: 1Gi
  accessModes: [ReadWriteMany]
  persistentVolumeReclaimPolicy: Retain
  storageClassName: ""
  claimRef:
    namespace: migration-demo
    name: data-demo-0
  nfs:
    server: "$NFS_SERVER"
    path: "$NFS_PATH"
EOF
kubectl --kubeconfig="$ONPRE1_KUBECONFIG" apply -f "$DEMO_ROOT/rendered/source-pv.yaml"

cat > "$DEMO_ROOT/rendered/demo.yaml" <<EOF
apiVersion: v1
kind: Service
metadata:
  name: demo
  namespace: migration-demo
spec:
  clusterIP: None
  selector: {app: legacy-demo}
  ports:
  - name: http
    port: 8080
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: demo
  namespace: migration-demo
spec:
  serviceName: demo
  replicas: 1
  selector:
    matchLabels: {app: legacy-demo}
  template:
    metadata:
      labels: {app: legacy-demo}
    spec:
      terminationGracePeriodSeconds: 20
      containers:
      - name: app
        image: "$ACCOUNT/legacy-video-demo:$TAG"
        ports:
        - containerPort: 8080
        volumeMounts:
        - name: data
          mountPath: /data
  volumeClaimTemplates:
  - metadata:
      name: data
    spec:
      accessModes: [ReadWriteMany]
      storageClassName: ""
      resources:
        requests:
          storage: 1Gi
---
apiVersion: policy.karmada.io/v1alpha1
kind: PropagationPolicy
metadata:
  name: demo-placement
  namespace: migration-demo
spec:
  resourceSelectors:
  - apiVersion: apps/v1
    kind: StatefulSet
    name: demo
  placement:
    clusterAffinity:
      clusterNames: [onpre1]
    replicaScheduling:
      replicaSchedulingType: Duplicated
EOF

# Service는 양쪽 member에 직접 설치, StatefulSet/PP는 Karmada에 설치
python3 - <<'PY'
import os, yaml
from pathlib import Path
root=Path(os.environ["DEMO_ROOT"])/"rendered"
docs=list(yaml.safe_load_all((root/"demo.yaml").read_text()))
(root/"demo-service.yaml").write_text(yaml.safe_dump(docs[0]))
(root/"demo-workload.yaml").write_text(yaml.safe_dump_all(docs[1:],sort_keys=False))
PY
kubectl --kubeconfig="$ONPRE1_KUBECONFIG" apply -f "$DEMO_ROOT/rendered/demo-service.yaml"
kubectl --kubeconfig="$ONPRE2_KUBECONFIG" apply -f "$DEMO_ROOT/rendered/demo-service.yaml"
kubectl --kubeconfig="$KARMADA_KUBECONFIG" apply --dry-run=server -f "$DEMO_ROOT/rendered/demo-workload.yaml"
kubectl --kubeconfig="$KARMADA_KUBECONFIG" apply -f "$DEMO_ROOT/rendered/demo-workload.yaml"
kubectl --kubeconfig="$ONPRE1_KUBECONFIG" -n migration-demo rollout status statefulset/demo --timeout=180s
kubectl --kubeconfig="$ONPRE1_KUBECONFIG" -n migration-demo get pods,pvc -o wide
kubectl --kubeconfig="$ONPRE2_KUBECONFIG" -n migration-demo get pods,pvc
kubectl --kubeconfig="$ONPRE1_KUBECONFIG" -n migration-demo exec demo-0 -c app -- sh -c 'printf "legacy-pv-demo\n" > /data/marker.txt'
kubectl --kubeconfig="$ONPRE1_KUBECONFIG" -n migration-demo exec demo-0 -c app -- python -c \
  'import urllib.request; print(urllib.request.urlopen("http://127.0.0.1:8080/state").read().decode())' \
  | tee "$DEMO_ROOT/evidence/source-memory.json"
kubectl --kubeconfig="$KARMADA_KUBECONFIG" get works -A -l pvsync.karmada.io/type=metadata -o yaml \
  > "$DEMO_ROOT/evidence/pv-metadata.yaml"
~~~

metadata Work에 migration-demo.demo/onpre1의 PV spec이 저장되었는지 확인한다. 이 Work의 suspendDispatching=true는 metadata를 저장하기 위한 의도적인 설정이며 오류가 아니다. 이 시점에는 onpre2에 demo Pod/PVC가 없어야 한다. 구버전 restore의 ordinal 추정과 충돌하므로, 이전 시도의 CR/Pod/PVC가 남아 있는 namespace는 재사용하지 않는다.

### 영상 화면 열기와 체크포인트 장면 고정

MGMT의 별도 터미널에서 실행하고 유지한다. 이 터미널에는 ONPRE1_KUBECONFIG를 다시 설정한다.

~~~bash
kubectl --kubeconfig="$ONPRE1_KUBECONFIG" -n migration-demo \
  port-forward pod/demo-0 18080:8080 --address=127.0.0.1
~~~

사용자 PC에서 `ssh -L 18080:127.0.0.1:18080 root@<MGMT_IP>`를 연결한 다음 브라우저에서 http://127.0.0.1:18080 을 연다. SSH 사용자와 MGMT_IP는 실제 접속값이다. 화면의 boot_id, frame, seconds가 보이게 녹화한다. 단일 브라우저 탭을 사용한다.

원하는 장면에서 **Pause for checkpoint**를 누른다. MGMT의 작업 터미널에서 상태를 저장한다.

~~~bash
curl --fail -X POST http://127.0.0.1:18080/pause
curl --fail http://127.0.0.1:18080/state \
  | tee "$DEMO_ROOT/evidence/source-memory.json"
~~~

paused=true와 frame>0을 확인한다. 브라우저의 **Disconnect for checkpoint**를 누르고 잠시 후 port-forward 터미널에서 Ctrl+C한다. HTTP 요청이 없는 상태로 다음 9절 백업을 실행한다. 체크포인트에 불필요한 활성 TCP 연결을 포함하지 않기 위한 절차다. HTTP probe는 데모 Pod에 추가하지 않는다.


## 9. 백업 생성

StatefulMigration은 **host/MGMT API**에 생성한다.

~~~bash
cat > "$DEMO_ROOT/rendered/sm.yaml" <<EOF
apiVersion: migration.dcnlab.com/v1
kind: StatefulMigration
metadata:
  name: demo-migration
  namespace: migration-demo
spec:
  resourceRef:
    apiVersion: apps/v1
    kind: StatefulSet
    namespace: migration-demo
    name: demo
  sourceClusters: [onpre1]
  registry:
    url: docker.io
    repository: "$ACCOUNT/checkpoints"
    secretRef:
      name: registry-credentials
      namespace: stateful-migration
  schedule: "*/5 * * * *"
EOF
kubectl --kubeconfig="$MGMT_KUBECONFIG" apply --dry-run=server -f "$DEMO_ROOT/rendered/sm.yaml"
kubectl --kubeconfig="$MGMT_KUBECONFIG" apply -f "$DEMO_ROOT/rendered/sm.yaml"
kubectl --kubeconfig="$KARMADA_KUBECONFIG" -n migration-demo get checkpointbackups
kubectl --kubeconfig="$ONPRE1_KUBECONFIG" -n migration-demo get checkpointbackups -o yaml
kubectl --kubeconfig="$ONPRE1_KUBECONFIG" -n stateful-migration logs daemonset/checkpoint-backup-controller --tail=100
~~~

최초 reconcile에서도 백업을 수행하고, 이후 cron 일정에 따라 반복한다. source의 CheckpointBackup.status.phase=Completed, lastCheckpointTime, spec.containers[].image와 registry에 실제로 저장된 image/digest를 확인한다. 일반적인 이름은 demo-migration-demo-0-onpre1이며, image tag는 onpre1_demo-0_app이다. 실제 CR에 기록된 값을 기준으로 한다.

Completed가 되기 전에는 이전하지 않는다. 구버전 restore가 항상 Completed 백업만 선택하는 것은 아니다. 확인 자료를 저장하고 이번 실습에 사용할 checkpoint를 고정한다.

~~~bash
kubectl --kubeconfig="$ONPRE1_KUBECONFIG" -n migration-demo get checkpointbackups -o yaml > "$DEMO_ROOT/evidence/checkpoint-source.yaml"
kubectl --kubeconfig="$MGMT_KUBECONFIG" -n stateful-migration scale deployment migration-backup-controller --replicas=0
# 첫 실습에서는 source DaemonSet도 중지하여 같은 tag로 다시 push하지 않게 한다.
kubectl --kubeconfig="$ONPRE1_KUBECONFIG" delete -f "$DEMO_ROOT/rendered/checkpoint.yaml"
kubectl --kubeconfig="$ONPRE1_KUBECONFIG" -n stateful-migration wait --for=delete pod   -l app.kubernetes.io/name=checkpoint-backup-controller --timeout=120s
kubectl --kubeconfig="$ONPRE1_KUBECONFIG" -n stateful-migration get pods -l app.kubernetes.io/name=checkpoint-backup-controller
~~~

DaemonSet/RBAC만 삭제한다. CRD, PVC, PV, registry image는 삭제하지 않는다. 재조회 결과 checkpoint-backup-controller Pod가 하나도 남지 않았는지 확인한 뒤, registry의 최종 image digest를 다시 조회하여 이번 복원에 사용할 값을 확정하고 기록한다. Pod 종료 전에 확인한 digest만으로는 같은 tag의 마지막 push가 끝났다고 판단할 수 없다. 이후에도 원래 실행 중이던 Pod의 상태는 checkpoint 시점보다 더 진행될 수 있다. 복원은 checkpoint 시점으로 돌아가는 것이므로, 그 이후의 모든 변경 사항까지 보존한다고 보장할 수 없다. 애플리케이션 정합성이 필요하면 checkpoint 전에 쓰기를 일시 중지한다.

## 10. 실행 순서를 확인하는 수동 migration 실습

**이 단계는 자동 장애 감지 시연이 아니다.** 컨트롤러의 각 단계를 확인하기 위해 backup/restore/suspension을 순서대로 실행한다. pvmigration 원본에는 restore-phase가 없을 때의 25초 fallback과 자체 unsuspend 동작이 있다. 복원 준비 전에 dispatch가 재개되는 것을 막기 위해 PV 준비 후 pvMigration만 일시 중지한다. PV 로직의 소스는 변경하지 않는다.

먼저 실제 StatefulSet의 RB 이름을 조회한다.

~~~bash
export RB="$(kubectl --kubeconfig="$KARMADA_KUBECONFIG" -n migration-demo get resourcebindings -o json | python3 -c \
'import json,sys; xs=[x["metadata"]["name"] for x in json.load(sys.stdin)["items"] if x["spec"]["resource"]["kind"]=="StatefulSet" and x["spec"]["resource"]["name"]=="demo"]; assert len(xs)==1,xs; print(xs[0])')"
kubectl --kubeconfig="$MGMT_KUBECONFIG" -n stateful-migration scale deployment migration-restore-controller suspension-controller --replicas=0
kubectl --kubeconfig="$KARMADA_KUBECONFIG" -n migration-demo annotate resourcebinding "$RB" \
  migration.dcnlab.com/restore-phase=working \
  stwmig.karmada.io/failed-cluster=onpre1 --overwrite
kubectl --kubeconfig="$KARMADA_KUBECONFIG" -n migration-demo patch resourcebinding "$RB" --type=merge \
  -p '{"spec":{"suspension":{"dispatching":true}}}' --dry-run=server
kubectl --kubeconfig="$KARMADA_KUBECONFIG" -n migration-demo patch resourcebinding "$RB" --type=merge \
  -p '{"spec":{"suspension":{"dispatching":true}}}'
~~~

source의 실제 Pod를 중지하여 두 프로세스가 동시에 쓰지 못하게 한다. 프로세스 강제 종료를 촬영할 경우 이 절 끝의 "영상 장애 및 재개 촬영 순서"를 먼저 읽고, source Work의 suspension 확인 후 해당 SIGKILL 및 fencing 블록을 사용한다. RB에서 Work로 suspension이 반영되는 과정은 비동기이므로, RB의 dispatching=true만 보고 source를 중지하지 않는다. 아래 조회에서 manifests에 apps/v1 StatefulSet migration-demo/demo가 들어 있는 source Work를 찾고, 그 Work의 spec.suspendDispatching=true를 확인한 뒤 다음 scale 명령을 실행한다. Work 이름은 조회 결과를 사용한다. source의 PVC/PV는 유지한다.

~~~bash
kubectl --kubeconfig="$KARMADA_KUBECONFIG" -n karmada-es-onpre1 get works -o yaml
# demo StatefulSet을 포함한 Work의 spec.suspendDispatching=true를 확인한 뒤 진행
kubectl --kubeconfig="$ONPRE1_KUBECONFIG" -n migration-demo scale statefulset demo --replicas=0
kubectl --kubeconfig="$ONPRE1_KUBECONFIG" -n migration-demo wait --for=delete pod/demo-0 --timeout=120s
# 네트워크 단절로 source에 접근할 수 없다면 이 wait만으로 fencing을 입증할 수 없다.
# 이 경우 노드/쓰기 경로의 fencing을 확인하기 전에는 target을 실행하지 않는다.
kubectl --kubeconfig="$KARMADA_KUBECONFIG" -n migration-demo patch propagationpolicy demo-placement --type=merge \
  -p '{"spec":{"placement":{"clusterAffinity":{"clusterNames":["onpre2"]}}}}'
kubectl --kubeconfig="$KARMADA_KUBECONFIG" -n migration-demo get resourcebinding "$RB" -o yaml
~~~

RB.spec.clusters가 onpre2를 가리키고, replicas=1, dispatching=true인지 확인한다. target 배치가 확정되기 전에 restore를 실행하면 source에 복원 요청을 생성할 수 있다. scheduler가 이전 배치를 유지하고 있다면 먼저 원인을 해결한다. RB의 cluster 배열을 수동으로 바꾸어 배치가 완료된 것처럼 만들지 않는다.

~~~bash
kubectl --kubeconfig="$KARMADA_KUBECONFIG" get works -A -l pvsync.karmada.io/type=pv-deployment -o yaml
kubectl --kubeconfig="$KARMADA_KUBECONFIG" -n migration-demo get resourcebinding "$RB" -o jsonpath='{.metadata.annotations}{"\n"}'
kubectl --kubeconfig="$ONPRE2_KUBECONFIG" get pv -o wide
~~~

stwmig.karmada.io/pv-migration=completed인지, onpre2에 생성된 PV가 동일한 NFS server/path를 참조하는지 확인한다. 이 단계의 PV 상태는 Available 또는 Bound다. 아직 StatefulSet을 dispatch하지 않았으므로 PVC가 없는 것 자체는 오류가 아니다.

다음으로 pvMigration만 중지하고, Work를 전달하는 나머지 manager controller는 계속 실행한다.

~~~bash
kubectl --kubeconfig="$MGMT_KUBECONFIG" -n karmada-system patch deployment karmada-controller-manager --type=json \
  --patch-file "$DEMO_ROOT/rendered/manager-pv-paused.json.patch.json"
kubectl --kubeconfig="$MGMT_KUBECONFIG" -n karmada-system rollout status deployment/karmada-controller-manager --timeout=180s
kubectl --kubeconfig="$MGMT_KUBECONFIG" -n stateful-migration scale deployment migration-restore-controller --replicas=1
kubectl --kubeconfig="$MGMT_KUBECONFIG" -n stateful-migration rollout status deployment/migration-restore-controller --timeout=180s
kubectl --kubeconfig="$KARMADA_KUBECONFIG" -n migration-demo get checkpointrestores -o yaml
kubectl --kubeconfig="$ONPRE2_KUBECONFIG" -n migration-demo get checkpointrestores -o yaml
~~~

target CR이 **실제 onpre2 API**에 도착했고, spec.podName=demo-0이며, spec.containers에 name=app과 완성된 checkpoint image가 있는지 확인한다. 해당 Pod와 일치하는 CR은 하나만 있어야 한다. restore-phase=succeeded는 Restore RB의 배치를 확인했다는 표시이며 Pod 복원 완료를 의미하지 않는다. succeeded를 수동으로 붙이지 않는다.

~~~bash
kubectl --kubeconfig="$ONPRE2_KUBECONFIG" -n stateful-migration get pods
kubectl --kubeconfig="$KARMADA_KUBECONFIG" -n migration-demo get resourcebinding "$RB" -o yaml
# pv-migration=completed / restore-phase=succeeded / 실제 target CR 존재 여부를 확인한 뒤 진행
kubectl --kubeconfig="$ONPRE1_KUBECONFIG" -n migration-demo get statefulset demo --ignore-not-found -o jsonpath='{.spec.replicas}{"\n"}'
kubectl --kubeconfig="$ONPRE1_KUBECONFIG" -n migration-demo get pod demo-0 --ignore-not-found
# source StatefulSet이 없거나 replicas=0이며 demo-0 Pod가 없음을 확인한 뒤 진행
kubectl --kubeconfig="$MGMT_KUBECONFIG" -n stateful-migration scale deployment suspension-controller --replicas=1
kubectl --kubeconfig="$MGMT_KUBECONFIG" -n stateful-migration logs deployment/suspension-controller --tail=100
kubectl --kubeconfig="$ONPRE2_KUBECONFIG" -n migration-demo get pods,pvc -w
~~~

Pod가 Ready가 되면 별도 터미널에서 확인한다.

~~~bash
kubectl --kubeconfig="$ONPRE2_KUBECONFIG" -n migration-demo get pod demo-0 \
  -o jsonpath='{.spec.containers[*].image}{"\n"}'
kubectl --kubeconfig="$ONPRE2_KUBECONFIG" -n migration-demo exec demo-0 -c app -- python -c \
  'import urllib.request; print(urllib.request.urlopen("http://127.0.0.1:8080/state").read().decode())' \
  | tee "$DEMO_ROOT/evidence/target-memory.json"
kubectl --kubeconfig="$ONPRE2_KUBECONFIG" -n stateful-migration logs deployment/checkpoint-restore-webhook --tail=100
kubectl --kubeconfig="$ONPRE2_KUBECONFIG" -n migration-demo get pvc -o yaml > "$DEMO_ROOT/evidence/target-pvc.yaml"
kubectl --kubeconfig="$ONPRE2_KUBECONFIG" get pv -o yaml > "$DEMO_ROOT/evidence/target-pv.yaml"
kubectl --kubeconfig="$MGMT_KUBECONFIG" -n migration-demo get statefulmigration demo-migration -o yaml
~~~

아래 조건을 모두 충족해야 성공으로 판단한다.

1. source의 원래 프로세스가 중지되어 있다.
2. target Pod의 image가 일반 app image에서 checkpoint image로 변경되어 있다.
3. target worker의 CRI-O 로그에 restore 성공이 기록되고 Pod가 Ready가 된다.
4. boot_id가 source와 같다. 영상 pause 체크포인트라면 frame과 paused 상태도 같아야 한다. 검증 후 resume하면 frame이 증가한다. 재생 중 백업은 checkpoint 이후 진행분을 잃을 수 있다.
5. PV marker가 같고, target PV의 NFS server/path도 같다.
6. RB의 dispatch가 재개되어 일반적인 Work 반영이 다시 이루어진다.

Pod Ready, 동일한 파일 접근, restore-phase=succeeded만으로는 성공으로 판단하지 않는다. checkpoint image의 Pod에서 kubectl exec를 사용할 수 없는 runtime이라면 port-forward로 HTTP 응답을 확인한다. checkpoint 전에 연결했다가 끊어진 port-forward는 새로 연결한다.

종료 시 PV migration을 다시 실행해야 한다면 RB의 suspension이 해제되었는지 확인하고 manager-active patch로 되돌린다. backup을 재개하기 전에 StatefulMigration.sourceClusters가 onpre2로 갱신되었는지 확인하고, onpre2에도 checkpoint DaemonSet과 push Secret을 설치한다. 첫 단방향 시연에서는 정기 백업을 자동으로 재개할 필요가 없다.

### 영상 장애 및 재개 촬영 순서

9절의 Completed 백업 및 이미지 고정 확인 이후에만 진행한다. member1은 NFS 서버이므로 전원을 끄거나 NFS를 중단하지 않는다.

1. 정확한 장면 비교: pause 상태의 백업을 사용하고 아래 10절의 RB/Work suspension 확인 -> source replicas=0으로 서비스 중단 -> onpre2 복원 순서를 따른다. 이것은 통제된 서비스 중단 및 수동 복구다.
2. 실제 프로세스 강제 종료를 포함하려면, **RB와 source Work의 dispatch 중지가 확인된 뒤**, 아래 명령으로 source 컨테이너를 종료하고 즉시 replicas=0으로 fencing한다. 종료 명령의 연결 끊김은 예상할 수 있다. kubelet이 잠시 일반 이미지로 재시작할 수 있으므로 source Pod 삭제 완료를 반드시 확인한다. 그 재시작은 checkpoint 복원이 아니다.

~~~bash
# 10절에서 source Work.spec.suspendDispatching=true를 확인한 뒤에만 실행
kubectl --kubeconfig="$ONPRE1_KUBECONFIG" -n migration-demo exec demo-0 -c app -- \
  python -c 'import os, signal; os.kill(1, signal.SIGKILL)'
# 위 명령이 종료 코드 137/연결 종료를 반환해도 다음 fencing 명령은 실행
kubectl --kubeconfig="$ONPRE1_KUBECONFIG" -n migration-demo scale statefulset demo --replicas=0
kubectl --kubeconfig="$ONPRE1_KUBECONFIG" -n migration-demo wait --for=delete pod/demo-0 --timeout=120s
~~~

이 블록은 set -e로 묶지 않는다. 강제 종료 실패 시 오류를 확인하고 source Pod가 사라지기 전에는 target을 실행하지 않는다. 이후 10절의 demo-placement를 onpre2로 변경하는 명령부터 계속한다. 복구 자동화 여부는 11절의 별도 조건을 따른다.

onpre2 Pod Ready 후 MGMT의 port-forward 터미널에서 대상만 바꾸어 같은 로컬 포트를 연다.

~~~bash
kubectl --kubeconfig="$ONPRE2_KUBECONFIG" -n migration-demo \
  port-forward pod/demo-0 18080:8080 --address=127.0.0.1
~~~

기존 SSH 터널은 유지한다. 브라우저에서 Reconnect를 누른다. 새 탭으로 접속해도 같은 서버 boot_id와 정지 프레임이 나와야 한다. MGMT 작업 터미널에서 **재생 재개 전에** 비교한다.

~~~bash
curl --fail http://127.0.0.1:18080/state \
  | tee "$DEMO_ROOT/evidence/target-memory.json"
python3 - <<'PY'
import json, os
from pathlib import Path
p=Path(os.environ['DEMO_ROOT'])/'evidence'
a=json.loads((p/'source-memory.json').read_text())
b=json.loads((p/'target-memory.json').read_text())
assert a['paused'] and b['paused'], 'pause 상태의 checkpoint인지 확인'
assert a['frame'] > 0 and a['frame']==b['frame'], (a,b)
assert a['boot_id']==b['boot_id'], '일반 재시작: 메모리 복원 아님'
assert a['marker']==b['marker'], 'PV 데이터 확인 필요'
print('PASS: same boot_id, paused frame and PV marker')
PY
~~~

PASS 이후 브라우저의 Play / Resume을 누르고 seconds/frame이 증가하는 모습을 촬영한다. 원본/대상 Pod 이름뿐 아니라 클러스터, checkpoint image, runtime restore 로그도 함께 기록한다. 복원 후에도 paused=true인 것은 checkpoint 시점 상태를 보존한 정상 결과다.

재생을 멈추지 않고 백업한 뒤 나중에 장애를 내는 두 번째 실험에서는 frame 동일성 검사를 사용하지 않는다. 백업 시각과 장애 시각의 차이만큼 되감길 수 있음을 영상에 표시한다. 같은 boot_id와 checkpoint 이후의 재생은 검증할 수 있지만 마지막 화면과의 완전 일치는 보장하지 않는다.


## 11. 자동 application failover 영상으로 진행하기 위한 조건

위 절차는 수동 실습이다. 완전 자동 동작으로 촬영하려면 구버전 fork의 applicationfailover controller가 수행하는 Unhealthy 판정과 suspend, scheduler의 target 재배치, restore/PV 양쪽 gate 통과, target의 실제 복원까지 수동 개입 없이 확인해야 한다. 이는 단순히 cluster를 NotReady로 만드는 시험과는 다른 경로다.

구버전 API에서 application failover의 기본 항목은 다음과 같다. 현재 v1.19 API에서 explain/server dry-run으로 확인한다.

~~~yaml
spec:
  propagateDeps: true
  failover:
    application:
      decisionConditions:
        tolerationSeconds: 60
      purgeMode: Graciously
      gracePeriodSeconds: 600
~~~

onpre1로만 고정한 demo-placement로는 onpre2에 자동 재배치할 수 없다. 두 cluster를 모두 허용하면서 최초 배치를 onpre1의 1 replica로 만드는 배치 전략과 RB.spec.clusters[].replicas를 확인한다. Duplicated를 두 cluster에 지정하면 양쪽에서 실행되므로, cluster 목록만 그대로 확장해서는 안 된다.

자동화 전에 해결하거나 확인해야 할 구현 조건은 다음과 같다.

- 기존 pvMigration과 suspension의 중복 unsuspend 동작을 정리하거나, target CR 도착까지 확인하는 공통 gate를 마련한다.
- pvMigration의 false 쓰기를 v1.19에 맞게 dispatching 필드 제거로 변경한다. 수동 실습에서는 해당 코드가 실행되기 전에 pvMigration을 중지한다.
- restore-phase가 없을 때의 25초 fallback으로 dispatch가 먼저 재개되지 않는지 확인한다.
- target CR의 배치 결정과 실제 target API 도착 사이의 시간 차이를 처리한다.
- source fencing과 checkpoint 시점의 애플리케이션 정합성을 확인한다.
- 실제 Unhealthy 판정, gracious eviction, stateful workload 전파가 현재 v1.19와의 혼합 구성에서 정상 동작하는지 확인한다.

이 항목들은 **아직 수정하거나 실증하지 않은 자동화 조건**이다. 이 문서의 수동 실습을 수동 개입 없는 자동 장애 복구라고 설명해서는 안 된다. 원래의 두 branch를 그대로 적용하면 현재 v1.19에서 완전 자동으로 성공한다고 코드만으로 보장할 수는 없다.

## 12. 원래 controller-manager로 복구

여러 차례 rollout한 뒤 rollout undo를 실행하면 바로 이전 상태로만 돌아가므로, 원래 v1.19로 복구된다고 보장할 수 없다. 저장해 둔 원본 template/strategy를 명시적으로 사용한다.

복원 진행 중이라면 source/target 중 어느 쪽을 실행할지 먼저 정해 동시 쓰기를 방지한다. manager를 되돌려도 이미 생성된 PV/Pod나 suspension 상태가 자동으로 되돌아가는 것은 아니다.

~~~bash
kubectl --kubeconfig="$MGMT_KUBECONFIG" -n stateful-migration scale deployment \
  migration-backup-controller migration-restore-controller suspension-controller --replicas=0
python3 - <<'PY'
import json, os
from pathlib import Path
root=Path(os.environ["DEMO_ROOT"])
d=json.loads((root/"evidence/manager-original.json").read_text())
patch=[{"op":"replace","path":"/spec/template","value":d["spec"]["template"]},
       {"op":"replace","path":"/spec/strategy","value":d["spec"].get("strategy",{"type":"RollingUpdate"})}]
(root/"rendered/manager-rollback.json").write_text(json.dumps(patch))
PY
kubectl --kubeconfig="$MGMT_KUBECONFIG" -n karmada-system patch deployment karmada-controller-manager --type=json \
  --patch-file "$DEMO_ROOT/rendered/manager-rollback.json"
kubectl --kubeconfig="$MGMT_KUBECONFIG" -n karmada-system rollout status deployment/karmada-controller-manager --timeout=180s
kubectl --kubeconfig="$MGMT_KUBECONFIG" -n karmada-system get deployment karmada-controller-manager -o wide
kubectl --kubeconfig="$KARMADA_KUBECONFIG" get clusters
~~~

PVC/PV/Work/CRD를 일괄 삭제하지 않는다. Retain PV에도 Work나 finalizer의 별도 동작이 존재한다. 확인 자료를 저장한 뒤 시연 전용 리소스를 개별적으로 정리한다.

## 13. 자주 막히는 지점

| 증상 | 확인할 사항 |
|---|---|
| deployment "kubectl" not found | 두 명령을 같은 줄에 이어 붙였는지 확인하고, 블록별로 실행 |
| custom manager CrashLoop | v1.19의 flags/feature-gates와 old binary의 차이, command 유지 여부 |
| host controller의 Karmada 연결 실패 | Secret key=kubeconfig, /etc/karmada/kubeconfig, 접근 가능한 server/SAN |
| checkpoint DS 실행 실패 | CRI-O/Buildah, privileged 허용 여부, hostNetwork의 8080/8081 충돌 |
| registry Unauthorized | source stateful-migration의 Opaque push Secret과 SM secretRef |
| target ImagePullBackOff | target namespace의 dockerconfigjson pull Secret, 실제 image 존재 여부 |
| PV metadata 없음 | source의 Bound PVC, volumeClaimTemplate 이름, STS Work, pvSync 실행 여부 |
| pv-migration 진행 중단 | native NFS, metadata Work, RB target replicas, failed-cluster |
| source에 restore CR 생성 | target 배치 전에 restore를 실행했는지 확인 |
| target이 일반 image로 실행 | CR 미도착/Pod 이름 불일치/RBAC 실패/namespaceSelector/구버전 webhook 혼동 |
| checkpoint image지만 실행 실패 | target CRI-O restore 구현, kernel/CPU, CRI-O/CRIU 로그 |
| dispatching false 거부 | true를 false로 바꾸는 대신 필드를 제거 |
| 파일만 같고 boot_id 변경 | 일반적인 신규 실행이며, 메모리 복원 성공이 아님 |

## 14. 확인한 근거와 검증 범위

- [Stateful Old-Version 고정 소스](https://github.com/GProjectdev/Stateful-Migration-Operator-with-PV/tree/cf5431275263cd76fc1e63036f8f17b74a5801ef): cmd, controllers, deploy, suspension, CRD를 확인했다.
- [legacy pvsync](https://github.com/GProjectdev/Karmada_with_PVMigration/tree/74c02e295fcba4fafbc4a79113c76988f87fed6c/pkg/controllers/pvsync): NFS metadata, PV Work, gate, cleanup을 확인했다.
- [custom controller 등록](https://github.com/GProjectdev/Karmada_with_PVMigration/blob/74c02e295fcba4fafbc4a79113c76988f87fed6c/cmd/controller-manager/app/controllermanager.go): 독립 PV Deployment가 아닌 manager 내장 기능이라는 근거다.
- [Karmada ResourceBinding API](https://karmada.io/docs/v1.14/reference/karmada-api/work-resources/resource-binding-v1alpha2/): dispatch suspension의 기본 규약이다. 실제 환경인 v1.19의 schema는 explain/dry-run으로 대조한다.
- [Kubelet Checkpoint API](https://kubernetes.io/docs/reference/node/kubelet-checkpoint-api/): checkpoint API와 runtime 지원 조건이다.
- [함께 제공한 webhook의 출처와 제약](legacy-demo-assets/webhook/README.md).

로컬에서 소스 참조, 보조 스크립트의 생성 결과 검사, Python 문법 검사, 본문 Bash 블록의 문법 검사, 생성된 YAML/JSON과 모드 지정을 확인했다. 함께 제공한 webhook은 캐시된 의존성으로 Windows/amd64 대상 Go 빌드에 성공했다. Buildah 실제 build/push, Linux 대상 빌드·실행, 실제 cluster admission, Karmada v1.19 혼합 구성의 동작, NFS 연결, CRI-O 메모리 복원은 여기서 실행하지 않았다. 각 단계의 성공 조건을 충족한 뒤 다음 단계로 진행한다.

Buildah 명령 근거: [build](https://github.com/containers/buildah/blob/main/docs/buildah-build.1.md), [login](https://github.com/containers/buildah/blob/main/docs/buildah-login.1.md), [push](https://github.com/containers/buildah/blob/main/docs/buildah-push.1.md), [run](https://github.com/containers/buildah/blob/main/docs/buildah-run.1.md). Containerfile 생성기는 고정 Old-Version 템플릿으로 검사했으며, 이 Windows 작업 환경에서 실제 Buildah 빌드는 수행하지 않았다.
