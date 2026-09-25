# FluidCR용 CRI-O와 CRIU 설치

Linux GPU Worker용 소스 설치 절차입니다. Windows에서 실행하는 명령이 아닙니다.
먼저 비어 있는 시험 노드에서 검증하고, source/target Worker에 동일한 검증된 조합을 적용하세요.
이 저장소에서 전체 CRI-O 빌드나 GPU 복원을 실행한 것은 아닙니다.

## 1. 제공된 fork와 복원 계약

- [leehun-cri-o](https://github.com/lehuannhatrang/leehun-cri-o)
- [leehun-criu](https://github.com/lehuannhatrang/leehun-criu)
- [CRI-O 설치 문서](https://github.com/lehuannhatrang/leehun-cri-o/blob/main/install.md)
- [CRIU 설치 문서](https://github.com/lehuannhatrang/leehun-criu/blob/criu-dev/INSTALL.md)
- [CUDA plugin 조건](https://github.com/lehuannhatrang/leehun-criu/blob/criu-dev/plugins/cuda/README.md)
- [NVIDIA cuda-checkpoint](https://github.com/NVIDIA/cuda-checkpoint)

검토한 로컬 CRI-O의 `server/container_create.go`는 CRI image 필드의 archive 파일이나
checkpoint OCI 이미지를 `CRImportCheckpoint`로 전달합니다.
하지만 FluidCR 예제의 `checkpoint-restore.crio.io/<container>`를 읽는 코드는 없습니다.
**이 두 fork 설치만으로 annotation 연결이 완성되지 않습니다.**

이 저장소의 [annotation adapter](../runtime/crio/annotation-adapter.patch)와
[helper](../runtime/crio/container_restore_annotation.go)를 함께 적용하면 현재 Member 웹훅과 연결됩니다.
Pod의 image는 정상 OCI 이미지 이름을 유지하고, CRI-O 내부에서 선택된 컨테이너만 archive 입력으로 변경합니다.
Pod image에 절대 archive 경로를 넣으면 kubelet 이미지 처리 단계에서 실패할 수 있으므로 그렇게 바꾸지 않습니다.

Adapter는 복원 annotation이 있을 때 CRIU 활성화, restore-plan label, 웹훅이 넣은 plan UID/generation,
제한된 경로 아래 일반 파일을 검사합니다. 파일 누락·경로 오류는 일반 실행으로 우회하지 않습니다.
다른 컨테이너나 init container는 변경하지 않습니다.
이 label은 인증 수단이 아닙니다. Member의 fail-closed restore 웹훅을 설치하고 신뢰된 운영자만
계획/노드 label/status를 변경하도록 RBAC를 제한해야 합니다.
기존 importer가 경로를 다시 열기 때문에 archive와 상위 디렉터리는 관리자 전용이며 복원 중 변경 금지입니다.

## 2. 소스와 호환성 고정

폴더명 main/criu-dev는 commit pin이 아닙니다. 로컬 ZIP과 원격 HEAD의 동일성을 가정하지 않습니다.
검토한 40자리 SHA를 기록하고 source/target에 같은 소스와 patch를 사용하세요.
로컬 CRI-O go.mod는 Go 1.26.3을 요구합니다. 실제 checkout의 go.mod/dependencies.yaml이 기준입니다.
컨트롤러의 Go 1.24 최소 조건과 혼동하지 마세요.
CRI-O와 Kubernetes minor 호환성을 확인하고 무조건 최신 main을 기존 노드에 설치하지 마세요.

```bash
set -euo pipefail
export CRIO_COMMIT='REPLACE_WITH_REVIEWED_40_HEX_SHA'
export CRIU_COMMIT='REPLACE_WITH_REVIEWED_40_HEX_SHA'
[[ "$CRIO_COMMIT" =~ ^[0-9a-f]{40}$ ]] || exit 1
[[ "$CRIU_COMMIT" =~ ^[0-9a-f]{40}$ ]] || exit 1
mkdir -p "$HOME/fluidcr-runtime-src"
cd "$HOME/fluidcr-runtime-src"
git init leehun-cri-o
git -C leehun-cri-o remote add origin https://github.com/lehuannhatrang/leehun-cri-o.git
git -C leehun-cri-o fetch --depth=1 origin "$CRIO_COMMIT"
git -C leehun-cri-o checkout --detach FETCH_HEAD
test "$(git -C leehun-cri-o rev-parse HEAD)" = "$CRIO_COMMIT"
git init leehun-criu
git -C leehun-criu remote add origin https://github.com/lehuannhatrang/leehun-criu.git
git -C leehun-criu fetch --depth=1 origin "$CRIU_COMMIT"
git -C leehun-criu checkout --detach FETCH_HEAD
test "$(git -C leehun-criu rev-parse HEAD)" = "$CRIU_COMMIT"
```

SHA fetch를 서버가 거부하면 해당 브랜치를 fetch한 뒤 검토한 commit을 선택합니다.
이미 로컬 소스를 Linux에 옮겼다면 복사본을 사용하되 출처/파일 hash를 별도 기록합니다.
OS/kernel, GPU 종류·개수, driver/CUDA/PyTorch/NCCL, OCI runtime, CNI, cgroup, CRI socket,
애플리케이션과 FluidCR payload 이미지 digest도 기록합니다.

## 3. 노드 유지보수와 백업

아래 Member 명령은 설치 운영자 작업입니다. MGMT 컨트롤러에 Member credential을 넣지 않습니다.

```bash
export MEMBER_CONTEXT='REPLACE_MEMBER_CONTEXT'
export NODE='REPLACE_NODE'
kubectl --context "$MEMBER_CONTEXT" cordon "$NODE"
kubectl --context "$MEMBER_CONTEXT" drain "$NODE" --ignore-daemonsets --timeout=10m
```

PDB/emptyDir 때문에 실패하면 소유자와 조정합니다. 무조건 force/delete-emptydir-data를 추가하지 않습니다.
진행 중인 migration 원본 노드를 임의 drain하지 마세요.

```bash
BACKUP="/root/fluidcr-runtime-backup-$(date -u +%Y%m%dT%H%M%SZ)"
sudo install -d -m 0700 "$BACKUP"
sudo cp -a /etc/crio "$BACKUP/"
sudo cp -a /etc/containers "$BACKUP/"
sudo systemctl cat crio | sudo tee "$BACKUP/crio-unit.txt" >/dev/null
sudo systemctl cat kubelet | sudo tee "$BACKUP/kubelet-unit.txt" >/dev/null
command -v crio
command -v criu
nvidia-smi
```

실제 바이너리, CRIU plugin/library, package 버전, systemd override도 백업합니다.
컨테이너 저장소나 kubelet 데이터 디렉터리를 지우지 마세요.
containerd에서 CRI-O로 전환하는 경우 빈 노드에서 socket/CNI/cgroup 전환을 먼저 검증합니다.

## 4. CRIU와 CUDA

Ubuntu 24.04 계열 의존성 시작 예시이며 실제 checkout INSTALL을 우선합니다.

```bash
sudo apt-get update
sudo apt-get install -y build-essential git pkg-config protobuf-c-compiler \
  libprotobuf-c-dev libnl-3-dev libcap-dev libaio-dev libgnutls28-dev \
  libnftables-dev libbsd-dev python3-dev python3-pip asciidoc xmlto
cd "$HOME/fluidcr-runtime-src/leehun-criu"
make -j"$(nproc)"
./criu/criu --version
sudo make install PREFIX=/usr/local
sudo ldconfig
/usr/local/sbin/criu --version
sudo /usr/local/sbin/criu check
find /usr/local/lib /usr/local/lib64 -name '*cuda*.so' -print 2>/dev/null
```

CUDA plugin이 빌드·설치됐는지 로그와 파일을 확인하세요. 승인된 NVIDIA cuda-checkpoint 바이너리를
checksum 확인 후 /usr/local/bin에 설치하고 서비스 PATH에서도 실행 가능하게 합니다.
검토한 plugin 문서는 r555 이상 driver와 유사 GPU·같은 GPU 개수를 요구합니다.
MIG/MPS/managed memory 제한도 해당 문서를 따르세요. driver 교체는 별도 유지보수 작업입니다.
`criu check`는 kernel 검사이지 GPU 복원 통합 테스트가 아닙니다.

## 5. Annotation adapter 적용

아래 STATEFUL_DIR는 앞서 clone한 Stateful 저장소의 절대 경로입니다.
소스가 달라 패치 검사가 실패하면 강제로 적용하지 말고 CreateContainer 분기 차이를 검토합니다.

```bash
export STATEFUL_DIR="$HOME/Stateful-Migration-System"
cd "$HOME/fluidcr-runtime-src/leehun-cri-o"
git apply --check "$STATEFUL_DIR/runtime/crio/annotation-adapter.patch"
test ! -e server/container_restore_annotation.go
test ! -e server/container_restore_annotation_test.go
git apply "$STATEFUL_DIR/runtime/crio/annotation-adapter.patch"
cp "$STATEFUL_DIR/runtime/crio/container_restore_annotation.go" server/
cp "$STATEFUL_DIR/runtime/crio/container_restore_annotation_test.go" server/
go test server/container_restore_annotation.go server/container_restore_annotation_test.go
git diff --check
sha256sum "$STATEFUL_DIR/runtime/crio/annotation-adapter.patch" \
  server/container_restore_annotation.go
```

helper 복사와 hook 패치가 **둘 다** 필요합니다.
기존 기능이 있는 다른 commit에는 중복 적용하지 마세요.
독립 helper 테스트는 전체 CRI-O 패키지의 컴파일·통합 테스트를 대신하지 않습니다.

## 6. CRI-O 빌드와 교체

```bash
sudo apt-get install -y build-essential pkg-config libseccomp-dev \
  libgpgme-dev libassuan-dev libsystemd-dev libselinux1-dev libglib2.0-dev
cd "$HOME/fluidcr-runtime-src/leehun-cri-o"
head -n 8 go.mod
go version
make -j"$(nproc)" binaries
./bin/crio --version
```

빌드 태그와 추가 의존성은 해당 checkout install.md를 따릅니다.
기존 OCI runtime, conmon, CNI, NVIDIA device plugin/CDI 설정을 보존합니다.
기존 노드에서 `make install`을 바로 실행하면 config/systemd까지 바뀔 수 있습니다.

```bash
sudo systemctl stop kubelet
sudo systemctl stop crio
sudo make install.bin-nobuild PREFIX=/usr/local
```

systemctl cat crio의 ExecStart가 새 바이너리를 가리키는지 확인합니다.
위 명령은 빌드 완료 후 바이너리만 설치합니다. 검토한 Makefile의 install.bin은 빌드 prerequisite도
실행하므로 서비스 중단 후 sudo 환경의 Go/PATH 차이로 재빌드에 실패하지 않도록 nobuild target을 사용합니다.
필요하면 executable 경로만 override하되 원래 인자·EnvironmentFile은 보존합니다.
새 노드는 install.config/install.systemd target을 검토한 뒤 설정합니다.

```bash
sudo install -d /etc/crio/crio.conf.d
sudo tee /etc/crio/crio.conf.d/99-fluidcr.conf >/dev/null <<'EOF'
[crio.runtime]
enable_criu_support = true
EOF
sudo systemctl edit crio
```

서비스 PATH에 설치한 CRIU/cuda-checkpoint가 들어가야 합니다.

```ini
[Service]
Environment="PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
```

```bash
sudo systemctl daemon-reload
sudo /usr/local/bin/crio config --validate
sudo systemctl start crio
sudo systemctl start kubelet
sudo systemctl --no-pager --full status crio kubelet
sudo journalctl -u crio -u kubelet --since '10 minutes ago'
sudo crictl --runtime-endpoint unix:///var/run/crio/crio.sock info
```

config --validate 지원과 경로는 버전별 help를 확인하고 서비스와 같은 --config 인자를 사용합니다.
kubelet endpoint, Checkpoint API 지원, 서버 인증서와 nodes/checkpoint RBAC를 확인합니다.
TLS 검증을 기본으로 유지하세요.

## 7. FluidCR과 수락 검증

[통합 가이드](two-replica-migration-guide.md)와 [README](../README.md)에 따라
FluidCR injection webhook과 완료 대기 payload overlay, Member restore webhook을 설치합니다.
source/target은 같은 최종 payload 이미지 digest를 사용합니다.
실행 중 Pod는 payload 이미지 변경만으로 갱신되지 않으므로 checkpoint 전에 새로 생성해야 합니다.

1. 시험용 단일 Pod에서 kubelet checkpoint와 adapter의 실제 import 분기를 검증합니다.
2. 유효한 RestorePlan을 사용하는 현재 admission 경로로 시험합니다. annotation만 있는 옛 예제는
   plan binding 검사를 통과하지 못합니다.
3. 없는 파일/손상 archive/잘못된 plan에서 실패하며 일반 프로세스로 시작하지 않는지 확인합니다.
4. GPU Pod의 학습 step·메모리 상태와 runtime 로그를 확인합니다. Running만으로 합격 처리하지 않습니다.
5. 두 Pod로 확장하고 모든 컨테이너가 복원된 뒤 FluidCR 잠금을 해제합니다.
   DDP 공유 generation/rendezvous 정합성은 별도의 분산 실험입니다.

Member admission 경로의 최초 시험에는 capability label이 필요합니다.
**별도 격리 시험 노드에서만 임시 label을 붙여 검증하고**, 실패하면 즉시 제거하세요.
아래 label은 자동 탐지가 아니라 운영자의 확인값입니다.
시험 통과 후 운영 노드에만 부여하고 uncordon합니다.

```bash
kubectl --context "$MEMBER_CONTEXT" label node "$NODE" \
  migration.dcnlab.com/restore-from-file=true --overwrite
kubectl --context "$MEMBER_CONTEXT" uncordon "$NODE"
```

검증을 위해 임시 label을 붙인 노드는 이 실험 외 workload가 유입되지 않도록 별도 taint/격리 정책을 사용합니다.
기존 노드가 cordon 상태라면 scheduler가 Pod를 배치하지 못하므로 격리 조건을 먼저 마련한 뒤 시험 중에만 uncordon합니다.

## 8. 롤백

원본/대상 writer를 동시에 재개하지 않습니다. 노드를 격리하고 작업 소유자와 중단을 조정한 뒤
kubelet/CRI-O를 정지하고 백업 바이너리·plugin/library·설정·systemd를 복구합니다.
새 runtime checkpoint가 이전 runtime에서 호환된다고 가정하지 마세요.

```bash
kubectl --context "$MEMBER_CONTEXT" label node "$NODE" migration.dcnlab.com/restore-from-file-
sudo systemctl daemon-reload
sudo ldconfig
sudo systemctl start crio
sudo systemctl start kubelet
```

일반 Pod 실행 및 노드 상태 확인 후에만 uncordon합니다.
archive와 설치 기록은 원인 분석이 끝날 때까지 보존합니다.
