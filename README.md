# stateful-migration-operator
# Deployment Guide for Stateful Migration Operator

This guide explains how to deploy both CheckpointBackup and MigrationBackup controllers using the automated `deploy.sh` script.


## Old-Version: Buildah로 설치 및 시연

기존 Karmada MGMT와 onpre1/onpre2에서 구버전 시연을 준비하는 절차는
[Buildah 설치·실습 가이드](docs/legacy-stateful-migration-onpre1-onpre2-20261023.md)를 참고하세요.

- Docker 데몬 없이 Buildah로 controller/webhook/demo 이미지를 빌드하고 push합니다.
- [설치 보조 파일](docs/legacy-demo-assets)은 이 브랜치에 포함되어 있어 별도 ZIP 전송이 필요하지 않습니다.
- 컨트롤러 빌드 소스는 가이드에 지정된 과거 commit을 사용하며, 이 추가 변경은 런타임 구현을 바꾸지 않습니다.
- Karmada v1.19 혼합 구성 및 실제 CRI-O checkpoint/restore는 대상 환경에서 별도 검증해야 합니다.

보조 파일만 준비하려면 MGMT에서 다음을 실행하세요. 이 경로가 이미 있으면 기존 checkout을 확인하고 업데이트하세요.

~~~bash
git clone --branch Old-Version --single-branch \
  https://github.com/GProjectdev/Stateful-Migration-Operator-with-PV.git \
  "$HOME/legacy-demo-guide-source"
export ASSETS="$HOME/legacy-demo-guide-source/docs/legacy-demo-assets"
ls -l "$ASSETS/prepare-stateful-containerfiles.py"
~~~
