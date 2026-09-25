# StatefulSet Suspension 게이트

새 MGMT controller는 Karmada API만 사용하며 Member에 직접 접속하지 않습니다.
자동 해제는 StatefulSet + PVMigration 조합에 한정됩니다. 기존 단독 Pod 복원은 수동 절차를 사용합니다.

## 순서와 등록

1. source의 PVMetadata snapshot을 확보합니다.
2. workload placement 변경과 checkpoint 전에 RB의 dispatch를 중지합니다.
3. checkpoint 완료, 원본 실행 차단, PV 준비와 archive 전송을 끝냅니다.
4. RestoreRequest가 Prepared가 되면 Karmada StatefulSet template에 restore-plan label을 설정합니다.
5. 아래 operation annotation을 붙이고 workload PP를 target 한 곳으로 변경합니다.
6. gate가 검증 후 dispatching 필드를 제거합니다. 초기 suspension 설정과 source fencing은 운영자 책임입니다.

```bash
kubectl --context karmada -n "$NS" patch resourcebinding "$RB_NAME" --type=merge \
  -p '{"spec":{"suspension":{"dispatching":true}}}'
# 같은 namespace의 현재 작업만 지정
RR_UID=$(kubectl --context karmada -n "$NS" get restorerequest "$RESTORE" -o jsonpath='{.metadata.uid}')
PV_UID=$(kubectl --context karmada -n "$NS" get pvmigration "$PV_MIGRATION" -o jsonpath='{.metadata.uid}')
kubectl --context karmada -n "$NS" annotate resourcebinding "$RB_NAME" --overwrite \
  migration.dcnlab.com/restore-request="$RESTORE" \
  migration.dcnlab.com/restore-request-uid="$RR_UID" \
  migration.dcnlab.com/pv-migration="$PV_MIGRATION" \
  migration.dcnlab.com/pv-migration-uid="$PV_UID"
```

RB 이름은 StatefulSet 이름과 다를 수 있으므로 spec.resource로 조회합니다.
기존 annotation 기반 suspension controller는 반드시 중지합니다.
새 gate와 수동 해제를 병행하면 검사를 우회하게 됩니다.

## 해제 조건

- RestoreRequest UID와 현재 observedGeneration, sourceFenced/volumesReady, Prepared 또는 Running.
- 소유 RestorePlan의 spec과 현재 target report 일치. Prepared 이전에 Running을 기다리지 않습니다.
- 현재 checkpoint UID/generation, source Completed 및 모든 Pod archive mapping.
- RB의 StatefulSet 이름·namespace·UID와 현재 Karmada template plan label 일치.
- 현재 replica/ordinal 전체와 restore Pod 매핑 일치. target 하나만 선택한 RB.
- PVMigration UID/generation, source/target/RB, Completed, planHash.
- PVMetadata의 source와 Karmada StatefulSet UID 일치. Member-local workloadUID와 혼동하지 않습니다.
- 모든 volumeClaimTemplate/복원 ordinal PVC를 같은 이름으로 매핑하고, 같은 수의 고유 Work가 applied/detached.
- 실패·누락·legacy none 값은 허용하지 않습니다. stale 완료 annotation만으로 해제하지 않습니다.

충돌하면 uncached API에서 전체 조건을 다시 검사합니다.
dispatching은 false가 아닌 **필드 제거**로 해제하며 scheduling 등 다른 필드는 보존합니다.
중첩 legacy/per-cluster suspension은 자동 처리하지 않습니다.
PropagationPolicy 자체에 dispatch suspension을 설정한 환경은 먼저 관리 주체를 정리하세요.
이 controller는 정책의 suspension을 수정하지 않으며, 다른 controller가 중단값을 재설정하는 상황을 해결하지 않습니다.

## 한계와 확인

Kubernetes의 여러 객체 조회는 하나의 원자적 트랜잭션이 아닙니다.
한 작업 동안 workload spec과 관련 CR을 별도 자동화가 수정하지 않도록 하고 status 쓰기 권한을 제한합니다.
PVMigration Completed는 확정된 과거 PV 준비 기록이지 현재 NFS 가용성 보장이 아닙니다.
Prepared도 GPU 복원 성공 증거가 아닙니다. 데이터 전송, source fencing, 애플리케이션 재개·정리는 별도입니다.
한 번 해제한 RB를 controller가 자동으로 다시 중단시키지는 않습니다.
해제 시 dispatch-released-for annotation에 RestoreRequest UID를 기록합니다.
같은 UID로 다시 중단된 RB는 재해제하지 않습니다. 다음 이전은 새 CR과 새 UID로 등록하세요.

```bash
kubectl --context karmada -n "$NS" get resourcebinding "$RB_NAME" -o yaml
kubectl --context host -n stateful-migration-system logs deployment/stateful-management --tail=100
```

[Karmada ResourceBinding API](https://karmada.io/docs/reference/karmada-api/work-resources/resource-binding-v1alpha2/)와
[2 Pod 통합 가이드](two-replica-migration-guide.md)를 참고하세요.
