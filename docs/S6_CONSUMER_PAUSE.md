# S6: quota 대기 중 Kafka pause/resume와 poll 유지

## 문제와 선택

앞 단계는 DB 트랜잭션 밖에서 quota를 기다렸지만 소비 스레드와 Worker 처리 자리를 사용했다.
대기를 길게 늘리면 `max.poll.interval.ms`를 넘을 수 있고, 짧게 제한하면 정상적인 한도 부족에도 소비가 중지됐다.
이 단계는 **정상적인 LIMITED만 자동 pause/resume**로 처리한다. Redis 오류는 계속 fail-closed/명시적 재시작이다.

| 방법 | 장점 | 비용/판단 |
| --- | --- | --- |
| 소비 스레드에서 sleep/대기 | 구현이 단순하고 앞 단계와 비교하기 쉬움 | quota 대기 동안 poll/처리 자리를 점유 |
| 별도 executor로 발송 위임 | poll과 HTTP 처리 분리 가능 | 완료 순서·오프셋·리밸런스·종료와 별도 큐 관리 필요; 이번에는 도입하지 않음 |
| 실패 레코드 보관 + 컨테이너 pause | 순차 처리와 RECORD ack를 유지하며 대기 중 poll 가능 | 해당 컨테이너의 다른 파티션도 함께 멈춤 |

세 번째 방식을 선택했다. 공급자 quota가 동일하므로 현재 앱의 Worker 컨테이너 전체를 잠시 멈춘다.
우선순위/파티션별 공정성을 제공한다는 뜻은 아니다. 이미 처리 중인 다른 소비 스레드의 작업은 끝낼 수 있다.

## 실제 처리 경계

1. 기존 WorkerAdmission과 WorkerStore의 짧은 트랜잭션으로 발송 후보인지 확인한다.
2. DB 잠금 해제 후 Redis를 **한 번** 호출한다.
3. ALLOWED면 새 트랜잭션에서 상태를 재확인하고 선점/HTTP/결과 저장을 진행한다.
4. LIMITED면 지연값만 가진 `QuotaDeferred`를 던진다. DB 시도/lease는 만들지 않는다.
   WorkerAdmission의 finally에서 처리 자리를 반환한다.
5. `WorkerPauseErrorHandler.handleOne()`이 컨테이너 pause를 요청하고 **false**를 반환한다.
   `seeksAfterHandling=false`, `isAckAfterHandle=false`이며 성공 복구/건너뛰기로 처리하지 않는다.
6. Spring Kafka가 실패 레코드를 보관하고 pause된 consumer로 계속 poll한다.
7. 예약 시간이 지나면 컨테이너 resume만 요청한다. 같은 레코드를 처음부터 다시 검사하고 Redis 허가도 다시 받는다.
8. 실제 DB 결과 또는 재시도/DLQ 인계가 커밋되어 리스너가 성공 반환한 뒤 기존 RECORD offset을 처리한다.

최초/Retry 레코드가 같은 경로를 쓴다. Kafka 재전달 횟수는 DB 발송 시도 횟수가 아니다.
완료/오래된 세대 재소비와 시도 상한 소진은 앞 단계처럼 quota 없이 영속 결과를 처리한다.
재개 시각은 토큰 예약이 아니며 다시 LIMITED면 반복 pause한다.
정상 quota 부족의 전체 대기 시간에는 상한을 두지 않지만, 메모리에 보관하는 레코드/예약 수에는 상한을 둔다.

## 메모리·스레드·종료

- 기본 `max.poll.records=1`, 소비 스레드 1~3개다. Spring이 스레드당 현재 poll의 미완료 레코드를 보관한다.
  기본 설정에서는 앱당 최대 3개다. Kafka fetch buffer와 JVM 전체 메모리의 상한을 뜻하지 않는다.
- 재개 전용 단일 스케줄러는 DB/Redis/HTTP를 호출하지 않는다. KafkaConsumer에도 직접 접근하지 않고
  스레드 안전한 컨테이너 pause/resume API만 사용한다.
- 현재 앱의 부모 컨테이너당 재개 예약 하나다. 내부 맵도 최대 3개로 제한한다.
  동시에 LIMITED를 받은 다른 자식 소비자는 기존 예약을 공유하며 별도 작업을 쌓지 않는다.
- 재확인 간격은 Redis retryAfterMs를 100~60000ms로 제한한다. 너무 긴 서버 시계 역행에도 주기적으로 다시 확인한다.
- 예약 취소 시 스케줄러 큐에서 제거한다. 예약 객체를 비교해 취소된 과거 콜백이 새 pause를 풀지 못하게 한다.
- ConsumerStoppedEvent에서 예약을 취소하고 pause 요청 플래그를 해제한다. `resume()`은 중지된 컨테이너를 시작하지 않는다.
  명시적 `start()` 시 이전 pause 때문에 영구 정지되지 않도록 한다.
- ContextClosedEvent에서는 WorkerAdmission → WorkerQuota → 재개 스케줄러 순으로 정지한다.
  종료 후 자동 resume를 요청하지 않으며 미완료 레코드는 다음 실행이 Kafka에서 다시 읽는다.
- 리밸런스로 소유권이 넘어간 레코드는 Spring의 pending-record 정리와 새 소유자의 미커밋 offset 재소비에 맡긴다.
  DB의 세대/lease/완료 재검증은 유지한다.

기본 오류 처리기의 유한 재시도 후 recover/skip을 사용하지 않는다.
정상 quota 부족만 분류하고 나머지는 기존 중지 처리기로 보낸다. SQL/JSON 예외 원문이나 payload를 새 예외에 붙이지 않는다.
중지/종료 경합 방지를 위해 종료 중에는 별도의 비정상 stop을 시작하지 않는다.

## 설정과 관측

실행 전 [v2 quota 초기화/전환 절차](S6_REDIS_RECOVERY.md)를 확인한다. 키가 없으면 정상 LIMITED가 아니라 UNAVAILABLE로 중지한다.

```bash
# README의 MySQL/Kafka/HTTP Mock 준비 후
# Redis가 필요한 연동을 명시적으로 켠다.
docker compose --profile redis up -d --wait redis
./gradlew bootRun --args='--app.provider.mode=HTTP --app.rate-limit.client-enabled=true'

# 이전 단계의 동기 유한 대기와 비교할 때만
./gradlew bootRun --args='--app.provider.mode=HTTP --app.rate-limit.client-enabled=true --app.rate-limit.pause-enabled=false'
```

`client-enabled`는 기본 false, `pause-enabled`는 기본 true다. Redis quota를 켜면 기본으로 pause 방식을 쓴다.
`wait-timeout-ms`(기본 2000, 허용 1~5000)는 `pause-enabled=false` 비교 모드에서만 대기 예산으로 쓰인다.
일반 poll timeout은 500ms, pause poll timeout은 100ms로 설정한다. `max.poll.interval.ms` 기본값은 60000이다.

| 상황 | 처리 |
| --- | --- |
| LIMITED | 미선점/미커밋 → pause → poll 유지 → 자동 resume → 재검사 |
| Redis UNAVAILABLE/설정 충돌/응답 timeout | 미완료 offset 보존, 소비 중지. 원인 해결 후 명시적 재시작 |
| DB/이벤트/WorkerAdmission 오류 | 기존 중지 정책. 자동 복구로 숨기지 않음 |
| 비교 모드 대기 timeout | 이전 단계처럼 중지/명시적 재시작 |
| 종료/컨테이너 stop | 예약 취소, 새 컨텍스트 또는 명시적 시작에서 미커밋 작업 복구 |

- `notification_worker_quota_pauses_pending`: 아직 실행되지 않은 재개 예약 수(실제 pause 확정 수와 구분).
- `notification_worker_quota_pauses_total`, `notification_worker_quota_resumes_total`: pause/resume 요청 수.
- `notification_worker_quota_waiting`: 이전 동기 대기 비교 모드의 대기 수. pause 중에는 0이다.
- 기존 Redis 판정/Worker active/running/consumer lag/Outbox age와 함께 확인한다.
  running=1인 pause 상태에서도 lag는 증가할 수 있다.

## 검증과 해석

`WorkerQuotaTest`와 `WorkerQuotaIntegrationTest`에서 앞 단계 비교 모드와 pause 모드를 함께 검증한다.
실제 MySQL/Kafka/Redis/HTTP Mock, 사례마다 별도 DB schema/토픽/group/quota 키를 사용한다.
사용자 로컬 DB/.env는 수정하지 않는다. 별도 JVM/호스트 대신 독립 Spring context를 사용하는 사례가 있다.

- max.poll.interval=5000ms, 테스트 Redis 시각을 앞으로 두어 긴 LIMITED를 만든다.
  7초 동안 pause, last-poll-seconds-ago<2, heartbeat-total 증가, rebalance-total/consumer member 불변을 확인한다.
  이때 DB 시도/HTTP/offset/Worker active는 모두 0이다. 다른 DB 연결의 UPDATE도 완료한다.
  이후 자동 재개로 같은 파티션의 두 레코드가 각각 한 번 처리되고 offset=2가 된다.
- 실제 Relay와 HTTP 500: 최초+재시도 합계 허가/HTTP/DB 시도/offset=3, DLQ Outbox=1.
- pause 후 Redis 상태 오류: 자동 재개 후 UNAVAILABLE로 소비 중지. 선점/offset 0을 보존하고 수동 재시작으로 복구한다.
- pause 중 컨테이너 stop/start와 앱 context 종료/재생성: 예약이 제거되고 1회 처리로 복구한다.
- pause 중 두 번째 앱 합류: 파티션 재할당과 미커밋 레코드 보존을 확인한 뒤 총 HTTP/시도/offset=2로 완료한다.
- 단위 검증은 LIMITED의 단일 확인/지연값 상한과 Redis 오류·종료의 비재개 분류를 확인한다.

테스트 Redis의 미래 시각/예산 복원은 관찰 조건을 만드는 장치다. 운영 자동 리필 기능이 아니다. 후속 [v2의 운영자 전용 빈 초기화](S6_REDIS_RECOVERY.md)와 구분한다.
최종 실행 명령·집계와 실패 이력은 [PROGRESS](PROGRESS.md)에 기록한다.

## 한계와 다음 단계

보장한 것은 **quota pause 동안의 poll 유지**다. DB 잠금/HTTP/WorkerAdmission 대기 자체는 여전히 소비 스레드에서 실행한다.
이들이 길어져 max.poll.interval을 넘는 문제까지 해결한 것으로 해석하지 않는다.
공급자 수락 뒤 DB 기록 전 중단의 불확실성과 외부 중복 가능성도 남아 있다.
스케줄러 지연은 재개를 늦출 수 있고, 앱 전체 pause는 다른 파티션 처리량에도 영향을 준다.
운영자 수동 pause 제어 API와의 pause 소유권 조합은 아직 구현하지 않았다.
[Redis 장애/상태 유실의 통제된 복구 정책](S6_REDIS_RECOVERY.md)을 추가했다. 자동 failover는 후속 범위다. [Circuit Breaker/half-open 복구](S6_CIRCUIT_BREAKER.md)는 같은 pause 경로에 연결했다.
[역할별 자원·예약 예산](S6_TRAFFIC_ISOLATION.md)도 추가했다. 각 프로세스의 부모 컨테이너를 pause하므로 다른 역할은 계속 처리할 수 있다.

근거: [Spring Kafka의 오류 처리와 보관 레코드/pause poll 계약](https://docs.spring.io/spring-kafka/reference/3.3-SNAPSHOT/kafka/annotation-error-handling.html).
실제 의존성 3.3.16의 CommonErrorHandler/KafkaMessageListenerContainer 소스도 확인했다.
