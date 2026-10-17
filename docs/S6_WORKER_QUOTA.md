# S6: 최초 발송과 재시도의 공통 quota

이 문서는 최초 연동의 **동기 유한 대기 비교 모드**(`pause-enabled=false`)를 설명한다.
현재 기본은 [pause/resume 모드](S6_CONSUMER_PAUSE.md)이며 DB 선점 전 검사와 공통 quota 경계는 그대로 사용한다.

## 문제와 선택

Redis 제한기를 만들었어도 Worker가 호출하지 않으면 실제 발송 예산을 제어하지 못한다.
최초 발송만 제한하면 실패가 늘 때 재시도가 공급자 한도를 우회한다.
반대로 DB 선점 후 quota를 기다리면 실제 발송 전에 시도 횟수와 30초 lease를 소모한다.

| 배치 방법 | 장점 | 비용/문제 |
| --- | --- | --- |
| PushProvider 내부에서 대기 | 최초/재시도 호출에 쉽게 공통 적용 | 이미 선점한 시도/lease를 소모하고, quota 오류가 공급자 실패로 기록될 수 있음 |
| 모든 Kafka 레코드에 무조건 quota 적용 | 단순한 선점 전 처리 | 완료 재소비도 토큰을 쓰고 Redis 장애 때문에 완료된 offset조차 처리하지 못함 |
| DB 상태 확인 → quota → DB 재확인/선점 | 실제 발송 후보만 허가를 요청하고 대기 중 시도/lease를 만들지 않음 | 짧은 DB 트랜잭션이 하나 추가되고, 중간 경합에서는 허가가 낭비될 수 있음 |

세 번째 방식을 선택했다. 이벤트/작업 정합성과 완료 여부를 확인하는 책임은 WorkerStore에 유지한다.
이미 완료된 작업이나 시도 상한 소진 처리는 공급자 호출이 없으므로 Redis 허가를 요구하지 않는다.

## 처리 순서와 트랜잭션

1. WorkerAdmission이 프로세스별 처리 자리를 확보한다. 기존 FIFO/처리 상한을 유지한다.
2. 이벤트 schemaVersion, 알림 키, eventId/taskId를 파싱한다.
3. 짧은 `WorkerStore.claim(..., sendPermitted=false)` 트랜잭션에서 알림/작업 행을 잠그고
   이벤트의 세대/작업 ID, 상태, 재시도 due 시각, 기존 lease를 확인한다.
4. 완료/오래된 세대는 빈 결과를 반환한다. 상한 소진은 기존 UNKNOWN/최종 실패/DLQ 인계를 함께 저장한다.
   실제 발송 후보는 **아무 쓰기 없이 NEEDS_PERMIT**을 반환하고 잠금을 해제한다.
5. DB 트랜잭션 밖에서 WorkerQuota가 Redis 허가를 확인/대기한다.
6. 허가를 받으면 새 DB 트랜잭션에서 같은 조건을 모두 재검증하고 PROCESSING/시도 이력/lease를 만든다.
7. 트랜잭션 밖 HTTP 호출 후 결과 또는 재시도/Outbox를 함께 저장한다. 리스너가 성공 반환한 뒤 RECORD offset을 커밋한다.

최초 토픽과 Retry 토픽은 동일 리스너/WorkerQuota/Redis 키를 사용한다. DLQ 선택 재발행도 새 작업의 최초 발송으로 이 경로에 들어온다.
Redis 호출 중 DB 잠금을 잡지 않는다. Redis와 MySQL을 하나의 원자적 트랜잭션으로 묶었다고 주장하지 않는다.

허가 후 다른 Worker가 완료/선점했다면 두 번째 DB 검사에서 재소비 또는 오류로 처리한다.
받은 토큰을 환불하지 않으므로 호출 수보다 허가 수가 많을 수 있다. 토큰을 되돌려 중복 예산을 만들지는 않는다.
허가 직후 JVM 정지/네트워크 지연이 생기면 허가 시점과 실제 HTTP 도착 시점도 달라질 수 있다.

## 활성화와 대기 정책

```bash
# README의 .env / MySQL / Kafka와 별도 HTTP Mock을 준비한 뒤
# Redis 서비스만 추가 실행
docker compose --profile redis up -d --wait redis
./gradlew bootRun --args='--app.provider.mode=HTTP --app.rate-limit.client-enabled=true --app.rate-limit.pause-enabled=false'
```

`client-enabled` 기본 false는 기존 기준선이다. true이면 비동기 Worker의 최초 발송과 재시도에 적용한다.
모든 참여 앱은 같은 Redis DB 0/키/rate/burst를 사용해야 한다.
기본 rate=10/초, burst=10이며 토큰 버킷 계약은 [제한기 문서](S6_REDIS_RATE_LIMIT.md)를 따른다.
memory/sync 비교 프로필에는 Worker가 없으므로 이 발송 제한을 적용하지 않는다.

`app.rate-limit.wait-timeout-ms` 기본 2000, 허용 범위 1~5000이다.
LIMITED이면 Redis가 알려 준 시간 이후에 다시 확인한다. 25ms보다 짧게 반복 확인하지 않는다.
재확인 시간은 예약이 아니므로 다시 허가를 받아야 한다. 별도 executor나 무제한 메모리 대기열을 만들지 않는다.
Worker 처리 자리 안에서 동기 대기하므로 quota 확인/대기 수는 Worker max-active 이하이다.

대기 예산은 monotonic clock으로 Redis 명령 전후에 확인한다. 지연돼 도착한 ALLOWED도 예산이 끝났으면 사용하지 않는다.
진행 중 Redis 명령을 강제 취소하는 전체 wall-clock deadline은 아니다. 네트워크 timeout과 DB 검사 시간은 별도로 고려한다.
기존 Worker FIFO 대기, quota 대기, HTTP deadline과 DB 시간이 합쳐지므로 max.poll.interval 60초를 모든 장애에서 보장하지 않는다.
실제 Redis 오류/연결 자리 포화는 자동 재시도하지 않고 바로 거절한다.
공정한 앱 간 순서/우선순위는 제공하지 않으며, 대기 예산보다 혼잡이 길면 소비가 중지될 수 있다.

| 상황 | DB 시도/상태 | offset과 후속 처리 |
| --- | --- | --- |
| 최초 발송의 quota 대기 | QUEUED, 시도 0, 새 lease 없음 | 리스너 미반환, offset 미커밋 |
| 재시도의 quota 대기 | WAITING_RETRY, 기존 시도 횟수/이력 유지 | Retry 레코드 offset 미커밋 |
| 만료 lease의 발송 후보 대기 | 기존 PROCESSING/STARTED/토큰 유지 | 허가 후 새 선점에서 UNKNOWN 기록/복구 시도 생성 |
| quota timeout / UNAVAILABLE | 선점 전 상태 보존, 공급자 실패/DLQ로 만들지 않음 | 기존 오류 처리기로 소비 중지; 원인 해결 후 명시적 재시작 |
| 완료/이전 세대 재소비 | 추가 시도/허가 없음 | 기존 영속 결과 확인 후 offset 처리 |
| 시도 상한 소진 | 허가 없이 UNKNOWN/최종 실패/DLQ 인계 | 영속 인계 후 offset 처리, 추가 HTTP 없음 |
| context 종료 중 quota 대기 | 새 시도 없음 | 대기자를 깨워 실패 반환, offset 보존 |

종료 이벤트에서는 WorkerAdmission을 먼저 정지 상태로 바꾸고 WorkerQuota 대기자를 깨운다.
오류 처리기가 종료 도중 별도의 비정상 stop을 시작하지 않도록 기존 graceful shutdown 정책과 순서를 맞췄다.
진행 중 Redis 명령은 자체 timeout까지 걸릴 수 있으며, 반환 시 정지 상태이면 허가를 사용하지 않는다.
이미 허가를 반환받은 작업은 진행 중 작업으로 기존 종료 유예 안에서 처리한다.

## 관측과 수동 복구

- `notification_worker_quota_waiting`: Redis 확인 또는 허가 대기 중인 Worker 수.
- `notification_worker_quota_rejected_total{reason=timeout|unavailable|stopping|interrupted}`: 고정 원인의 거절 수.
- `notification_rate_limit_decisions_total`: Redis 허가 판정 수. 중단/경합 때문에 실제 HTTP 시도 수와 같지 않을 수 있다.
- Worker running, lag, Outbox age 및 DB의 attempt_count/이력을 함께 확인한다. health UP만으로 소비 정상 여부를 판단하지 않는다.

quota timeout이면 rate/burst와 트래픽, 대기 예산을 확인한다. UNAVAILABLE이면 Redis 접근과 같은 키의 설정 일치 여부를 확인한다.
원인을 해결한 뒤 앱/소비를 명시적으로 재시작한다. quota 대기만 했다면 새 lease는 없으므로 이를 기다릴 필요가 없다.
이전에 PROCESSING이던 복구 후보라면 기존 lease/UNKNOWN 정책도 함께 확인한다.
예산을 열기 위해 운영 Redis 키를 삭제하거나 재시도 횟수를 초기화하는 복구 절차는 제공하지 않는다.

## 실제 검증과 재현

```bash
./gradlew --no-daemon --console=plain test \
  --tests '*WorkerQuotaTest' --tests '*WorkerQuotaIntegrationTest'
caffeinate -i ./gradlew --no-daemon --console=plain build --rerun-tasks
```

집중 검증 **13개 통과**: 단위 7개, 실제 통합 6개.
MySQL 8.4.8/Kafka 4.0.2/Redis 7.4.11/HTTP Mock을 사용했다. 각 통합 사례는 테스트 컨테이너 안의 별도 DB schema를 사용한다.
사용자 로컬 DB/.env는 수정하지 않는다. Docker 부재 시 자동 스킵하지 않는다.

| 사례 | 실제 확인 |
| --- | --- |
| 실제 Relay, HTTP 500, r=1/B=1 | 최초+재시도 합계 허가 3/HTTP 3/이력 3/offset 3/DLQ 1. 재시도 대기 중 시도 1/WAITING_RETRY 보존 |
| 두 앱 context, 같은 group/Redis 키, r=1/B=1 | 두 앱 모두 대기할 때 시도 0/offset 0/HTTP 0/DB worker_token 없음. 최종 각 앱 허가 1, HTTP/시도/offset 합 2 |
| quota wait 100ms, 같은 파티션의 메시지 2개 | 앞선 메시지 timeout에서 둘 다 미선점/offset 0. 재시작 후 합 2. Redis 상태 오류 중 완료 재소비는 추가 허가/HTTP 없이 offset 3 |
| Redis CLIENT PAUSE 1500ms, 클라이언트 timeout 100ms | UNAVAILABLE로 소비 중지, QUEUED/시도 0/offset 0/HTTP 0. 해제 후 명시적 재시작으로 1회 완료 |
| quota 대기 중 context 종료 | 종료 후 DB QUEUED/시도 0/offset 0/HTTP 0 확인. 새 context에서 시도/이력/HTTP/offset 1 |
| 만료 시도와 상한 소진 | 발송 후보 probe는 기존 STARTED/횟수/토큰 무변경. 상한 소진은 Redis 허가 0/HTTP 0으로 UNKNOWN+최종 실패+DLQ 저장 후 offset 1 |

대기 중 다른 DB 연결에서 delivery_task UPDATE를 500ms 이내 마쳐 DB 잠금을 잡고 기다리지 않는지 확인했다.
첫 사례는 실제 자동 Relay를 사용한다. 나머지는 파티션/중단 경계를 통제하려고 Relay를 끄고 저장된 Outbox payload를 직접 발행한다.
직접 발행한 Outbox는 PENDING으로 유지하며 PUBLISHED로 위조하지 않는다. DB schema를 분리해 이후 자동 Relay 사례에 섞이지 않게 했다.
두 앱은 하나의 테스트 JVM 안의 독립 Spring context/소비자/Redis 연결 풀이다. 별도 JVM/호스트 실험은 아니다.
초기 대기를 관찰하기 위해 일부 사례는 테스트 Redis에 미래 last_ms를 지정하고, 명시적 복구 전에 테스트 예산을 복원한다.
운영 코드에는 예산 초기화/시계 주입/토큰 환불 기능이 없다.

단위 테스트는 LIMITED 후 재확인, timeout, UNAVAILABLE 즉시 중지, 종료 깨우기, interrupt 보존,
대기 예산 뒤 도착한 허가 거절, 비활성/잘못된 설정을 확인한다.
전체 회귀의 실제 집계와 실패 수정 기록은 [PROGRESS](PROGRESS.md)에 남긴다.

## 남은 범위

후속 [pause/resume 단계](S6_CONSUMER_PAUSE.md)는 정상 LIMITED의 대기에서 poll을 유지한다. 이 문서의 비교 모드는 짧은 동기 대기와 중지/명시적 재시작 정책이다.
Redis 장기 장애/예산 유실/재시작·failover, Circuit Breaker, 429 Retry-After, 우선순위 배분은 남겨 둔다.
이번 결과는 지정한 구성의 정합성 검증이며 운영 처리량/모든 장애에서의 공급자 도착률/exactly-once 보장이 아니다.
