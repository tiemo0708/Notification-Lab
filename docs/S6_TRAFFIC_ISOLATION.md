# S6: 실시간·캠페인 자원 분리와 예약 예산

## 문제와 선택

한 Worker/HTTP 풀/토픽에 캠페인이 먼저 쌓이면 실시간 요청이 뒤에서 기다린다.
소비 스레드만 늘리면 공급자 quota는 그대로이고 캠페인이 늘어난 자리를 먼저 차지할 수도 있다.
이번 목표는 **캠페인이 대기하거나 HTTP 자리를 점유해도 실시간 처리 자원과 발송 몫을 남기는 것**이다.

| 대안 | 장점 | 비용·한계 |
| --- | --- | --- |
| 하나의 메모리 우선순위 큐 | 단일 프로세스에서 선택 순서를 바꾸기 쉬움 | 이미 읽은 Kafka 레코드의 순서/offset 보존과 메모리 상한이 복잡해짐 |
| 토픽만 분리하고 실행 자원 공유 | Kafka 적체를 구분할 수 있음 | 같은 HTTP/DB 연결 풀과 Worker 자리에서 다시 경합함 |
| 실시간이 있으면 캠페인 완전 차단 | 실시간에 최대 용량 제공 | 지속적인 실시간 부하에서 캠페인이 굶을 수 있고 전역 대기 수 추적이 필요함 |
| 역할별 실행 + 고정 quota 예약 **(선택)** | 자원 상한을 따로 두고 두 역할의 예산을 보존함 | 유휴 몫을 다른 역할이 쓸 수 없어 총 활용률이 낮을 수 있음 |

우선순위는 메시지를 추월시키는 전역 스케줄러가 아니라 **실시간에 더 큰 독립 예산을 예약하는 정책**이다.
기본 공급자 r=10/B=10에서 실시간 r=8/B=8, 캠페인 r=2/B=2다.
실시간만 있어도 보충률은 8/s, 최대 축적량은 8개이며 캠페인의 남은 몫을 빌리지 않는다. 반대도 같다.
8:2는 실험 설정이지 운영 권장 수치나 FCM/APNs의 실제 한도가 아니다.

## 역할과 데이터 흐름

같은 실행 JAR를 별도 프로세스로 실행한다. 저장소/서비스를 MSA로 나누지 않는다.

| 자원 | REALTIME | CAMPAIGN |
| --- | --- | --- |
| 로컬 API 예시 | 127.0.0.1:8080 | 127.0.0.1:8082 |
| 접수 역할 | 기본 REALTIME | `app.traffic.lane=CAMPAIGN` |
| 요청 토픽 | `notification.requested.v1` | `notification.requested.v1.campaign` |
| Retry 토픽 | `notification.requested.v1.retry` | `notification.requested.v1.retry.campaign` |
| DLQ 토픽 | `notification.requested.v1.dlq` | `notification.requested.v1.dlq.campaign` |
| consumer group | `notification-worker-v1` | `notification-worker-v1.campaign` |
| Redis 키 | 두 역할이 **같은 키·정책** 사용 | 같은 Hash 안의 캠페인 몫 사용 |

각 프로세스는 API, 역할별 Relay와 Worker, 유한 Worker 진입/대기, HTTP/Redis/DB 연결 풀,
Circuit Breaker, pause 예약을 각각 가진다. 각 토픽은 기존처럼 파티션 3개/복제본 1개다.
`app.async`의 기본 이름은 두 프로세스에 같게 설정하고, 캠페인 역할이 `.campaign`을 자동으로 붙인다.
직접 suffix를 붙여 이중 변경하지 않는다. 분리 모드에서는 두 역할의 6개 토픽 이름이 서로 겹치면 기동을 거절한다.

MySQL 서버/테이블, Kafka 브로커, Redis 서버, 공급자와 호스트 CPU·메모리는 여전히 공유한다.
따라서 DB/브로커 전체 포화와 공통 장애까지 격리한 구조는 아니다. 프로세스 수만큼 풀/스레드 총량도 늘어난다.
실험은 프로세스별 기본 Worker 처리 1/대기 2, HTTP 연결 2(통합 테스트는 1) 등의 상한을 유지한다.

### 접수부터 재발행까지 역할 보존

1. `/notifications`는 기존 본문/응답을 유지한다. 요청 본문이나 Kafka payload가 우선순위를 선택하지 않는다.
   **접수한 프로세스의 역할**을 notification과 Outbox의 `traffic_class`에 저장하고 작업과 함께 커밋한 후 202를 반환한다.
   캠페인 전용 경로는 가상 요청을 접수하는 학습용 진입점이다. 캠페인 생성/대상자 확장/Batch는 S7에서 구현한다.
2. V7 migration은 기존 행을 REALTIME으로 보존한다. Outbox의 복합 FK가 부모 알림과 역할 불일치를 막는다.
   `(traffic_class, status, available_at, event_id)` 인덱스로 Relay가 자기 역할의 due 이벤트만 선점한다.
3. Relay는 자기 역할의 토픽에 보내고 ACK 이후 PUBLISHED를 저장한다.
4. Worker는 DB 잠금 안에서 저장된 역할을 확인한다. 다른 역할의 메시지는 완료 재소비여도 선점/허가/발송 전에 거절한다.
   기존 정책대로 미완료 offset을 보존하고 해당 역할의 소비를 중지한다. 자동 skip이나 우선순위 승격은 없다.
5. 최초/Retry 모두 같은 역할의 quota를 소모한다. Retry와 DLQ Outbox도 원래 역할을 유지한다.
6. DLQ redrive는 원래 역할의 API에서만 허용한다. 다른 역할로 요청하면 409다.
   같은 역할의 새 작업/원본 보존/계보 최대 3회 규칙은 유지한다.
7. Idempotency-Key는 두 역할을 합친 DB 전체 범위다. 같은 키·본문·역할은 200, 다른 역할 재사용은 409다.
   이벤트 스키마 v1/v2와 API 응답 필드를 변경하지 않았다. GET 조회는 여전히 DB 전체에서 가능하다.

역할은 인증/권한 경계가 아니다. 공개 배포 시 발신 주체별 인증과 허용 역할 정책이 별도로 필요하다.
현재는 loopback의 가상 데이터만 사용한다.

## Redis 분할 계약

`app.traffic.split-enabled=true`는 `client-enabled=true`, `pause-enabled=true`, 비동기 프로필을 요구한다.
기본값 false는 기존 단일 REALTIME/v2 bucket 비교 모드를 유지한다. CAMPAIGN은 분리 모드 없이 실행할 수 없다.

- 공급자 전체 r/B: `app.rate-limit.rate-per-second`, `burst`.
- 캠페인 몫: `app.traffic.campaign-rate`, `campaign-burst` (각 기본 2).
- 실시간 몫: 각각 전체 값에서 캠페인 몫을 뺀 값. 양쪽 모두 정수이며 1 이상이어야 한다.
- `provider-split-bucket.lua`는 **한 Redis Hash**의 version=3/rate/burst/campaign_rate/campaign_burst/
  realtime_credit/campaign_credit/last_ms 총 8개 필드를 사용한다. 요청/대상자별 키를 만들지 않는다.
- 한 EVAL에서 두 몫을 Redis 시간으로 보충한 후 자기 역할에서 토큰 1개만 차감한다. 몫 사이 차용은 없다.
  여러 프로세스가 같은 역할이어도 같은 몫을 나눠 쓴다. 상태/정책 확인 실패는 양쪽에서 fail-closed다.
- credit은 1/1000 토큰 단위, TTL 없음, 역방향 시간 이동 시 보충/저장 시각 후퇴 없음.
  idle 보충은 각 burst로 제한한다. 네트워크 timeout 후 재시도/환불/로컬 fallback은 없다.
- `initialize-split-quota.lua`는 **없는 키만 두 credit=0**으로 초기화하며 기존 상태를 덮어쓰지 않는다.
  앱이 자동으로 초기화하지 않는다. 초기화 직후에도 정상 보충을 기다려야 한다.
- v2 단일 정책과 v3 분할 정책은 같은 키에서 서로 거절한다. 일부 프로세스만 새 정책으로 바꾸거나 별도 키를 만들어 우회하지 않는다.

정상 상태에서 임의 구간 Δ초의 역할별 허가 수는 각각 `B역할 + r역할 × Δ` 이하이며,
합은 `B전체 + r전체 × Δ` 이하다. 두 독립 토큰 버킷의 상한을 합한 것이다.
이는 **허가 발급 시점**의 token-bucket 계약이다. 고정 1초 창의 정확히 10건 제한, 실제 HTTP 도착률,
공급자 수락률이나 지연 SLO는 아니다. 저장 시각 rollback/허가 후 장시간 정지의 기존 한계도 유지한다.

## 실행과 전환

처음부터 분리 모드를 설치할 때의 예시다. README대로 DB/Kafka/.env를 준비하고 HTTP Mock도 실행한다.
기존 단일 v2 예산이 있다면 아래 초기화는 실패한다. 그 경우 다음 절차를 먼저 따른다.

1. 기존/다른 호스트의 모든 발송 앱을 중지하고 진행 중 HTTP와 이전 허가 사용을 정리한다.
2. DB 백업/기존 quota·offset 증거를 보존한다. 새 JAR는 V7을 적용하므로 구버전 Relay/Worker와 혼합 실행하지 않는다.
   기존 앱은 캠페인 필터가 없어 새 행을 잘못 처리할 수 있다. 캠페인 데이터 생성 후 구버전으로 단순 롤백하지 않는다.
3. [Redis 복구 절차](S6_REDIS_RECOVERY.md)에 따라 마지막 허가 사용 후 조용한 구간을 확보한다.
   단일↔분할/비율 변경 시 **이전·새 정책의 전체 및 각 역할 B/r 중 가장 긴 시간** 이상 기다린다.
   공급자별 추가 회복 조건이 있으면 함께 적용한다. 기존 유효 예산을 매 시작마다 초기화하지 않는다.
4. 정책 전환으로 기존 키를 없애야 한다면 운영자가 증거·정지·대기 확인 뒤 **그 키 하나만** 삭제한다.
   이 프로젝트는 자동 삭제/전환 기능을 제공하지 않는다. DB/Outbox/offset은 삭제하지 않는다.
5. 동일 키를 v3 빈 예산으로 초기화하고 모든 앱에 동일한 r/B/캠페인 몫과 분리 모드를 적용한다.
   역할별로 재시작 후 DB 이력/HTTP/offset과 두 지표 endpoint를 함께 확인한다.

```bash
docker compose --profile redis up -d --wait redis
# 최초 설치 또는 위 통제된 전환 완료 후 한 번만 수행
# 인자: 전체 rate, 전체 burst, 캠페인 rate, 캠페인 burst
docker compose exec -T redis redis-cli -e --eval /dev/stdin \
  notification-lab:provider:mock:quota , 10 10 2 2 \
  < src/main/resources/redis/initialize-split-quota.lua
# 예상: QUOTA_INITIALIZED_EMPTY
./gradlew bootJar
```

각 터미널에 README의 DB 환경변수를 불러온 뒤 같은 JAR를 실행한다.

```bash
# 터미널 A: 실시간 접수/Relay/Worker
java -jar build/libs/notification-lab-0.3.0-SNAPSHOT.jar \
  --server.port=8080 --app.provider.mode=HTTP \
  --app.rate-limit.client-enabled=true --app.traffic.split-enabled=true \
  --app.traffic.lane=REALTIME
# 터미널 B: 캠페인 테스트 요청 접수/Relay/Worker
java -jar build/libs/notification-lab-0.3.0-SNAPSHOT.jar \
  --server.port=8082 --app.provider.mode=HTTP \
  --app.rate-limit.client-enabled=true --app.traffic.split-enabled=true \
  --app.traffic.lane=CAMPAIGN
```

POST 본문은 동일하며 실시간은 8080, 캠페인 테스트 요청은 8082로 보낸다.
재시작/증설 시 같은 역할은 같은 group을 사용한다. 다른 group으로 우회하면 재소비가 생긴다.
Worker/HTTP/DB/Redis 풀 상한은 각 프로세스에 별도로 적용한다. 회로를 켜면 회로 역시 프로세스별이다.

Redis 장애 확인은 v3의 두 credit/정책 필드 모두를 검사한다. 단순 연결 복구에서 최신 상태가 보존됐다면
키를 바꾸지 않고 해당 Worker를 명시적으로 재시작한다. v3에도 정상 형식의 오래된 snapshot 복원을 자동 감지하는 기능은 없다.

## 관찰과 검증

`outbox_unpublished`, `outbox_failed`, `outbox_oldest_age_seconds`는 이제 접속한 프로세스 역할의 값이다.
`notification_consumer_lag`는 그 역할의 요청/Retry 두 토픽과 group 기준이다.
역할 전체 합계가 필요하면 두 역할의 endpoint를 구분해 확인한다. 같은 역할 복제본의 동일 backlog를 합산하면 중복 집계된다.
카운터는 인스턴스별이고 최종 대조는 DB/offset을 사용한다. 원래의 낮은 카디널리티 label을 유지한다.

집중 검증 명령:

```bash
caffeinate -i ./gradlew --no-daemon --console=plain test \
  --tests '*TrafficIsolationIntegrationTest' \
  --tests '*RedisRateLimiterTest' \
  --tests '*RateLimitConfigurationTest'
```

- 실제 Redis: 네 독립 클라이언트의 동시 48회 요청에서 허가는 실시간 8/캠페인 2.
  시간 보충 중 역할별 상한, 남은 다른 몫 차용 없음, 설정/TTL/상태 오류와 v2/v3 상호 거절을 검증한다.
- 실제 MySQL/Kafka/Redis/HTTP: 독립 Spring 앱 context 두 개, 같은 DB/브로커/공급자와 각자 풀을 사용한다.
  캠페인이 HTTP/Worker 한 자리를 점유할 때 실시간 완료/offset 진행과 캠페인 미완료 offset 보존을 확인한다.
- 캠페인 quota pause에서 시도/lease/HTTP/offset 0, 실시간 성공, 역할별 lag와 재개 예약 분리를 확인한다.
  캠페인 Relay 중지 후 미발행 Outbox도 다른 역할이 가져가지 않으며 역할별 미발행 지표를 확인한다.
  캠페인 context 종료/재생성 중 실시간을 계속 처리하고 대기/미발행 캠페인 두 작업을 복구한다.
- 캠페인 500→Retry→DLQ→선택 redrive에서 역할 보존, 최초/재시도/재발행의 캠페인 quota 소비,
  다른 역할의 Idempotency-Key 재사용과 redrive 409를 확인한다.
- 잘못된 실시간 토픽에 캠페인 메시지를 넣으면 허가/시도 없이 소비가 중지되고 offset을 보존한다.
  DB 역할 FK와 V6의 기존 행을 보존하는 V7 migration도 검증한다.

실제 테스트 건수/실패와 수정/전체 빌드 결과는 [PROGRESS](PROGRESS.md)에 기록한다.
이 검증은 같은 테스트 JVM 내 독립 context이며 이번 단계에서 별도 OS 프로세스/호스트의 장애 격리나
대량 캠페인 부하·운영 처리량·실시간 지연 개선율을 측정한 것은 아니다. 새 1만 건 대조 실험도 별도 실행 대상이다.
한 역할 내부의 Worker 간 공정 순서, 동적 예산 차용, 공급자별 큐, 자동 failover는 제공하지 않는다.
다음 작업은 S7 캠페인 생성/시작/조회와 중복 시작 방지다.
