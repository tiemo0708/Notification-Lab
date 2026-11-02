# Notification Lab

Kafka 기반 알림 시스템을 단계별로 만드는 Java/Spring 학습 프로젝트.
현재 v0.3(S3)는 **요청과 Outbox를 MySQL에 함께 저장하고 Kafka Worker가 Mock을 호출하는 비동기 API**이다.
S4에서 선택적 `Idempotency-Key` 접수 중복 방지, 발송 작업/시도 이력, 일시 실패의 유한 지연 재시도, Retry 토픽/DLQ, 선택 재발행을 추가했다. 실제 푸시나 단말 도착을 의미하지 않는다.

## 실행

Java 21, Docker(Compose 포함), 인터넷 연결이 필요하다. Gradle은 Wrapper가 설치한다.
AWS/FCM 키는 필요하지 않다. 비밀번호는 로컬 실험용 값을 쓴다.

```bash
test -f .env || cp .env.example .env
# .env의 MYSQL_PASSWORD / MYSQL_ROOT_PASSWORD를 각각 채운 뒤 실행
docker compose up -d --wait
set -a
. ./.env
set +a
./gradlew bootRun
```

Compose는 MySQL 8.4.8(`127.0.0.1:13316`), Kafka 4.0.2(`127.0.0.1:19092`)를 시작한다.
Kafka는 KRaft 단일 브로커다. 앱이 `notification.requested.v1`, `notification.requested.v1.retry`,
`notification.requested.v1.dlq`를 각각 파티션 3개, 복제본 1개로 생성한다.
앱은 `127.0.0.1:8080`에서 실행한다. 인증과 TLS가 없는 로컬 실험이므로 외부에 공개하지 않는다.

Compose는 `.env`를 자동으로 읽지만 Spring Boot는 읽지 않으므로 위 환경변수 로딩이 필요하다.
bash/zsh 외 환경에서는 같은 값을 환경변수로 설정한다. Windows에서는 `gradlew.bat`을 사용한다.
`MYSQL_PORT`, `KAFKA_PORT`를 바꾸면 Compose와 앱에 함께 반영된다.
별도 로컬 주소는 `DB_URL`, `KAFKA_BOOTSTRAP_SERVERS`로 설정한다.

MySQL/Kafka는 각각 named volume을 사용한다. `docker compose down`은 데이터를 보존하며
`docker compose down -v`는 실험 데이터를 삭제한다(Redis 프로필 사용 시 해당 volume도 포함). 의도한 초기화에만 사용한다.
서로 다른 시점의 DB/토픽/consumer offset을 임의로 섞으면 재처리 또는 추적 불일치가 생긴다.

## API와 상태

```bash
curl -i http://127.0.0.1:8080/notifications \
  -H 'Content-Type: application/json' \
  -d '{"recipientId":"user-100","title":"테스트 알림","body":"알림 처리 확인"}'

curl http://127.0.0.1:8080/notifications/응답에서_받은_UUID
curl http://127.0.0.1:8080/notifications/응답에서_받은_UUID/delivery
curl http://127.0.0.1:8080/actuator/health
curl http://127.0.0.1:8080/actuator/prometheus
```

POST의 `202 Accepted`와 `Location`은 요청/발송 작업/Outbox가 DB에 커밋되었다는 뜻이다.
응답 스냅샷은 `QUEUED`이며 실제 진행 상황은 GET으로 조회한다. 기동 후 브로커 장애 중에도 DB 접수는 가능하다.
브로커 없이 처음 기동해 토픽이 생성되지 않았다면 복구 후 앱을 재시작한다.

기동 후 Kafka 컨테이너를 실제 중단하고 복구하는 [S5 실험](docs/S5_KAFKA_RECOVERY.md)에서
장애 중 5건의 202 접수와 앱 재시작 없는 후속 처리를 확인했다. 단, Outbox 발행 상한을 소진한 `FAILED`는
복구 후에도 자동 처리하지 않는다. Kafka lag가 0이어도 이 미발행 요청은 남을 수 있다.

| 상태/응답 | 의미 |
| --- | --- |
| `QUEUED` | 접수 완료, Worker 처리 대기 |
| `PROCESSING` | Worker가 선점하고 처리 중 |
| `WAITING_RETRY` | 일시 실패 후 다음 시도 예약. completedAt은 null |
| `PROVIDER_ACCEPTED` | Mock 공급자 수락. 단말 도착 아님 |
| `RETRYABLE_FAILURE` | 일시 실패로 종료. 새 비동기 작업은 전체 시도 상한 소진, 과거 결과/비교 프로필은 한 번의 결과 |
| `PERMANENT_FAILURE` | 영구 공급자 실패 |
| `INTERNAL_FAILURE` | 예상 밖 공급자 오류 또는 복구 시도 상한 소진 |
| `400` / `404` | 잘못된 요청 또는 UUID / 알림 없음 |
| `503` | DB 접근 또는 트랜잭션 오류 (`memory`에서는 용량 초과 포함) |

응답 유실이나 DB 오류 후 같은 알림의 중복 접수를 막으려면 `Idempotency-Key`를 보낸다.
처음 접수는 202, 같은 키·같은 필드 재요청은 기존 알림의 현재 상태와 같은 Location을 200으로 반환한다.
같은 키에 recipientId/title/body 중 하나라도 다르거나 접수 역할이 다르면 409이다. 200은 처리 성공을 뜻하지 않는다.
키가 없으면 기존처럼 요청마다 새로운 ID가 생긴다.

```bash
curl -i http://127.0.0.1:8080/notifications \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: example-request-001' \
  -d '{"recipientId":"user-100","title":"테스트 알림","body":"알림 처리 확인"}'
```

키는 대소문자 구분 `[A-Za-z0-9._:-]` 1~128자이며 복수 헤더/잘못된 값은 400이다.
키는 알림 행과 함께 보관하고 만료하지 않는다. 현재 범위는 이 DB의 알림 접수 API 전체다.
`sync`/`memory` 비교 프로필은 이 헤더를 지원하지 않아 전달 시 400을 반환한다.
상세 계약과 트랜잭션은 [S4 멱등성 문서](docs/S4_IDEMPOTENCY.md)를 참고한다.

## 처리 흐름과 복구

1. API: notification(`QUEUED`), delivery_task, Outbox(`PENDING`)를 한 DB 트랜잭션으로 저장한다.
2. Relay: 짧은 트랜잭션으로 한 이벤트를 선점하고 커밋한다. DB 트랜잭션 밖에서 Kafka publish future 성공을 기다린 뒤 `PUBLISHED`를 저장한다.
3. Worker: 레코드를 하나씩 처리한다. 짧은 DB 선점 후 트랜잭션 밖에서 Mock을 호출하고, 최종 결과 또는 이력/재시도 예약/Outbox를 함께 커밋한 후 offset을 커밋한다.

Relay는 30초 lease/token으로 오래된 소유자의 상태 덮어쓰기를 막는다. 실패하면 1~60초 지수 backoff,
최대 10회 시도 후 `FAILED`로 보관한다. `FAILED`는 자동 재발행하지 않으며 Outbox 지표/DB로 조사한다.
이때 알림은 `QUEUED` 또는 `WAITING_RETRY`일 수 있다. `GET`은 알림 결과이고 Relay의 상세 상태는 아니다.

[ACK 직후 Relay 종료 실험](docs/S5_OUTBOX_ACK_CRASH.md)에서는 Kafka에 기록했지만 Outbox는 PROCESSING인
상태에서 Relay JVM을 강제 종료했다. 실제 lease 만료 후 같은 이벤트가 재발행되어 Kafka 레코드는 2개가 되었고,
살아 있는 Worker는 이미 완료된 DB 결과를 확인해 공급자 호출을 1회로 유지했다.

[DB 완료 후 Worker 종료 실험](docs/S5_WORKER_OFFSET_CRASH.md)에서는 offset 커밋 직전 Worker JVM을 강제 종료했다.
같은 group의 새 Worker가 기존 Kafka 레코드 1개를 재소비했고, DB 완료 상태를 확인해 추가 공급자 호출 없이 offset을 커밋했다.

[재시작·리밸런스 실험](docs/S5_RESTART_REBALANCE.md)에서는 처리 중 Worker 합류와 1 → 2 → 1 → 2대 전환을 검증했다.
Relay 중단 중 접수된 요청도 기존 ID로 복구했으며, 단계마다 접수/호출/레코드 수와 파티션별 offset을 대조했다.

SIGTERM 종료 시 HTTP 신규 접수를 먼저 중지한 뒤 현재 Kafka 레코드의 완료를 기다린다.
Relay도 종료 신호 이후 다음 선점 루프를 진행하지 않는다. 유예는 phase마다 10초이며 전체 프로세스의 10초 제한은 아니다.
[종료 실험과 복구 절차](docs/S5_GRACEFUL_SHUTDOWN.md)에서 정상 완료와 유예 초과를 각각 검증했다.

Worker DB/이벤트 오류는 컨슈머를 멈춘다. 오류를 해결하고 진행 중 lease(30초)가 만료된 후 앱을 재시작한다.
그 전에는 미완료 offset을 넘기지 않는다. 완료된 DB 결과를 다시 읽으면 공급자를 호출하지 않고 offset만 처리한다.
공급자 성공 직후 DB 저장 전 종료되면 재호출이 가능하다. 외부 발송 exactly-once는 보장하지 않는다.
UNKNOWN/상한 소진은 미발송 확정이 아니다. [불확실한 결과의 확인·복구 정책](docs/S4_UNCERTAIN_OUTCOMES.md)을 따른다.

발송 작업은 요청당 가상 대상 하나이며 taskId와 시도 이력을 `/notifications/{id}/delivery`로 조회한다.
만료된 시도는 다음 복구에서 `UNKNOWN`으로 기록한다. 선점은 최초 포함 기본 3회(설정 범위 1~10)이며,
마지막 선점의 lease까지 만료되면 추가 호출 없이 `INTERNAL_FAILURE / ATTEMPTS_EXHAUSTED`로 보관한다. 공급자 실패 확정을 뜻하지 않는다.
일반 일시 실패도 같은 횟수 상한 안에서 자동 재시도한다. 기본 대기는 500~1000ms, 1000~2000ms이며
지수 backoff와 equal jitter를 적용한다. 예약 시각은 DB에 저장하고 delivery 조회의 nextAttemptAt으로 확인한다.
재시도는 due 이후 별도 Retry 토픽으로 보내고, 최종 실패는 결과와 함께 DLQ Outbox를 저장한다.
원본 offset은 이 영속 인계 후 처리하며, Relay는 Kafka ACK를 확인한 후 PUBLISHED로 바꾼다.
DLQ는 자동 소비/재발송하지 않는다. 선택한 PUBLISHED DLQ는 사유와 중복 가능성 확인을 입력해
`POST /dead-letters/{eventId}/redrive`로 재접수할 수 있다. 최초 202/동일 요청 200이며 원본을 보존하고 새 작업을 만든다.
같은 DLQ는 한 번, 원본 기준 계보 전체는 최대 3회 재발행한다.
[선택 재발행 API](docs/S4_DLQ_REDRIVE.md)와 [토픽·DLQ 정책](docs/S4_RETRY_TOPICS_DLQ.md)을 참고한다.
[작업/복구 정책](docs/S4_DELIVERY_TASKS.md)과 [재시도 정책](docs/S4_RETRY_POLICY.md)을 참고한다.
기존 앱을 모두 종료한 뒤 새 버전을 실행한다. V9 DB를 사용하며 실시간/캠페인 역할을 모르는 이전 앱·Relay와 혼합 실행하지 않는다. 기존 행은 REALTIME으로 보존한다.

기본 Mock 지연은 100ms(허용 0~5000ms), SUCCESS다. 앱 재시작으로 설정한다.

```bash
./gradlew bootRun --args='--app.mock.outcome=TRANSIENT_FAILURE'
./gradlew bootRun --args='--app.mock.outcome=PERMANENT_FAILURE'
./gradlew bootRun --args='--app.mock.delay-ms=1000'
```

DB 풀은 최대 8개, Relay는 한 번에 한 publish/한 주기 최대 50건, Worker 기본 소비 스레드 1/처리 1/poll 1건이다.
단일 브로커 고가용성, 다중 인스턴스의 모든 장애 조합, 운영 성능을 입증한 구성이 아니다.

## HTTP Mock과 Worker 연동 (S6)

독립 HTTP Mock을 DB/Kafka 없이 `127.0.0.1:18081`에서 실행할 수 있다.
기본 Worker는 기존 내부 Mock을 사용하며 `app.provider.mode=HTTP`로 실제 HTTP 호출을 선택한다.
HTTP 모드도 알림 앱에는 기존 MySQL/Kafka와 DB 환경변수가 필요하다.

```bash
# Mock 실행 예시 중 하나를 선택
./gradlew mockServer
./gradlew mockServer --args='--mock.http.mode=SERVER_ERROR'
./gradlew mockServer --args='--mock.http.response-delay-ms=2000'
# 별도 터미널에서 알림 앱 실행
./gradlew bootRun --args='--app.provider.mode=HTTP'
```

`POST /pushes`로 수락/429/500/영구 실패와 클라이언트 timeout을 재현하고 `/mock/stats`에서 집계를 확인한다.
요청 한도와 동시 처리 상한이 있으며, timeout 뒤에도 수락 기록이 남을 수 있다.
[Mock 서버 설정](docs/S6_HTTP_MOCK.md)과 [HTTP 연동·timeout·연결 풀](docs/S6_HTTP_CLIENT.md)을 참고한다.
HTTP 클라이언트 기본값은 연결 2개/추가 호출 4개, 풀 대기 200ms/연결 500ms/응답 1000ms/전체 deadline 3000ms다.
자동 HTTP 재시도는 끄며 기존 DB 재시도 최대 3회를 사용한다. timeout 후 중복 수락 가능성과 429 Retry-After 미반영 한계가 있다.

## Worker 처리/대기 상한 (S6)

`app.worker.consumers`(기본 1, 최대 3)과 `max-active`(기본 1)로 소비 스레드와 실제 처리 수를 구분한다.
처리 자리가 없으면 DB 선점 전 FIFO에서 기다린다. `queue-capacity` 기본 2(0~3), `queue-wait-ms` 기본 1000(최대 5000)이다.
대기열 포화/timeout은 해당 offset을 넘기지 않고 소비를 멈추며 원인 해결 후 명시적으로 재시작한다.

```bash
./gradlew bootRun --args='--app.provider.mode=HTTP --app.worker.consumers=3 --app.worker.max-active=2 --app.worker.queue-capacity=1 --app.worker.queue-wait-ms=5000'
```

파티션별 순차 처리와 DB 완료 후 RECORD offset 커밋을 유지한다. 처리/대기 상한은 프로세스별이다.
[문제·대안·선택 이유·종료 정책·검증 결과](docs/S6_WORKER_CONCURRENCY.md)를 참고한다.

## Redis 전역 요청 한도와 Worker 연동 (S6)

여러 클라이언트가 같은 Redis 키에서 토큰을 공유하는 Lua 제한기를 추가했다.
기본 정책은 **초당 10개 보충 / 최대 버스트 10개**이며, Redis 서버 시간으로 보충·확인·차감을 원자적으로 수행한다.
`ALLOWED`, `LIMITED`(재확인 대기 시간), `UNAVAILABLE`(오류/포화/설정 충돌/예산 부재)을 구분하며 오류 시 허용하지 않는다.
최초 설치도 명시적인 빈 예산 초기화가 필요하다. 키 삭제·TTL·예산 유실에서 자동 버스트를 만들지 않는다.

`app.rate-limit.client-enabled=true`이면 비동기 Worker의 **최초 발송과 재시도 모두** 같은 quota를 사용한다.
완료된 메시지 재소비는 quota 없이 처리한다. 발송이 필요하면 DB 트랜잭션을 끝낸 뒤 허가를 기다리고,
허가 후 새 트랜잭션에서 상태를 재확인하여 시도 횟수/lease를 만든다. memory/sync 비교 프로필은 적용 대상이 아니다.

```bash
docker compose --profile redis up -d --wait redis
# 최초 설치, 모든 참여 앱을 실행하기 전 한 번만 초기화
docker compose exec -T redis redis-cli -e --eval /dev/stdin \
  notification-lab:provider:mock:quota , 10 10 \
  < src/main/resources/redis/initialize-provider-quota.lua
./gradlew bootRun --args='--app.provider.mode=HTTP --app.rate-limit.client-enabled=true'
```

기존 MySQL/Kafka와 HTTP Mock 실행도 필요하다. 기본값 false는 Redis 없이 기존 기준선을 실행한다.
`127.0.0.1:${REDIS_PORT:-16379}`, Redis 7.4.11, AOF/전용 volume/`noeviction`을 사용한다.
모든 참여 앱에 같은 Redis/키/rate/burst를 설정한다. 초기화는 토큰 0개에서 시작하고 기존 키는 덮어쓰지 않는다.
**기존 version=1 예산은 새 앱과 호환되지 않는다.** 전체 발송 중지·확인·대기 후 [v2 전환/복구 절차](docs/S6_REDIS_RECOVERY.md)를 따른다.
단순 연결 장애에서 신뢰할 수 있는 기존 v2 예산은 유지한다. 정상 형식의 오래된 snapshot 복원은 자동 감지하지 못하므로 자동 failover를 지원한다고 해석하지 않는다.

`app.rate-limit.wait-timeout-ms` 기본 2000(1~5000)이다. 두 모드 모두 quota 대기 중 DB 시도/lease를 만들지 않는다.
기본 `app.rate-limit.pause-enabled=true`에서는 정상 한도 부족(LIMITED)에 **소비 pause → poll 유지 → 자동 resume**를 사용한다.
DB 선점과 Worker 처리 자리 없이 기다리고, 재개 후 같은 레코드의 상태와 허가를 다시 확인한다.
`wait-timeout-ms`는 `pause-enabled=false`인 이전 동기 대기 비교 모드에만 적용된다.
Redis 오류는 두 모드 모두 소비 중지/명시적 재시작이며, 자동 재개는 정상 한도 부족에만 적용한다.
[설계·종료/리밸런스·poll/heartbeat 검증](docs/S6_CONSUMER_PAUSE.md)을 참고한다.

[문제·알고리즘 비교·정책·실제 Redis 검증·한계](docs/S6_REDIS_RATE_LIMIT.md)를 참고한다.
[연동 경계·대안·복구·실제 통합 검증](docs/S6_WORKER_QUOTA.md)을 함께 참고한다.

## 공급자 Circuit Breaker (S6)

HTTP 공급자의 연속 장애 시 **DB 선점과 Redis quota 소비 전에** 발송을 보류한다.
기본은 꺼져 있으며 비동기 HTTP 모드에서 `--app.circuit-breaker.enabled=true`로 선택한다.
임계값 `failure-threshold=3`, 차단 시간 `open-ms=5000`이 기본이다.

CLOSED → OPEN → HALF_OPEN으로 전환하며, HALF_OPEN에서는 앱당 한 요청으로 복구를 확인한다.
5xx/통신 오류는 장애로 세고 429/개별 요청 오류/로컬 자원 부족은 제외한다.
차단 중에는 기존 pause/poll/resume를 사용해 시도 횟수와 미완료 offset을 보존한다.
상태는 프로세스별이며 재시작하면 CLOSED로 초기화한다. Redis quota와 독립적으로 켤 수 있다.
[문제·대안·선택 이유·상태 전이·실제 인프라 검증·한계](docs/S6_CIRCUIT_BREAKER.md)를 참고한다.

## 실시간·캠페인 분리 (S6)

같은 JAR를 `REALTIME`/`CAMPAIGN` 역할의 별도 프로세스로 실행한다.
각 역할은 요청/Retry/DLQ 토픽, consumer group, Relay 선점 경로와 Worker/HTTP/DB 연결 풀을 따로 사용한다.
공급자 Redis 키는 하나를 공유하며 기본 전체 10/s·burst 10을 **실시간 8/8, 캠페인 2/2**로 예약한다.
사용하지 않는 다른 역할의 몫을 빌리지 않는다. 이 정책은 처리 순서 추월이나 지연 SLO를 보장하지 않는다.

기본 `app.traffic.split-enabled=false`는 기존 단일 REALTIME/v2 예산을 유지한다.
분리는 두 앱 모두 `--app.traffic.split-enabled=true --app.rate-limit.client-enabled=true`로 켜고,
역할만 `--app.traffic.lane=REALTIME` 또는 `CAMPAIGN`으로 구분한다. 기본 pause 모드를 유지해야 한다.
**분리 모드는 v3 빈 예산을 명시적으로 초기화해야 하며 기존 v2와 혼합할 수 없다.**
[선택 이유·역할별 실행·v2/v3 전환·실제 검증·한계](docs/S6_TRAFFIC_ISOLATION.md)를 먼저 확인한다.

개별 캠페인 알림은 역할별 `/notifications`로 가상 대상 한 명씩 접수한다. API 본문으로 역할을 바꾸지 않는다.
재시도/DLQ/선택 재발행은 원래 역할을 보존하며 다른 역할의 redrive는 409다.
S7에서 캠페인 정의와 시작 요청 API를 추가했다. 명시적 Batch 명령으로 대상별 Outbox 인계도 지원한다. MySQL/Kafka/Redis 서버와 호스트 자원은 공유한다.

## 캠페인 생성·시작·조회와 가상 대상자 (S7)

CAMPAIGN 역할의 비동기 앱에서 `POST /campaigns`로 정의를 저장하고,
`POST /campaigns/{id}/start`로 시작 의도를 기록하며 `GET /campaigns/{id}`로 조회한다.
생성에는 `Idempotency-Key`가 필수다. 같은 키/같은 정의는 기존 캠페인을 반환하고 다른 정의는 409다.
최초 시작은 202, 중복 시작은 200이며 DB에 최초 요청 시각을 한 번 저장한다.

캠페인 상태는 `DRAFT → START_REQUESTED`이며 시작 의도를 나타낸다. **시작 요청 저장만으로 Batch를 자동 실행하지 않는다.**
가상 대상 수 1~100만을 정의에 기록한다. `GET /campaigns/{id}/audience?after=0&limit=100`으로
고정 가상 대상자를 최대 1,000명씩 읽고, 응답의 nextAfter로 이어 읽는다.
[집합 고정·keyset·재시작 계약](docs/S7_CAMPAIGN_AUDIENCE.md)을 참고한다.
가상 집합을 규칙으로 계산한다. 시작 요청 후 `./gradlew campaignBatch --args='캠페인_UUID'`로
해당 캠페인의 대상별 알림/발송 작업/Outbox와 checkpoint를 chunk 단위로 함께 저장한다.
FAILED 실행은 같은 명령으로 마지막 커밋에서 재시작하며, 완료 재요청은 기존 결과를 반환한다.
강제 종료된 새 버전 실행도 MySQL 세션 소유권을 확인한 뒤 재개한다. [복구 조건과 한계](docs/S7_CAMPAIGN_RECOVERY.md)를 참고한다.
**Batch COMPLETED는 DB 인계 완료이며 발송 완료가 아니다.** 별도 CAMPAIGN Relay/Worker가 기존 발송 경로를 수행한다.
[실행·checkpoint·ACK 경계·복구 한계](docs/S7_CAMPAIGN_BATCH.md)를 참고한다.
명령은 Relay/Worker를 끄고 종료하지만, 다른 앱에서 이미 실행 중이면 커밋된 Outbox가 처리될 수 있다.
[요청 예제·트랜잭션·동시성·실패 정책·검증](docs/S7_CAMPAIGN_API.md)을 참고한다.

## 테스트와 빌드

테스트가 격리된 MySQL/Kafka/Redis 컨테이너를 생성·삭제하므로 `.env`와 Compose 사전 실행은 필요 없다.
Docker가 없으면 실패하며 자동 스킵하지 않는다. Batch JVM 정지/재개 테스트는 macOS/Linux의 POSIX 신호를 사용한다.

```bash
./gradlew test
./gradlew build
# 별도 1만 건 HTTP → MySQL → Kafka → Worker 정합성 실험
./gradlew reconciliationTest
# 별도 캠페인 DB 인계 부하 (발송하지 않음)
./gradlew campaignLoadTest -PcampaignTargets=100000
# HTTP 발송 사전 실험 (기본 1만 건 + 실시간 동시 요청)
./gradlew campaignDeliveryPilot
# 별도 10만 건 HTTP 발송 실험 (최대 task 90분)
./gradlew campaignDeliveryLoad
# 별도 100만 건 장시간 실험 (최대 task 12시간)
# 별도 실행하기로 결정한 경우에만 다음 명령 사용
# ./gradlew campaignDeliveryMillion
# 실행 JAR
java -jar build/libs/notification-lab-0.3.0-SNAPSHOT.jar
```

`build`는 일반 테스트와 알림 앱/HTTP Mock의 실행 JAR 생성을 포함한다. 1만 건 실험은 별도 명령이며 성능 벤치마크가 아니다.
캠페인 대량 DB 인계도 별도 task로 실행한다. 10만/100만 건의 저장·전체 매핑·완료 재요청을 검증했다.
최종 100만 건 DB 인계는 약 42분 25초였다.
[HTTP 발송 사전 실험](docs/S7_DELIVERY_PILOT.md)은 1만 건을 약 115초에 처리하고 실시간 564건도 함께 대조했다.
[10만 건 HTTP 발송](docs/S7_DELIVERY_LOAD.md)은 캠페인100000+실시간5342건을 전수 대조했다.
발송 구간은 약22분27초/초당74.239건이었다. 혼합 실시간p95는918.136ms로 단독553.657ms보다 증가했다.
[100만 건 실험 도구](docs/S7_DELIVERY_MILLION.md)는 준비했고 2026-11-02 실제 실험을 시작했다.
첫 실행은 실험용 실시간 접수 클라이언트의5초 응답 대기 초과로 실패했다. 마지막 캠페인 진행 관측은 약38.4만 건이며 최종 전수 대조는 미완료다.
실패 원본과 진단 보강을 기록했고 원인 진단 후 재실행을 결정한다. 운영 성능/실시간 SLO 보장과 구분한다.
[timeout 후 작은 진단](docs/S7_TIMEOUT_DIAGNOSIS.md)에서는1만 건 HTTP와1만/10만 행 SQL을 확인했고 같은 timeout은 미재현이다.
별도 `./gradlew outboxQueryProbe`로 DB 실행 계획을 관측한다. 발송 성공을 검증하는 task는 아니다.
`./gradlew outboxStatisticsProbe`는 격리 테이블의 오래된 통계를 고정하고 ANALYZE TABLE 전후 계획·실행 행 수를 비교한다.
통계 갱신 후 실시간 스캔 감소를 확인했지만 이전100만 건 timeout의 원인은 아직 미확정이다.
[교차 역할 잠금 재현과 수정](docs/S7_OUTBOX_CLAIM_LOCKS.md)에서는 역할별 인덱스 명시와 claim 전용 READ COMMITTED를 적용했다.
`./gradlew outboxLockProbe`로 기본 자동 통계·잠금·대기 대조군을 확인한다. 기존 진단 task는 현재 선점 SQL을 캡처한다.
수정 후10만 건 재검증도 캠페인100000+실시간7269건을 전수 대조했다(발송약30분41초/54.333건·초).
이번 혼합 APIp95는203.917ms/최대1904.024ms였다. 정합성은 통과했지만 이전보다 처리량은 낮았고100만 건 재실행은 미실행이다.
[DB 인계 측정 범위·설정·실패와 재실행 결과](docs/S7_CAMPAIGN_LOAD.md)를 참고한다.
[Relay 구간 계측과 완료 저장 묶음 비교](docs/S7_RELAY_PUBLISH_BATCH.md)도 진행했다.
1만 건 각2회에서 단건109.349~131.447초/최대10건93.949~100.122초였고 HTTP42,160건을 전수 대조했다.
기본값은1이며 `app.async.relay.publish-batch-size=10`은 선택 옵션이다. 재발행 가능 범위가 늘어나는 비용이 있다.
실시간 p95 개선과10만/100만 건의 같은 속도는 보장하지 않는다.
[후속10만 건 비교와 잠금 수정](docs/S7_RELAY_100K_COMPARISON.md)에서 구 OR구현은 처리량이 늘었지만 실시간 tail 지연도 커졌다.
별도 SQL 진단의 PRIMARY 전체 스캔/역할 간 차단을 수정해 현재는 최대10개의 기본키 단건 UPDATE를 한 번 커밋한다.
수정판557b2cf도10만 건1170.631초/85.424건·초, HTTP104639건의 전수 대조를 통과했다.
첫 단건 기준선 재사용 대비 처리량29.24% 증가였지만 혼합 APIp95는297.106→364.875ms로 악화했다.
기본값1과 실시간 설정을 유지하며 캠페인10건은 선택 옵션이다.100만 건 재실행은 아직 하지 않았다.

[로컬 Relay1/2개 사전 비교](docs/S7_RELAY_SCALING_PILOT.md)는 같은 Worker3/Hikari4/캠페인200/s에서
1만 건을1→2→2→1 순서로 처리했다. 네 실행의 HTTP41711건 전수 대조를 통과했지만,
처리량 범위는1개120.646~131.381/s, 2개98.426~137.346/s로 일관된 개선을 확인하지 못했다.
`./gradlew relayScalingPilot -PcampaignRelays=1` 또는2로 재현한다(완료 저장10건 고정).
테스트 전용 같은 JVM의 독립 루프이며 기본 앱 설정이나 여러 서버 배포 기능을 변경하지 않는다.
다음 작은 진단은 공유 DB 연결 취득/명령/commit과offset 저장 비용을 나눠 확인하는 것이다.

10만 건 발송은 별도 `campaignDeliveryLoad` task로 실행한다. [상한·전수 검증·측정 경계](docs/S7_DELIVERY_LOAD.md)를 참고한다.
각 테스트 결과는 `build/reports/tests/test/`, `build/reports/tests/reconciliationTest/`, `build/reports/tests/campaignLoadTest/`, `build/reports/tests/campaignDeliveryPilot/`, `build/reports/tests/campaignDeliveryLoad/`에 저장된다.
100만 건 task를 실제 실행할 경우 `build/reports/tests/campaignDeliveryMillion/`에 별도 결과를 만든다.
코드를 바꾸지 않고 JAR만 다시 만들려면 `./gradlew bootJar`를 사용한다.

## 이전 단계 비교

| 프로필 | 생성 응답 | 저장 | Kafka |
| --- | --- | --- | --- |
| 기본(S3) | 접수 커밋 후 202 | MySQL + Outbox | 사용 |
| `sync`(S2) | 동기 결과 커밋 후 201 | MySQL | 사용하지 않음 |
| `memory`(S1) | 동기 처리 후 201 | 메모리 최대 10,000건 | 사용하지 않음 |

```bash
./gradlew bootRun --args='--spring.profiles.active=sync'
./gradlew bootRun --args='--spring.profiles.active=memory'
```

`memory` 앱은 Docker 없이 실행 가능하지만 전체 테스트는 Docker가 필요하다.
S2 시절 이미 남은 `PROCESSING` 레코드는 Outbox가 없으므로 S3에서 자동 복구하지 않는다.

## 지표

- `notification_provider_attempts_total`, `notification_completed_total{status=...}`: 시도/저장된 최종 결과. 중간 재시도 예약은 완료에 포함하지 않음.
- `notification_retry_scheduled_total`: 성공적으로 저장한 일시 실패의 후속 예약 수.
- `notification_processing_seconds_*`: 공급자 처리와 결과 저장 시간. 큐 대기 제외.
- `http_server_requests_seconds_*`: HTTP 처리 시간.
- `outbox_unpublished`, `outbox_failed`, `outbox_oldest_age_seconds`: 해당 프로세스 역할의 미발행 수(FAILED 포함), 실패 수, 가장 오래된 미발행 시간.
- `notification_consumer_lag`: 해당 역할의 요청/Retry 두 토픽의 end offset에서 Worker 그룹 committed offset을 뺀 파티션별 합계. DLQ 제외.
- `outbox_sample_age_seconds`, `notification_consumer_sample_age_seconds`: 마지막 지표 수집 성공 후 경과 시간.
- `notification_worker_active`, `notification_worker_waiting`, `notification_worker_admission_rejected_total{reason=...}`: 처리/대기 수와 진입 거절 횟수.
- `notification_worker_running`: 소비 중 1, 정지 0.
- `notification_rate_limit_decisions_total{outcome=allowed|limited|unavailable}`, `notification_rate_limit_connections_active`: 제한기 호출 결과와 사용 중 Redis 연결 수. 클라이언트 활성화 시 등록. 허가 판정 지표이며 실제 HTTP 호출 수와 다를 수 있음.
- `notification_worker_quota_waiting`, `notification_worker_quota_rejected_total{reason=timeout|unavailable|stopping|interrupted}`: 이전 동기 비교 모드의 대기 수와 두 모드의 진입 거절. pause 중 waiting은 0.
- `notification_provider_circuit_state`, `notification_provider_circuit_failures`, `notification_provider_circuit_probe_active`: 프로세스별 회로 상태(0/1/2), 실패 수, 탐침 점유. 상세 카운터는 [회로 문서](docs/S6_CIRCUIT_BREAKER.md) 참고.
- `notification_worker_circuit_pauses_pending`, `notification_worker_circuit_pauses_total`, `notification_worker_circuit_resumes_total`: 회로 차단 사유의 재개 예약과 요청 수.
- `notification_worker_quota_pauses_pending`, `notification_worker_quota_pauses_total`, `notification_worker_quota_resumes_total`: 재개 예약 수와 pause/resume 요청 수. 실제 pause 확정은 컨테이너 상태와 함께 확인.
- `notification_worker_exhausted_total`: 복구 선점 상한을 소진해 최종 저장한 작업 수.
- `outbox_publish_failures_total`, `outbox_published_total`, `notification_worker_failures_total`, `notification_worker_replays_total`.

DB/Kafka 지표는 약 5초 주기로 수집하며 수집 실패는 `NaN`으로 표시한다. 실패를 lag 0으로 표현하지 않는다.
카운터는 프로세스 기준이며 정확한 대조는 DB/offset으로 수행한다. ID나 수신자/payload를 메트릭 label에 넣지 않는다.
`health UP`만으로 Worker 정상 처리나 lag 0을 판단하지 않는다.

## 한계와 문서

자동 Redis failover, 실제 회원 DB 대상자 조회, 다중 DB/failover 환경의 Batch 복구, 실제 FCM/APNs, 인증과 운영 배포는 아직 구현하지 않았다.
데이터 보관/정리 정책도 없다. 브로커 보관 기간이 지난 미처리 메시지, volume 손실까지 보호하지 않는다.

- [PLAN.md](PLAN.md): 순서와 완료 조건
- [docs/PROGRESS.md](docs/PROGRESS.md): 실제 실행 결과와 다음 작업
- [docs/S6_TRAFFIC_ISOLATION.md](docs/S6_TRAFFIC_ISOLATION.md): 역할별 자원/토픽, 고정 예약 quota와 초기화·전환
- [docs/S6_REDIS_RECOVERY.md](docs/S6_REDIS_RECOVERY.md): Redis 장애/예산 유실의 발송 중지, 최초 초기화·v2 전환·통제된 복구
- [docs/S6_CIRCUIT_BREAKER.md](docs/S6_CIRCUIT_BREAKER.md): 공급자 회로 차단과 단일 half-open 탐침, 선점/offset 보존
- [docs/S6_CONSUMER_PAUSE.md](docs/S6_CONSUMER_PAUSE.md): quota 부족 시 자동 pause/resume, poll 유지·종료·리밸런스 검증
- [docs/S6_WORKER_QUOTA.md](docs/S6_WORKER_QUOTA.md): 최초/재시도 공통 quota, 선점 전 대기와 offset 보존
- [docs/S6_REDIS_RATE_LIMIT.md](docs/S6_REDIS_RATE_LIMIT.md): Redis Lua 공유 토큰 버킷, 속도/버스트 계약과 검증
- [docs/S6_WORKER_CONCURRENCY.md](docs/S6_WORKER_CONCURRENCY.md): Worker 처리/대기 상한과 파티션별 offset·종료 검증
- [docs/S6_HTTP_CLIENT.md](docs/S6_HTTP_CLIENT.md): Worker HTTP 선택, timeout/연결 풀/전체 deadline과 실패 분류
- [docs/S6_HTTP_MOCK.md](docs/S6_HTTP_MOCK.md): 별도 HTTP Mock, 지연/429/500/한도 및 timeout의 불확실성
- [docs/S5_GRACEFUL_SHUTDOWN.md](docs/S5_GRACEFUL_SHUTDOWN.md): SIGTERM 종료 순서, 신규 접수 차단과 유예 초과 후 복구/중복 호출
- [docs/S5_RESTART_REBALANCE.md](docs/S5_RESTART_REBALANCE.md): 처리 중 합류, Relay/Worker JVM 재시작과 파티션·호출·offset 대조
- [docs/S5_KAFKA_RECOVERY.md](docs/S5_KAFKA_RECOVERY.md): 실제 브로커 중단·복구, 접수/발행 대조와 상한 소진의 한계
- [docs/S5_WORKER_OFFSET_CRASH.md](docs/S5_WORKER_OFFSET_CRASH.md): DB 완료 후 Worker JVM 종료, 미커밋 레코드 재소비와 공급자 재호출 방지
- [docs/S5_OUTBOX_ACK_CRASH.md](docs/S5_OUTBOX_ACK_CRASH.md): ACK 후 Relay JVM 강제 종료, 동일 이벤트 재발행과 완료 작업의 중복 발송 차단
- [docs/S4_UNCERTAIN_OUTCOMES.md](docs/S4_UNCERTAIN_OUTCOMES.md): 중단 지점별 증거, 중복 가능성, 확인·복구 절차
- [docs/S4_DLQ_REDRIVE.md](docs/S4_DLQ_REDRIVE.md): 선택 재발행 API, 감사 이력, 계보·횟수 상한
- [docs/S4_RETRY_TOPICS_DLQ.md](docs/S4_RETRY_TOPICS_DLQ.md): 토픽 분리, DLQ 계약, ACK/offset 관계
- [docs/S4_RETRY_POLICY.md](docs/S4_RETRY_POLICY.md): 오류 분류, 횟수 상한, backoff+jitter, 영속 예약
- [docs/S4_DELIVERY_TASKS.md](docs/S4_DELIVERY_TASKS.md): 작업 ID, 이력, 유한 복구와 버전 전환
- [docs/S3_ASYNC.md](docs/S3_ASYNC.md): 트랜잭션, 장애 상태와 실험
- [docs/S2_PERSISTENCE.md](docs/S2_PERSISTENCE.md): 동기 MySQL 기준선
- [CONTRIBUTING.md](CONTRIBUTING.md), [AGENTS.md](AGENTS.md): 개발/커밋/푸시 규칙
- [.github/pull_request_template.md](.github/pull_request_template.md): 선택적 PR 양식
