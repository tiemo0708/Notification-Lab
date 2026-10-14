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
`docker compose down -v`는 두 시스템의 실험 데이터를 삭제한다. 의도한 초기화에만 사용한다.
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
같은 키에 recipientId/title/body 중 하나라도 다르면 409이다. 200은 처리 성공을 뜻하지 않는다.
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
기존 앱을 모두 종료한 뒤 새 버전을 실행한다. V6 DB를 사용하며 재시도/DLQ 이벤트는 이전 앱·Relay와 혼합 실행하지 않는다.

기본 Mock 지연은 100ms(허용 0~5000ms), SUCCESS다. 앱 재시작으로 설정한다.

```bash
./gradlew bootRun --args='--app.mock.outcome=TRANSIENT_FAILURE'
./gradlew bootRun --args='--app.mock.outcome=PERMANENT_FAILURE'
./gradlew bootRun --args='--app.mock.delay-ms=1000'
```

DB 풀은 최대 8개, Relay는 한 번에 한 publish/한 주기 최대 50건, Worker concurrency 1/poll 1건이다.
단일 브로커 고가용성, 다중 인스턴스의 모든 장애 조합, 운영 성능을 입증한 구성이 아니다.

## 테스트와 빌드

테스트가 격리된 MySQL/Kafka 컨테이너를 생성·삭제하므로 `.env`와 Compose 사전 실행은 필요 없다.
Docker가 없으면 실패하며 자동 스킵하지 않는다.

```bash
./gradlew test
./gradlew build
# 별도 1만 건 HTTP → MySQL → Kafka → Worker 정합성 실험
./gradlew reconciliationTest
# 실행 JAR
java -jar build/libs/notification-lab-0.3.0-SNAPSHOT.jar
```

`build`는 일반 테스트와 실행 JAR 생성을 포함한다. 1만 건 실험은 별도 명령이며 성능 벤치마크가 아니다.
각 테스트 결과는 `build/reports/tests/test/`, `build/reports/tests/reconciliationTest/`에 저장된다.
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
- `outbox_unpublished`, `outbox_failed`, `outbox_oldest_age_seconds`: 미발행 수(FAILED 포함), 실패 수, 가장 오래된 미발행 시간.
- `notification_consumer_lag`: 요청/Retry 두 토픽의 end offset에서 Worker 그룹 committed offset을 뺀 파티션별 합계. DLQ 제외.
- `outbox_sample_age_seconds`, `notification_consumer_sample_age_seconds`: 마지막 지표 수집 성공 후 경과 시간.
- `notification_worker_running`: 소비 중 1, 정지 0.
- `notification_worker_exhausted_total`: 복구 선점 상한을 소진해 최종 저장한 작업 수.
- `outbox_publish_failures_total`, `outbox_published_total`, `notification_worker_failures_total`, `notification_worker_replays_total`.

DB/Kafka 지표는 약 5초 주기로 수집하며 수집 실패는 `NaN`으로 표시한다. 실패를 lag 0으로 표현하지 않는다.
카운터는 프로세스 기준이며 정확한 대조는 DB/offset으로 수행한다. ID나 수신자/payload를 메트릭 label에 넣지 않는다.
`health UP`만으로 Worker 정상 처리나 lag 0을 판단하지 않는다.

## 한계와 문서

Redis rate limit, 캠페인/Batch, 실제 FCM/APNs, 인증과 운영 배포는 아직 구현하지 않았다.
데이터 보관/정리 정책도 없다. 브로커 보관 기간이 지난 미처리 메시지, volume 손실까지 보호하지 않는다.

- [PLAN.md](PLAN.md): 순서와 완료 조건
- [docs/PROGRESS.md](docs/PROGRESS.md): 실제 실행 결과와 다음 작업
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
