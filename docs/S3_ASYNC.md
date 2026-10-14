# S3: Outbox와 Kafka 비동기 흐름

이 문서는 S3 기준선이다. 이후 추가된 요청 접수 중복 방지는 [S4 API 멱등성](S4_IDEMPOTENCY.md),
현재 이벤트 스키마와 유한 복구 정책은 [S4 발송 작업/시도 이력](S4_DELIVERY_TASKS.md)을 참고한다.

## 계약과 비교 실행

기본 POST는 `QUEUED` 알림과 `NOTIFICATION_REQUESTED` Outbox를 한 트랜잭션으로 커밋한 후
HTTP 202 + Location을 반환한다. Kafka 연결 성공은 접수 조건이 아니다. 브로커 없이 앱을 처음 기동해 토픽 생성에 실패했다면
브로커 복구 후 앱을 재시작해 토픽 생성을 완료해야 한다.
`sync` 프로필은 S2의 MySQL/HTTP 201, `memory`는 S1의 비영속/HTTP 201 비교 실행이다.

V1 migration은 보존하고 V2로 QUEUED, Relay/Worker lease 컬럼과 CHECK 제약을 추가한다.
S2에서 남은 PROCESSING에는 Outbox가 없으므로 자동으로 발송 대상으로 전환하지 않는다.
과거 결과와 상태를 임의로 덮어쓰지 않는다.

## 트랜잭션 경계

| 구간 | DB 트랜잭션 | 완료 기준 |
| --- | --- | --- |
| API 접수 | 알림 INSERT + Outbox INSERT | 둘 다 커밋한 후 202 |
| Relay 선점 | due PENDING/PROCESSING 후보 잠금, token과 lease 갱신 | 30초 lease, attempt +1 |
| Kafka publish | 없음 | future 성공 확인 (acks=all) |
| Relay 완료 | event ID + token 조건 UPDATE | PUBLISHED 저장 |
| Worker 선점 | 이벤트 연결 확인 + 알림 조건 UPDATE | QUEUED 또는 만료된 PROCESSING만 획득 |
| Mock 호출 | 없음 | 성공/실패 분류 |
| Worker 결과 | 알림 ID + token 조건 UPDATE | 최종 결과 커밋 |
| Kafka offset | DB와 별도 | listener 정상 반환 후 RECORD 커밋 |

두 시스템을 하나의 로컬 트랜잭션으로 취급하지 않는다. 중간 실패 구간을 숨기지 않는다.
Kafka 메시지에는 schemaVersion=1/eventId/notificationId만 넣으며 본문/수신자 정보는 DB에서 조회한다.
notificationId를 key로 써 동일 알림의 이벤트가 같은 파티션으로 가게 한다.

Relay는 PENDING과 PROCESSING을 각각 `status = ?`, `available_at <= now`로 조회한다.
기존 `(status, available_at, event_id)` 인덱스로 상태별 최대 한 건을 `FOR UPDATE SKIP LOCKED`하고,
두 후보 중 더 오래된 것을 선점한다. 전체 backlog를 매번 정렬하지 않는다.
`PROCESSING.available_at`은 lease 만료 시간, `PENDING.available_at`은 다음 시도 가능 시간이다.

## 상한과 실패 정책

- Relay: 동시 publish 1, 주기당 최대 50건, 기본 fixed delay 100ms.
- DB 선점 lease 30초, publish 대기 12초, producer max.block 3초/delivery timeout 10초.
- producer buffer 1MiB, 라이브러리 내부 retry 3회. Outbox 시도 최대 10회(프로세스 종료로 만료된 시도도 포함).
- 발행 실패 backoff 1, 2, 4, 8, 16, 32, 60초 상한. 최종 실패는 `FAILED`로 남는다.
- Worker concurrency 1, max.poll.records 1, max.poll.interval 60초. Mock 지연 최대 5초.
- consumer auto commit 비활성, AckMode.RECORD, 오류 핸들러는 CommonContainerStoppingErrorHandler.
- Worker 오류는 자동 무한 재시도/skip하지 않는다. 원인 해결과 lease 만료 후 앱 재시작이 필요하다.
- DB Hikari 최대 8, 접속 대기 3초. HTTP thread 최대 32/accept queue 64/connection 256.

| 실패 시점 | 남는 상태와 복구 |
| --- | --- |
| API Outbox INSERT 실패 | 알림 INSERT도 롤백, HTTP 503 |
| 접수 후 브로커 불가 | QUEUED + PENDING, Relay backoff 후 재시도 |
| Relay 선점 후 종료 | lease 만료 후 새 token으로 재선점 |
| publish ACK 이후 DB 저장 실패 | 재발행 가능. 완료 결과가 있으면 Worker가 Mock 재호출 없이 처리 |
| 최종 publish 시도 실패/lease 만료 | FAILED Outbox 보존. 미발행 지표에 포함, 수동 조사 필요 |
| Worker 결과 DB 저장 실패 | PROCESSING, consumer 중지, offset 미커밋 |
| Worker 결과 커밋 후 offset 미커밋 | 재소비 시 DB 최종 결과를 확인하고 offset 처리 |
| 공급자 성공 직후 DB 저장 전 종료 | 재호출 가능, 외부 exactly-once 보장 불가 |
| 오래된 소유자 결과 저장 | token 조건이 불일치하면 업데이트 거절 |

Worker 선점은 원자적 조건 UPDATE이며 단순 exists 검사만으로 중복 실행 방지를 주장하지 않는다.
다만 lease는 외부 공급자에 대한 fencing이 아니다. 오래 멈춘 프로세스와 새 프로세스가
겹치면 외부 호출 중복은 여전히 가능하다. 다중 인스턴스/rebalance/강제 종료 실험은 S5에 남긴다.
S3의 RETRYABLE_FAILURE는 분류된 최종 결과이다. 자동 재발송, 요청 Idempotency-Key, Retry/DLQ는 S4다.

## 운영 관찰과 수동 복구

Prometheus의 `outbox_unpublished`에는 PENDING/PROCESSING/FAILED가 포함된다.
`outbox_failed`, `outbox_oldest_age_seconds`, `notification_consumer_lag`,
`notification_worker_running` 및 두 sample age 지표를 같이 본다.
수집 실패는 NaN이고, health UP 자체는 Worker 처리 완료를 뜻하지 않는다.

FAILED 이벤트의 `notification_id`로 알림 결과를 함께 조회한다. ACK 결과가 불확실했으면
알림이 이미 완료되었을 수도 있다. 원인을 확인하지 않고 전체 이벤트/offset을 초기화하지 않는다.
이번 단계에는 FAILED 재발행 관리 API를 만들지 않았다.

Kafka는 단일 KRaft broker, 파티션 3, replication factor 1, consumer group `notification-worker-v1`이다.
기존 토픽의 파티션/보관 정책을 임의로 바꾸는 것은 별도 실험이다.
브로커 보관 기간 이후 미처리 메시지 복구, volume 손실과 HA는 보장하지 않는다.

## 재현 명령과 실험 조건

```bash
./gradlew --no-daemon --console=plain build
./gradlew --no-daemon --console=plain reconciliationTest
```

일반 테스트와 1만 건 실험은 각각 별도 Gradle 테스트 보고서에 기록된다.
후자는 실제 loopback HTTP POST 10,000건, 제출 동시성 8(8개 작업이 각 1,250건 순차 요청),
실제 MySQL 8.4.8/Kafka 4.0.2, 실제 MockPushProvider SUCCESS/지연 0ms를 사용한다.
Worker concurrency 1, 파티션 3, Relay/DB/producer 설정은 위 상한을 따른다.
10,000건 대기 작업을 메모리 executor queue에 한꺼번에 넣지 않는다.

접수 수 = DB 알림 수 = 공급자 수락 + 실패 + 미완료 수를 대조하고,
Outbox PUBLISHED 수/미발행 수, provider 시도 수와 committed offset 합계/lag를 추가 검증한다.
이 실험은 정상 흐름의 정합성 검사이며 운영 성능이나 실제 단말 도착을 측정하지 않는다.
실행 환경·결과·미검증 범위는 [PROGRESS.md](PROGRESS.md)에 기록한다.

## 참고한 공식 문서

- [Spring Kafka 3.3 오류 처리](https://docs.spring.io/spring-kafka/reference/3.3/kafka/annotation-error-handling.html): 컨테이너 중지와 미완료 offset 보존 정책.
- [Apache Kafka 4.0 Quick Start](https://kafka.apache.org/40/getting-started/quickstart/): 공식 Docker 이미지와 KRaft 실행.
- [Testcontainers Kafka](https://java.testcontainers.org/modules/kafka/): Apache Kafka 컨테이너 기반 통합 테스트.
