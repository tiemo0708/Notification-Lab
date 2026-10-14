# S4 Retry 토픽과 DLQ

## 토픽과 지연

기본 비동기 프로필은 다음 세 토픽을 각각 파티션 3개, 복제본 1개로 생성한다.

| 설정 | 기본 이름 | 역할 |
| --- | --- | --- |
| `app.async.topic` | `notification.requested.v1` | 최초 요청, 이전 버전이 이미 발행한 재시도 |
| `app.async.retry-topic` | `${app.async.topic}.retry` | due가 지난 공급자 재시도 |
| `app.async.dead-letter-topic` | `${app.async.topic}.dlq` | 최종 실패 조사용 이벤트 |

세 이름은 서로 달라야 하며 겹치면 설정 검증에서 기동을 거절한다.
Worker 하나가 요청/Retry 토픽을 함께 구독한다. concurrency=1, max-poll-records=1은 유지한다.
DLQ에는 자동 소비자나 자동 재발송이 없다. 처리량을 높이기 위한 별도 Worker 증설은 이번 범위가 아니다.

지연은 토픽 이름이나 Kafka 기능에 맡기지 않는다.
[재시도 정책](S4_RETRY_POLICY.md)의 DB next_attempt_at과 Outbox available_at을 사용해
예약 시간이 지난 뒤 Retry 토픽으로 발행한다. 대기 중인 재시도는 Kafka에 아직 없으므로
정상 흐름에서 소비 스레드 sleep/pause 없이 새 요청을 처리한다.
Worker도 next_attempt_at을 확인한다. 강제로 조기 발행된 이벤트는 offset을 넘기지 않고 소비를 중지한다.
이 경우 due 이후 명시적으로 Worker를 재시작해야 한다. 자동 pause/resume은 구현하지 않았다.

## 최종 실패의 영속 인계

다음 결과를 새로 저장할 때 `NOTIFICATION_DEAD_LETTER` Outbox를 같은 DB 트랜잭션에 넣는다.

- 영구 공급자 실패 (`PERMANENT_FAILURE`).
- 전체 선점 상한까지 사용한 일시 실패 (`RETRYABLE_FAILURE`).
- 예상 밖 공급자 예외 (`INTERNAL_FAILURE`).
- 마지막 lease 만료 후 복구 상한 소진 (`INTERNAL_FAILURE / ATTEMPTS_EXHAUSTED`).

마지막 항목은 외부 실패가 확정됐다는 뜻이 아니다. UNKNOWN 이력과 함께 조사해야 한다.
성공 결과에는 DLQ를 만들지 않는다. 과거에 이미 완료된 실패도 임의로 backfill하거나 다시 발송하지 않는다.
DB 오류, JSON 해석 실패, 알 수 없는 이벤트, taskId 불일치는 여전히 fail-stop이다.
출처를 검증하지 못한 원문을 DLQ에 복사하거나 처리 성공으로 인정하지 않는다.

DLQ payload는 다음 형식이다. 예시 ID는 가상 값이며 attemptCount는 실제 공급자 호출 수가 아니라 선점 수다.

```json
{
  "schemaVersion": 1,
  "eventType": "NOTIFICATION_DEAD_LETTER",
  "eventId": "00000000-0000-0000-0000-000000000004",
  "notificationId": "00000000-0000-0000-0000-000000000001",
  "taskId": "00000000-0000-0000-0000-000000000002",
  "sourceEventId": "00000000-0000-0000-0000-000000000003",
  "retryGeneration": 2,
  "status": "RETRYABLE_FAILURE",
  "errorCode": "MOCK_UNAVAILABLE",
  "attemptCount": 3,
  "maxAttempts": 3
}
```

수신자, 제목, 본문, lease 토큰, 예외 원문은 담지 않는다.
sourceEventId는 마지막 논리적 요청/재시도 이벤트이며 lease 복구로 새 generation을 만들지 않는다.
상세 시도 이력은 `GET /notifications/{id}/delivery` 또는 DB에서 조회한다.
최종 상태가 보인다고 DLQ가 이미 Kafka에 발행된 것은 아니다. Outbox 상태를 별도로 확인한다.

## ACK와 offset의 관계

| 상황 | DB/Outbox | 원본 offset |
| --- | --- | --- |
| 재시도 또는 DLQ INSERT 실패 | 결과/이력/후속 이벤트 전체 롤백 | 커밋하지 않고 소비 중지 |
| 영속 인계 성공, 후속 Kafka 발행 전 | 후속 Outbox PENDING | 처리 가능 |
| 후속 publish ACK 대기 | Outbox PROCESSING, published_at null | 이미 영속 인계된 원본은 처리 가능 |
| 후속 publish 실패 | 기존 유한 backoff로 PENDING, 한도 소진 시 FAILED | 되돌리지 않음; Outbox에 책임 보관 |
| 후속 publish ACK 성공 | 그 뒤 DB에 PUBLISHED 저장 | DB와 Kafka의 원자적 동시 커밋 아님 |
| ACK 후 PUBLISHED 저장 실패 | lease 만료 후 같은 이벤트 재발행 가능 | 원본을 다시 호출할 이유가 아님 |

Worker가 직접 DLQ publish ACK를 기다리는 구조가 아니다.
원본 offset 전에 DB에 후속 책임을 영속화하는 Outbox 방식이다.
Relay의 실제 publish는 DB 트랜잭션 밖에서 실행하며 ACK 전에는 PUBLISHED를 저장하지 않는다.

Outbox 유니크 제약으로 알림마다 DLQ 이벤트 하나를 생성한다.
DLQ Outbox의 retry_generation은 0이고 payload의 retryGeneration은 실패한 작업의 세대를 나타낸다.
ACK/DB 갱신 사이 장애로 Kafka에는 같은 eventId가 여러 번 올 수 있다.
향후 DLQ 소비/재발행 도구는 이 eventId 기준 중복 방지를 해야 한다. exactly-once를 주장하지 않는다.
DLQ 발행 자체도 기존 Relay 최대 10회 정책을 따른다. FAILED는 수동 조사하며 또 다른 DLQ로 순환시키지 않는다.

## 조회와 업그레이드

`notification_consumer_lag`는 이제 요청과 Retry 토픽의 Worker 미커밋 offset 합계다.
DLQ는 자동 소비하지 않으므로 이 지표에 넣지 않는다.
Outbox 미발행/FAILED/age 지표에는 요청·재시도·DLQ 이벤트를 모두 포함한다.
ID를 메트릭 label로 추가하지 않는다.

개별 DLQ 발행 상태는 로컬 DB에서 다음과 같이 확인할 수 있다.

```sql
SELECT event_id, notification_id, status, attempt_count, created_at, published_at
FROM outbox_event
WHERE event_type = 'NOTIFICATION_DEAD_LETTER'
ORDER BY created_at, event_id
LIMIT 100;
```

새 스키마 변경은 없다. V5의 event_type/JSON payload/유니크 제약을 사용한다.
기존 앱과 Relay/Worker를 모두 종료하고 새 버전을 시작한다.
이전 Relay는 DLQ까지 요청 토픽에 발행하므로 혼합 버전 실행은 지원하지 않는다.
기존 요청 토픽의 미처리 재시도도 새 Worker가 계속 읽으며, DB에 남은 재시도는 새 Relay가 Retry 토픽으로 보낸다.
이미 쌓인 메시지가 있는 토픽 이름/그룹을 임의로 변경하지 않는다. 변경 시 기존 토픽/offset 이관이 별도로 필요하다.

## 검증 범위와 다음 작업

실제 MySQL/Kafka 테스트로 토픽 분리, due 전 미발행과 새 요청 처리,
Retry lag, DLQ 내용/발행/비소비, 완료 replay, DLQ INSERT 실패 롤백을 검증한다.
publish future 실패와 ACK 이후 DB 저장 실패를 각각 주입한다.
브로커 프로세스의 실제 중단이나 다중 인스턴스 복구 실험을 대신하는 것은 아니다.
`./gradlew --no-daemon --console=plain build`: **82개 통과, 실패/오류/스킵 0**.
상세 실행 결과는 [PROGRESS.md](PROGRESS.md)에 기록한다.

다음은 DLQ 선택 재발행과 재발행 원인/시도 이력 정책이다.
이번 단계는 실패 이벤트를 보존하고 전달하는 범위이며 관리 API/자동 재발송 기능은 포함하지 않는다.
