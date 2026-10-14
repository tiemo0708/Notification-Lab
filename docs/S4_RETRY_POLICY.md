# S4 일시 실패의 지연 재시도

기본 비동기 프로필에 자동 재시도 분류, 유한 횟수, 지수 backoff와 equal jitter를 추가했다.
최초 구현은 기존 Outbox와 `notification.requested.v1` 토픽을 사용했다.
이후 [Retry 토픽/DLQ 단계](S4_RETRY_TOPICS_DLQ.md)에서 재시도 목적지를 분리하고 최종 실패의 DLQ 인계를 추가했다.
수동 선택 재발행은 [DLQ 재발행 정책](S4_DLQ_REDRIVE.md)에 따라 원본을 보존하고 새 작업으로 접수한다. sync/memory 비교 프로필은 한 번만 호출한다.

## 상태와 횟수

| 공급자 결과 | 남은 선점 예산 | 저장 동작 |
| --- | --- | --- |
| `RETRYABLE_FAILURE` | 있음 | 시도 결과를 기록하고 `WAITING_RETRY` + 다음 Outbox 예약 |
| `RETRYABLE_FAILURE` | 없음 | 최종 `RETRYABLE_FAILURE`, 공급자 오류 코드 유지 |
| `PERMANENT_FAILURE` | 무관 | 최종 영구 실패, 자동 재시도 없음 |
| 예상 밖 예외 / `INTERNAL_FAILURE` | 무관 | 최종 내부 오류, 자동 재시도 없음 |
| `PROVIDER_ACCEPTED` | 무관 | 최종 공급자 수락, 단말 도착 보장 아님 |

`app.delivery.max-attempts`는 최초 선점, 일시 실패 재시도, 결과 불확실성에 따른 lease 복구를
모두 합친 상한이다. 기본 3, 허용 1~10이며 접수할 때 작업에 고정한다.
새 설정으로 재시작해도 기존 작업의 상한은 늘어나지 않는다. max-attempts=1이면 자동 재시도하지 않는다.

`WAITING_RETRY`의 completedAt은 null, errorCode는 마지막 일시 실패 코드다.
`GET /notifications/{id}/delivery`의 nextAttemptAt에 UTC 예약 시각을 노출하고,
선점하면 null로 지운다. 개별 이력에는 각 시도의 `RETRYABLE_FAILURE`가 남는다.
같은 Idempotency-Key 재요청은 현재 상태를 조회할 뿐 예약이나 횟수를 초기화하지 않는다.
최종 실패 이후 같은 키 또는 Kafka 이벤트 replay로 새 발송을 만들지 않는다.

최종 선점의 lease까지 만료되면 기존 정책대로 추가 호출 없이
`INTERNAL_FAILURE / ATTEMPTS_EXHAUSTED`와 `UNKNOWN` 이력을 저장한다.
이 상태는 외부 발송 실패 확정이 아니다. 자세한 경계는 [작업/시도 이력](S4_DELIVERY_TASKS.md)을 참고한다.

## backoff와 예약

실패한 시점의 전체 선점 횟수를 n이라 할 때 지연을 밀리초로 계산한다.

```text
ceiling = min(retryMaxDelayMs, retryBaseDelayMs * 2^(n - 1))
delay   = uniform integer [ceil(ceiling / 2), ceiling]
nextAttemptAt = DB UTC 현재 시각 + delay
```

- `app.delivery.retry-base-delay-ms`: 기본 1000, 허용 100~60000.
- `app.delivery.retry-max-delay-ms`: 기본 60000, 허용 100~60000, base 이상.
- 기본 상한 3회라면 연속 일시 실패의 대기는 500~1000ms, 1000~2000ms다.
- 지연 상한은 예약 계산의 상한이다. Relay polling/DB/Kafka 장애/처리 대기 때문에 실제 발송은 더 늦을 수 있다.
- 예약 시각은 DB에 저장하므로 앱 재시작으로 초기화하지 않는다. 지연 설정 변경은 이후 생성하는 예약부터 적용된다.

Outbox의 기존 `(status, available_at)` 인덱스로 도래한 이벤트만 선점한다.
소비 스레드를 재시도 대기 시간 동안 sleep시키거나 미래 이벤트를 미리 발행하지 않는다.
Relay는 available_at을 발행 재시도/lease에도 사용하므로, Worker의 조기 실행 방지는
별도 `delivery_task.next_attempt_at`으로 확인한다. Kafka 토픽 이름 자체에는 지연 기능이 없다.

## 트랜잭션과 offset

1. Worker가 짧은 DB 트랜잭션에서 작업을 선점한다.
2. DB 트랜잭션 밖에서 공급자를 호출한다.
3. 일시 실패이고 예산이 남으면 같은 DB 트랜잭션에서 시도 결과, WAITING_RETRY,
   retry_generation 증가, next_attempt_at, `NOTIFICATION_RETRY` Outbox를 함께 커밋한다.
4. 이 영속 인계가 성공한 뒤 원본 Kafka offset을 처리한다. 저장 실패는 전체 롤백하고 소비를 중지한다.
5. due가 되면 Relay가 트랜잭션 밖에서 Kafka publish를 수행한다.
   publish future의 ACK 확인 후에만 Outbox를 PUBLISHED로 저장한다.

원본 offset 커밋 시점에는 다음 Kafka 메시지의 ACK를 기다리지 않는다.
커밋된 Outbox가 재발행 책임을 보관한다. DB와 Kafka의 원자적 동시 커밋을 주장하지 않는다.
publish 실패 시 기존 1~60초 backoff/최대 10회 정책을 사용한다.
최종 FAILED Outbox는 자동 복구하지 않으므로 알림이 WAITING_RETRY에 남을 수 있다.
`outbox_failed`와 DB 이력을 보고 수동 조사해야 한다. 후속 단계의 DLQ는 공급자 최종 실패 대상이며,
발행 한도를 소진한 Outbox를 다시 DLQ로 순환시키지는 않는다.

## 중복과 버전

한 작업은 같은 taskId를 유지하며 재시도마다 새 eventId와 증가한 retry_generation을 저장한다.
Outbox의 `(notification_id, event_type, retry_generation)` 유니크 제약과
notification → delivery_task 행 잠금 순서로 동시 완료가 후속 예약을 두 개 만들지 못하게 한다.
이전 lease 토큰은 결과를 덮어쓸 수 없다.

- 이전 generation의 검증된 이벤트는 후속 예약에 책임이 넘어갔으므로 호출 없이 replay 처리한다.
  현재 재시도가 처리 중이어도 오래된 이벤트가 lease를 빼앗지 않는다.
- 현재 generation은 예약 시각이 도래한 경우만 선점한다.
  강제로 조기 발행한 이벤트는 공급자 호출/offset 커밋 없이 소비를 중지한다. due 이후 재시작해야 한다.
- 알 수 없는 이벤트, 미래 generation, taskId 불일치는 오류로 중지한다.
- 재시도 이벤트도 schemaVersion 2를 사용한다. generation은 eventId로 DB에서 확인하며 payload에는 넣지 않는다.
  기존 version 1/2 이벤트는 계속 처리한다.

V1~V4 migration은 보존하고 V5로 상태 제약, 예약 컬럼, Outbox 유니크 제약을 확장했다.
과거 최종 일시 실패를 자동으로 다시 열지 않는다. 기존 행은 generation 0, next_attempt_at null로 보존한다.
**기존 API/Worker를 모두 종료한 뒤** 새 앱으로 migration을 적용한다.
옛 앱은 WAITING_RETRY/재시도 이벤트를 처리하지 못하므로 혼합 실행과 단순 JAR 다운그레이드는 지원하지 않는다.
MySQL DDL은 전체 migration의 원자적 롤백을 보장하지 않는다.

## 지표와 검증

`notification_retry_scheduled_total`은 현재 프로세스가 성공적으로 예약한 횟수다.
예약된 중간 실패를 `notification_completed_total`에 더하지 않는다.
`notification_processing_seconds_*{status="WAITING_RETRY"}`에는 해당 시도 처리/저장 시간을 기록한다.
카운터는 재시작 시 초기화되므로 영속 대조는 작업/이력/Outbox/offset을 사용한다.

재시도 정책 최초 구현 시 `./gradlew --no-daemon --console=plain build`: 75개 통과, 실패/오류/스킵 0.
실제 MySQL/Kafka에서 재시작 후 예약 복구, 상한 소진, 오래된 이벤트 replay,
동시 완료, 조기 이벤트 차단, Outbox INSERT 실패 전체 롤백, 발행 실패 후 복구,
고정 예산과 불확실한 복구의 예산 공유를 검증했다. V4 → V5에서 기존 최종 결과도 보존했다.
지연 경계와 설정 검증은 단위 테스트로 확인하며, 통합 테스트에서는 저장된 due 시각을
앞당겨 기다림을 줄인다. 프로세스 강제 종료/브로커 실제 중단/다중 인스턴스 실험은 수행하지 않았다.

이번 단계의 실제 결과와 다음 작업은 [PROGRESS.md](PROGRESS.md)에 기록한다.
