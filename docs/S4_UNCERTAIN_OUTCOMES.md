# S4 외부 발송 결과가 불확실한 경우의 복구 정책

## 현재 선택과 보장 경계

공급자 수락과 MySQL 결과 저장은 하나의 트랜잭션이 아니다.
공급자가 수락한 직후 프로세스가 종료되거나 DB 저장이 실패하면 DB에는 PROCESSING/STARTED만 남을 수 있다.
현재 정책은 **재소비 시 남은 선점 예산 안에서 재호출을 허용하며, 외부 중복 가능성을 보존하는 유한 복구**다.
미확정 작업을 자동으로 성공 처리하거나 offset을 건너뛰지 않는다.

API 멱등 키는 접수 중복을, 작업 ID/토큰/세대는 DB 작업 중복과 오래된 결과 덮어쓰기를 막는다.
이 기능들은 이미 실행한 외부 호출을 취소하거나 외부 수락을 한 번으로 제한하지 않는다.
PROVIDER_ACCEPTED는 Mock 공급자 수락이며 단말 도착을 뜻하지 않는다.
Mock은 같은 notificationId에 같은 messageId를 반환하지만 호출 결과를 기억하여 중복을 제거하는 공급자가 아니다.
메시지 ID가 같다는 사실로 exactly-once를 주장하지 않는다.

이 문서는 기본 비동기 프로필의 구현 정책이다. sync/memory 비교 프로필에는 Kafka lease 복구가 없다.
실제 FCM/APNs/HTTP 공급자와 수락 조회 API, 공급자 측 멱등 키는 아직 구현하지 않았다.

## 중단 지점별 DB 증거와 재소비 동작

| 중단/오류 지점 | 확인 가능한 DB 상태 | 같은 이벤트의 재소비 |
| --- | --- | --- |
| 선점 트랜잭션 커밋 전 | 선점/STARTED/횟수 증가가 함께 롤백 | 유효한 작업이면 새 선점. 이 실패한 선점으로 공급자를 호출하지 않음 |
| 선점 커밋 후 공급자 호출 전 | PROCESSING, STARTED, 증가한 attemptCount | lease 만료 후 이전 시도를 UNKNOWN으로 기록. 예산이 남으면 재호출 |
| 공급자 실행 중 또는 응답 수신 후 결과 저장 전 | 위와 같은 상태일 수 있음 | 호출 전 중단과 구별할 수 없으므로 같은 유한 복구. 외부 중복 가능 |
| 결과/이력/후속 Outbox 저장 트랜잭션 실패 | 전체 롤백 시 PROCESSING/STARTED | 실패 offset 미커밋, 소비 중지. 원인 해결·lease 만료 후 재소비 |
| 결과 DB 커밋 성공 후 offset 처리 전 | 최종 결과와 이력, 필요하면 DLQ Outbox 존재 | 최종 상태를 확인하고 공급자 호출 없이 replay 처리 |
| 재시도 예약 DB 커밋 후 원본 offset 처리 전 | WAITING_RETRY, 증가한 generation, due 시각, 후속 Outbox 존재 | 이전 generation을 호출 없이 처리. 후속 예약이 책임을 유지 |
| 후속 Kafka ACK 후 Outbox PUBLISHED 저장 전 | PROCESSING Outbox가 남을 수 있음 | Relay lease 복구로 같은 eventId가 재발행될 수 있음. 작업/재발행 API의 중복 방지 적용 |

연결 단절로 DB COMMIT의 응답 자체가 유실된 경우에는 롤백됐다고 단정할 수 없다.
재접속 후 커밋된 상태를 확인한다. Worker는 재소비 시 DB 상태에 따라 최종 replay 또는 미완료 복구를 결정한다.
DB 오류의 원문에 포함될 수 있는 payload나 수신자 정보를 로그로 출력하지 않는다.

현재 Listener는 단건 RECORD ACK이며 DB/이벤트 처리 오류에 소비 컨테이너를 중지한다.
자동으로 오류를 무시하고 다음 offset을 확정하지 않는다. 요청/Retry 두 토픽이 같은 Worker를 사용하므로
한 오류로 그 Worker의 두 토픽 처리가 함께 멈출 수 있다.

## UNKNOWN, 내부 오류, 횟수의 의미

- `STARTED`: 선점이 커밋됐다는 기록이다. 실제 외부 요청이 시작됐거나 도착했다는 증거가 아니다.
- `UNKNOWN / OUTCOME_UNKNOWN`: 만료된 STARTED를 다음 복구 트랜잭션에서 정리한 기록이다.
  lease 시각만 지나면 자동으로 기록되는 것은 아니다. 이력의 completedAt은 복구 시각이며 공급자 처리 종료 시각이 아니다.
- `INTERNAL_FAILURE / ATTEMPTS_EXHAUSTED`: 마지막 선점도 결과를 확정하지 못한 채 복구 예산이 소진됐다.
  추가 호출 없이 UNKNOWN 정리·최종 상태·DLQ Outbox를 함께 저장한다. 외부 미발송 확정이 아니다.
- `INTERNAL_FAILURE / INTERNAL_PROVIDER_ERROR`: 프로세스가 살아 있는 동안 공급자 호출의 RuntimeException을 잡아 저장한 결과다.
  명시적 일시 실패가 아니므로 자동 공급자 재시도를 하지 않고 DLQ에 인계한다. 외부 부작용이 없었다고 단정하지 않는다.
- `RETRYABLE_FAILURE`: 공급자가 명시적으로 일시 실패를 반환한 결과다. 남은 예산이 있으면 WAITING_RETRY로 예약하고,
  없으면 최종 실패와 DLQ를 저장한다. UNKNOWN과 구분한다.

attemptCount는 최초 선점·일시 실패 재시도·불확실한 결과 복구를 합한 수다.
기본 3회, 허용 1~10이며 접수 시 고정한다. 선점 직후 종료하면 실제 호출 없이도 한 번을 소비한다.
완료된 작업의 replay와 이전 generation replay는 횟수를 늘리지 않는다.
재시도 backoff+jitter는 명시적 일시 실패에 적용한다. lease 복구에 별도 jitter 예약은 없으며,
재소비 시 만료 여부/남은 예산으로 결정한다.

최종 PROVIDER_ACCEPTED여도 과거 UNKNOWN 이력은 남는다. 마지막 시도의 수락은 이전 시도가 미발송됐다는 증거가 아니다.
프로세스 메트릭은 재시작 시 초기화될 수 있으며 영속 이력이나 실제 외부 호출 횟수를 대체하지 않는다.

## lease와 토큰이 막는 것

Worker lease는 DB UTC 기준 30초다. 유효한 lease에 현재 generation이 다시 도착하면 오류로 중지한다.
다음 소유자가 만료된 작업을 선점하면 토큰을 교체하고 이전 STARTED를 UNKNOWN으로 만든다.
이전 소유자의 늦은 결과는 새 토큰과 다르므로 DB 결과를 덮어쓸 수 없다.

**lease 만료만으로 이전 호출이 취소되거나 토큰이 자동 무효화되지는 않는다.**
다른 소유자가 아직 선점하지 않았다면 기존 소유자는 같은 토큰으로 결과를 저장할 수 있다.
이미 진행 중인 외부 호출이 만료 후 계속 실행되면 새 호출과 외부에서 겹칠 수 있다.
토큰은 DB 기록의 소유권을 제어하며 외부 공급자의 실행을 제어하지 않는다.
실제 HTTP timeout/취소/장시간 호출과 다중 Worker의 조합은 후속 실험 범위다.

## 로컬 확인과 복구 절차

1. Worker가 멈췄다면 오류 원인과 DB 연결 상태를 먼저 확인한다. 재시작하면 미처리 작업 전반이 재개될 수 있다.
2. `/notifications/{id}/delivery`에서 상태, attemptCount/maxAttempts, nextAttemptAt, 시도 이력을 읽는다.
   DB에서는 아래 조회로 lease와 관련 Outbox 발행 상태를 함께 확인한다.
3. 이미 최종 결과가 커밋됐다면 원본 이벤트 replay는 공급자를 다시 호출하지 않는다.
   WAITING_RETRY라면 저장된 예약과 Outbox를 확인한다. 임의로 새 요청을 접수하거나 generation을 초기화하지 않는다.
4. PROCESSING/STARTED가 남았다면 외부 수락 여부는 DB만으로 확정할 수 없다.
   실제 공급자를 도입한 환경에서는 별도 수락 증거를 조사해야 한다. 현재 Mock에는 사후 수락 조회 기능이 없다.
5. 현재 정책에 따라 중복 가능성을 감수하고 복구할 경우 DB/이벤트 오류를 해결하고 lease 만료 후 같은 그룹으로 Worker를 재시작한다.
   남은 예산 안에서 재소비가 복구하며, 예산이 없으면 추가 호출 없이 DLQ에 인계한다.
6. 외부 중복을 허용할 수 없는 경우 Worker를 중지한 채 조사한다. 현재 앱에는 개별 불확실 작업의 보류/수락 확정 API가 없다.
   조사만 필요한 재기동은 `--app.async.worker-enabled=false --app.async.relay-enabled=false`로 소비·발행을 끈다.
7. 최종 실패의 수동 재발행은 PUBLISHED DLQ를 선택하고 사유·중복 가능성 확인을 기록하는
   [재발행 API](S4_DLQ_REDRIVE.md)를 사용한다. 원본은 보존하고 새 작업을 만들며 계보 전체 최대 3회다.

```sql
-- 로컬 DB에서 실제 대상 UUID로 바꾸어 조회한다. 수신자/본문/토큰은 조회하지 않는다.
SELECT n.notification_id, n.status, n.error_code, n.completed_at, n.worker_lease_until,
       t.task_id, t.attempt_count, t.max_attempts, t.retry_generation, t.next_attempt_at
FROM notification n JOIN delivery_task t USING (notification_id)
WHERE n.notification_id = 'NOTIFICATION_UUID';

SELECT event_id, event_type, retry_generation, status, attempt_count, available_at, published_at
FROM outbox_event
WHERE notification_id = 'NOTIFICATION_UUID'
ORDER BY created_at, event_id;
```

복구를 위해 offset을 앞으로 이동하거나 기존 상태/횟수/이력을 직접 초기화하지 않는다.
애플리케이션은 DLQ 발행 한도 소진(FAILED Outbox)을 자동 복구하지 않는다. 원인을 조사하며,
PUBLISHED 조건을 우회하여 재발행 접수를 성공시킨 것으로 처리하지 않는다.
외부 수락 증거를 얻었어도 현재 API로 원본 결과를 성공으로 정정하는 기능은 없다.
수동 DB 덮어쓰기로 이력 정합성을 깨뜨리는 대신 후속 명시적 판정 기능이 필요하다.

## 구현 근거와 실제 검증 범위

구현은 [WorkerStore](../src/main/java/dev/jinhyeon/notificationlab/worker/WorkerStore.java)의 claim/complete,
[NotificationWorker](../src/main/java/dev/jinhyeon/notificationlab/worker/NotificationWorker.java)의 처리 순서,
[NotificationService](../src/main/java/dev/jinhyeon/notificationlab/notification/NotificationService.java)의 공급자 예외 분류를 기준으로 한다.

[AsyncNotificationTest](../src/test/java/dev/jinhyeon/notificationlab/outbox/AsyncNotificationTest.java)의 기존 검증을 정책 근거로 연결한다.

| 테스트 | 입증한 범위 |
| --- | --- |
| `historyCompletionFailureRollsBackResultAndPreservesOffsetUntilRecovery` | Mock 수락 후 이력 저장 실패로 전체 롤백/offset 0, 복구 후 UNKNOWN + PROVIDER_ACCEPTED, 공급자 호출 총 2회 |
| `staleWorkerCannotOverwriteNewLeaseAndActiveLeaseIsNotAcknowledged` | 활성 lease 선점 거절, 새 소유자 이후 이전 토큰 완료 거절 |
| `expiredRecoveryStopsAtPersistedAttemptCapBeforeCallingProvider` | 선점 3회만 소비한 경우 UNKNOWN 3개, 추가 공급자 호출 0, 상한 소진/DLQ |
| `exhaustedLeaseCannotCommitWithoutItsDeadLetterHandoff` | 상한 소진 시 DLQ INSERT 실패도 최종 상태/이력을 롤백하고 offset 유지 |
| `realKafkaCompletesBeforeCommittingOffsetAndReplayDoesNotCallProviderAgain` | 완료한 작업을 실제 Kafka로 재전달해도 공급자 추가 호출 없음 |
| `uncertainRecoveryAndTransientRetryShareOneTotalBudget` | 불확실 복구와 명시적 일시 실패 재시도의 공통 예산 |
| `unexpectedProviderExceptionDoesNotCreateAutomaticRetry` | RuntimeException의 최종 INTERNAL_FAILURE와 DLQ, 자동 재시도 없음 |
| `selectedRedrivePreservesOriginalAndCompletesNewWorkAfterRestart` | 수동 재접수의 원본 보존, 구 이벤트 replay와 새 작업 분리 |

테스트는 실제 MySQL/Kafka와 프로세스 내 Mock을 사용한다.
DB 트리거로 저장 오류를 주입하고 lease 시각을 앞당겨 복구를 재현했다.
프로세스 kill, DB COMMIT 응답 유실, 실제 공급자 네트워크 timeout, 장시간 호출 중 lease 교체를
모두 실행했다는 뜻은 아니다. 이 문서의 중단 지점 표는 구현에서 도출한 정책이며 실제 장애 실험과 구분한다.
S5의 브로커 중단·프로세스 종료·rebalance·Graceful Shutdown은 별도 미완료 항목으로 유지한다.

이번 정책 정리 시 `./gradlew --no-daemon --console=plain test --rerun-tasks`로 기존 90개를 재실행했다.
실패/오류/스킵 0이며, 실행 결과와 미검증 항목은 [PROGRESS.md](PROGRESS.md)에 기록한다.
