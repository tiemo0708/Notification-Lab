# S4 발송 작업과 시도 이력

후속 단계에서 추가한 WAITING_RETRY와 영속 지연 예약은 [재시도 정책](S4_RETRY_POLICY.md)을 참고한다.

## 범위

현재 API의 수신자 하나를 가상 발송 대상 하나로 취급한다. 실제 디바이스 등록이나 여러 디바이스로의 fan-out은 아직 없다.
접수 시 생성한 `taskId`는 재전달/복구에서도 유지한다. API 멱등 키를 재사용하면 같은 작업을 조회한다.
`delivery_task.notification_id`의 유니크 제약으로 한 알림에 작업 하나만 허용한다.
향후 여러 대상 지원은 별도 API/스키마 변경이 필요하다.

접수 트랜잭션은 notification, delivery_task, outbox_event를 함께 커밋한다.
작업의 상태는 notification에 한 번만 저장하며 조회 시 결합한다. 대상 스냅샷은 `target_id`에 UTF-8 바이트로 보존한다.

## 선점과 결과 저장

1. Worker는 notification 행을 잠그고 이벤트/작업 ID를 검증한다. 완료된 작업은 공급자를 호출하지 않는다.
2. 후속 재시도로 인계한 이전 generation은 replay 처리한다. 현재 generation의 30초 lease가 유효하면 오류로 소비를 중단하고 offset을 넘기지 않는다.
3. 만료된 lease의 이전 `STARTED` 시도는 `UNKNOWN / OUTCOME_UNKNOWN`으로 기록한다.
4. 상한 안이면 선점 토큰, 시도 번호 증가, `STARTED` 이력을 같은 트랜잭션으로 커밋한다.
5. DB 트랜잭션 밖에서 Mock을 호출한다.
6. 동일 토큰 조건으로 알림 결과와 시도 결과를 함께 커밋한 뒤 Kafka RECORD offset을 처리한다.
   일시 실패 재시도 시에는 다음 Outbox와 예약 시각도 이 트랜잭션에 포함한다.

알림 결과 또는 시도 이력 저장이 실패하면 둘 다 롤백된다. 오래된 소유자는 새 토큰으로 바뀐 작업을 완료할 수 없다.
이력의 `(task_id, attempt_no)` 기본 키와 `lease_token` 유니크 제약도 중복 기록을 막는다.

## 유한 복구와 불확실성

`app.delivery.max-attempts`는 기본 3, 허용 범위 1~10이며 **최초 선점도 포함**한다.
접수 당시 값을 작업에 저장하므로 설정 변경은 새 작업에만 적용된다.

```bash
./gradlew bootRun --args='--app.delivery.max-attempts=3'
```

선점 커밋 직후 실제 호출 전에 종료되어도 시도 하나를 소비한다. 시도 수는 실제 공급자 호출 수와 같다고 단정할 수 없다.
마지막 lease까지 만료되고 다시 소비하면 추가 호출 없이 `INTERNAL_FAILURE / ATTEMPTS_EXHAUSTED`로 끝낸다.
그 최종 상태와 마지막 `UNKNOWN` 이력도 같은 트랜잭션으로 저장한 뒤 offset을 처리한다.
완료 이벤트 재전달은 시도 수를 늘리지 않는다.

`UNKNOWN`은 공급자 실패 확정이 아니다. 공급자 수락 후 DB 기록 전에 종료하면 재호출로 외부 중복이 생길 수 있다.
토큰은 DB 결과 덮어쓰기를 차단하지만 진행 중인 외부 호출을 취소하지 않는다.
상한 소진 역시 전달 실패의 증명이 아니며 자동 재전송 대신 이력을 조사한다. 외부 exactly-once는 보장하지 않는다.
중단 지점별 판단과 lease/토큰의 경계, 운영 절차는 [불확실한 결과 정책](S4_UNCERTAIN_OUTCOMES.md)에 정의한다.

현재 복구는 DB/이벤트 오류 해결 및 lease 만료 후 앱을 재시작하는 수동 절차다.
만료 시각만으로 `UNKNOWN`이 기록되는 것은 아니며 다음 선점에서 갱신한다.
일반 `RETRYABLE_FAILURE`는 예산이 남으면 자동 재시도한다. 최초/복구/재시도가 같은 전체 상한을 공유한다.
backoff+jitter와 예약/offset 관계는 [재시도 정책](S4_RETRY_POLICY.md)에 정의한다. 토픽 분리와 최종 실패 인계는 [Retry 토픽/DLQ](S4_RETRY_TOPICS_DLQ.md)에 정의한다.

## 조회 API

`GET /notifications/{id}/delivery`는 다음 형식으로 응답한다. 기존 POST/GET 알림 응답은 유지한다.

```json
{
  "taskId": "00000000-0000-0000-0000-000000000001",
  "status": "PROVIDER_ACCEPTED",
  "attemptCount": 1,
  "maxAttempts": 3,
  "nextAttemptAt": null,
  "attempts": [
    {
      "attemptNo": 1,
      "outcome": "PROVIDER_ACCEPTED",
      "errorCode": null,
      "startedAt": "2026-10-14T00:00:00Z",
      "completedAt": "2026-10-14T00:00:00.100Z"
    }
  ]
}
```

상태와 이력을 하나의 읽기 트랜잭션으로 조회한다. 이력은 시도 번호 순서로 최대 10개다.
대상, 본문, lease 토큰은 이 응답에 노출하지 않는다.
작업이 없는 ID/S2 데이터 및 sync/memory 프로필은 404, 잘못된 UUID는 400이다.
`notification_worker_exhausted_total`은 해당 프로세스에서 lease 복구 상한 소진을 최종 저장한 횟수다.
다른 카운터처럼 재시작 시 초기화되고 DB의 영속 이력을 대체하지 않는다.

## 이벤트와 기존 DB 업그레이드

요청/재시도 Outbox 이벤트는 schemaVersion 2이며 `eventId`, `notificationId`, `taskId`를 포함한다.
Worker는 taskId가 DB와 같은지 확인한다. 기존 schemaVersion 1 이벤트는 notificationId로 이미 저장된 작업을 찾는다.
토픽 이름 `notification.requested.v1`은 유지하며 토픽 이름과 payload의 schemaVersion은 별개다.

V1~V3 migration을 수정하지 않고 V4를 추가했다.

- `NOTIFICATION_REQUESTED` Outbox가 있는 기존 알림에만 작업을 생성한다. 기존 행/이벤트 payload/결과는 보존한다.
- 기존 진행 중 lease는 토큰을 유지한 `STARTED` 시도 1개로 옮긴다. 과거 시작 시각은 알 수 없어 `startedAt=null`이다.
- 기존 완료 알림은 작업의 현재 결과만 조회하며 과거 시도 이력을 만들어 내지 않는다. 따라서 완료 상태여도 attemptCount 0일 수 있다.
- 이전 버전에서 발생한 전체 호출 수는 복원할 수 없다. 업그레이드한 작업의 상한 3은 가져온 활성 시도부터의 예산이다.
- S2의 Outbox 없는 알림에는 새 작업을 만들지 않는다.

로컬 업그레이드 시 **기존 앱/Worker를 모두 종료한 후** 새 버전을 기동한다.
이전 Worker는 schemaVersion 2를 지원하지 않고 이전 API는 delivery_task를 생성하지 않으므로 혼합 버전 실행을 지원하지 않는다.
V4 이후 단순히 옛 JAR로 되돌리지 않는다. 데이터 보존이 필요한 복귀는 DB와 Kafka/offset까지 검토하는 별도 절차다.
MySQL DDL migration은 전체 원자적 롤백을 보장하지 않으므로 실패 시 원인을 확인하고 복구한다.
