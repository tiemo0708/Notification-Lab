# S4 DLQ 선택 재발행과 이력

## 선택 재발행 계약

기본 비동기 프로필에서 **PUBLISHED로 확인된 DLQ 한 건**을 선택하여 다시 접수한다.
원본 notification/task/attempt/Outbox와 Idempotency-Key는 보존하고, 같은 내용의 새 notification/task/요청 Outbox를 만든다.
원래 완료 이벤트를 그대로 재전송하면 기존 멱등 처리에 의해 무시되므로, 재발행은 새로운 발송 시도를 뜻한다.
새 작업의 결과는 응답 Location으로 조회한다. DLQ를 자동 소비하거나 일괄 재발행하지 않는다.

같은 DLQ의 재발행 식별자는 source eventId다. 별도 Idempotency-Key 없이 DB 유니크 제약으로 한 번만 접수한다.

| 요청 | 결과 |
| --- | --- |
| 최초 유효한 재발행 | 202, 새 작업과 감사 이력 커밋 후 응답 |
| 같은 DLQ + 같은 사유 | 200, 기존 감사 이력과 같은 Location 반환 |
| 같은 DLQ + 다른 사유 | 409, 기존 기록 보존 |
| 미발행/PENDING/PROCESSING/FAILED DLQ | 409, 원본 DLQ 발행 문제부터 해결 |
| 원본 기준 수동 재발행 3회 소진 | 409, 새 작업 생성 없음 |
| 잘못된 UUID/사유/중복 가능성 확인 누락 | 400 |
| 없는 이벤트 또는 요청/Retry 이벤트 ID | 404 |
| DB/트랜잭션 저장 실패 | 503, 같은 DLQ·사유로 재요청해 커밋 여부 확인 |

응답 유실 후에는 같은 이벤트/사유로 다시 요청한다. DLQ Kafka 중복 레코드도 동일 eventId로 같은 결과를 조회한다.
200/202는 공급자 수락이나 단말 도착을 뜻하지 않는다. 기존 재발행 작업이 완료됐어도 같은 요청으로 다시 열지 않는다.

## 사용 예시

대상 eventId는 DLQ 또는 로컬 DB에서 찾는다. 조회는 payload를 노출하지 않는다.

```sql
SELECT event_id, notification_id, status, created_at
FROM outbox_event
WHERE event_type = 'NOTIFICATION_DEAD_LETTER'
ORDER BY created_at, event_id
LIMIT 100;
```

```bash
curl http://127.0.0.1:8080/dead-letters/DLQ_EVENT_UUID

curl -i http://127.0.0.1:8080/dead-letters/DLQ_EVENT_UUID/redrive \
  -H 'Content-Type: application/json' \
  -d '{"reason":"PROVIDER_RECOVERED","acknowledgePossibleDuplicate":true}'
```

사유는 `PROVIDER_RECOVERED`, `CONFIGURATION_FIXED`, `MANUAL_REVIEW` 중 하나다.
사유는 운영자의 판단 기록이며 시스템이 장애 해결을 확인했다는 뜻은 아니다.
자유 텍스트/수신자/토큰을 감사 사유에 저장하지 않는다.
`acknowledgePossibleDuplicate=true`는 외부에서 이미 수락했을 가능성을 검토했다는 명시적 입력이다.
특히 UNKNOWN/ATTEMPTS_EXHAUSTED는 외부 실패 확정이 아니므로 시도 이력을 먼저 확인한다.

재발행 응답은 sourceEventId, sourceNotificationId, rootNotificationId, targetNotificationId,
redriveNo, reason, acknowledgedPossibleDuplicate, createdAt을 담는다.
Location은 `/notifications/{targetNotificationId}`이다.
`GET /dead-letters/{eventId}`는 원본 taskId/최종 상태/오류 코드/시도 수/상한/발행 상태와 재발행 이력을 함께 반환한다.
발송 이력은 각 알림의 `/notifications/{id}/delivery`로 조회한다.

sync/memory 프로필에는 이 API가 없고 404이다. 현재 앱은 loopback에 바인딩하는 로컬 실험이며
인증/운영자 신원 검증은 아직 없다. 감사 기록은 사유·시각·대상 연결을 남기며 인증된 행위자 증명을 제공하지 않는다.

## 횟수와 계보

하나의 DLQ 이벤트/원본 알림에서 자식 알림은 하나만 만들 수 있다.
재발행된 알림이 다시 실패하면 그 새 DLQ를 선택할 수 있지만, 최초 rootNotificationId 기준 최대 3회로 제한한다.
따라서 루트와 자식을 합쳐 최대 4개 작업이다. 각 작업은 원본의 maxAttempts를 상속한다.
새 설정으로 상한을 늘려 재시도 예산을 우회하지 않는다. 기본 3회 예산이면 계보 전체 최대 12번 선점이다.
선점은 실제 공급자 호출보다 많을 수 있다. 설정 상한 10일 때도 계보 전체 선점은 최대 40번이다.

이 한도는 이 재발행 API의 계보에 적용한다. 별개의 POST /notifications 신규 접수까지 막는 전역 발송 한도가 아니다.
새 요청으로 같은 내용을 보내는 행위는 별개이며, 외부 rate limit은 후속 S6 범위다.

원본의 상태/시도 번호/예산을 초기화하지 않는다. 이전 이벤트가 재전달돼도 원본의 완료 결과를 조회하며
새 자식 작업을 선점하지 않는다. 원래 API Idempotency-Key도 계속 원래 실패를 가리킨다.
재발행 API에서는 요청 내용을 바꾸는 기능을 제공하지 않는다.

계보를 로컬 DB에서 조회하는 예시는 다음과 같다.

```sql
SELECT source_event_id, source_notification_id, target_notification_id,
       redrive_no, reason, acknowledged_possible_duplicate, created_at
FROM dead_letter_redrive
WHERE root_notification_id = 'ROOT_NOTIFICATION_UUID'
ORDER BY redrive_no;
```

## 트랜잭션과 보존

재발행 접수는 `REQUIRES_NEW`의 독립 DB 트랜잭션이다. 호출자의 이전 snapshot/롤백에 의존하지 않는다.
선택한 source 행을 잠근 뒤 기존 감사 기록을 locking read로 확인한다.
없으면 원본 상태·DLQ 발행 완료·계보 한도를 확인하고 새 알림/작업/요청 Outbox/감사 기록을 함께 커밋한다.
이력 INSERT나 Outbox INSERT 실패 시 전부 롤백하며 원본 결과와 Kafka offset은 바꾸지 않는다.
트랜잭션 안에서 Kafka 발행 또는 공급자 호출을 하지 않는다.

V6는 dead_letter_redrive 테이블을 추가한다. 기존 migration/데이터를 수정하거나 과거 재발행 이력을 만들어 내지 않는다.
source_event_id 기본 키, source/target 알림 유니크 제약, (root_notification_id, redrive_no) 유니크 제약,
1~3 CHECK, 사유/확인 CHECK 및 외래 키로 기록 관계를 보존한다. 기존 이력의 삭제/수정 API는 없다.
앱을 종료한 뒤 V6를 적용하고 새 버전으로 실행한다. API가 추가되며 기존 POST/GET 응답 계약은 유지된다.

DLQ offset을 재발행 API에서 커밋하거나 되돌리지 않는다. 원본 DLQ는 보존한다.
새 Outbox의 발행/ACK/lease 복구는 [토픽·DLQ 정책](S4_RETRY_TOPICS_DLQ.md)을 따른다.
새 공급자 호출이 성공했지만 DB 저장 전에 종료하면 외부 중복 발송 가능성은 여전히 남는다.

## 검증과 다음 작업

실제 MySQL/Kafka/HTTP로 같은 사유의 동시 요청, 다른 사유의 충돌, 저장 실패 전체 롤백,
재시작 후 접수 결과 재사용, 원본 보존, 구 이벤트 replay와 새 작업 성공,
상한 3회 및 예산 상속, 오래된 호출자 snapshot과 독립 커밋을 검증한다.
V5 → V6의 기존 데이터 보존과 memory 프로필의 API 미노출도 확인한다.
`./gradlew --no-daemon --console=plain build`: **90개 통과, 실패/오류/스킵 0**.
상세 실행 결과는 [PROGRESS.md](PROGRESS.md)에 기록한다.

공급자 수락 후 DB 기록 전 종료에 대한 [불확실한 결과 정책](S4_UNCERTAIN_OUTCOMES.md)을 함께 확인한다.
실제 브로커/프로세스 중단 실험은 S5에서 별도로 진행한다.
