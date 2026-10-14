# S4 첫 단계: API 요청 멱등성

이 문서는 S4의 Idempotency-Key 접수 정책을 설명한다. 이후 추가한 Worker 작업 모델은
[발송 작업과 시도 이력](S4_DELIVERY_TASKS.md), 자동 재시도는 [지연 재시도 정책](S4_RETRY_POLICY.md)을 참고한다.
별도 Retry topic/DLQ는 후속 작업이다.

## API 계약

기본 비동기 프로필의 `POST /notifications`에서 선택적으로 `Idempotency-Key` 헤더를 받는다.

| 요청 | 응답과 저장 동작 |
| --- | --- |
| 키 없음 | 기존처럼 새 알림/Outbox를 저장하고 202 |
| 처음 쓰는 키 | 알림/Outbox와 키를 함께 커밋하고 202 |
| 같은 키 + 같은 요청 필드 | 기존 알림의 현재 상태와 동일한 Location을 200으로 반환. 새 알림/Outbox 없음 |
| 같은 키 + 다른 요청 필드 | 409 Conflict. 기존 레코드 변경 없음 |
| 잘못된 키, 복수 헤더 | 400. 키를 예약하거나 저장하지 않음 |
| DB 오류/잠금 대기 실패 | 503. 새 키를 만들지 말고 같은 키/내용으로 재시도하여 결과 확인 |

200은 요청 재사용을 뜻하며 Mock 수락이나 단말 도착을 뜻하지 않는다.
처리가 진행됐다면 재요청 응답의 status/completedAt은 최초 202 스냅샷과 다를 수 있다.
실패로 완료된 알림도 같은 키로 다시 보내면 그 결과를 조회하며 자동 재발송하지 않는다.

키는 대소문자를 구분하는 `[A-Za-z0-9._:-]` 1~128자이다. 빈 값, 쉼표로 합친 값,
따옴표가 들어간 값, 같은 값이라도 복수 헤더는 거절한다. UUID 같은 불투명 식별자를 사용하고 수신자/본문을 넣지 않는다.
키 자체는 API 응답, 애플리케이션 로그, 메트릭 label에 추가하지 않는다.

payload 비교는 JSON을 파싱한 `recipientId`, `title`, `body` 세 필드의 정확한 문자열 비교이다.
JSON 속성 순서나 직렬화 공백은 무시되지만 필드 안의 공백·대소문자·유니코드 구성은 그대로 비교한다.
본문이 잘못되어 400인 요청은 키를 소모하지 않는다.

```bash
# 같은 키와 같은 내용으로 두 번 보내면 202 → 200, notificationId/Location 동일
curl -i http://127.0.0.1:8080/notifications \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: example-request-001' \
  -d '{"recipientId":"user-100","title":"테스트 알림","body":"알림 처리 확인"}'
```

`sync`/`memory`는 이전 동기 기준선이다. 이 프로필에서 헤더를 보내면 400으로 거절해
지원하지 않는 멱등성 보장을 제공하는 것처럼 보이지 않게 한다. 헤더가 없으면 기존 201 동작을 유지한다.

## 저장과 트랜잭션

V1/V2는 보존하고 V3에서 `notification.idempotency_key VARBINARY(128)`와 유니크 제약을 추가한다.
키가 없는 요청과 기존 행은 NULL이며 각각 독립적으로 저장할 수 있다.
키가 있는 두 요청이 동시에 들어오면 유니크 제약이 하나만 허용한다.

1. `IdempotentSubmissionService`는 호출자 트랜잭션을 중단(`NOT_SUPPORTED`)한다.
2. 별도 `SubmissionService` 프록시가 알림 INSERT(키 포함) + 발송 작업 INSERT + Outbox INSERT를 한 트랜잭션으로 커밋한다.
3. DuplicateKeyException이면 위 트랜잭션의 롤백이 끝난 뒤 키로 기존 알림을 조회한다.
4. 세 필드가 같으면 기존 리소스를 반환하고, 다르면 409를 반환한다.
5. 해당 키가 없다면 다른 제약에서 발생한 중복 오류이므로 성공 재사용으로 처리하지 않고 503을 반환한다.

exists 조회 결과만 믿고 INSERT하지 않으며, 프로세스 메모리 캐시나 락에 의존하지 않는다.
DB 잠금 오류에 무한 재시도를 추가하지 않는다. 커밋 결과가 불확실해도 같은 키로 재요청하면
이미 커밋된 알림을 재사용하거나, 롤백된 경우 새로 한 번 접수할 수 있다.

## 범위와 보관 정책

- 현재 인증 없는 로컬 실험 API이므로 키 범위는 이 DB의 POST /notifications 전체이다.
  다중 사용자/운영 인증 도입 시 사용자·테넌트별 키 범위를 먼저 설계해야 한다.
- 키는 알림 행과 함께 보관하며 TTL/자동 삭제/키 재사용 기간은 없다.
  DB 초기화나 수동 행 삭제 후에는 이전 키의 보장도 사라진다.
- 이는 요청 접수 중복 방지이다. 공급자 호출과 결과 기록 사이 중단으로 생기는 외부 발송 중복까지 제거하지 않는다.
- Outbox/Worker lease와 실패 시 offset을 건너뛰지 않는 정책을 유지한다.
  공급자 일시 실패의 재시도 예약을 추가했으며 별도 Retry topic/DLQ는 아직 구현하지 않았다.

## 검증

```bash
./gradlew --no-daemon --console=plain build
```

실제 MySQL/Kafka HTTP 테스트에서 같은 키 재사용, 필드별 충돌, 8개 동시 동일 요청,
8개 동시 상충 요청, 실패 롤백 후 키 재사용, 완료/앱 재시작 후 재사용,
키 대소문자·길이·중복 헤더, 키 없는 요청, 유효성 오류, 호출자 트랜잭션의 오래된 snapshot을 검증한다.
메모리 비교 프로필의 헤더 거절도 확인한다. 실행 결과는 [PROGRESS.md](PROGRESS.md)에 기록한다.

선택 근거: MySQL은 [UNIQUE 인덱스](https://dev.mysql.com/doc/refman/8.4/en/create-index.html)에서
중복 non-NULL 값을 거절하고 여러 NULL을 허용한다. [InnoDB의 INSERT 잠금](https://dev.mysql.com/doc/refman/8.4/en/innodb-locks-set.html)은
동시 요청에서 대기 또는 잠금 오류를 만들 수 있으므로 DB 오류를 무조건 재사용 성공으로 바꾸지 않는다.
