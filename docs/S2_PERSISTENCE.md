# S2: MySQL 영속화

이 문서는 S2 동기 기준선을 설명한다. 현재 기본 동작은 [S3](S3_ASYNC.md)이며,
S2 비교 실행은 `./gradlew bootRun --args='--spring.profiles.active=sync'`를 사용한다.

## 범위와 선택

S1에서 재시작하면 사라지던 알림을 MySQL 8.4.8에 저장한다. Spring JDBC를 사용해 SQL과
트랜잭션 경계를 명시한다. Spring Boot 3.5.16 BOM이 JDBC 드라이버/Flyway/Testcontainers 버전을 관리한다.
스키마는 Flyway만 관리하며 H2로 MySQL 검증을 대체하지 않는다.

S2는 동기식 POST 201과 기존 응답 필드를 유지한다. Outbox는 테이블만 준비하며 앱에서는 쓰지 않는다.
요청/Outbox를 함께 저장하는 비동기 API, Relay, Kafka, Worker, HTTP 202는 S3 범위다.

## 트랜잭션과 상태

1. 입력 검증 후 `PROCESSING` 레코드를 짧은 트랜잭션으로 생성하고 커밋한다.
2. DB 트랜잭션 없이 Mock 공급자를 호출한다.
3. 짧은 트랜잭션으로 최종 결과를 저장하고 커밋한 뒤 HTTP 201을 반환한다.

`NotificationService.send`는 기존 호출자 트랜잭션이 있더라도 `NOT_SUPPORTED`로 중단한다.
각 저장소 쓰기는 `REQUIRED`로 수행한다. HTTP 요청에는 상위 트랜잭션이 없으므로 각각 독립적으로 커밋된다.
DB 저장소를 다른 트랜잭션에서 직접 사용하면 그 트랜잭션에 참여한다.

상태는 `PROCESSING`에서 `PROVIDER_ACCEPTED`, `RETRYABLE_FAILURE`, `PERMANENT_FAILURE`,
`INTERNAL_FAILURE` 중 하나로 전이한다. 조건부 UPDATE로 이미 완료된 결과를 덮어쓰지 않는다.
S2에 멱등 요청이나 외부 호출 exactly-once 보장이 생기는 것은 아니다.

| 실패 시점 | 남는 상태와 동작 |
| --- | --- |
| 최초 DB 저장/연결 실패 | 503. 공급자 호출 전 중단. 커밋 응답이 유실되면 저장 여부는 불확실할 수 있음 |
| 최초 커밋 후 프로세스 종료 | PROCESSING 유지. 자동 재개 없음 |
| 공급자 일시/영구 실패 | 실패 결과를 DB에 저장하고 201. 자동 재시도 없음 |
| 공급자 예외 | INTERNAL_FAILURE로 저장. 예외 메시지와 payload는 로그/응답에 포함하지 않음 |
| 공급자 호출 후 결과 저장 실패 | DB 오류는 503. 실제 공급자 호출은 이미 일어났을 수 있음 |
| 결과 커밋 후 응답 유실 | 저장된 결과는 조회 가능하지만 클라이언트가 ID를 받지 못할 수 있음 |

실패한 SQL 문장의 롤백 후에는 PROCESSING이 남는다. 네트워크 단절 등으로 커밋 결과를 받지 못한 경우에는
DB에 최종 상태가 반영됐는지도 불확실할 수 있다. 503만으로 재호출의 안전성을 보장하지 않는다.

## 스키마

- `notification`: UUID PK, 수신자/제목/본문, 상태, 공급자 메시지 ID/오류 코드, 생성/완료 시각.
- `idx_notification_status_created(status, created_at, notification_id)`: 상태별 오래된 레코드 조회를 위한 준비.
- CHECK 제약: 상태 값, 완료 시각의 존재 여부, 성공/실패 결과 필드의 일관성.
- `outbox_event`: 이벤트 UUID PK, 알림 FK, 이벤트 타입/JSON payload, PENDING/PUBLISHED, 시도 수, 생성/예약/발행 시각.
- `(notification_id, event_type)` 유니크 제약: 알림별 같은 유형의 이벤트 중복 저장 방지용 준비. 요청 멱등성과는 별개다.
- `idx_outbox_status_available(status, available_at, event_id)`: 발행 대상 조회용 준비. S2에서 Relay는 실행하지 않는다.
- S3의 선점 상태/lease 등은 새 migration으로 추가한다. 이미 적용된 V1을 수정하지 않는다.

InnoDB, `utf8mb4`, UTC `DATETIME(6)`을 사용한다. UUID는 ASCII 대소문자 구분 컬럼으로 저장한다.
JDBC는 시간 값을 UTC `LocalDateTime`으로 변환하고 앱 Clock도 마이크로초 정밀도로 맞춘다.
인덱스 구성은 실제 DB 메타데이터로 확인하며, 성능 개선 수치는 아직 측정하지 않았다.

## 실행과 검증

로컬 MySQL은 Compose의 `127.0.0.1:13316`, named volume으로 실행한다. 앱의 HTTP 포트도 loopback이다.
테스트는 같은 MySQL 버전의 임시 컨테이너와 무작위 loopback 포트를 사용하며 Compose 데이터와 분리한다.
임시 비밀번호는 실행 시 생성한다. 실제 수신자와 외부 공급자는 사용하지 않는다.

`./gradlew test`에는 기존 단위/메모리 HTTP 테스트와 `MySqlNotificationTest`가 포함된다.
실제 HTTP 서버 및 DB로 다음을 검증한다.

- 성공 결과/한글·이모지/UTC 시간의 저장과 앱 재시작 후 동일 응답, Flyway 재실행 안전성.
- 공급자 호출 이전의 DB 커밋과 공급자 호출 시 트랜잭션 부재.
- 실패 결과 저장, 중복 PK 동시 삽입, 완료 상태 덮어쓰기 방지.
- 알림/Outbox SQL의 같은 트랜잭션 내 롤백, 완료 UPDATE 롤백.
- FK/유니크/CHECK/JSON 제약과 복합 인덱스 컬럼 순서.
- DB 쓰기 장애/연결 풀 사용 불가 시 503, 오류 상세 비노출, 미완료 상태의 재시작 후 유지.
- 입력 오류, health, Prometheus, env 비노출.

테스트의 Outbox 삽입은 스키마/롤백 검증용 SQL이며 API의 Outbox 기능 구현이 아니다.
장애 주입용 trigger의 생성/삭제만 임시 컨테이너의 root로 수행한다. 앱은 별도 DB 사용자로 실행한다.
DB 미설치/미기동을 이유로 테스트를 자동 스킵하지 않는다.

## 남은 한계

PROCESSING 자동 복구, 요청 멱등성, 재시도, Outbox 발행, Kafka 전달 보장, 대량 부하,
DB 고가용성/백업 및 레코드 보관 정책은 이 단계에서 제공하지 않는다.
연결 풀은 8개, 연결 대기는 3초로 제한하지만 이것이 전역 Rate Limit은 아니다.
