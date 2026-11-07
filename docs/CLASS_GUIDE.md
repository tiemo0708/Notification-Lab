# Notification Lab 클래스별 학습 안내

검토 기준: `13a3663`, 2026-11-07(Asia/Seoul). 런타임 Java 파일71개와 별도 HTTP Mock4개의 실제 선언을 기준으로 정리했다.
클래스·record·enum·interface를 모두 포함한다. Java의 실제 중첩 타입도 뒤에서 별도로 설명한다.
이 문서는 코드를 재구성한 결과가 아니라 현재 코드의 역할·호출 관계·읽는 순서를 정리한 것이다.

## 먼저 구분할 실행 방식

| 방식 | 접수 경로 | 저장·발송 |
| --- | --- | --- |
| 기본 비동기 | Controller → IdempotentSubmissionService → SubmissionService | MySQL 알림/작업/Outbox 저장 후202, Relay/Kafka/Worker가 후속 처리 |
| `sync` 비교 | Controller → NotificationService.send | MySQL에 PROCESSING 저장 → 공급자 호출 → 결과 저장 후201 |
| `memory` 비교 | Controller → NotificationService.send | 유한 메모리 저장과 공급자 호출 후201, 재시작 시 데이터 소실 |

기본 역할은 REALTIME, 공급자는 JVM 내부 MOCK, Redis 제한과 Circuit Breaker는 OFF다.
HTTP Mock·Redis·CAMPAIGN 역할은 이미 구현된 선택 구성이며 항상 켜져 있는 기본값이 아니다.
같은 JAR의 역할별 실행과 별도 물리 서버 배포를 구분한다. 현재 AWS/실제 FCM·APNs/단말 도착 검증은 미실행이다.

## 전체 흐름과 읽는 순서

```text
실시간 접수
NotificationController.create
  → IdempotentSubmissionService.submit
  → SubmissionService.accept
      ├─ JdbcNotificationStore.create → notification
      ├─ DeliveryTaskStore.create     → delivery_task
      └─ Outbox INSERT               → outbox_event
           [위 세 저장은 한 DB 트랜잭션]

후속 발송
OutboxRelay.relay → OutboxStore.claim
  → Kafka 발행 성공 확인 → OutboxStore.published
  → NotificationWorker.receive → WorkerAdmission.execute
  → WorkerStore.claim(후보 확인)
  → 필요한 경우 ProviderCircuitBreaker.acquire / WorkerQuota.awaitPermit
  → WorkerStore.claim(실제 선점·재검증)
  → NotificationService.attempt → PushProvider.send
  → WorkerStore.complete(결과·이력·필요한 후속 Outbox)
  → 정상 반환 뒤 Spring Kafka 컨테이너가 기본 RECORD 정책으로 offset 커밋

캠페인 DB 인계
CampaignController → CampaignService → CampaignStore
  → 별도 CampaignBatchCommand 실행
  → CampaignBatchService → CampaignTargetReader / CampaignTargetWriter
  → 대상별 알림·작업·Outbox·checkpoint 커밋
  → CAMPAIGN 역할 Relay/Kafka/Worker가 후속 발송
```

허가 기능이 꺼져 있으면 첫 claim에서 바로 선점할 수 있다. 이미 끝난 이벤트의 재전달은 공급자 호출 없이 종료한다.
DB/Kafka는 별도 시스템이다. Kafka publish 성공은 공급자 수락이 아니고 공급자 수락도 단말 도착이 아니다.

아래 표에서는 **역할 → 주요 메서드 → 연결·주의점** 순서로 읽는다. 타입 이름은 실제 소스 링크다.

## 진입점

| 타입 | 역할 | 주요 메서드 | 연결·읽을 때 확인할 점 |
| --- | --- | --- | --- |
| [NotificationLabApplication](../src/main/java/dev/jinhyeon/notificationlab/NotificationLabApplication.java) | 기동 | `main` | Spring Boot를 시작하고 같은 기본 패키지 아래의 Bean을 구성한다. 패키지 하나가 서버 하나인 구조는 아니다. |

## api — HTTP 요청·응답

| 타입 | 역할 | 주요 메서드 | 연결·읽을 때 확인할 점 |
| --- | --- | --- | --- |
| [ApiExceptionHandler](../src/main/java/dev/jinhyeon/notificationlab/api/ApiExceptionHandler.java) | 오류 응답 변환 | `storageUnavailable, invalidInput, idempotencyConflict` | 입력400/없음404/충돌409/저장소 오류503 등으로 변환한다. SQL·거절된 본문을 응답에 그대로 노출하지 않는다. |
| [CampaignController](../src/main/java/dev/jinhyeon/notificationlab/api/CampaignController.java) | 캠페인 HTTP 입구 | `create, start, get, audience` | CAMPAIGN 역할에서 CampaignService를 호출한다. 생성201/재요청200, 시작 의도202/재요청200을 구분하며 Batch를 자동 실행하지 않는다. |
| [DeadLetterController](../src/main/java/dev/jinhyeon/notificationlab/api/DeadLetterController.java) | DLQ HTTP 입구 | `get, redrive` | DeadLetterService를 호출해 선택한 DLQ 이벤트를 조회하거나 새 알림으로 재발행한다. |
| [NotificationController](../src/main/java/dev/jinhyeon/notificationlab/api/NotificationController.java) | HTTP 입구 | `create, get, delivery` | IdempotentSubmissionService로 비동기 접수하고 NotificationService/DeliveryTaskStore로 조회한다. 비교 프로필에서는 동기 send 경로를 사용한다. |

## notification — 알림 값·공급자 호출·기본 저장소

| 타입 | 역할 | 주요 메서드 | 연결·읽을 때 확인할 점 |
| --- | --- | --- | --- |
| [IdempotencyConflictException](../src/main/java/dev/jinhyeon/notificationlab/notification/IdempotencyConflictException.java) | 업무 오류 | `—` | 같은 멱등 키에 다른 요청 정의가 들어온 충돌. ApiExceptionHandler에서409. |
| [InMemoryNotificationStore](../src/main/java/dev/jinhyeon/notificationlab/notification/InMemoryNotificationStore.java) | 메모리 비교 저장소 | `create, update, find` | memory 프로필의 유한 ConcurrentHashMap 저장소다. 재시작 시 보존되지 않으며 영속 Outbox 기능이 아니다. |
| [InvalidIdempotencyKeyException](../src/main/java/dev/jinhyeon/notificationlab/notification/InvalidIdempotencyKeyException.java) | 입력 오류 | `—` | 멱등 키의 형식·개수·지원 조건 오류. HTTP400. |
| [JdbcNotificationStore](../src/main/java/dev/jinhyeon/notificationlab/notification/JdbcNotificationStore.java) | MySQL 저장소 | `create, update, find, findByIdempotencyKey` | NotificationStore 구현. notification INSERT/조건부 UPDATE/행 매핑을 맡는다. 기본·sync에서 사용하고 memory에서는 제외한다. |
| [Notification](../src/main/java/dev/jinhyeon/notificationlab/notification/Notification.java) | 알림 값 record | `completed` | 수신 대상·내용·상태·공급자 ID·시각을 묶는다. completed는 PROCESSING에서 허용된 결과로 바뀐 새 값을 반환하며 DB 저장은 하지 않는다. |
| [NotificationNotFoundException](../src/main/java/dev/jinhyeon/notificationlab/notification/NotificationNotFoundException.java) | 조회 오류 | `—` | 요청한 알림을 찾지 못함. HTTP404. |
| [NotificationRequest](../src/main/java/dev/jinhyeon/notificationlab/notification/NotificationRequest.java) | 접수 DTO record | `—` | recipientId/title/body를 받는다. 빈 값과 길이 제한은 Bean Validation으로 검사한다. |
| [NotificationService](../src/main/java/dev/jinhyeon/notificationlab/notification/NotificationService.java) | 공급자 호출·알림 조회 | `send, attempt, recordCompletion, get` | attempt는 PushProvider 호출 결과를 Notification으로 변환하고 recordCompletion은 지표를 남긴다. send는 동기 비교 경로, 비동기 발송 제어는 NotificationWorker가 맡는다. |
| [NotificationStatus](../src/main/java/dev/jinhyeon/notificationlab/notification/NotificationStatus.java) | 상태 enum | `—` | QUEUED/PROCESSING/WAITING_RETRY와 공급자 수락·각 실패 상태를 정의한다. PROVIDER_ACCEPTED는 단말 도착이 아니다. |
| [NotificationStore](../src/main/java/dev/jinhyeon/notificationlab/notification/NotificationStore.java) | 저장소 interface | `create, update, find` | 알림 값의 저장/갱신/조회 계약이다. 이 인터페이스만으로 Outbox·작업·재시도 트랜잭션 전체를 표현하지 않는다. |
| [StoreCapacityExceededException](../src/main/java/dev/jinhyeon/notificationlab/notification/StoreCapacityExceededException.java) | 메모리 용량 오류 | `—` | 메모리 비교 저장소의 상한 초과. HTTP503이며 MySQL 전체 행 수 제한을 의미하지 않는다. |

## outbox — 영속 접수와 Kafka 인계

| 타입 | 역할 | 주요 메서드 | 연결·읽을 때 확인할 점 |
| --- | --- | --- | --- |
| [IdempotentSubmissionService](../src/main/java/dev/jinhyeon/notificationlab/outbox/IdempotentSubmissionService.java) | 멱등 접수 조정 | `submit` | 키 형식 검사→SubmissionService.accept. DuplicateKeyException 뒤 롤백이 끝난 상태에서 승자 행의 역할·내용을 비교해 재사용 여부를 반환한다. |
| [OutboxRelay](../src/main/java/dev/jinhyeon/notificationlab/outbox/OutboxRelay.java) | DB→Kafka 발행 조정 | `relay, stopClaiming` | OutboxStore로 선점 후 DB 트랜잭션 밖에서 Kafka 성공을 기다린다. 확인된 항목만 published 처리한다. 제출 창·완료 저장 묶음은 기본1/선택1~10이다. |
| [OutboxStore](../src/main/java/dev/jinhyeon/notificationlab/outbox/OutboxStore.java) | Outbox 선점·발행 상태 | `claim, published, failed` | 짧은 READ COMMITTED 트랜잭션, 역할별 인덱스, SKIP LOCKED로 후보를 선점한다. lease token을 확인해 완료/실패를 갱신한다. Kafka 전송은 하지 않는다. |
| [PipelineMetrics](../src/main/java/dev/jinhyeon/notificationlab/outbox/PipelineMetrics.java) | 파이프라인 관찰 | `refresh` | 역할별 미발행/실패/최고 대기 나이, 요청·Retry의 Kafka lag, 표본 나이와 Worker 실행 상태를 갱신한다. 조회 실패를 정상0으로 감추지 않는다. |
| [SubmissionService](../src/main/java/dev/jinhyeon/notificationlab/outbox/SubmissionService.java) | 접수 트랜잭션 | `accept` | JdbcNotificationStore와 DeliveryTaskStore를 호출하고 Outbox를 INSERT한다. 세 데이터 저장을 같은 @Transactional 경계로 묶는다. |

## worker — 소비·허가·작업 소유권·결과

| 타입 | 역할 | 주요 메서드 | 연결·읽을 때 확인할 점 |
| --- | --- | --- | --- |
| [DeliveryTaskStore](../src/main/java/dev/jinhyeon/notificationlab/worker/DeliveryTaskStore.java) | 발송 작업 생성·조회 | `create, find` | 최초 delivery_task를 만들고 작업의 상태/시도 이력 조회 DTO를 조립한다. Worker의 전체 선점·완료 상태 머신은 WorkerStore 책임이다. |
| [NotificationWorker](../src/main/java/dev/jinhyeon/notificationlab/worker/NotificationWorker.java) | Kafka 발송 조정 | `receive` | WorkerAdmission→작업 후보 확인→필요 시 circuit/quota 허가→재선점→NotificationService.attempt→WorkerStore.complete를 연결한다. 실패를 정상 반환으로 숨기지 않는다. |
| [RetryPolicy](../src/main/java/dev/jinhyeon/notificationlab/worker/RetryPolicy.java) | 재시도 판단·지연 계산 | `shouldRetry, delayMillis` | RETRYABLE_FAILURE와 남은 시도 예산을 검사하고 상한 있는 backoff+jitter를 계산한다. 스레드 sleep이나 토픽 발행을 직접 하지 않는다. |
| [WorkerAdmission](../src/main/java/dev/jinhyeon/notificationlab/worker/WorkerAdmission.java) | 프로세스 안 동시성·FIFO 대기 | `execute, active, waiting, stopAdmission` | Consumer 호출 스레드에서 동기 실행한다. 활성 자리/유한 대기열/대기 시간을 제한하며 별도 무한 Executor 작업 큐를 만들지 않는다. |
| [WorkerPauseErrorHandler](../src/main/java/dev/jinhyeon/notificationlab/worker/WorkerPauseErrorHandler.java) | 소비 pause·복구·중단 | `handleOne, handleRemaining, consumerStopped, close` | quota/circuit 대기는 레코드를 미처리로 보존하며 pause/resume한다. opt-in Redis 일시 오류 복구에는 횟수·레코드 상한을 적용하고 상태 오류/기타 오류는 중단한다. |
| [WorkerQuota](../src/main/java/dev/jinhyeon/notificationlab/worker/WorkerQuota.java) | 발송 허가 조정 | `awaitPermit, stopAdmission` | RedisRateLimiter의 ALLOWED/LIMITED/UNAVAILABLE를 해석한다. 기본 pause 경로에서는 한 번 확인 후 필요 시 지연 신호를 던진다. 비교용 유한 대기 경로와 구분한다. |
| [WorkerStore](../src/main/java/dev/jinhyeon/notificationlab/worker/WorkerStore.java) | Worker 소유권·완료 트랜잭션 | `claim, complete` | 작업/이벤트 세대·역할·lease를 검사하고 시도 횟수/STARTED를 저장한다. 완료 시 소유 token을 재검사해 결과·이력·Retry/DLQ Outbox를 함께 커밋한다. |

## provider — 공급자 계약과 구현

| 타입 | 역할 | 주요 메서드 | 연결·읽을 때 확인할 점 |
| --- | --- | --- | --- |
| [HttpPushProvider](../src/main/java/dev/jinhyeon/notificationlab/provider/HttpPushProvider.java) | 별도 HTTP Mock 어댑터 | `send, close` | /pushes 호출, 연결/대기 상한과 전체 deadline, 제한된 응답 크기·오류 분류를 맡는다. 클라이언트 자동 재시도는 꺼서 DB 재시도 정책과 겹치지 않게 한다. |
| [MockPushProvider](../src/main/java/dev/jinhyeon/notificationlab/provider/MockPushProvider.java) | JVM 내부 Mock | `send` | 기본 MOCK 모드 구현. MockProperties의 지연과 결과 설정으로 합성 응답을 만들며 외부 푸시 전송을 하지 않는다. |
| [ProviderCircuitBreaker](../src/main/java/dev/jinhyeon/notificationlab/provider/ProviderCircuitBreaker.java) | 프로세스별 공급자 회로 | `acquire, state, enabled` | CLOSED/OPEN/HALF_OPEN과 단일 probe를 관리한다. HTTP 결과를 Permit으로 관찰하며 DB 완료 실패를 공급자 장애로 취급하지 않는다. Redis 공유 회로가 아니다. |
| [PushProvider](../src/main/java/dev/jinhyeon/notificationlab/provider/PushProvider.java) | 공급자 호출 interface | `send` | PushRequest→PushResult 계약. 현재 구현은 MockPushProvider와 HttpPushProvider이며 실제 FCM/APNs 구현은 없다. |
| [PushRequest](../src/main/java/dev/jinhyeon/notificationlab/provider/PushRequest.java) | 공급자 요청 record | `—` | notificationId/recipientId/title/body를 공급자 구현에 전달하는 값이다. Kafka 이벤트의 ID 중심 payload와 구분한다. |
| [PushResult](../src/main/java/dev/jinhyeon/notificationlab/provider/PushResult.java) | 공급자 응답 record | `—` | Outcome과 providerMessageId/errorCode를 반환한다. 결과 enum은 ACCEPTED/RETRYABLE_FAILURE/PERMANENT_FAILURE다. |

## ratelimit — 공유 발송 예산

| 타입 | 역할 | 주요 메서드 | 연결·읽을 때 확인할 점 |
| --- | --- | --- | --- |
| [RedisRateLimiter](../src/main/java/dev/jinhyeon/notificationlab/ratelimit/RedisRateLimiter.java) | 여러 Worker의 공유 호출 예산 | `tryAcquire, diagnostics, close` | 작은 연결 풀과 timeout 안에서 Lua를 실행해 토큰을 원자적으로 확인·차감한다. 정상 한도 부족과 조회 불가를 분리하고 상태를 자동 초기화하지 않는다. |

## deadletter — 최종 실패의 선택 재발행

| 타입 | 역할 | 주요 메서드 | 연결·읽을 때 확인할 점 |
| --- | --- | --- | --- |
| [DeadLetterNotFoundException](../src/main/java/dev/jinhyeon/notificationlab/deadletter/DeadLetterNotFoundException.java) | DLQ 조회 오류 | `—` | 선택한 최종 실패 이벤트가 없음. HTTP404. |
| [DeadLetterService](../src/main/java/dev/jinhyeon/notificationlab/deadletter/DeadLetterService.java) | 최종 실패 조회·선택 재발행 | `find, redrive` | 원본 DLQ의 PUBLISHED·역할·최종 실패 상태를 검사한다. 원본을 보존한 새 알림과 감사 이력을 만들고 같은 이벤트의 재발행·계보 상한을 제어한다. |
| [RedriveConflictException](../src/main/java/dev/jinhyeon/notificationlab/deadletter/RedriveConflictException.java) | 재발행 조건 충돌 | `—` | 미발행 DLQ/다른 이유의 재요청/계보 상한/역할 등 재발행 계약 위반. HTTP409. |
| [RedriveRequest](../src/main/java/dev/jinhyeon/notificationlab/deadletter/RedriveRequest.java) | 수동 재발행 입력 record | `—` | Reason과 중복 가능성 확인을 받는다. 서버가 원본을 덮어쓰거나 확인 없이 자동 재발행하도록 만드는 요청이 아니다. |

## campaign — 대량 발송 정의와 가상 대상

| 타입 | 역할 | 주요 메서드 | 연결·읽을 때 확인할 점 |
| --- | --- | --- | --- |
| [Campaign](../src/main/java/dev/jinhyeon/notificationlab/campaign/Campaign.java) | 캠페인 정의 record | `—` | 이름·내용·고정 audience·대상 수·시작 의도를 담는다. Status는 DRAFT/START_REQUESTED이며 Batch JobExecution 상태와 다르다. |
| [CampaignAudiencePage](../src/main/java/dev/jinhyeon/notificationlab/campaign/CampaignAudiencePage.java) | 대상자 페이지 record | `—` | campaignId/audience/targetCount/items/nextAfter를 반환한다. Target은 ordinal과 합성 recipientId를 담는다. |
| [CampaignAudienceUnavailableException](../src/main/java/dev/jinhyeon/notificationlab/campaign/CampaignAudienceUnavailableException.java) | 대상 정의 오류 | `—` | 지원되지 않는 audience/정의 상태. HTTP503. |
| [CampaignNotFoundException](../src/main/java/dev/jinhyeon/notificationlab/campaign/CampaignNotFoundException.java) | 캠페인 조회 오류 | `—` | 캠페인이 없음. HTTP404. |
| [CampaignRequest](../src/main/java/dev/jinhyeon/notificationlab/campaign/CampaignRequest.java) | 캠페인 생성 DTO record | `—` | 이름·제목·본문·대상 수를 받는다. 실제 회원 목록이나 임의 SQL을 입력받는 모델은 아니다. |
| [CampaignService](../src/main/java/dev/jinhyeon/notificationlab/campaign/CampaignService.java) | 캠페인 업무 조정 | `create, get, audience, start` | 키가 필수인 생성 멱등성을 처리하고 CampaignStore/SyntheticCampaignAudience로 위임한다. start는 DB 시작 의도 저장까지다. |
| [CampaignStore](../src/main/java/dev/jinhyeon/notificationlab/campaign/CampaignStore.java) | 캠페인 MySQL 저장소 | `create, find, findByKey, start` | 정의를 저장하고 조건부 UPDATE로 시작 요청의 승자를 정한다. 현재 완료된 발송 수를 근거 없이 캠페인 상태에 반영하지 않는다. |
| [CampaignTargetCountDeserializer](../src/main/java/dev/jinhyeon/notificationlab/campaign/CampaignTargetCountDeserializer.java) | 정수 입력 역직렬화 | `deserialize` | targetCount를 명시적으로 정수 토큰에서 읽는다. 임의의 소수·문자열을 조용히 정수로 변환하는 것을 방지한다. |
| [InvalidCampaignPageException](../src/main/java/dev/jinhyeon/notificationlab/campaign/InvalidCampaignPageException.java) | 페이지 입력 오류 | `—` | after/limit 범위가 잘못됨. HTTP400. |
| [SyntheticCampaignAudience](../src/main/java/dev/jinhyeon/notificationlab/campaign/SyntheticCampaignAudience.java) | 고정 가상 대상 조회 | `read` | SYNTHETIC_V1의 1..targetCount를 after 뒤부터 제한된 페이지로 생성한다. 실제 회원 DB의 SQL 성능을 검증하는 reader는 아니다. |

## campaign/batch — 대상별 영속 인계

| 타입 | 역할 | 주요 메서드 | 연결·읽을 때 확인할 점 |
| --- | --- | --- | --- |
| [CampaignBatchCommand](../src/main/java/dev/jinhyeon/notificationlab/campaign/batch/CampaignBatchCommand.java) | 별도 Batch CLI 진입점 | `main` | UUID 한 개를 받아 웹/Worker/Relay 없는 CAMPAIGN 컨텍스트에서 실행한다. Batch COMPLETED이면 종료0을 반환한다. |
| [CampaignBatchConfiguration](../src/main/java/dev/jinhyeon/notificationlab/campaign/batch/CampaignBatchConfiguration.java) | Batch 인프라 Bean 설정 | `campaignBatchService` | 명시적 batch-enabled 조건에서 DefaultBatchConfiguration과 공통 DataSource/트랜잭션 관리자 및 chunk 설정을 연결한다. |
| [CampaignBatchService](../src/main/java/dev/jinhyeon/notificationlab/campaign/batch/CampaignBatchService.java) | 한 캠페인의 Job/Step 실행 | `execute` | 진행 중 실행·중단 실행의 복구 조건을 검사하고 Reader/Writer로 chunk step을 조립한다. Job 결과는 DB 인계 결과다. |
| [CampaignBatchSession](../src/main/java/dev/jinhyeon/notificationlab/campaign/batch/CampaignBatchSession.java) | Batch의 물리 세션 소유권 | `open, close` | 한 MySQL 물리 세션의 GET_LOCK과 같은 업무/메타데이터 연결을 사용한다. 자동 재연결을 거부한다. 시퀀스 번호 할당용 연결은 별도로 둔다. |
| [CampaignTargetIds](../src/main/java/dev/jinhyeon/notificationlab/campaign/batch/CampaignTargetIds.java) | 결정적 ID record | `of` | campaignId+ordinal+버전별 namespace에서 알림/작업/이벤트 UUID를 만든다. 같은 대상의 재실행에 ID가 바뀌지 않도록 한다. |
| [CampaignTargetReader](../src/main/java/dev/jinhyeon/notificationlab/campaign/batch/CampaignTargetReader.java) | checkpoint를 가진 ItemStreamReader | `open, read, update, close` | campaign.lastOrdinal에서 재개해 합성 대상을 한 페이지씩 읽는다. 읽은 위치는 chunk 트랜잭션의 checkpoint로 저장한다. |
| [CampaignTargetWriter](../src/main/java/dev/jinhyeon/notificationlab/campaign/batch/CampaignTargetWriter.java) | 대상별 영속 인계 ItemWriter | `write` | 대상 identity를 확인하고 notification/delivery_task/outbox_event/campaign_target을 저장한다. 재실행 시 이미 일치하는 행은 보존한다. |

## config — Bean 조립과 설정 값

| 타입 | 역할 | 주요 메서드 | 연결·읽을 때 확인할 점 |
| --- | --- | --- | --- |
| [AsyncConfiguration](../src/main/java/dev/jinhyeon/notificationlab/config/AsyncConfiguration.java) | Kafka 비동기 Bean 설정 | `commitWakeupRetry, notificationTopic, notificationRetryTopic, notificationDeadLetterTopic, shutdownOrder` | 요청/Retry/DLQ 토픽을 기본 각3파티션/복제본1로 생성하고 commit 어댑터·컨테이너 종료/poll 설정을 연결한다. |
| [AsyncProperties](../src/main/java/dev/jinhyeon/notificationlab/config/AsyncProperties.java) | 비동기 설정 record | `isTopicSeparationValid, destination` | 토픽·group·발행 시도 상한을 바인딩하고 이벤트 종류별 기본 목적지를 결정한다. |
| [CommitWakeupRetry](../src/main/java/dev/jinhyeon/notificationlab/config/CommitWakeupRetry.java) | 완료 offset 커밋 어댑터 | `wrap` | 명시적 offset Map+Duration의 commitSync에서 WakeupException만 원래 deadline 잔여 시간으로1회 재시도한다. poll/공급자/implicit/async commit은 이 재시도 대상이 아니다. |
| [DeliveryProperties](../src/main/java/dev/jinhyeon/notificationlab/config/DeliveryProperties.java) | 발송 재시도 설정 record | `isRetryDelayRangeValid` | 최대 시도 횟수와 기본/최대 지연을 바인딩하고 범위를 검증한다. |
| [HttpProviderConfiguration](../src/main/java/dev/jinhyeon/notificationlab/config/HttpProviderConfiguration.java) | HTTP 공급자 선택 | `httpPushProvider` | app.provider.mode=HTTP일 때 close 수명주기를 가진 HttpPushProvider를 등록한다. |
| [LabConfiguration](../src/main/java/dev/jinhyeon/notificationlab/config/LabConfiguration.java) | 공통 Bean·프로필 검증 | `clock, validateTrafficProfile` | UTC 마이크로초 단위 Clock을 제공하고 memory/sync와 traffic split의 잘못된 조합을 막는다. |
| [MockProperties](../src/main/java/dev/jinhyeon/notificationlab/config/MockProperties.java) | 내부 Mock 설정 record | `—` | JVM 내부 Mock의 지연과 Outcome을 바인딩한다. 별도 HTTP Mock의 HttpMockProperties와 다르다. |
| [ProviderProperties](../src/main/java/dev/jinhyeon/notificationlab/config/ProviderProperties.java) | 공급자·HTTP 설정 record | `—` | Mode와 중첩 Http 설정을 검증한다. 현재 HTTP origin은 명시적 포트의127.0.0.1로 제한한다. |
| [RateLimitConfiguration](../src/main/java/dev/jinhyeon/notificationlab/config/RateLimitConfiguration.java) | Redis 제한기 선택 | `redisRateLimiter` | client-enabled=true일 때 공유 RedisRateLimiter를 등록하고 종료 시 연결을 정리한다. |
| [RateLimitProperties](../src/main/java/dev/jinhyeon/notificationlab/config/RateLimitProperties.java) | Redis 연결·예산 설정 record | `—` | host/port/key/rate/burst/연결 상한/timeout을 검증한다. 현재 host는127.0.0.1만 허용한다. |
| [RelayProperties](../src/main/java/dev/jinhyeon/notificationlab/config/RelayProperties.java) | Relay 선택 옵션 record | `—` | 완료 저장 묶음과 동시 제출 창을 각각1~10으로 제한한다. 둘 다 기본1이다. |
| [StoreProperties](../src/main/java/dev/jinhyeon/notificationlab/config/StoreProperties.java) | 메모리 저장 상한 record | `—` | memory 비교 저장소의 최대 레코드 수를 제한한다. |
| [TrafficProperties](../src/main/java/dev/jinhyeon/notificationlab/config/TrafficProperties.java) | 배포 역할·예약 몫 설정 record | `topic` | Lane, split 활성화, 캠페인 rate/burst를 묶는다. 역할은 프로세스 설정이며 Kafka payload가 마음대로 바꾸지 않는다. |
| [TrafficRouting](../src/main/java/dev/jinhyeon/notificationlab/config/TrafficRouting.java) | 역할별 목적지 계산 | `topic, retryTopic, deadLetterTopic, group, destination` | REALTIME/CAMPAIGN 토픽·group을 결정한다. CAMPAIGN suffix와 토픽 중복, split에 필요한 quota/pause 조건을 검증한다. |
| [WorkerProperties](../src/main/java/dev/jinhyeon/notificationlab/config/WorkerProperties.java) | Worker 용량 설정 record | `—` | Consumer 수, 활성 상한, 큐 길이/대기 시간의 유한 범위를 검증한다. 기본1/1/2는 실험3/3/0과 다르다. |

## 별도 src/mockServer — 외부 공급자 시뮬레이터

| 타입 | 역할 | 주요 메서드 | 연결·읽을 때 확인할 점 |
| --- | --- | --- | --- |
| [FixedWindowQuota](../src/mockServer/java/dev/jinhyeon/notificationmock/FixedWindowQuota.java) | Mock 프로세스별 고정 윈도우 | `acquire` | 같은 Mock 서버의 요청 수를 유한 시간 창에서 센다. 여러 Worker의 Redis 공유 예산과 다른 제한이다. |
| [HttpMockProperties](../src/mockServer/java/dev/jinhyeon/notificationmock/HttpMockProperties.java) | HTTP Mock 설정 record | `—` | Mode/응답 지연/윈도우 요청 상한/윈도우 길이/동시 처리 상한을 검증한다. |
| [MockPushEndpoint](../src/mockServer/java/dev/jinhyeon/notificationmock/MockPushEndpoint.java) | 합성 공급자 HTTP API | `push, stats, config` | /pushes의 입력·요청 크기·rate·동시 처리 상한을 검사한다. 수락을 기록한 뒤 응답 지연을 적용해 timeout 불확실성을 재현한다. |
| [MockServerApplication](../src/mockServer/java/dev/jinhyeon/notificationmock/MockServerApplication.java) | 별도 Mock 서버 진입점 | `main, start` | mock-server 설정을 사용해 별도 Spring Boot HTTP 서버를 시작한다. DB/Flyway/Kafka 자동 구성을 제외한다. |

## 이름이 비슷한 클래스의 책임 차이

| 비교 | 구분 |
| --- | --- |
| NotificationController / NotificationService | HTTP 접수·상태 코드와 공급자 호출 결과 변환. 비동기 API가 NotificationService.send로 즉시 발송하는 구조는 아니다 |
| NotificationService / NotificationWorker | 공급자 한 번 호출·결과 변환과 Kafka 레코드 전체의 허가/선점/저장 조정 |
| JdbcNotificationStore / WorkerStore | notification 기본 CRUD와 Worker의 작업/이력/Retry·DLQ 트랜잭션 |
| DeliveryTaskStore / WorkerStore | 최초 작업 생성·조회 DTO 조립과 작업 소유권/시도 시작/완료 상태 머신 |
| OutboxStore / OutboxRelay | DB의 선점/상태 변경과 DB 밖 Kafka I/O 조정 |
| WorkerAdmission / WorkerQuota | 한 JVM의 자리·대기열과 여러 Worker가 공유하는 외부 호출 예산 |
| RedisRateLimiter / FixedWindowQuota | Worker들이 Redis에서 공유하는 예산과 HTTP Mock 한 프로세스의 별도 공급자 제한 |
| RetryPolicy / WorkerPauseErrorHandler | 실제 공급자 실패 뒤 업무 재시도 계획과 아직 발송 허가가 없는 레코드의 소비 대기/복구 |
| CampaignStore / CampaignBatchService | 정의·시작 의도 저장과 명시적 대상별 DB 인계 실행 |

## 실제 코드로 확인하는 다섯 지점

다음 Java 블록은 연결된 파일의 연속된 행을 그대로 발췌했다. 메서드 전체가 아닌 블록도 있으며 독립 실행용 예제가 아니다.

### 접수 결과: 새 접수202와 재사용200

[NotificationController](../src/main/java/dev/jinhyeon/notificationlab/api/NotificationController.java) 47~50행. submit이 반환되기 전에 접수 트랜잭션이 끝난다. 이 응답은 공급자 수락을 의미하지 않는다.

```java
            var result = submission.submit(request, key);
            return ResponseEntity.status(result.replayed() ? 200 : 202)
                    .location(URI.create("/notifications/" + result.notification().notificationId()))
                    .body(result.notification());
```

### 세 저장의 트랜잭션

[SubmissionService](../src/main/java/dev/jinhyeon/notificationlab/outbox/SubmissionService.java) 35~51행. 호출된 JdbcNotificationStore/DeliveryTaskStore는 같은 트랜잭션에 참여한다. notification·delivery_task·outbox_event 중 일부만 커밋하지 않는다.

```java
    @Transactional
    public Notification accept(NotificationRequest request, String idempotencyKey) {
        var notification = new Notification(UUID.randomUUID(), request.recipientId(), request.title(),
                request.body(), NotificationStatus.QUEUED, null, null, clock.instant(), null);
        notifications.create(notification, idempotencyKey);
        jdbc.update("UPDATE notification SET traffic_class = ? WHERE notification_id = ?",
                traffic.lane().name(), notification.notificationId().toString());
        UUID taskId = tasks.create(notification);
        String eventId = UUID.randomUUID().toString();
        jdbc.update("""
                INSERT INTO outbox_event (event_id, notification_id, event_type, payload, traffic_class, created_at, available_at)
                VALUES (?, ?, 'NOTIFICATION_REQUESTED', JSON_OBJECT('schemaVersion', 2,
                    'eventId', ?, 'notificationId', ?, 'taskId', ?), ?, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6))
                """, eventId, notification.notificationId().toString(), eventId,
                notification.notificationId().toString(), taskId.toString(), traffic.lane().name());
        return notification;
    }
```

### Kafka 성공 대기

[OutboxRelay](../src/main/java/dev/jinhyeon/notificationlab/outbox/OutboxRelay.java) 110~117행. 선점 메서드가 반환되며 DB 트랜잭션은 끝나 있다. 이후 확인된 성공만 flush에서 published 처리한다. Kafka ACK 뒤 DB 저장 전에 죽으면 재발행 가능성이 남는다.

```java
                    var next = claimTime.record(() -> store.claim(properties.maxPublishAttempts()));
                    if (next.isEmpty()) return;
                    var claim = next.get();
                    try {
                        var ack = Timer.start(registry);
                        try {
                            // No surrounding DB transaction. Only confirmed ACKs can become PUBLISHED.
                            kafka.send(routing.destination(claim.eventType()), claim.notificationId(), claim.payload()).get(ackWaitNanos, TimeUnit.NANOSECONDS);
```

### 발송 호출과 결과 저장

[NotificationWorker](../src/main/java/dev/jinhyeon/notificationlab/worker/NotificationWorker.java) 75~87행. 첫 claim은 허가 전 후보 검사일 수 있다. 허가 뒤 다시 선점하고, 공급자 호출 중 DB 트랜잭션을 유지하지 않는다. complete의 true는 재시도 예약을 함께 저장했다는 뜻이며 단순 성공 플래그가 아니다.

```java
            var claimed = initialClaimTime.record(() -> store.claim(id, eventId, token, taskId, !quota.enabled() && !circuit.enabled()));
            try (var permit = claimed.isPresent() && claimed.get().needsPermit() ? circuit.acquire() : null) {
                if (claimed.isPresent() && claimed.get().needsPermit()) {
                    quota.awaitPermit();
                    claimed = admittedClaimTime.record(() -> store.claim(id, eventId, token, taskId)); // Revalidate after admission, then start the attempt.
                }
                if (claimed.isPresent()) {
                    Timer.Sample sample = Timer.start(registry);
                    var claim = claimed.get();
                    if (claim.send()) {
                        var completed = providerTime.record(() -> service.attempt(claim.notification())); // Outside all DB transactions.
                        if (permit != null) permit.complete(completed); // Observe HTTP outcome, independently of DB completion.
                        if (completionTime.record(() -> store.complete(completed, token))) { // History and retry outbox commit before the original offset.
```

### Batch chunk 조립

[CampaignBatchService](../src/main/java/dev/jinhyeon/notificationlab/campaign/batch/CampaignBatchService.java) 57~61행. Reader가 대상 페이지를 읽고 Writer가 DB에 인계한다. Batch COMPLETED를 전체 발송 완료로 해석하지 않는다.

```java
                var step = new StepBuilder("expand-targets-v1", session.repository)
                        .<Target, Target>chunk(chunkSize, session.transactions)
                        .reader(new CampaignTargetReader(campaign, chunkSize))
                        .writer(new CampaignTargetWriter(campaign, session.jdbc, maxAttempts))
                        .build();
```

## 클래스 안에 선언된 실제 중첩 타입

여기부터는 `Outer.Inner`처럼 소스 안에 함께 선언된 타입이다. 패키지 안의 별도 Java 파일과 구분한다.
이 소스의 멤버 record/enum은 데이터를 묶거나 선택지를 제한하는 중첩 타입이다.
특히 `ProviderCircuitBreaker.Permit`은 바깥 인스턴스의 상태를 사용하는 비정적 내부 클래스다.

| 중첩 타입 | 종류 | 맡은 역할 |
| --- | --- | --- |
| [Campaign.Status](../src/main/java/dev/jinhyeon/notificationlab/campaign/Campaign.java) (9행) | enum | 캠페인 정의의 DRAFT/START_REQUESTED. Batch 실행 결과 enum과 다르다. |
| [CampaignAudiencePage.Target](../src/main/java/dev/jinhyeon/notificationlab/campaign/CampaignAudiencePage.java) (12행) | record | 대상 번호 ordinal과 합성 수신자 ID 한 쌍. |
| [CampaignService.Creation](../src/main/java/dev/jinhyeon/notificationlab/campaign/CampaignService.java) (21행) | record | 캠페인 값과 재사용 여부. Controller가201/200을 선택하는 근거. |
| [CampaignStore.Start](../src/main/java/dev/jinhyeon/notificationlab/campaign/CampaignStore.java) (53행) | record | 시작 요청 뒤 캠페인 값과 이전에 시작 요청됐는지 여부. |
| [MockProperties.Outcome](../src/main/java/dev/jinhyeon/notificationlab/config/MockProperties.java) (14행) | enum | 내부 Mock의 SUCCESS/TRANSIENT_FAILURE/PERMANENT_FAILURE 설정. |
| [ProviderProperties.Mode](../src/main/java/dev/jinhyeon/notificationlab/config/ProviderProperties.java) (8행) | enum | JVM 내부 MOCK 또는 별도 HTTP 어댑터 선택. |
| [ProviderProperties.Http](../src/main/java/dev/jinhyeon/notificationlab/config/ProviderProperties.java) (14행) | record | HTTP origin·연결 자리·대기·각 timeout·전체 deadline 설정 값. |
| [TrafficProperties.Lane](../src/main/java/dev/jinhyeon/notificationlab/config/TrafficProperties.java) (12행) | enum | 프로세스 역할 REALTIME/CAMPAIGN. 메시지 본문에서 선택하지 않는다. |
| [DeadLetterService.Redrive](../src/main/java/dev/jinhyeon/notificationlab/deadletter/DeadLetterService.java) (35행) | record | 원본/루트/신규 알림 ID, 재발행 순번·이유·중복 가능성 확인·시각의 감사 결과. |
| [DeadLetterService.Result](../src/main/java/dev/jinhyeon/notificationlab/deadletter/DeadLetterService.java) (38행) | record | 재발행 감사 값과 같은 이벤트의 재요청인지 여부. |
| [DeadLetterService.DeadLetter](../src/main/java/dev/jinhyeon/notificationlab/deadletter/DeadLetterService.java) (39행) | record | 최종 실패 이벤트의 발행 상태·작업·실패 결과·시도 예산·재발행 정보를 담는 조회 값. |
| [DeadLetterService.Source](../src/main/java/dev/jinhyeon/notificationlab/deadletter/DeadLetterService.java) (41행) | record | private SQL 행 매핑: 원본 알림 ID와 DLQ 발행 상태. |
| [DeadLetterService.Parent](../src/main/java/dev/jinhyeon/notificationlab/deadletter/DeadLetterService.java) (42행) | record | private SQL 행 매핑: 재발행 계보의 루트와 순번. |
| [RedriveRequest.Reason](../src/main/java/dev/jinhyeon/notificationlab/deadletter/RedriveRequest.java) (7행) | enum | 수동 재발행 사유: 공급자 복구/설정 수정/수동 검토. |
| [IdempotentSubmissionService.Result](../src/main/java/dev/jinhyeon/notificationlab/outbox/IdempotentSubmissionService.java) (33행) | record | 알림 값과 재사용 여부. 접수202와 재요청200을 구분한다. |
| [OutboxRelay.Pending](../src/main/java/dev/jinhyeon/notificationlab/outbox/OutboxRelay.java) (144행) | record | 제출한 Outbox Claim, Kafka future와 시작 시간을 함께 보관하는 private 값. 제출 창의 미완료 항목을 추적한다. |
| [OutboxStore.Claim](../src/main/java/dev/jinhyeon/notificationlab/outbox/OutboxStore.java) (27행) | record | 선점한 이벤트 ID·알림 ID·payload·소유 token·시도·종류. published/failed가 같은 소유자를 검사하는 데 사용한다. |
| [OutboxStore.Candidate](../src/main/java/dev/jinhyeon/notificationlab/outbox/OutboxStore.java) (53행) | record | private 선점 후보와 availableAt. 요청 후보들의 정렬 기준을 함께 가진다. |
| [ProviderCircuitBreaker.State](../src/main/java/dev/jinhyeon/notificationlab/provider/ProviderCircuitBreaker.java) (18행) | enum | CLOSED/OPEN/HALF_OPEN 회로 상태. |
| [ProviderCircuitBreaker.Result](../src/main/java/dev/jinhyeon/notificationlab/provider/ProviderCircuitBreaker.java) (19행) | enum | private HTTP 결과 분류 SUCCESS/FAILURE/IGNORED. 모든4xx·429·로컬 오류를 공급자 장애로 세지 않는다. |
| [ProviderCircuitBreaker.Permit](../src/main/java/dev/jinhyeon/notificationlab/provider/ProviderCircuitBreaker.java) (98행) | class | 비정적 내부 클래스. 바깥 회로의 세대와 probe 상태에 연결된 AutoCloseable 허가 객체다. |
| [ProviderCircuitBreaker.CircuitDeferred](../src/main/java/dev/jinhyeon/notificationlab/provider/ProviderCircuitBreaker.java) (133행) | class | static 중첩 예외. bounded delay만 전달해 pause를 요청한다. |
| [PushResult.Outcome](../src/main/java/dev/jinhyeon/notificationlab/provider/PushResult.java) (4행) | enum | 공급자 응답의 ACCEPTED/RETRYABLE_FAILURE/PERMANENT_FAILURE. |
| [RedisRateLimiter.Outcome](../src/main/java/dev/jinhyeon/notificationlab/ratelimit/RedisRateLimiter.java) (28행) | enum | 발송 허가 ALLOWED, 정상 한도 부족 LIMITED, 조회 불가 UNAVAILABLE. |
| [RedisRateLimiter.FailureReason](../src/main/java/dev/jinhyeon/notificationlab/ratelimit/RedisRateLimiter.java) (31행) | enum | 조회 불가 원인 분류. SLOT_BUSY/TIMEOUT/CONNECTION만 일시 복구 후보이며 STATE_ERROR 등과 구분한다. |
| [RedisRateLimiter.Phase](../src/main/java/dev/jinhyeon/notificationlab/ratelimit/RedisRateLimiter.java) (35행) | enum | private 진단 단계 SLOT/ACQUIRE/EVAL/RELEASE. 예외를 어느 호출 구간에서 봤는지 분류한다. |
| [RedisRateLimiter.Decision](../src/main/java/dev/jinhyeon/notificationlab/ratelimit/RedisRateLimiter.java) (37행) | record | 허가 결과·참고 대기 시간·오류 분류. retryAfterMs는 예약된 발송 권리가 아니다. |
| [DeliveryTaskStore.Attempt](../src/main/java/dev/jinhyeon/notificationlab/worker/DeliveryTaskStore.java) (39행) | record | 한 번의 시도 번호·결과·오류·시작/완료 시각 조회 값. |
| [DeliveryTaskStore.Delivery](../src/main/java/dev/jinhyeon/notificationlab/worker/DeliveryTaskStore.java) (40행) | record | 작업 상태·시도 수·상한·다음 시도 시각·Attempt 목록을 합친 조회 값. |
| [WorkerPauseErrorHandler.RetryKey](../src/main/java/dev/jinhyeon/notificationlab/worker/WorkerPauseErrorHandler.java) (39행) | record | private 일시 오류 복구 key: 컨테이너+토픽+파티션. |
| [WorkerPauseErrorHandler.RetryBudget](../src/main/java/dev/jinhyeon/notificationlab/worker/WorkerPauseErrorHandler.java) (40행) | record | private 동일 offset의 일시 오류 횟수 추적 값. |
| [WorkerPauseErrorHandler.Pause](../src/main/java/dev/jinhyeon/notificationlab/worker/WorkerPauseErrorHandler.java) (44행) | class | private static 중첩 클래스. 재개 예약과 취소/한 번만 재개하는 상태를 보관한다. |
| [WorkerQuota.QuotaUnavailableDeferred](../src/main/java/dev/jinhyeon/notificationlab/worker/WorkerQuota.java) (110행) | class | static 중첩 예외. 일시 복구 가능한 Redis 조회 불가 분류를 handler로 전달한다. |
| [WorkerQuota.QuotaDeferred](../src/main/java/dev/jinhyeon/notificationlab/worker/WorkerQuota.java) (119행) | class | static 중첩 예외. 정상 한도 부족의 유한 대기 시간만 전달한다. |
| [WorkerStore.Disposition](../src/main/java/dev/jinhyeon/notificationlab/worker/WorkerStore.java) (31행) | enum | SEND/EXHAUSTED/NEEDS_PERMIT. 실제 전송/예산 소진/허가 전 후보 상태를 구분한다. |
| [WorkerStore.Claim](../src/main/java/dev/jinhyeon/notificationlab/worker/WorkerStore.java) (32행) | record | 알림과 Disposition 결과. send()/needsPermit()로 다음 단계를 결정한다. |
| [WorkerStore.Task](../src/main/java/dev/jinhyeon/notificationlab/worker/WorkerStore.java) (36행) | record | private 작업 조회 값: ID·시도 수·상한·재시도 세대. |
| [HttpMockProperties.Mode](../src/mockServer/java/dev/jinhyeon/notificationmock/HttpMockProperties.java) (17행) | enum | HTTP Mock의 SUCCESS/TOO_MANY_REQUESTS/SERVER_ERROR/INVALID_RECIPIENT 모드. |
| [MockPushEndpoint.Payload](../src/mockServer/java/dev/jinhyeon/notificationmock/MockPushEndpoint.java) (116행) | record | Mock HTTP 서버가 검증하는 JSON 입력 DTO. |
| [MockPushEndpoint.Stats](../src/mockServer/java/dev/jinhyeon/notificationmock/MockPushEndpoint.java) (118행) | record | Mock 유효 요청·수락·오류·현재/최대 활성 수의 집계. 단말 도착 수가 아니다. |

### Result와 Permit의 차이

`IdempotentSubmissionService.Result`는 반환 값을 묶는 record다.

```java
public record Result(Notification notification, boolean replayed) { }
```

서비스의 상태를 변경하는 객체가 아니라 결과를 Controller에 전달한다.
반면 `ProviderCircuitBreaker.Permit`은 `acquire()`가 만들어 주는 허가 객체이며 바깥 회로를 참조한다.

```java
public void complete(Notification notification) {
    synchronized (ProviderCircuitBreaker.this) {
```

`complete()`는 해당 HTTP 결과를 회로에 반영한다. `close()`는 결과 없이 끝난 probe도 정리한다.
Worker의 try-with-resources에서 허가를 획득한 뒤 quota 대기/DB 경쟁으로 HTTP를 호출하지 못한 경우도 정리하는 이유다.
`Result`와 `Claim`이라는 같은 짧은 이름도 소속 클래스가 다르면 다른 계약이다.
예를 들어 OutboxStore.Claim은 **Kafka 발행 선점**, WorkerStore.Claim은 **공급자 발송 선점**을 뜻한다.

## resources와 클래스의 연결

| 리소스 | 사용하는 코드·용도 |
| --- | --- |
| [application.yml](../src/main/resources/application.yml) | 기본 비동기 실행 값. 설정 record와 일부 컴포넌트의 Value 바인딩으로 읽음 |
| [application-memory.yml](../src/main/resources/application-memory.yml) | DB/Flyway/Kafka 자동 구성을 제외하는 동기 메모리 비교 |
| [application-sync.yml](../src/main/resources/application-sync.yml) | Kafka 자동 구성을 제외하는 동기 MySQL 비교 |
| [db/migration](../src/main/resources/db/migration) | Flyway가 notification/Outbox/작업/시도/재발행/캠페인/Batch 스키마와 제약을 버전별 적용 |
| [provider-token-bucket.lua](../src/main/resources/redis/provider-token-bucket.lua) | RedisRateLimiter의 공유 예산 확인·차감 |
| [provider-split-bucket.lua](../src/main/resources/redis/provider-split-bucket.lua) | 같은 Redis key 안의 역할별 고정 예약 몫. 남은 몫의 자동 차용은 하지 않음 |
| [initialize-provider-quota.lua](../src/main/resources/redis/initialize-provider-quota.lua) | 운영자가 명시적으로 새 빈 예산 생성. Worker가 자동 실행하지 않음 |
| [initialize-split-quota.lua](../src/main/resources/redis/initialize-split-quota.lua) | 운영자가 명시적으로 역할 분리용 새 빈 예산 생성. 기존 상태를 덮어쓰지 않음 |

`resources/redis`는 Redis 데이터 파일 폴더가 아니다. 실행할 Lua 코드이며 실제 예산 상태는 Redis 서버에 있다.
기존 예산 초기화/전환은 [Redis 복구 계약](S6_REDIS_RECOVERY.md)을 따른다.

## 장애 상황에서 클래스를 따라가는 방법

| 상황 | 먼저 확인할 클래스·계약 |
| --- | --- |
| API가202를 반환한 직후 앱 종료 | SubmissionService의 커밋 여부와 OutboxStore의 영속 후보. 응답 성공 자체가 발송 완료는 아님 |
| Kafka ACK 뒤 PUBLISHED 저장 전 Relay 종료 | OutboxRelay/OutboxStore의 lease 복구. 재발행을 허용하고 WorkerStore가 이미 끝난 작업을 다시 보내지 않는지 확인 |
| 공급자 수락 뒤 DB 저장 전 Worker 종료 | WorkerStore의 시도 이력 UNKNOWN/lease/시도 예산. 외부 재호출 가능성이 남으며 exactly-once라고 표현하지 않음 |
| 공급자 결과 DB 저장 뒤 offset 커밋 전 종료 | WorkerStore의 완료 상태 재검사와 NotificationWorker의 replay 경로 |
| Redis 한도 부족 | WorkerQuota의 QuotaDeferred와 WorkerPauseErrorHandler의 pause/resume. 발송 실패 시도를 먼저 늘리지 않음 |
| HTTP500이 연속 발생 | HttpPushProvider 오류 분류, RetryPolicy의 남은 예산, ProviderCircuitBreaker의 OPEN/probe |
| Batch chunk 저장 실패 | CampaignTargetWriter 롤백, CampaignTargetReader checkpoint, CampaignBatchSession 소유 세션 |
| DLQ 한 건을 다시 보내기 | DeadLetterService의 원본 보존·중복 확인·계보/시도 상한 |

## 테스트와 함께 읽기

아래는 역할을 검증하는 테스트의 위치다. 모든 메서드가 여기 나열된 한 가지 주제만 검사하는 것은 아니다.

| 코드 주제 | 실제 테스트 |
| --- | --- |
| API 입력/비교 프로필/멱등성 | [NotificationApiTest](../src/test/java/dev/jinhyeon/notificationlab/api/NotificationApiTest.java), [AsyncNotificationTest](../src/test/java/dev/jinhyeon/notificationlab/outbox/AsyncNotificationTest.java) |
| Worker 자리/동시 완료/offset | [WorkerAdmissionTest](../src/test/java/dev/jinhyeon/notificationlab/worker/WorkerAdmissionTest.java), [WorkerConcurrencyTest](../src/test/java/dev/jinhyeon/notificationlab/worker/WorkerConcurrencyTest.java) |
| quota/pause/Redis 경계 | [WorkerQuotaTest](../src/test/java/dev/jinhyeon/notificationlab/worker/WorkerQuotaTest.java), [RedisRateLimiterTest](../src/test/java/dev/jinhyeon/notificationlab/ratelimit/RedisRateLimiterTest.java) |
| HTTP/회로 차단 | [HttpPushProviderTest](../src/test/java/dev/jinhyeon/notificationlab/provider/HttpPushProviderTest.java), [ProviderCircuitBreakerTest](../src/test/java/dev/jinhyeon/notificationlab/provider/ProviderCircuitBreakerTest.java) |
| Batch 복구/재실행 | [CampaignBatchTest](../src/test/java/dev/jinhyeon/notificationlab/campaign/batch/CampaignBatchTest.java) |
| 여러 JVM/공유 예산/작업 완료 후 제외 | [LocalTopologyIntegrationTest](../src/test/java/dev/jinhyeon/notificationlab/topology/LocalTopologyIntegrationTest.java) |
| 완료 offset의 Wakeup 재시도 | [CommitWakeupRetryTest](../src/test/java/dev/jinhyeon/notificationlab/config/CommitWakeupRetryTest.java) |

학습 순서는 다음처럼 나눌 수 있다.

1. **접수**: NotificationRequest → NotificationController → IdempotentSubmissionService → SubmissionService.
2. **DB 인계**: JdbcNotificationStore → DeliveryTaskStore → migration의 제약.
3. **Kafka 발행**: OutboxStore → OutboxRelay → TrafficRouting/AsyncConfiguration.
4. **발송**: NotificationWorker → WorkerStore → NotificationService → PushProvider/HttpPushProvider.
5. **보호**: WorkerAdmission → WorkerQuota → RedisRateLimiter/Lua → ProviderCircuitBreaker → WorkerPauseErrorHandler.
6. **복구**: RetryPolicy → WorkerStore.complete → DeadLetterService.
7. **대량 입력**: CampaignService/Store → CampaignBatchService/Session → Reader/Writer.
8. **관찰**: PipelineMetrics와 [Prometheus/Grafana](S8_MONITORING.md), 마지막으로 [S9 설계 판단](S9_DECISIONS.md).

각 메서드에서 입력/출력, DB 트랜잭션의 시작·끝, 외부 I/O, 실패 시 남는 상태를 표시하며 읽는다.
먼저 설정/예외 파일을 전부 외우는 대신 한 요청의 경로를 따라간 뒤 필요한 보조 타입으로 이동한다.

## 검토 범위와 현재 한계

- 파일별 표는 앱71개/HTTP Mock4개의 Java 파일을 모두 연결한다. 중첩 타입40개도 선언과 대조했다.
- 검색 보조로 기존 코드 전용 Graphify 인덱스를 사용했다. 정적 인덱스에는 참조 끝점이 연결되지 않은 관계238개,
  같은 끝점을 가진 관계의 축약165개와 자기 참조4개가 있어 완전한 런타임 호출 그래프로 취급하지 않는다.
  Spring DI/프로필/트랜잭션 경계와 본문의 설명은 실제 선언·메서드 본문을 직접 확인했다. 외부 모델 호출은 없었다.
- 그림의 패키지/프로세스 분리, 실제 여러 호스트 배포, Kafka broker 고가용성은 서로 다른 범위다.
- 캠페인 자동 정각 실행, 실제 회원 대상 SQL, 실제 FCM/APNs와 단말 도착은 아직 구현·검증 범위 밖이다.
- HTTP/Redis 연결 대상 검증은 현재 loopback 전용이다. 클래스 재사용만으로 실제 외부 공급자·여러 호스트에 연결되지 않는다.
- 이 안내 작업에서는 런타임 코드를 바꾸거나 새 발송/부하 실험을 실행하지 않는다.

문서 자체의 파일/중첩 선언/메서드/코드 발췌/링크 검사는
[검증 기록](experiments/class-guide-2026-11-07-validation.json)에 남긴다.
기본 test 명령은8초/종료0이지만 UP-TO-DATE여서 새 JUnit 실행0개다.
기존398개 통과는 앞선 런타임 코드의 원격 CI 결과이며 이번 문서 작업의 새 실행으로 합산하지 않는다.
