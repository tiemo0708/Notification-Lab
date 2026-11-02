# Notification Lab: 순차 구현 계획

작성일: 2026-10-13 (Asia/Seoul)

## 목표

대량 알림 처리에서 처리량, 정합성, 외부 API 보호, 장애 복구를 작은 실험으로 검증한다.
수치와 장애 시나리오를 근거로 아키텍처 변화를 설명할 수 있게 만드는 것이 목적이다.
실제 1천만 사용자나 운영 실적을 주장하지 않는다.

## 진행 방식

1. 다음 미완료 작업 하나를 고른다.
2. 완료 조건과 재현할 문제를 먼저 정한다.
3. 구현하고 자동 테스트/실험을 실행한다.
4. 통과한 항목만 완료 표시한다.
5. 한계와 다음 작업을 `docs/PROGRESS.md`에 기록한다.

기간은 하루 1~2시간 기준 약 4주를 가정하며, 날짜보다 완료 조건을 우선한다.
현재 대화가 끝난 뒤 자동으로 개발이 계속되지는 않는다. 다음 작업 요청 시 이 파일을 기준으로 이어간다.

## 설계 결정

- Java 21, Spring Boot 3.5.16, Gradle Wrapper 8.14.5(Groovy DSL)를 사용한다.
- 로컬 우선, 단일 저장소. 별도 클라우드 계정/유료 자원/실제 수신자 사용 없음.
- 첫 코드(S1)는 동기식 Mock API 기준선이다. 성공/실패 결과 리소스를 만들고 HTTP 201 반환.
- S1의 메모리 저장은 비영속적이다. 202/유실 방지/재시도/멱등성을 제공한다고 주장하지 않는다.
- S2는 MySQL 영속화와 Outbox 스키마 준비까지 수행하며 동기식 처리/HTTP 201을 유지한다.
- S3에서 요청과 Outbox의 원자적 저장, Kafka 발행/소비를 구현하고 DB 커밋 후 HTTP 202를 반환한다.
- 목표 전달 보장은 at-least-once 기반 + 중복 작업 방지. 외부 푸시 도착 exactly-once는 보장 범위 밖이다.
- API 인증/인증정보 저장은 공개 배포 전에 별도 단계로 추가한다. 기본 서버는 loopback에만 바인딩.

## 1주차: 기준선과 정상 비동기 흐름

### S0. 계획과 작업 규칙
- [x] 목표/제외 범위와 단계별 완료 조건 작성
- [x] AGENTS.md 공통 작업 규칙 작성 (별도 실행 에이전트 아님)

### S1. 동기식 Mock API 기준선
- [x] Spring Boot 프로젝트 생성
- [x] PushProvider 인터페이스와 deterministic Mock 구현
- [x] POST /notifications, GET /notifications/{id}
- [x] 성공/일시 실패/영구 실패 분류 및 입력 검증
- [x] 용량 제한이 있는 메모리 저장, loopback 바인딩
- [x] Actuator health, Prometheus, 처리 시도/결과/시간 지표
- [x] 단위/HTTP 계층 테스트 코드 작성
- [x] Java 21 빌드와 자동 테스트 18개 통과 (실패/오류/스킵 0)
- [x] 실제 HTTP 서버 기동 후 성공/조회/입력 오류/health/Prometheus smoke test

완료 조건: 정상/실패 결과를 조회할 수 있고 잘못된 요청이 거절되며, 한계가 문서화되어 있다.
S1은 트래픽 벤치마크가 아니다. 1만 건 검증은 S3 이후 진행한다.

### S2. MySQL 영속화
- [x] Docker Compose에 MySQL 추가 (127.0.0.1:13316, named volume)
- [x] Flyway로 notification과 outbox_event 스키마 도입
- [x] Testcontainers로 실제 MySQL 트랜잭션 테스트 (14개 + 기존 18개 통과)
- [x] 동기식 기준선과 DB 버전의 API 차이/상태 전이 문서화

완료 조건: 재시작 후 조회 가능. 제약/인덱스/롤백을 실제 MySQL에서 검증.
실행 JAR와 Compose MySQL 컨테이너 재생성 후 기존 volume의 알림 조회도 확인.
S2와 Gradle 전환은 2026-10-14 main에 푸시했으며 해당 원격 CI 성공을 확인했다.

### S3. Outbox + Kafka 수직 흐름
- [x] 요청/Outbox를 하나의 트랜잭션으로 저장한 후 202 반환
- [x] Kafka 단일 브로커 KRaft 로컬 구성, 토픽/파티션 수 명시
- [x] Relay: 짧은 DB 선점 트랜잭션 -> DB 트랜잭션 밖 publish -> ACK 확인 -> 상태 갱신
- [x] 선점 lease와 PROCESSING 복구, publish 실패 backoff
- [x] Worker: 단건 순차 처리, 결과 영속화 후 offset 커밋
- [x] Outbox age/미발행 수, consumer lag 측정 추가
- [x] 테스트 데이터 1만 건 접수/처리/실패/대기 개수 대조

완료 조건: DB 커밋 후 메시지 작업이 끝까지 추적된다. 단일 브로커로 브로커 고가용성을 증명하지 않는다.

로컬 일반 테스트 43개와 별도 1만 건 정합성 실험 1개 통과. S3를 포함한 기존 완료 커밋은2026-10-31 main에 푸시했다. 원격 CI의 실패·취소와 보완은 PROGRESS에 기록한다.

## 2주차: 유실·중복·재시도

### S4. 멱등성·재시도·DLQ
- [x] API Idempotency-Key 정책 (같은 키/같은 payload 재사용, 다른 payload 충돌)
- [x] 발송 작업 식별자, 유니크 제약, 원자적 처리 상태 변경 (현재 요청당 가상 대상 1개)
- [x] 재시도 가능/불가능 오류 분류, 최대 횟수, backoff+jitter
- [x] Retry topic/DLQ, 재발행 성공 확인과 원본 offset 커밋 관계 테스트
- [x] 지연 토픽은 이름만으로 지연되지 않음: DB Outbox dueAt 스케줄링 구현 (컨테이너 pause 미사용)
- [x] DLQ 선택 재발행, 원인/시도 이력 기록
- [x] 공급자 성공 후 DB 기록 전 종료의 불확실성 정책 문서화

S4는 로컬 완료했다. API 멱등성, 발송 작업/시도 이력, 유한 지연 재시도, Retry 토픽/DLQ와 선택 재발행을 구현했다.
최초 선점/lease 복구/일시 실패 재시도는 같은 예산(기본 3회)을 사용한다. DB due 시각 이후 Retry 토픽으로 보낸다.
최종 실패와 DLQ Outbox를 함께 커밋한 뒤 원본 offset을 처리한다. 후속 publish ACK 이후에만 PUBLISHED를 저장한다.
선택 재발행은 원본을 보존한 새 작업으로 접수하고 DLQ당 한 번/원본 계보당 최대 3회로 제한한다.
불확실한 결과의 중복 가능성·lease 경계·확인/복구 절차는 [S4 정책](docs/S4_UNCERTAIN_OUTCOMES.md)에 정리했다.
로컬 일반 테스트 90개 재실행 통과. 실제 프로세스 중단/브로커 장애 실험까지 완료했다는 뜻은 아니다.
S5에서 Kafka 중단·복구와 ACK 후 Outbox 기록 전 Relay 프로세스 종료를 검증했다.
Worker DB 완료 후 offset 커밋 전 종료와 재소비도 검증했다.
Relay/Worker 재시작, 처리 중 합류와 작업 완료 후 이탈에 따른 리밸런스도 검증했다.
SIGTERM 정상 종료와 유예 초과 복구까지 검증했다.
S6에서는 별도 HTTP Mock, HTTP 어댑터의 timeout/연결 풀 상한, Worker 동시성/유한 FIFO 대기열과 Redis 공유 제한기를 추가했다. 최초 발송/재시도의 quota와 정상 한도 부족의 소비 pause/resume를 연결했다. 프로세스별 Circuit Breaker/half-open 복구까지 연결했다. Redis 장애/예산 유실에는 fail-closed와 명시적 빈 초기화를 적용했다. 역할별 자원 분리와 8:2 고정 예약 예산까지 추가했다. S7 첫 단계로 캠페인 생성/시작/조회와 중복 시작 방지를 추가했다. 고정 가상 대상자와 keyset 조회도 추가했다. Batch checkpoint와 Outbox 인계/실패 재시작을 추가했다. 강제 종료 후 MySQL 세션 소유권 확인과 안전한 Batch 복구도 추가했다. 10만→100만 건 DB 인계 부하와 전수 정합성을 검증했다. HTTP Mock 1천/1만 건 사전 실험도 통과했다. 10만 건 HTTP 발송도 통과했다. 다음은 100만 건 발송 실험이다.
실제 검증 결과는 docs/PROGRESS.md에 기록한다.

### S5. 장애·배포 실험
- [x] Kafka 중단 후 접수, Kafka 복구 후 발행
- [x] publish ACK 후 PUBLISHED 저장 전 종료 -> 중복 재발행 재현
- [x] Worker 처리 완료/offset 커밋 전 종료 -> 재처리 재현
- [x] Relay와 Worker 재시작 및 리밸런스 검증
- [x] Graceful Shutdown: 신규 수신 중지, 진행 중 작업 정리, 유예 시간 초과 시 복구

완료 조건: 정의한 장애에서 접수 건 각각의 상태가 설명된다. 무조건적인 '유실 0' 표현을 사용하지 않는다.

첫 항목은 테스트 전용 Kafka 컨테이너의 실제 stop/start로 검증했다.
기준 1건 + 중단 중 접수 5건이 앱 재시작 없이 모두 처리되었고 기존 작업/이벤트 ID를 유지했다.
발행 상한을 1회로 줄인 별도 실험에서는 FAILED Outbox가 보존되고, 복구 후 신규 요청만 정상 처리되었다.
lag 0과 미발행 0은 다르다. [실험 구성과 한계](docs/S5_KAFKA_RECOVERY.md)를 참고한다.
두 번째 항목은 테스트 전용 Relay JVM을 ACK 후 DB 갱신 전에 강제 종료하고 새 JVM으로 재시작했다.
실제 30초 lease 만료 후 동일 이벤트가 재발행되어 Kafka 레코드는 2개, 완료된 작업의 공급자 호출은 1회였다.
API/Worker는 살아 있는 조건이며 [중단 지점과 한계](docs/S5_OUTBOX_ACK_CRASH.md)를 참고한다.
세 번째 항목은 DB 완료 후 offset 커밋 전 Worker JVM을 강제 종료하고 같은 group으로 재시작했다.
Kafka 레코드 1개를 재소비하되 공급자 호출은 총 1회, 작업/이력은 그대로 유지되었다.
[DB/offset 경계 실험과 한계](docs/S5_WORKER_OFFSET_CRASH.md)를 참고한다.
네 번째 항목은 독립 Relay/Worker JVM에서 처리 중 합류, Relay 중단 중 접수와 재시작, Worker 이탈/복귀를 검증했다.
Worker 1 → 2 → 1 → 2명 구성에서 파티션 배정, 각 파티션 offset, 고유 접수/공급자 호출 수를 대조했다.
[재시작·리밸런스 실험과 한계](docs/S5_RESTART_REBALANCE.md)를 참고한다.
마지막 항목은 HTTP 접수 중지 → Worker 종료 대기 순서, Relay의 후속 선점 중지를 보완하고
Worker/Relay 각각의 SIGTERM 유예 내 완료와 초과 후 lease 복구를 검증했다.
Worker 수락 후 DB 미기록 종료에서는 UNKNOWN과 실제 공급자 재호출도 재현했다.
[종료 정책·재현·한계](docs/S5_GRACEFUL_SHUTDOWN.md)를 참고한다. S5는 이 정의된 실험 범위에서 로컬 완료다.

## 3주차: 외부 API 트래픽 제어

### S6. HTTP Mock + 보호 장치
- [x] Mock을 별도 HTTP 서버로 확장: 지연, timeout, 429, 5xx, 요청 한도
- [x] connect/response timeout 및 연결 풀 상한
- [x] Worker 동시성 제한 + 유한 대기열
- [x] Redis Lua 전역 Rate Limit (속도와 허용 버스트 정의)
- [x] 재시도/최초 발송에 동일 provider quota 적용
- [x] 소비 pause/resume, poll/heartbeat 유지 검증
- [x] Circuit Breaker 및 half-open 복구
- [x] Redis 장애 정책 결정 (대기/fail-closed 등)과 검증
- [x] 실시간/캠페인 자원 분리와 공급자 한도 내 우선순위 배분

첫 항목은 독립 실행 JAR/Gradle task의 loopback HTTP Mock과 실제 HTTP 테스트로 검증했다.
고정 윈도우 요청 한도·동시 처리 슬롯·유한 응답 지연을 제공한다. [설정과 한계](docs/S6_HTTP_MOCK.md)를 참고한다.
기본 내부 Mock을 유지하며 `app.provider.mode=HTTP`로 실제 HTTP 연동을 선택한다.
[HTTP 어댑터](docs/S6_HTTP_CLIENT.md)는 연결 풀/대기/응답 timeout과 전체 deadline을 제한하고 DB 기반 재시도를 유지한다.
실제 MySQL/Kafka/HTTP로 성공·500·429·영구 실패·timeout의 이력과 offset을 대조했다.
[Worker 진입 제어](docs/S6_WORKER_CONCURRENCY.md)는 소비 스레드/처리 수/대기 수를 제한하며 DB 선점 전에 기다린다.
포화·timeout은 미커밋 상태로 소비를 멈춘다. 실제 파티션별 offset·DB 이력·종료/재시작을 검증했다.
[Redis Lua 제한기](docs/S6_REDIS_RATE_LIMIT.md)는 같은 공급자 키의 속도/버스트를 공유한다.
독립 클라이언트 4개의 동시 접근, 실제 Redis 시간 보충, 설정 충돌/timeout을 검증했다.
[Worker quota 연동](docs/S6_WORKER_QUOTA.md)은 DB 선점 전 유한 대기와 최초/재시도의 공통 허가를 적용한다.
완료 재소비/시도 상한 소진은 허가 없이 영속 처리하고, 대기 초과/Redis 오류는 미선점·미커밋 상태로 중지한다.
[소비 pause/resume](docs/S6_CONSUMER_PAUSE.md)는 정상 LIMITED에서 처리 자리를 반환하고 미완료 레코드를 보관한 채 poll을 유지한다.
5초 max.poll.interval보다 긴 7초 pause에서 poll/heartbeat와 offset 보존, 자동 재개·종료·리밸런스를 검증했다.
앞 문단의 유한 대기는 `pause-enabled=false` 비교 모드다. 기본 pause 모드도 Redis 오류에서는 중지한다.
[공급자 회로 차단](docs/S6_CIRCUIT_BREAKER.md)은 선점 전 OPEN 보류와 앱당 한 HALF_OPEN 탐침을 적용한다.
실제 HTTP 500→202, 병렬 소비, Redis 오류 중 탐침 반환과 종료/재시작을 검증했다.
[Redis 장애 정책](docs/S6_REDIS_RECOVERY.md)은 키 부재/TTL도 fail-closed로 처리하고 명시적인 v2 빈 예산 초기화를 요구한다.
실제 Redis 강제 종료/AOF 복구, 키 유실 후 앱 재시작과 미선점/offset 보존을 검증했다.
정상 형식의 snapshot rollback은 자동 감지하지 못하며 자동 failover를 지원하지 않는다.
[실시간/캠페인 자원 분리](docs/S6_TRAFFIC_ISOLATION.md)는 역할별 토픽/group/Relay/Worker/연결 풀과
단일 Redis 키의 고정 예약 예산(v3)을 사용한다. 기본 실시간 8/8·캠페인 2/2이며 유휴 몫을 빌리지 않는다.
최초/Retry/DLQ/redrive 역할 보존, 잘못된 역할의 미선점 중지, V7 기존 행 보존을 실제 인프라에서 검증했다.
전체 222개 실행의 220개 통과와 V7 기대값 2개 수정 후 MySQL 16개 재검증 통과를 확인했다.
기능 코드는 전체 실행 이후 동일하며 최종 build 성공이다. 단일 전체 실행에서 222개 모두 통과했다고 기록하지 않는다.
S6는 정의한 로컬 실험 범위에서 완료했다. 동적 예산 차용·운영 성능·공유 인프라 장애 격리를 입증한 것은 아니다.
S7 첫 API 단계를 구현했다. 고정 가상 대상자와 keyset 조회도 추가했다. Batch checkpoint와 Outbox 인계/실패 재시작을 추가했다. 강제 종료 후 MySQL 세션 소유권 확인과 안전한 Batch 복구도 추가했다. 10만→100만 건 DB 인계 부하와 전수 정합성을 검증했다. HTTP Mock 1천/1만 건 사전 실험도 통과했다. 10만 건 HTTP 발송도 통과했다. 다음은 100만 건 발송 실험이다.

완료 조건: 워커를 늘려도 제한 정책을 유지하고 공급자 장애가 메모리 폭증으로 이어지지 않는다.
실험용 공급자 한도는 실제 FCM/APNs의 보장/쿼터로 제시하지 않는다.

## 4주차: 대량 처리와 비교 실험

### S7. 캠페인과 Spring Batch
- [x] 캠페인 생성/시작/조회 API와 중복 시작 방지
- [x] 고정된 테스트 대상자 집합과 keyset pagination
- [x] chunk checkpoint, Kafka publish ACK와 재시작 시 중복 처리 검증
- [x] 대상자별 결정적 작업 ID, 실패 후 동일 범위 재처리
- [x] Batch 강제 종료 후 소유권 확인과 안전한 재시작
- [x] 10만 -> 100만 건 Batch DB 인계 부하 테스트
- [x] HTTP Mock 발송 사전 실험: 1천/1만 건 전수 대조와 실시간 지연 측정
- [x] 10만 건 HTTP Mock 발송 경로 부하와 실시간 간섭 측정
- [x] 100만 건 HTTP 발송 도구·시간/메모리 상한 준비와 경계값·작은 실제 경로 검증
- [x] timeout 후1만 건 HTTP와1만/10만 행 Outbox 쿼리 진단: 미재현과 인덱스 선택 관측 기록
- [x] 동일 적체의 통계 갱신 전후 계획·실행 노드 행 수·지연 비교(오래된 통계 고정 조건)
- [x] 기본 자동 통계 조건에서 선점 계획·교차 역할 잠금 및 갭 대기 관측
- [x] 역할별 선점 인덱스와 READ COMMITTED 수정의 회귀·HTTP 검증
- [x] 수정 경로의10만 건 HTTP 재검증과 접수 지연 관측(40f00e7, 107269건 전수 대조)
- [x] 1만 건 단계별 비용 계측과 ACK 후 완료 저장1/10건 비교(각2회, 대형 부하 미실행)
- [x] 37d4d3c의 단건/구 OR10건10만 건 HTTP 비교와 지연 관측(정합성 통과, tail 지연 악화)
- [x] 구 OR 완료 저장의 전체 스캔 잠금 재현과 한 트랜잭션의 기본키 단건 갱신 수정/기본 회귀 추가
- [x] 기본키 갱신 수정판557b2cf의 최대10건10만 건 HTTP/실시간 지연 추가 재검증(처리량 증가, p95 악화·기본값1 유지)
- [x] 로컬 Relay1/2개 사전 비교: Worker/DB 풀/200/s 한도를 고정한1만 건 반복과 정합성·실시간 간섭 확인(일관된 개선 미확인·기본값 유지)
- [x] 작은 동일 조건에서 DB 연결 취득/SQL/commit과Kafka offset 저장의 비용 세분화 진단(건별 offset 평균5.770~6.008ms 관측·기본값 유지)
- [x] 완료된 offset의 개수·시간 기준 묶음 저장 선택 옵션 비교와 재소비/미완료 offset 보존 검증(호출78~80% 감소·기본 RECORD 유지)
- [x] COUNT_TIME의 HTTP 재시도·Retry/DLQ와 commit RPC 전/성공 후 응답 유실 경계 추가 검증(합성 오류/실제 브로커 offset 대조)
- [x] 같은 offset 정책의10만 건 HTTP/실시간 tail 검증과 적용 여부 판단(처리량19.54% 증가·실시간 p95 악화 관측으로 기본 RECORD 유지)
- [x] 실시간 접수 코드/트랜잭션 경계와 요청별 연결·SQL·commit 분리 계측/1천·1만 건 검증(10만 건 p95 수준 미재현·원인 미확정)
- [x] 접수 지연의 정상/실제 DB 잠금/커밋 후 호출자 대기 제어군 구분과 CPU·wall·DB 가시성 검증(SQL 전용6건)
- [ ] 자연 발생 혼합 요청의 세션·시간과 DB 대기 관찰 대응 및10만 건 실시간 tail의 작은 재현
- [ ] 100만 건 실행 중 접수 probe의5초 응답 timeout 원인 진단과 작은 재현
- [ ] 100만 건 HTTP Mock 발송 경로 부하 테스트

[S7 첫 API 단계](docs/S7_CAMPAIGN_API.md)는 CAMPAIGN 앱의 정의 저장과 `DRAFT → START_REQUESTED` 수락까지다.
생성 키의 유니크 제약/정확한 정의 비교와 시작 조건부 UPDATE로 두 앱의 동시 요청을 처리한다.
START_REQUESTED는 영속 시작 의도이며 현재 자동 실행되지 않는다. 명시적 campaignBatch 명령으로 한 캠페인을 확장한다.
[고정 가상 집합](docs/S7_CAMPAIGN_AUDIENCE.md)은 SYNTHETIC_V1/targetCount로 대상 번호를 재현하고,
마지막 번호 뒤를 최대 1,000명씩 읽는다. 10만/100만 열거의 순서·총수·고정 해시를 대조했다.
실제 회원 테이블의 SQL keyset 성능이나 100만 건 발송 부하를 검증한 것은 아니다.
[Batch 인계/재시작](docs/S7_CAMPAIGN_BATCH.md)은 동일 MySQL 트랜잭션에 알림/작업/Outbox/checkpoint를 커밋한다.
Kafka ACK는 별도 Relay 경계이며 Batch COMPLETED는 DB 인계 완료다. 대상별 고정 ID와 DB 유니크 제약을 함께 적용했다.
FAILED 후 새 앱 재시작, checkpoint 실패 롤백, 두 앱 중복 실행 거절, ACK 불확실성 뒤 중복 소비를 검증했다.
[강제 종료 복구](docs/S7_CAMPAIGN_RECOVERY.md)는 단일 MySQL 세션으로 이전 실행의 업무/메타데이터 쓰기를 차단하고 같은 인스턴스에서 재개한다.
소유권 표시가 없는 구버전 STARTED/UNKNOWN은 복구하지 않는다.
[DB 인계 부하](docs/S7_CAMPAIGN_LOAD.md)는 10만→100만 건 저장과 전수 매핑/완료 재요청을 검증했다.
최종 100만 인계는 2545.391초였으며 공급자 호출은 0건이다. HTTP Mock 1천/1만 건 사전 실험도 통과했다. 10만 건 HTTP 발송도 통과했다. 다음은 100만 건 발송 실험이다.
[HTTP 발송 사전 실험](docs/S7_DELIVERY_PILOT.md)은 1만 캠페인과 564개 실시간 요청의 HTTP·DB·offset을 대조했다.
[10만 건 발송 실험](docs/S7_DELIVERY_LOAD.md)은 캠페인100000+실시간5342개의 HTTP·DB·전체 참조·offset을 대조했다.
발송1346.993초/74.239건·초, 실시간p95 단독553.657ms/혼합918.136ms였다. 공유 자원 간섭과 소수 표본 한계를 남긴다.
페이지1000행/고정 배열/발송60분·task90분 상한을 두었다. 100만 건 발송은 계속 미완료다.
[100만 건 준비](docs/S7_DELIVERY_MILLION.md)는 별도 opt-in task와 표본·샘플·메모리·시간 상한을 정리한 것이다.
2026-10-31에는 사용자 선택에 따라 준비만 했고, 2026-11-02 후속 요청으로 장시간 실험을 시작했다.
첫 실행은 약38.4만 건 진행 후 접수 probe의5초 응답 timeout으로 실패했다.
실패 원본과 진단 도구를 보강했으며 원인 진단·작은 재현 후 재실행을 결정한다. 실제100만 건 발송 항목은 미완료다.
[작은 진단](docs/S7_TIMEOUT_DIAGNOSIS.md)에서는 같은 timeout이 재현되지 않았다. 역할별 인덱스 대신 상태 인덱스를 선택한
계획과 통계 추정의 한계를 기록했다. 후속 고정 조건 비교에서 통계 갱신 후 실시간 선점의 스캔 노드가100001→1행으로 줄었다.
이는 오래된 통계를 만든 진단이며 실제1m 장애 원인 확정은 아니다.
[후속 잠금 검증](docs/S7_OUTBOX_CLAIM_LOCKS.md)에서 기본 조건의 교차 역할 레코드 잠금과 갭 대기를 재현했다.
선점 인덱스와 claim의 READ COMMITTED 경계를 수정했고 기본269개/별도 잠금·HTTP 검증을 통과했다. 새 접수는 같은 대기가 관측되지 않아 원래 API timeout 원인은 여전히 미확정이다.
수정 후10만 건 HTTP도107269건 전수 대조를 통과했다. 발송1840.512초/54.333/s로 이전보다 처리량은 낮았고,
혼합 APIp95 203.917ms/최대1904.024ms였다. 각1회/공유 호스트/계측 차이로 수정의 성능 효과를 확정하지 않는다.
[구간 계측/완료 저장 묶음](docs/S7_RELAY_PUBLISH_BATCH.md)은1만 건을 각 조건2회 비교했다.
단건109.349~131.447초/최대10건93.949~100.122초였고 HTTP42,160건을 대조했다.
[후속10만 건 비교](docs/S7_RELAY_100K_COMPARISON.md)에서는 같은37d4d3c의 단건1512.978초/66.095/s와
구 OR10건1132.741초/88.281/s를 관측했지만, 혼합 APIp95는297.106→334.034ms로 증가했다.
별도 SQL 진단에서 구 OR의 전체 PRIMARY 스캔/역할 간 잠금 결함을 재현해 기본키 단건 UPDATE최대10번을 한 번 커밋하도록 수정했다.
수정판557b2cf도10만 건을1170.631초/85.424/s에 처리하고 HTTP104639건을 대조했다.
단건 기준선 재사용 대비 처리량은29.24% 증가했지만 혼합 APIp95는364.875ms로 악화했다.
기본값1을 유지하며 캠페인10건은 선택 옵션이다. 실시간1을 유지하고100만 건은 별도 단계로 남긴다.
[로컬 Relay 병렬 사전 비교](docs/S7_RELAY_SCALING_PILOT.md)는 같은 Worker3/Hikari4/200/s에서1만 건을1→2→2→1로 처리했다.
HTTP41711건 전수 대조는 통과했지만1개120.646~131.381/s, 2개98.426~137.346/s로 일관된 개선을 확인하지 못했다.
동일 JVM의 테스트 전용 루프이며 앱 기본값/다중 서버 배포를 변경하지 않는다.
[JDBC/offset 세부 진단](docs/S7_DELIVERY_TIMING.md)에서는 같은1만 건을1→2→2→1로 처리하고 HTTP41717건을 전수 대조했다.
이 설정의 Worker JDBC 실행16회/DB 커밋3회와 기존 handler 밖의 건별 offset 저장5.770~6.008ms를 확인했다.
연결 획득 외 SQL·트랜잭션 상태·commit 비용도 남아 있다. 기본값을 유지하고 다음은 완료된 offset의 유한 묶음 저장 비교를 검토한다.
[offset 묶음 저장 비교](docs/S7_OFFSET_COMMIT_PILOT.md)는 기본 RECORD를 유지한 테스트 전용 COUNT_TIME10개/100ms다.
같은 Relay2/Worker3에서1만 건 각2회, HTTP41910건 전수 대조를 통과했다. 동기 커밋은10000→2163/2019회로 줄었고,
RECORD89.676~98.933/s, COUNT_TIME115.079~123.383/s를 관측했다. 마지막 offset 반영과 장애 경계도 확인했다.
[후속 오류 경계와10만 건 비교](docs/S7_OFFSET_100K.md)는 HTTP5종×두 모드와 실제 commit RPC 앞뒤의 합성 오류 복구를 검증했다.
같은740d2ef에서10만 건 각1회/HTTP209563건 전수 대조, 커밋100000→23891회/처리량77.180→92.263/s였다.
실시간 접수p95가97.694→202.728ms로 높게 관측돼 기본 RECORD를 유지한다. 다음은 제한된 표본의 실시간 지연 구간 분리 진단이다.
[실시간 접수 구간 진단](docs/S7_API_ADMISSION_TIMING.md)은 API의한 트랜잭션/SQL4회/commit1회와 요청별 JDBC·서버·클라이언트 시간을 확인했다.
같은 소스1만 건 각1회/HTTP21034건을 대조했으나10만 건의p95 수준은 미재현이다. 긴 SQL/commit의 내부 원인은 후속 검증으로 남긴다.
100만 건 재실행은 아직 하지 않았으며 이후 같은 자원/예산/5초 제한의 별도 장시간 실행으로 판단한다.
실제 검증 명령과 결과는 [PROGRESS](docs/PROGRESS.md)의 S7 기록을 따른다.

### S8. 성능 실험
- [ ] 컨슈머 수를 변경하고 처리량/lag/DB/CPU 측정
- [ ] 파티션 수 변경 시 key mapping/순서/offset 초기화 영향 검증
- [ ] producer batching과 압축 비교
- [ ] 병목이 외부 I/O일 때만 스레드 풀 vs 가상 스레드 비교
- [ ] 병렬 완료 순서가 달라도 미완료 offset을 건너뛰지 않는지 검증
- [ ] Testcontainers 회귀 테스트 + Grafana 대시보드
- [ ] 동일 환경/데이터에서 반복 측정, 원본 결과와 실행 명령 보존

### S9. 결과 정리와 선택적 배포
- [ ] 3개 주제: 외부 API 보호 / 유실·중복 대응 / 병렬 처리·배포
- [ ] 문제 -> 변경 이유 -> 전후 지표 -> 남은 한계로 정리
- [ ] AWS 배포는 별도 승인/예산 설정 후. MSK·NAT·RDS를 자동 생성하지 않는다.
- [ ] 실제 FCM 연동은 본인 테스트 디바이스에 한정, 대량 부하는 HTTP Mock 사용

## 공통 지표 정의

- accepted requests: API가 명시한 접수 기준을 충족한 고유 요청 수 (S1과 S3의 기준 구분)
- provider attempts: 재시도 포함 외부 호출 시도 수
- provider accepted: 공급자 수락 성공 수, 단말 도착 수 아님
- completion throughput: 단위 시간 내 처리된 고유 작업 수, 재시도를 중복 집계하지 않음
- end-to-end latency: 접수 시각 -> 최종 상태 시각 (p50/p95/p99)
- pending age: 아직 끝나지 않은 가장 오래된 작업의 나이 (outbox와 Kafka를 분리)
- duplicates: 이벤트 재전달 수, 중복 작업 차단 수, 관측 가능한 중복 provider 호출 수를 구분
- 자원: CPU/RSS/GC/스레드/DB connection/in-flight 요청/네트워크

## 이번 버전에서 제외

회원가입 UI, 관리자 UI, 이메일/SMS, MSA 다중 저장소, K8s, Service Mesh, Cassandra, Temporal, 자동 클라우드 확장.
Claude Code 또는 멀티 에이전트는 전제 조건이 아니다. 우선 한 도구에서 구현/테스트/기록을 완결한다.
