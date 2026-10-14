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

로컬 일반 테스트 43개와 별도 1만 건 정합성 실험 1개 통과. S3는 사용자 검토 후 푸시한다.

## 2주차: 유실·중복·재시도

### S4. 멱등성·재시도·DLQ
- [x] API Idempotency-Key 정책 (같은 키/같은 payload 재사용, 다른 payload 충돌)
- [x] 발송 작업 식별자, 유니크 제약, 원자적 처리 상태 변경 (현재 요청당 가상 대상 1개)
- [x] 재시도 가능/불가능 오류 분류, 최대 횟수, backoff+jitter
- [ ] Retry topic/DLQ, 재발행 성공 확인과 원본 offset 커밋 관계 테스트
- [ ] 지연 토픽은 이름만으로 지연되지 않음: dueAt 스케줄링/컨테이너 제어 구현
- [ ] DLQ 선택 재발행, 원인/시도 이력 기록
- [ ] 공급자 성공 후 DB 기록 전 종료의 불확실성 정책 문서화

S4는 진행 중이다. API 멱등성, 발송 작업/시도 이력, 일시 실패의 유한 지연 재시도를 구현했다.
최초 선점/lease 복구/일시 실패 재시도는 같은 예산(기본 3회)을 사용한다. DB의 due 시각과 기존 Outbox/같은 토픽으로 예약한다.
로컬 일반 테스트 75개 통과. 다음은 별도 Retry topic/DLQ와 재발행/원본 offset 관계 검증이다.
지연 토픽 및 컨테이너 제어 항목은 후속 토픽 구조에서 검증할 때 완료 표시한다.
실제 검증 결과는 docs/PROGRESS.md에 기록한다.

### S5. 장애·배포 실험
- [ ] Kafka 중단 후 접수, Kafka 복구 후 발행
- [ ] publish ACK 후 PUBLISHED 저장 전 종료 -> 중복 재발행 재현
- [ ] Worker 처리 완료/offset 커밋 전 종료 -> 재처리 재현
- [ ] Relay와 Worker 재시작 및 리밸런스 검증
- [ ] Graceful Shutdown: 신규 수신 중지, 진행 중 작업 정리, 유예 시간 초과 시 복구

완료 조건: 정의한 장애에서 접수 건 각각의 상태가 설명된다. 무조건적인 '유실 0' 표현을 사용하지 않는다.

## 3주차: 외부 API 트래픽 제어

### S6. HTTP Mock + 보호 장치
- [ ] Mock을 별도 HTTP 서버로 확장: 지연, timeout, 429, 5xx, 요청 한도
- [ ] connect/response timeout 및 연결 풀 상한
- [ ] Worker 동시성 제한 + 유한 대기열
- [ ] Redis Lua 전역 Rate Limit (속도와 허용 버스트 정의)
- [ ] 재시도/최초 발송에 동일 provider quota 적용
- [ ] 소비 pause/resume, poll/heartbeat 유지 검증
- [ ] Circuit Breaker 및 half-open 복구
- [ ] Redis 장애 정책 결정 (대기/fail-closed 등)과 검증
- [ ] 실시간/캠페인 자원 분리와 공급자 한도 내 우선순위 배분

완료 조건: 워커를 늘려도 제한 정책을 유지하고 공급자 장애가 메모리 폭증으로 이어지지 않는다.
실험용 공급자 한도는 실제 FCM/APNs의 보장/쿼터로 제시하지 않는다.

## 4주차: 대량 처리와 비교 실험

### S7. 캠페인과 Spring Batch
- [ ] 캠페인 생성/시작/조회 API와 중복 시작 방지
- [ ] 고정된 테스트 대상자 집합과 keyset pagination
- [ ] chunk checkpoint, Kafka publish ACK와 재시작 시 중복 처리 검증
- [ ] 대상자별 결정적 작업 ID, 실패 후 동일 범위 재처리
- [ ] 10만 -> 100만 건 단계적 부하 테스트

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
