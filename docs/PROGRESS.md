# 진행 기록

아래 기록은 실행 당시의 도구와 명령을 보존한다. 현재 빌드/실행 명령은 README의 Gradle Wrapper 기준을 따른다.

## 2026-10-13: S0 계획 + S1 동기식 Mock 기준선

### 작성됨

- Java 21 / Spring Boot 3.5.16 / Maven 프로젝트
- PushProvider 분리, 지연/성공/일시 실패/영구 실패 설정
- 단건 요청과 조회 API, 입력 검증, 루프백 바인딩
- 최대 건수가 제한된 메모리 저장소
- 처리 시도/결과/시간 메트릭, health와 Prometheus
- 단위 테스트와 Spring HTTP 계층 통합 테스트
- PLAN.md / README.md / AGENTS.md

### 검증

- Java: Temurin 21.0.12.1. Maven: 3.9.11. Spring Boot: 3.5.16.
- `mvn test`: **18개 통과 / 실패 0 / 오류 0 / 스킵 0**.
  - MockPushProviderTest: 4개
  - NotificationServiceTest: 5개
  - InMemoryNotificationStoreTest: 3개 (동시 100건 중 최대 10건만 저장되는 테스트 포함)
  - NotificationApiTest: 5개
  - NotificationFailureApiTest: 1개
- Maven Wrapper 생성 완료 (wrapper 3.3.4 / Maven 3.9.11).
- executable JAR 패키징 성공. 패키징 단계는 직전 테스트를 재실행하지 않기 위해 `-DskipTests`로 수행.
- JAR를 실제 HTTP 서버로 실행해 POST 201 / GET 200 / 잘못된 요청 400 / 없는 ID 404 검증.
- `/actuator/health`, `/actuator/prometheus`와 실제 발송 시도/성공 메트릭 확인.
- `/actuator/env`는 404로 비노출 확인.
- smoke 서버 정상 종료, graceful shutdown 완료 로그 확인. 진행 중 대량 작업의 종료/복구 실험은 아직 아님.
- Docker/MySQL/Kafka 통합 테스트와 부하 테스트는 **아직 수행하지 않음**.

### 검증 중 조정

- 작업 환경의 표준 프록시/시스템 인증서 저장소를 사용해 Maven 의존성을 내려받음. TLS 검증을 끄지 않음.
- 이 테스트는 실제 객체/수제 fake만 사용하므로 불필요한 Mockito 의존성을 제외. JVM self-attach에 의존하지 않도록 함.
- 테스트 컨텍스트에서 Prometheus 검증을 위해 AutoConfigureObservability를 명시.
- 입력 오류에서 수신자/본문을 노출하지 않도록 공통 예외 응답 추가.

S0/S1 완료. 이번 검증은 동기식 Mock 기준선에 한정하며 비동기 시스템의 신뢰성을 증명하지 않는다.

### 현재 보장하지 않는 것

영속성, 메시지 전달 보장, 실제 푸시, 멱등 요청, 재시도, Kafka 처리량.
동기 Mock 기준선을 곧바로 운영 비동기 시스템이라고 표현하지 않는다.

### 다음 작업

S2: MySQL Compose 구성 -> Flyway 스키마 -> 저장소 교체 -> 실제 MySQL에서 재시작/롤백 검증.
Docker 가용성을 먼저 확인하고, 없으면 Testcontainers 통과를 주장하지 않는다.

## 2026-10-13: 커밋 및 PR 규칙 반영

### 변경

- `CONTRIBUTING.md`: Conventional Commits + 한글 설명, 논리적 커밋 단위, 검증과 성능 측정 기록 규칙.
- `main` + 작업 브랜치, 작은 PR + Squash merge 방식 문서화.
- `.github/pull_request_template.md`: 목적/변경/검증/한계/호환성 체크리스트.
- `AGENTS.md`에 규칙 문서 참조를 추가하고, README에 문서 링크 추가.
- 애플리케이션 코드와 의존성은 변경하지 않음. S2 구현은 아직 시작하지 않음.

### 검증 및 원격 반영 상태

- 기존 자동 테스트 재실행: **18개 통과 / 실패 0 / 오류 0 / 스킵 0** (`mvn test`, Java 21 / Maven 3.9.11).
- 이번 실행은 작업 환경용 프록시/인증서 설정을 적용한 Maven으로 수행했다. 일반 개발 환경의 실행 명령은 `./mvnw test`이다.
- 문서 5개의 로컬 링크/코드 블록/줄 끝 공백 검증 통과. 수정 대상 밖의 기존 파일 30개는 이전 배포 ZIP과 바이트 단위로 동일함을 확인했다.
- 사용자 지정 원격: `https://github.com/tiemo0708/Notification-Lab.git`.
- 이 변경 시점에는 작업 폴더가 Git 저장소로 초기화되지 않았고, GitHub 연결이 미설치 상태임.
- Git 초기화/커밋/원격 푸시/PR 생성/병합은 아직 수행하지 않음.
- GitHub Actions와 브랜치 보호는 문서화만으로 설정되지 않으며, 이번 작업에서 설정하지 않음.

## 2026-10-13: macOS 환경 재검증 및 초기 Git 등록

- 작업 폴더: `/Users/jinhyeon/notification-lab`. 필수 문서와 S1 소스/테스트 파일 확인.
- Git 작성자: `JinHyeon <tiemo0708@naver.com>` (기존 설정 사용).
- GitHub CLI의 `tiemo0708` 인증 및 지정 저장소의 push 권한 확인.
- 원격 조회: `git ls-remote`에 참조가 없고 GitHub commits API가 빈 저장소(HTTP 409)임을 확인. 보존할 기존 원격 커밋 없음.
- 작업 브랜치: `feat/s1-mock-api`. 초기 프로젝트와 작업 규칙을 하나의 논리적 커밋으로 등록.
- `./mvnw test`: **18개 통과 / 실패 0 / 오류 0 / 스킵 0**.
- 실행 환경: macOS 15.7.7 / arm64, Amazon Corretto 21.0.6, Maven Wrapper의 Maven 3.9.11, Spring Boot 3.5.16.
- Maven 캐시 쓰기와 네트워크 접근을 허용한 실행에서 검증. TLS 검증을 끄거나 프로젝트 의존성을 변경하지 않음.
- 비밀정보 패턴과 포함 파일 확인. `target/` 등 빌드 산출물은 커밋에서 제외.
- 이번 실행에서는 실제 서버 smoke test, Docker/MySQL/Kafka 통합 및 부하 테스트를 재실행하지 않음. 이전 환경의 검증 기록과 구분.
- 초기 커밋 `7bd347631a978144c180680180e57c5c597d227f` 생성 및 `git push -u origin feat/s1-mock-api` 성공. 이후 `git ls-remote`로 원격 브랜치와 로컬 커밋 해시 일치 확인.
- 빈 저장소의 첫 브랜치 푸시로 GitHub 기본 브랜치가 `feat/s1-mock-api`로 설정됨. `main`은 아직 없으며, CONTRIBUTING.md의 목표 구조로 전환하는 작업은 이번 요청에서 수행하지 않음.
- S2는 시작하지 않음. force push, PR 생성, 병합 및 GitHub 설정 변경은 수행하지 않음.

## 2026-10-13: 개인 개발 운영 규칙과 CI 준비

- 기존 `b206023` 이력에서 로컬 `main` 생성. 기존 S1 브랜치와 커밋은 보존.
- `CONTRIBUTING.md`: main 직접 개발, 작업 브랜치/PR 선택, 사용자 검토 후 푸시 방식으로 변경.
- Java/테스트/DB/API 최소 컨벤션과 `.editorconfig` 추가. S2의 HTTP 201 유지와 S3의 202 전환을 명확히 구분.
- `.github/workflows/test.yml`: push/PR에서 Java 21 + Maven Wrapper 테스트 실행. 공식 checkout/setup-java v5 커밋에 고정.
- `./mvnw --batch-mode --no-transfer-progress test`: 기존 **18개 통과 / 실패 0 / 오류 0 / 스킵 0**.
- 사용자 검토 전이므로 푸시/원격 기본 브랜치 변경은 대기. CI 원격 실행 및 브랜치 보호 설정은 아직 수행하지 않음.

## 2026-10-13: S2 MySQL 영속화 — 로컬 검증 완료, 푸시 검토 대기

### 구현

- Spring JDBC + MySQL 저장소를 기본으로 사용. `memory` 프로필로 S1 비교 실행 가능.
- PROCESSING 생성 커밋 -> 트랜잭션 없는 Mock 호출 -> 결과 커밋 -> HTTP 201.
- Flyway V1: notification/outbox_event, PK/FK/유니크/CHECK 제약과 상태/시간 복합 인덱스.
- Outbox는 스키마만 준비. API의 Outbox 저장/발행, Kafka, HTTP 202는 S3에서 구현.
- Compose: MySQL 8.4.8, loopback 바인딩, named volume, healthcheck, 메모리 상한.
- 기존 로컬 SSH 터널이 13306을 사용 중이어서 기본 MySQL 포트는 13316으로 선택.
- 연결 풀 상한 8, 연결 대기 3초. DB/트랜잭션 오류는 세부 데이터 없이 503으로 응답.
- README와 `docs/S2_PERSISTENCE.md`에 실행, 스키마, 실패 상태, 트랜잭션 경계와 한계 기록.

### 실제 검증

- 환경: macOS arm64 / Amazon Corretto 21.0.6 / Maven 3.9.11 / Docker Engine 27.5.1 / Compose 2.32.4.
- Spring Boot 3.5.16, BOM 관리 Flyway 11.7.2 / Testcontainers 1.21.4 / MySQL Connector/J 9.7.0.
- `./mvnw --batch-mode --no-transfer-progress test`: **32개 통과 / 실패 0 / 오류 0 / 스킵 0**.
  - 기존 단위/메모리 HTTP 테스트: 18개.
  - `MySqlNotificationTest`: 실제 MySQL 8.4.8 + HTTP 서버로 14개.
  - 앱 재시작 후 동일 결과, 한글/이모지/시간 보존, Flyway 중복 적용 방지.
  - DB 커밋 이후 트랜잭션 없이 공급자 호출, 중복 PK 동시 삽입 차단, 최종 결과 덮어쓰기 방지.
  - 같은 트랜잭션의 알림/Outbox 롤백, UPDATE 롤백, FK/유니크/CHECK/JSON 제약과 인덱스 구성.
  - 저장/연결 실패 503과 오류 상세 비노출, 결과 저장 실패 후 PROCESSING 유지 및 자동 재시도 부재.
- `./mvnw --batch-mode --no-transfer-progress -DskipTests package`: 실행 JAR 패키징 성공. 직전 테스트를 반복하지 않음.
- 별도 검증용 Compose 프로젝트에서 JAR 실제 기동: POST 201, GET 200, health UP, Prometheus 확인.
- 앱 종료 -> `docker compose down`(volume 보존) -> `up -d --wait` -> 앱 재기동 후 같은 알림 응답 일치 확인.
- `127.0.0.1:13316` 바인딩 확인. 이번에 생성한 검증용 컨테이너/volume/임시 비밀번호 파일 정리 완료.
- 기존 다른 프로젝트의 컨테이너와 SSH 터널은 변경하지 않음.
- POM XML, 문서 로컬 링크/코드 블록, diff 공백 검사 통과. 빌드 산출물과 비밀번호는 커밋에서 제외.

### 검증 중 조정과 한계

- MySQL CHECK 위반은 Spring의 예외 하위 타입에 의존하지 않고 실제 MySQL 오류 코드(3819)를 검증하도록 조정.
- 장애 주입용 trigger DDL은 임시 테스트 컨테이너의 root로 실행하며 앱 사용자의 권한이나 MySQL 보안 설정은 확대하지 않음.
- 초기 DB 저장과 결과 저장은 별도 커밋. 중간 종료 시 PROCESSING이 남고 자동 복구하지 않는다.
- 요청 멱등성, 재시도, Outbox/Kafka 처리, 부하/성능, DB 고가용성 검증은 미구현/미실행.
- 로컬 main에서 구현과 검증 완료. 사용자 검토 후 푸시 및 GitHub 기본 브랜치 main 지정 예정.
- GitHub Actions는 파일만 준비했으며 원격 실행 결과는 아직 없음. PR/병합/force push는 수행하지 않음.

다음 구현은 S3이며, 이번 작업에서는 시작하지 않는다.

## 2026-10-13: Maven에서 Gradle로 전환 — 로컬 완료, 푸시 검토 대기

### 변경

- 사용자가 익숙한 Gradle로 빌드 도구를 전환. Java 21 / Spring Boot 3.5.16 / 프로젝트 0.2.0-SNAPSHOT 유지.
- `build.gradle`/`settings.gradle`의 Groovy DSL과 Gradle Wrapper 8.14.5 도입.
- Spring Boot BOM을 `enforcedPlatform`으로 사용해 기존 관리 의존성 버전 유지. Mockito 제외도 유지.
- Gradle JUnit Platform 실행을 위한 launcher 의존성을 테스트 런타임에 명시.
- `pom.xml`, Maven Wrapper 스크립트와 설정 제거. 과거 커밋의 Maven 이력과 당시 검증 기록은 보존.
- CI는 Gradle 캐시와 `./gradlew --no-daemon --console=plain build`로 변경. 테스트와 실행 JAR를 함께 확인.
- README/AGENTS/CONTRIBUTING/PLAN/S2 문서의 현재 실행 명령 및 보고서/산출물 경로 갱신.
- `.gradle/`, `build/`를 Git에서 제외. 이전 `target/` 산출물의 제외도 유지.

### 실제 검증

- 공식 배포 ZIP SHA-256을 Wrapper에 설정하고 Wrapper JAR도 Gradle 공식 체크섬과 일치 확인.
- `./gradlew --no-daemon --console=plain clean build`: **32개 통과 / 실패 0 / 오류 0 / 스킵 0**, 실행 JAR 생성 성공.
- 실제 MySQL Testcontainers 14개와 기존 단위/메모리 HTTP 테스트 18개를 모두 재실행.
- `src/` 아래 애플리케이션·테스트·리소스 28개 파일은 전환 전과 SHA-256이 동일.
- Maven/Gradle 실행 JAR 내 런타임 라이브러리 54개의 파일명과 내용 SHA-256이 모두 동일.
- Gradle 실행 JAR의 memory 프로필을 임시 loopback 포트로 기동해 POST 201 / GET 200 / health UP / Prometheus 확인 후 종료.
- 실제 MySQL에 연결한 앱의 재시작 후 조회는 위 14개 테스트에서 재검증. DB 컨테이너 재생성/volume 보존 smoke는 이전 S2에서 검증했으며 이번 전환에서는 반복하지 않음.
- 문서 로컬 링크/코드 블록, workflow YAML, diff 공백 검사 통과.
- macOS에서 검증했으며 Windows `gradlew.bat` 실행과 원격 GitHub Actions는 미실행.
- 사용자 검토 전이므로 푸시 및 원격 main 기본 브랜치 지정은 계속 대기. S3 미시작.

선택 근거: [Spring Boot 3.5 빌드 도구 호환 범위](https://docs.spring.io/spring-boot/3.5/system-requirements.html),
[BOM 의존성 관리](https://docs.spring.io/spring-boot/3.5/gradle-plugin/managing-dependencies.html),
[Gradle Wrapper 검증 방법](https://docs.gradle.org/current/userguide/gradle_wrapper.html).

## 2026-10-14: main 푸시와 기본 브랜치 전환

- 사용자 승인으로 로컬 main의 개인 개발 규칙/CI, S2 MySQL, Gradle 전환 커밋을 함께 푸시.
- 원격 main과 HEAD 모두 `c95a444b19d4454f52a2731d76dc93d147cc5183` 일치 확인.
- GitHub 기본 브랜치를 `main`으로 변경. 기존 `feat/s1-mock-api`와 이력은 보존.
- [GitHub Actions 실행 38436621291](https://github.com/tiemo0708/Notification-Lab/actions/runs/38436621291): **success** 확인.
  해당 c95a444의 Gradle build/기존 32개 테스트에 대한 결과이며 이후 S3 코드의 원격 검증은 아니다.
- force push, PR/병합, 브랜치 보호 변경은 수행하지 않음.
- 커밋은 작은 논리적 변경마다 로컬에 쌓고, 검증된 단계의 커밋들을 사용자 검토 후 일괄 푸시하는 방식을 CONTRIBUTING에 명시.

## 2026-10-14: S3 Outbox + Kafka — 로컬 구현 및 정합성 검증

### 구현

- 기본 POST를 HTTP 202 + QUEUED로 변경. notification/Outbox를 같은 DB 트랜잭션으로 저장.
- S1 `memory`, S2 `sync` 비교 프로필은 HTTP 201 동기 동작 유지.
- V1 SQL은 보존하고 V2 migration으로 상태/CHECK/Relay와 Worker lease/token 추가.
- Kafka 4.0.2 공식 이미지, 단일 KRaft broker, 파티션 3/복제본 1. loopback 기본 19092, named volume.
- Relay: 짧은 DB 선점 -> 트랜잭션 없는 publish -> future ACK -> token 조건 PUBLISHED 업데이트.
- 선점 30초, 실패 backoff 1~60초, 최대 10회 후 FAILED 보존. producer 내부 retry도 3회로 제한.
- Worker: concurrency 1/max.poll.records 1, 원자적 DB 선점, 트랜잭션 밖 Mock 호출, 결과 커밋 후 RECORD offset 커밋.
- DB/이벤트 오류는 컨슈머 중지. 미완료 offset을 성공 처리하지 않음. 완료 결과 재소비 시 Mock 재호출하지 않음.
- Outbox 미발행/FAILED/최고 age, consumer committed lag, sample age/Worker running 지표 추가. 수집 오류는 NaN.
- README, S3 상세 문서, PLAN, 개발 규칙을 현재 동작과 배치 푸시 방식에 맞춰 갱신.

### 실제 자동 검증

`./gradlew --no-daemon --console=plain build reconciliationTest` 성공.

- 일반 테스트 **43개 통과 / 실패 0 / 오류 0 / 스킵 0**.
  - 기존 32개 회귀 테스트.
  - S2 결과가 들어 있는 V1 DB에 V2를 적용하고 결과/시간/ID 보존 확인 1개.
  - 실제 MySQL/Kafka를 사용하는 S3 검증 10개: 원자적 접수/롤백, 선점 경합/만료/token fencing,
    ACK 전 PUBLISHED 금지, 발행 실패 backoff/최대 횟수, 처리 후 offset/완료 replay,
    결과 저장 실패 시 offset 보존 및 복구, Worker stale token 거절, 공급자 실패 결과, 잘못된 이벤트 중지.
  - ACK 지연과 발행 예외 타이밍은 제어 가능한 KafkaTemplate fake로 검증. 나머지 Kafka 소비/offset 흐름은 실제 브로커 사용.
- 별도 1만 건 정합성 실험 **1개 통과 / 실패 0 / 오류 0 / 스킵 0**.
  - HTTP 접수 10,000 = DB 알림 10,000 = Mock 수락 10,000 + 실패 0 + 대기 0.
  - Outbox 10,000 / PUBLISHED 10,000 / 미발행 0, provider 시도 10,000, committed offset 합계 10,000, lag 0.
  - 실험 구간 **158.122초**, 정상 종료한 실행 1회. 성능 비교/운영 처리량으로 해석하지 않음.
  - HTTP 동시성 8(각 작업 1,250건), 실제 Mock SUCCESS/지연 0ms, Worker 1, 파티션 3, DB 풀 8.
  - Apple M4 10코어 / RAM 16GiB / Docker 할당 10 CPU·8,217,575,424 bytes / macOS 15.7.7 arm64.
  - Amazon Corretto 21.0.6, Gradle 8.14.5, Boot 3.5.16, Spring Kafka 3.3.16, kafka-clients 3.9.2, MySQL 8.4.8.
  - [명령·환경·결과와 원본 요약 출력](experiments/s3-reconciliation-2026-10-14.json) 보존.
- 실행 JAR `build/libs/notification-lab-0.3.0-SNAPSHOT.jar` 생성 성공.

### 검증 중 수정과 한계

- 첫 1만 건 실험 도중 Relay 조회의 전체 스캔/filesort를 확인해 해당 테스트 실행을 중단.
  중단된 실행은 통과나 변경 전 성능 기준으로 계산하지 않았다.
- 상태별 한 건 조회로 수정한 뒤 전체 빌드/실험 재실행. 실제 EXPLAIN에서 기존
  `idx_outbox_status_available`의 range/Using index condition 사용 확인. 성능 개선 비율은 주장하지 않음.
- 초기 Compose smoke에서 Kafka volume 권한 오류 확인. 이미지 안의 appuser 소유 경로
  `/var/lib/kafka/data`로 수정. Testcontainers 성공만으로 Compose 성공을 주장하지 않음.
- BOM의 Flyway 11.7.2는 MySQL 8.4 관련 지원 버전 경고를 출력한다. 실제 migration/업그레이드/제약 검증은 통과했으며 경고를 숨기거나 BOM을 임의로 바꾸지 않았다.
- FAILED Outbox는 수동 조사 필요. 공급자 성공과 DB 결과 저장 사이 종료 시 외부 호출 중복 가능.
- 브로커 실제 중단/프로세스 강제 종료/rebalance/다중 인스턴스 실험, 운영 HA, API 멱등성/Retry/DLQ는 미검증 또는 다음 단계.
- S3는 로컬 검증 결과이며 사용자 검토 전에는 푸시하지 않는다. 원격 CI 성공은 위 c95a444까지만 해당.

### 최종 Compose / 실행 JAR 확인

- volume 경로 수정 후 별도 `notification-lab-s3-check` Compose 프로젝트와 임시 비밀번호/포트로 재실행 성공.
- POST **202 QUEUED** -> GET **PROVIDER_ACCEPTED**, health UP 확인.
- Kafka 토픽의 실제 **파티션 3개 / replication factor 1** 확인.
- 앱 종료 -> MySQL/Kafka 컨테이너 제거(volume 보존) -> 재생성 -> 앱 재시작 후 기존 알림 응답 완전 일치.
- 재시작한 Worker의 running 지표 1 확인. 진행 중 강제 종료/불완전 처리 복구 전체를 검증한 것은 아님.
- 검증용 컨테이너·volume·네트워크·임시 비밀번호 파일 정리 완료. 다른 프로젝트 자원은 변경하지 않음.
- 최종 diff 공백, Markdown 로컬 링크/코드 블록, YAML/결과 JSON 검사 완료. 비밀정보/빌드 산출물 제외.

S3 로컬 완료. 다음은 S4의 API Idempotency-Key 정책/원자적 중복 방지부터 진행한다.
이번 요청에서는 S4를 시작하지 않는다.


## 2026-10-14: S4 첫 단계 — API 멱등성 로컬 완료

### 범위와 변경

- PLAN의 다음 한 항목인 API Idempotency-Key 정책 구현. S4 전체 완료가 아니며 공급자 재시도/Retry topic/DLQ는 시작하지 않음.
- 선택적 키: 최초 202, 같은 키/같은 recipientId·title·body는 기존 알림의 현재 상태와 Location을 200으로 재사용.
- 같은 키의 다른 내용은 409, 잘못된 값/복수 헤더는 400. 키 없는 요청은 계속 독립적인 202 접수.
- 키는 대소문자 구분 ASCII `[A-Za-z0-9._:-]` 1~128자. 현재 로컬 API 전체 범위, 알림 행 보존 동안 유지하며 TTL 없음.
- V1/V2 SQL은 보존. V3에서 nullable VARBINARY(128) 컬럼과 유니크 제약 추가.
- 키/알림/Outbox를 같은 트랜잭션에서 저장하고, 중복 INSERT 롤백이 끝난 후 기존 행 조회/필드 비교.
- 호출자 트랜잭션의 오래된 snapshot을 피하도록 외부 접수 서비스에서 NOT_SUPPORTED로 경계 분리.
- 다른 DB 중복 오류를 키 재사용 성공으로 바꾸지 않음. 별도 캐시/분산락/자동 무한 재시도 없음.
- sync/memory 비교 프로필은 키 헤더를 400으로 거절하며 헤더 없는 201 동작 유지.
- [S4 멱등성 정책](S4_IDEMPOTENCY.md), README, PLAN 갱신.

### 실제 검증

- `./gradlew --no-daemon --console=plain build`: **54개 통과 / 실패 0 / 오류 0 / 스킵 0**, 실행 JAR 생성 성공.
- 기존 43개 회귀 검증 + 신규 11개(실제 MySQL/Kafka/HTTP 멱등성 10개, memory 프로필 헤더 거절 1개).
- 같은 키/같은 요청은 JSON 속성 순서·직렬화 차이에 영향 없이 기존 ID 재사용.
- recipientId 대소문자, title 공백, body 변경을 각각 충돌 처리. 오류 응답에서 키/본문 상세 비노출.
- 8개 동시 동일 요청: 최초 접수 1건 + 재사용 7건, 알림/Outbox 각각 1행.
- 8개 동시 상충 요청(내용별 4개): 202 한 건, 200 세 건, 409 네 건, 알림/Outbox 각각 1행.
- Outbox 중복 오류 강제 주입: 503, 키/알림 전체 롤백. 같은 키로 정상 재시도 후 한 번만 접수.
- 처리 완료 -> 앱 재시작 -> 같은 키 요청: 기존 PROVIDER_ACCEPTED 결과 조회, 새 이벤트/Mock 호출 없음.
- 키 없는 요청, 키 대소문자·128자 경계·129자 거절, 빈 값/잘못된 문자/복수 헤더, 잘못된 payload 후 키 재사용 확인.
- 상위 트랜잭션이 과거 snapshot을 가진 경우에도 커밋된 기존 접수 결과를 올바르게 읽는지 확인.
- 기존 데이터가 있는 V1 DB의 후속 migration 적용 및 결과 보존도 회귀 테스트에서 통과.
- 이번에는 1만 건 실험 및 Compose 컨테이너 재생성 smoke를 재실행하지 않음. 이전 S3 결과와 구분.
- S3/S4의 새 원격 CI 실행은 없음. 마지막 원격 성공은 c95a444의 S2/Gradle 빌드이다.

### 남은 작업

API 접수 중복 방지를 외부 발송 exactly-once로 표현하지 않는다.
다음 구현은 S4 작업 식별자/원자적 상태 관리와 공급자 재시도 정책이다.
이번 변경은 로컬 커밋으로 보관하며 기존 S3 커밋과 함께 사용자 검토 후 푸시한다.

## 2026-10-14: S4 두 번째 단계 — 발송 작업/시도 이력 로컬 완료

### 범위와 변경

- 요청당 가상 대상 하나에 영속 taskId 부여. 실제 디바이스 등록/여러 대상 fan-out은 미구현.
- notification/작업/Outbox를 원자적으로 접수. 같은 알림의 작업 중복은 DB 유니크 제약으로 거절.
- Worker 선점과 시도 번호/STARTED 이력을 함께 저장하고, 공급자 호출은 트랜잭션 밖에서 수행.
- 완료 결과와 이력을 같은 트랜잭션으로 저장. 이전 토큰의 결과 덮어쓰기 차단 및 완료 replay 시 재호출 방지 유지.
- lease 만료 복구는 최초 선점 포함 기본 3회(설정 1~10, 접수 시 고정). 만료된 이전 시도는 UNKNOWN으로 기록.
- 마지막 lease 만료 후 재소비하면 추가 호출 없이 INTERNAL_FAILURE / ATTEMPTS_EXHAUSTED로 보관하고 offset 처리.
- GET /notifications/{id}/delivery로 taskId/현재 상태/시도 수/상한/이력 조회. 대상·본문·토큰 비노출.
- 새 이벤트 schemaVersion 2에 taskId 추가. Worker는 기존 version 1도 DB 작업 조회로 처리.
- V4에서 기존 비동기 알림에 작업 backfill. 활성 lease의 소유권 보존, 알 수 없는 과거 시작 시각은 NULL,
  완료된 과거 시도는 임의로 복원하지 않음. S2의 Outbox 없는 행에는 작업을 만들지 않음.
- [정책과 업그레이드 절차](S4_DELIVERY_TASKS.md), README, PLAN, 기존 단계 문서 연결 갱신.

### 실제 검증

- 최종 `./gradlew --no-daemon --console=plain build`: **62개 통과 / 실패 0 / 오류 0 / 스킵 0**, BUILD SUCCESSFUL (40초).
- 기존 54개 회귀 + 새 8개 테스트. 실제 MySQL/Kafka/Testcontainers 사용, 실행 JAR 생성 성공.
- 작업 ID의 멱등 키 재사용, DB 유니크/상한 제약, 완료 이벤트 replay 후 동일 이력/Mock 1회 확인.
- 동시 Worker 선점 두 건 중 하나만 성공하며 STARTED 이력/시도 수는 1.
- 세 번의 만료된 선점 이후 복구: UNKNOWN 이력 3개, 상한 소진 상태 저장, 추가 공급자 호출 0, offset 처리.
- 시도 INSERT 실패: 선점/카운터/이력 전체 롤백. 이력 UPDATE 실패: 알림 결과도 롤백, offset 0으로 중지.
- 오류 제거 및 lease 만료 후 재소비: UNKNOWN → 새 시도 PROVIDER_ACCEPTED, 공급자 호출 총 2회로 불확실한 중복 재현.
- schemaVersion 1 처리 성공. version 2의 불일치 taskId는 선점/공급자 호출/offset 커밋 없이 중단.
- 실제 V3 DB에 대기/진행/완료 비동기 행과 S2 행을 넣고 V4 적용: 알림/Outbox 원본 행 완전 보존,
  비동기 작업 3개 및 활성 시도 1개만 생성. 기존 V1 결과의 후속 migration 회귀 검증도 통과.
- Git 작성자 JinHyeon / tiemo0708@naver.com, GitHub tiemo0708 인증 확인.
  원격 기본 브랜치는 main이며 HEAD는 계속 c95a444. 기존 원격 이력은 변경하지 않음.

### 한계와 다음 단계

- 이번에는 1만 건 실험/Compose 재생성 smoke를 재실행하지 않음. 이전 S3 측정 결과를 새 버전의 성능으로 해석하지 않음.
- 새 원격 CI 실행 없음. 로컬 main 커밋으로 쌓고 사용자 검토 후 함께 푸시한다.
- 새 schemaVersion 2를 내보내므로 이전 앱/Worker를 모두 종료한 후 업그레이드해야 한다. 혼합 버전/자동 다운그레이드 미지원.
- UNKNOWN과 상한 소진은 외부 전달 실패 확정이 아니다. 공급자 exactly-once나 실제 호출 횟수 복원을 보장하지 않음.
- 일반 RETRYABLE_FAILURE는 아직 최종 결과. 다음은 공급자 자동 재시도 정책(backoff+jitter)과 Retry topic/DLQ 연결이다.

## 2026-10-14: S4 세 번째 단계 — 일시 실패 지연 재시도 로컬 완료

### 범위와 변경

- PLAN의 오류 분류/최대 횟수/backoff+jitter 항목 구현. 별도 Retry topic/DLQ는 다음 단계로 유지.
- 명시적인 RETRYABLE_FAILURE만 예산 안에서 자동 재시도. 영구 실패와 예상 밖 공급자 예외는 최종 결과로 저장.
- 최초 선점/불확실한 lease 복구/일시 실패 재시도가 같은 전체 예산(기본 3, 허용 1~10)을 사용하며 접수 시 고정.
- 기본 1000ms base/60000ms cap의 지수 backoff + equal jitter. 최초 대기 500~1000ms, 다음 1000~2000ms.
- 결과/시도 이력/WAITING_RETRY/DB 예약 시각/후속 Outbox를 한 트랜잭션으로 저장한 뒤 원본 offset 처리.
- 같은 Kafka 토픽과 기존 Relay의 due 조회를 사용. publish future ACK 후 PUBLISHED 저장 원칙 유지.
- task의 next_attempt_at을 별도 보존해 Relay lease/발행 backoff와 분리하고, delivery 조회에 nextAttemptAt 추가.
- retry_generation으로 이전 이벤트를 판별해 이미 예약된 다음 작업을 앞당기거나 진행 중 lease를 빼앗지 못하게 함.
- 중간 예약은 완료 카운터에서 제외. notification_retry_scheduled_total 추가.
- V1~V4 SQL 보존, V5 추가. 과거 최종 일시 실패를 다시 열지 않으며 새 상태/이벤트 때문에 혼합 버전 실행 미지원.
- [재시도 정책](S4_RETRY_POLICY.md), README, PLAN 및 기존 단계 문서 연결 갱신.

### 실제 검증

- `./gradlew --no-daemon --console=plain build`: **75개 통과 / 실패 0 / 오류 0 / 스킵 0**, BUILD SUCCESSFUL (58초), 실행 JAR 생성 성공.
- 기존 62개 회귀 + 신규 13개(실제 MySQL/Kafka 통합 10개, 정책/설정 단위 3개).
- 일시 실패 후 WAITING_RETRY, completedAt null, 예약/Outbox 동시 저장, 키 재사용 시 현재 상태 반환 확인.
- 앱 종료/재기동 후 예약/이력 보존. due 전에는 발행/호출 없음, due 후 두 번째 시도 성공 및 nextAttemptAt null 확인.
- 연속 일시 실패 3회 후 최종 RETRYABLE_FAILURE, 이력/Outbox 각 3행, 예약 카운터 2/완료 카운터 1.
  이전/최신 이벤트를 재전달해도 추가 호출/예약 없음. 동일 generation 중복 INSERT는 DB가 거절.
- 오래된 이벤트는 대기 중 및 새 시도 처리 중에도 무해한 replay. 강제로 조기 전송한 현재 이벤트는 offset/호출 없이 중단.
- 후속 Outbox INSERT 실패 주입: 상태/시도 결과/예약 전체 롤백, 원본 offset 0. 오류 제거/lease 만료 후 복구 성공.
- 재시도 publish 실패 주입: PENDING/WAITING_RETRY 유지. 저장된 예약으로 원본 offset은 처리됐으며, 발행 복구 후 최종 성공.
- 접수 시 상한 1을 이후 설정 3으로 바꿔도 기존 작업은 추가 호출하지 않음. UNKNOWN 복구와 일시 실패가 하나의 예산을 공유.
- 두 완료 트랜잭션 경합: 하나만 성공하며 후속 Outbox/예약이 중복되지 않음.
- V3 → V4 기존 데이터 검증을 유지하고 V4 → V5 업그레이드를 추가 검증. 과거 최종 RETRYABLE_FAILURE/알림 행 보존.
- 단위 테스트에서 equal jitter 하한/상한/60초 cap, 재시도 분류, 잘못된 횟수/지연 설정 거절 확인.
- Git 작성자 JinHyeon / tiemo0708@naver.com 및 GitHub tiemo0708 인증 확인.
  원격 기본 브랜치 main, HEAD c95a444 유지. 인증 조회는 샌드박스 안에서 실패했지만 keyring 접근 가능한 환경에서 정상 확인.

### 한계와 다음 단계

- 통합 테스트는 저장된 due 시각을 앞당겨 검증한다. 실제 60초 대기/부하 성능을 측정한 결과가 아니다.
- 브로커 실제 중단 대신 publish 실패를 주입했다. 프로세스 강제 종료, rebalance, 다중 인스턴스 복구는 미검증.
- 1만 건 실험/Compose 재생성 smoke는 이번 변경에서 재실행하지 않음. 사용자 로컬 MySQL에는 테스트 migration을 적용하지 않음.
- FAILED 재시도 Outbox는 WAITING_RETRY를 남길 수 있어 수동 조사 대상. DLQ/선택 재발행은 아직 없음.
- 다음 작업은 별도 Retry topic/DLQ와 재발행/원본 offset 관계 검증. S4 전체 완료가 아님.
- 로컬 main에 논리적 커밋으로 보관하며 사용자 검토 전에는 푸시하지 않는다. 새 원격 CI 실행 없음.

## 2026-10-14: S4 네 번째 단계 — Retry 토픽과 DLQ 로컬 완료

### 범위와 변경

- 요청/Retry/DLQ 토픽 분리. 기본 이름은 notification.requested.v1과 그 이름의 .retry/.dlq이며 각각 파티션 3개/복제본 1개.
- 이벤트 타입별 Relay 목적지 지정. 토픽 이름 충돌은 기동 설정 검증에서 거절하며 알 수 없는 타입은 발행하지 않음.
- 기존 Worker 하나가 요청/Retry 두 토픽을 구독. concurrency 1/poll 1건, DLQ 자동 소비/재발송 없음.
- DB due 시각 이후에만 Retry 토픽으로 발행. 예약된 재시도를 기다리느라 새 요청 처리를 멈추지 않음.
- 영구 실패, 재시도 상한 소진, 내부 오류, 마지막 lease 복구 소진에 DLQ Outbox 생성.
  최종 결과/시도 이력/DLQ 인계를 같은 트랜잭션으로 저장한 뒤 원본 offset 처리.
- DLQ에는 이벤트/작업/원본 이벤트 ID, generation, 오류 분류/코드, 시도 수/상한만 포함. 수신자/본문/토큰/예외 원문 제외.
- 유니크 제약과 기존 행 잠금/토큰으로 중복 완료 시 새 DLQ 생성 방지. ACK 후 DB 실패 재발행은 같은 eventId 유지.
- DB/해석 불가 이벤트 오류는 기존처럼 소비 중단. 검증되지 않은 원문을 DLQ로 넘기고 offset을 건너뛰지 않음.
- lag는 요청/Retry 두 토픽 합계로 수정, DLQ 제외. Outbox 지표는 세 종류 모두 포함.
- 스키마 변경 없이 기존 V5 사용. 새 정책은 이후 처리하는 실패에 적용하며 과거 최종 실패를 backfill하지 않음.
- [토픽/DLQ 정책](S4_RETRY_TOPICS_DLQ.md), README, PLAN, 기존 S4 정책 문서 갱신.

### 실제 검증

- 첫 전체 빌드 81개 통과(1분 17초). 이후 lease 복구 DLQ 롤백 검증 및 Kafka 미발행 확인을 보강.
- 최종 `./gradlew --no-daemon --console=plain build`: **82개 통과 / 실패 0 / 오류 0 / 스킵 0**, BUILD SUCCESSFUL (1분 12초).
  테스트는 재실행됐고 변경 없는 실행 JAR 생성 등 4개 작업은 up-to-date였다.
- 기존 75개 회귀 + 신규 7개(실제 MySQL/Kafka 통합 6개, 설정 검증 1개).
- due 전 Retry 토픽 end offset 0, 그동안 새 요청 성공. due 이후 Retry 토픽의 별도 offset 증가/처리 성공.
- Retry 레코드만 미처리 상태로 남겨 lag 1 확인. replay 처리 후 추가 공급자 호출 없음.
- 영구 실패의 실제 DLQ 메시지/키/원인/원본 ID/시도 수 확인, 수신자/본문/토큰 비포함.
  앱 재시작 후 원본 이벤트 replay에서도 공급자 재호출 0, DLQ 행/이벤트 ID 유지, Worker DLQ offset 없음.
- 최종 일시 실패/내부 예외/lease 소진의 DLQ 분류와 원본 generation 확인도 기존 테스트에서 보강.
- DLQ INSERT 실패 주입: 일반 완료와 마지막 lease 복구 모두 상태/이력 전체 롤백, 원본 offset 0.
  오류 제거 후 복구하여 최종 결과/DLQ 영속 인계 성공.
- DLQ publish ACK 대기 중 PROCESSING/published_at null, 원본 offset은 이미 영속 Outbox를 근거로 1.
  future 실패 후 PENDING, 앱 재시작/발행 복구 후 같은 payload를 실제 DLQ에서 조회.
- ACK 후 Outbox PUBLISHED UPDATE 실패 주입: PROCESSING 유지. lease 만료 후 같은 이벤트 2개를 실제 DLQ에서 확인.
  공급자 호출은 1회, DLQ 자동 소비 없음. at-least-once 중복을 숨기지 않음.
- Git 작성자 JinHyeon / tiemo0708@naver.com 및 GitHub tiemo0708 인증 확인.
  원격 기본 브랜치 main/HEAD c95a444 유지. 사용자 로컬 MySQL/IDE 설정과 .env는 변경하지 않음.

### 한계와 다음 단계

- 실제 브로커 중단 대신 publish future 실패를 주입. 프로세스 강제 종료/다중 인스턴스/rebalance는 미검증.
- 1만 건 실험과 Compose 재생성 smoke는 재실행하지 않음. 새 원격 CI 실행 없음.
- DLQ 발행도 최대 10회 한도를 사용하며 FAILED Outbox는 수동 조사. Outbox 발행 실패를 다른 DLQ로 순환시키지 않음.
- 새 Relay 토픽 분류 때문에 기존 앱/Relay/Worker를 모두 종료한 후 업그레이드해야 함. 혼합 버전 실행 미지원.
- 다음 단계는 DLQ 선택 재발행과 원인/시도 이력 기록. S4 전체 완료가 아니며 이번 변경은 로컬 main에 커밋하고 푸시는 보류.

## 2026-10-14: S4 다섯 번째 단계 — DLQ 선택 재발행과 감사 이력 로컬 완료

### 범위와 변경

- GET /dead-letters/{eventId}로 DLQ 발행/실패 상태·작업·재발행 이력 조회.
- POST /dead-letters/{eventId}/redrive로 PUBLISHED DLQ 한 건을 선택 재접수.
  사유 코드와 acknowledgePossibleDuplicate=true 필수. 최초 202/같은 사유 재요청 200/다른 사유 409.
- 원본 알림/작업/시도 이력/멱등 키 보존. 새 notification/task/요청 Outbox를 만들고 원본과 연결한 감사 이력 저장.
- 같은 DLQ/원본에서 자식 하나, 원본 계보 전체 수동 재발행 최대 3회. 새 작업은 원본의 maxAttempts 상속.
- REQUIRES_NEW 트랜잭션으로 호출자의 이전 snapshot에서 분리. source 행 잠금과 DB 유니크 제약으로 동시 중복 방지.
- 신규 접수/Outbox/감사 기록을 원자적으로 커밋하며 Kafka 발행/공급자 호출은 트랜잭션 안에서 수행하지 않음.
- V1~V5 보존, V6에서 dead_letter_redrive 추가. 사유·명시적 확인·시각·source/root/target 연결·회차 보존.
- DLQ 자동 소비/일괄 재발행/내용 수정/이력 삭제 API는 없음. 기존 DLQ offset을 바꾸지 않음.
- [재발행 API와 정책](S4_DLQ_REDRIVE.md), README, PLAN, 기존 S4 문서 연결 갱신.

### 실제 검증

- 첫 빌드 90개 중 1개 실패: 새 migration 보존 assertion이 VARBINARY의 byte[]를 객체 동일성으로 비교함.
  기존 데이터 내용은 같았으며 recursive 내용 비교로 assertion 수정. 실패 실행을 통과로 집계하지 않음.
- 최종 `./gradlew --no-daemon --console=plain build`: **90개 통과 / 실패 0 / 오류 0 / 스킵 0**, BUILD SUCCESSFUL (1분 31초).
  테스트 재실행, 변경 없는 실행 JAR 생성 등 4개 작업 up-to-date.
- 기존 82개 회귀 + 신규 8개(실제 MySQL/Kafka/HTTP 재발행 7개, memory 프로필 비노출 1개).
- 최초 202로 새 ID/작업 생성, 원본 API 멱등 키·실패 상태·시도 이력 유지. 원본 이벤트 replay가 자식 작업을 선점하지 않음.
- 앱 재시작 후 동일 요청 200/동일 감사 응답·Location. 새 작업 성공, 원본 영구 실패 유지, DLQ Worker offset 없음.
- 동시 동일 요청 8건: 202 한 건/200 일곱 건, 알림/작업 각각 총 2개, Outbox 3개, 감사 기록 1개.
- 동시 상충 사유 2건: 202 한 건/409 한 건, 감사 이력과 자식 작업 각각 하나.
- 잘못된 사유/확인 누락/UUID는 400, 없는 ID·다른 타입 이벤트는 404, PENDING/FAILED DLQ는 409. 새 접수 없음.
- 감사 INSERT 및 Outbox INSERT 실패 각각 주입: 503, 새 알림/작업/Outbox/감사 기록 모두 롤백,
  원본 이력 및 offset 보존. 원인 제거 후 같은 이벤트/사유로 202 접수 성공.
- 원본 maxAttempts=1을 새 설정 10에서도 상속. 재실패 계보 3회 후 네 번째 재발행 409,
  작업/시도 총 4개, 감사 기록 3개. 최초 이벤트 재요청은 기존 200 응답으로 유지.
- 오래된 호출자 트랜잭션 snapshot에서도 커밋된 감사 기록을 재사용. 호출자 롤백은 독립 접수를 취소하지 않음.
- V5 → V6에서 기존 알림/작업/Outbox 보존, 감사 테이블 빈 상태로 생성. 기존 V1 이후 upgrade 회귀 검증 통과.
- Git 작성자 JinHyeon / tiemo0708@naver.com, GitHub tiemo0708 인증, 원격 기본 main/HEAD c95a444 확인.
  사용자 로컬 DB/IDE 설정/.env 변경 없음.

### 한계와 다음 단계

- 현재 로컬 API에는 인증된 운영자 신원 기록이 없음. 사유·시각·명시적 확인·대상 관계의 감사 이력만 제공.
- 수동 재발행은 외부에서 이미 수락했을 가능성을 제거하지 않음. UNKNOWN/상한 소진은 전달 실패 확정이 아님.
- 원본 계보 상한은 별도 POST /notifications의 신규 접수까지 제한하는 전역 rate limit이 아님.
- 실제 프로세스/브로커 중단, 다중 인스턴스, 1만 건/Compose 재생성 실험은 이번에 실행하지 않음. 새 원격 CI 없음.
- 다음은 공급자 수락 후 DB 저장 전 종료의 불확실성 정책 종합 정리. S4 전체 완료 표시는 아직 하지 않음.
- 로컬 main에 커밋으로 보관하며 사용자 검토 전에는 푸시하지 않는다.

## 2026-10-14: S4 마지막 단계 — 외부 결과 불확실성 정책 정리, S4 로컬 완료

### 범위와 판단

- 코드/스키마/API 동작을 변경하지 않고 [불확실한 결과 정책](S4_UNCERTAIN_OUTCOMES.md)을 작성.
- 선점 전/호출 전/공급자 수락 후/DB 커밋 후/offset 처리 전 중단을 구분하고 DB 증거와 재소비 동작을 표로 정리.
- 현재 선택은 남은 선점 예산 안의 복구를 허용하면서 외부 중복 가능성을 인정하는 방식임을 명시.
- UNKNOWN은 lease 만료 즉시가 아닌 다음 복구에서 기록되며 시각은 공급자 종료 시각이 아니라 복구 시각.
- ATTEMPTS_EXHAUSTED와 INTERNAL_PROVIDER_ERROR는 미발송 확정이 아님.
  명시적 일시 실패의 자동 backoff 재시도와 구분.
- 토큰 교체 후 이전 소유자의 DB 완료를 차단하지만 외부 호출을 취소하지 않음.
  lease 만료만으로 토큰이 자동 무효화되는 것은 아니라는 현재 구현의 경계도 명시.
- DB COMMIT 응답 유실 시 롤백을 추정하지 않고 재조회해야 함. Mock의 동일 messageId를 외부 멱등성 증거로 쓰지 않음.
- Worker 중지/조회/오류 해결/lease 만료 후 재소비/선택 재발행 절차와 읽기 전용 SQL 제공.
- 개별 작업 보류, 외부 수락 조회, 원본 결과 정정 API는 없음을 명시.
- 관련 구현 파일과 기존 실패 주입 테스트 8개를 연결. README·작업/재발행 문서·PLAN 갱신.

### 실제 검증

- `./gradlew --no-daemon --console=plain test --rerun-tasks`: **90개 통과 / 실패 0 / 오류 0 / 스킵 0**,
  BUILD SUCCESSFUL (1분 13초), 4개 작업 모두 실행. up-to-date 결과를 신규 테스트 실행으로 집계하지 않음.
- 실제 MySQL/Kafka/Testcontainers와 Mock을 사용한 기존 회귀 테스트를 재실행. 새 테스트나 기능 코드는 추가하지 않음.
- Mock 수락 후 이력 저장 실패 → offset 0/롤백 → lease 복구 시 UNKNOWN + 수락, Mock 호출 2회의 기존 검증 통과.
- 활성 lease/오래된 토큰 차단, 상한 소진 시 추가 호출 없음, DLQ 인계 롤백, 완료 replay,
  공유 예산, 내부 예외 분류, 선택 재발행 원본 보존도 재확인.
- 문서 로컬 링크/코드 블록/인용 테스트 메서드 존재 및 git diff 공백 검사 통과.
- Git 작성자 JinHyeon / tiemo0708@naver.com, GitHub tiemo0708 인증, 원격 기본 main/HEAD c95a444 확인.

### 완료 범위와 다음 단계

- S4 계획 항목은 로컬 구현·검증·문서화 완료. 게시/새 원격 CI 완료를 뜻하지 않으며 사용자 검토 전 푸시하지 않음.
- 문서의 중단 지점 표는 코드로부터 도출한 정책. 실제 kill/DB COMMIT 응답 유실/외부 네트워크 timeout/
  장시간 호출 중 lease 교체/다중 인스턴스 실험을 수행한 결과로 표현하지 않음.
- 1만 건·Compose 재생성 실험 및 실행 JAR 생성은 이번 문서 작업에서 재실행하지 않음. 로컬 사용자 DB 변경 없음.
- 다음은 S5의 Kafka 중단 중 API 접수와 복구 후 발행 실험. 이번 변경에서는 S5를 시작하지 않음.

## 2026-10-14: S5 첫 단계 — 실제 Kafka 중단 중 접수와 복구 검증

### 범위와 구현

- 기본 테스트에 `KafkaRecoveryTest` 2개 추가. 앱 기능 코드와 DB migration은 변경하지 않음.
- 별도 Testcontainers MySQL 8.4.8/Kafka 4.0.2와 실제 HTTP API 사용. 기존 Compose DB/volume/.env/IDE 설정 변경 없음.
- 정상 기동과 기준 요청 처리 후 테스트 전용 Kafka 컨테이너만 실제 stop/start.
  같은 컨테이너 데이터·주소를 유지하고 Spring Boot 앱/Relay 스케줄러/Worker는 재시작하지 않음.
- 장애 중 202 접수, QUEUED 조회, Outbox 발행 실패와 PENDING 복귀, 공급자 호출 전 상태를 검증.
- 복구 후 요청·작업·이벤트 ID 보존, PUBLISHED, 공급자 수락/시도 이력, committed offset, 지표 대조.
- 발행 예산을 1회로 줄인 별도 테스트에서 FAILED Outbox를 보존하며 복구 후 신규 요청은 정상 처리되는지 확인.
- 학습용 [실험 문서](S5_KAFKA_RECOVERY.md)에 재현 명령·단계별 상태·lag와 Outbox의 차이·한계를 정리.
  README와 PLAN은 검증한 S5 첫 항목만 갱신.

### 실제 실행 결과

- 최초 선택 실행 `./gradlew --no-daemon --console=plain test --tests '*KafkaRecoveryTest'`:
  **2개 통과 / 실패·오류·스킵 0**, BUILD SUCCESSFUL (1분 41초).
- 첫 전체 실행은 **기존 90개 통과 + 새 클래스 초기화 오류 1개**로 실패 (1분 44초).
  빈 포트 선택과 Docker 바인딩 사이에 `address already in use`가 발생했으며 새 2개 테스트는 시작되지 못함.
  이 실행을 전체 통과로 집계하지 않음.
- 초기 인프라 생성만 최대 3회로 제한하고 생성 시도마다 새 포트를 선택하도록 보완.
  테스트 assertion 재실행이나 중단·복구 중 컨테이너 교체로 오류를 숨기지 않음.
- 최종 `./gradlew --no-daemon --console=plain test --rerun-tasks`:
  **92개 통과 / 실패 0 / 오류 0 / 스킵 0**, BUILD SUCCESSFUL (2분 51초), 4개 작업 모두 실행.
  JUnit XML 10개 suite와 Gradle 종료 코드 0을 확인.
- 자동 복구: 장애 중 접수 5건 + 기준 1건 = 총 6건. 기존 ID 유지,
  발행/공급자 수락/시도 이력/Mock 호출/요청 토픽 committed offset 합계 각각 6, 미발행·FAILED·lag 0.
  중단 중 미발행 5, lag NaN, 공급자 추가 호출 0. Retry/DLQ 레코드 0.
- 상한 소진: 총 접수 3건 중 기준·복구 후 신규 2건 처리. 장애 중 1건은 QUEUED/발송 시도 0,
  Outbox FAILED/발행 시도 1로 보존. 미발행 1, FAILED 1이지만 lag 0. DLQ 레코드 0.
- Java 21.0.6 / Gradle 8.14.5 / Docker Engine 27.5.1 로컬 환경. 소량 정합성 실험이며 성능 벤치마크가 아님.
- 문서 내부 링크·코드 블록 및 diff 공백 검사 통과.
- Git 작성자 JinHyeon / tiemo0708@naver.com, GitHub tiemo0708 인증, 원격 기본 main/HEAD c95a444 확인.

### 한계와 다음 단계

- S5 첫 항목만 완료. Kafka 없이 최초 기동, volume 손실, 다중 브로커/인스턴스, 장시간 장애는 검증하지 않음.
- stop/start의 종료 유예는 10초. 앱 강제 종료·ACK/DB 경계 종료·offset 경계 종료·명시적 리밸런스 실험은 남아 있음.
- 발행 상한 소진은 1회 설정으로 검증했으며 기본 10회 전체 대기 시간을 측정하지 않음.
  FAILED Outbox는 자동 복구/현재 DLQ 재발행 API의 대상이 아니므로 별도 조사가 필요.
- 1만 건 실험·실행 JAR 생성·새 원격 CI는 이번에 실행하지 않음.
- 다음은 publish ACK 후 PUBLISHED 저장 전 종료로 인한 중복 재발행 재현.
- 로컬 main에 커밋으로 보관하며 사용자 검토 전에는 푸시하지 않는다.

## 2026-10-14: S5 두 번째 단계 — ACK 후 Relay JVM 강제 종료와 중복 발행 검증

### 범위와 구현

- `OutboxProcessCrashTest` 1개와 테스트 전용 `RelayProcess` 진입점을 추가.
  앱 기능 코드·DB migration은 변경하지 않고 Gradle test에 자식 JVM용 runtime classpath 전달만 추가.
- 전용 Testcontainers MySQL 8.4.8/Kafka 4.0.2 사용. 부모 앱은 API/Worker, 자식 앱은 Relay 역할로 실행해
  Relay 종료의 영향을 분리했다. 운영 구조를 별도 서비스로 변경한 것이 아니다.
- 첫 자식에서만 테스트용 저장소가 실제 Kafka ACK 이후 `published()` 진입 시 표식을 남기고 대기.
  DB 갱신 SQL/트랜잭션 시작 전이며 부모는 Kafka 레코드·Worker 완료·offset을 독립 확인했다.
- 아직 Outbox가 PROCESSING/발행 시도 1/유효한 lease인 상태에서 부모가 자신이 만든 자식 JVM만 강제 종료.
  비정상 종료 코드와 프로세스 종료, 종료 전후 Outbox 행 동일성을 확인했다.
- 중단 장치 없는 새 PID의 Relay를 시작하고 실제 30초 lease 만료를 기다렸다.
  DB 시각/available_at을 앞당기지 않았으며 PUBLISHED 시각이 원래 lease 만료 시각 이후임을 검증했다.
- Kafka 레코드 2개의 동일 key/payload/eventId/taskId, 동일 파티션·연속 offset을 직접 읽어 대조했다.
- README·PLAN 갱신 및 [중단 지점/재현/한계 문서](S5_OUTBOX_ACK_CRASH.md) 추가. S5 두 번째 항목만 완료 표시.

### 실제 검증

- `./gradlew --no-daemon --console=plain test --tests '*OutboxProcessCrashTest'`:
  **1개 통과 / 실패·오류·스킵 0**, BUILD SUCCESSFUL (53초).
- `./gradlew --no-daemon --console=plain build --rerun-tasks`:
  **93개 통과 / 실패 0 / 오류 0 / 스킵 0**, BUILD SUCCESSFUL (3분 32초), 6개 작업 모두 실행.
  기존 92개와 새 프로세스 종료 테스트 1개 포함. JUnit XML 11개 suite와 종료 코드 0 확인.
- 고유 요청/작업/Outbox 각각 1개 유지, 발행 시도 2, Kafka 레코드 2, eventId 1, taskId 1.
  공급자 호출·작업 시도·시도 이력은 각각 1, Worker replay 1, 요청 토픽 committed offset 합계 2.
- 재발행 후 알림 결과 전체와 첫 시도 이력은 변경되지 않음. Retry/DLQ 레코드 0, 미발행·consumer lag 0.
- 실제 실행 JAR 생성 후 ZIP 항목 검사: 프로덕션 OutboxRelay 포함, 테스트 진입점/ACK 중단 장치 미포함.
- 문서 내부 링크·코드 블록과 diff 공백 검사 통과.
- 종료 후 Relay 자식 프로세스 및 테스트 컨테이너 잔존 없음. 기존 로컬 MySQL은 healthy 상태 유지.
- Git 작성자 JinHyeon / tiemo0708@naver.com, GitHub tiemo0708 인증, 원격 기본 main/HEAD c95a444 확인.

### 한계와 다음 단계

- 이 실험에서는 첫 이벤트의 DB 결과와 offset이 완료된 Worker가 계속 살아 있다.
  Worker 동시 종료나 공급자 수락 후 결과 미기록 구간을 검증한 것이 아니다.
- 테스트 전용 Spring Boot 진입점을 자식 JVM에서 실행했다. 생성한 실행 JAR 자체를 별도 기동한 실험은 아니다.
- 브로커·DB 손실, 다중 Relay 경쟁, 명시적 리밸런스, Graceful Shutdown, 단말 도착/exactly-once는 검증 범위 밖.
- 1만 건 실험과 새 원격 CI는 실행하지 않음. 로컬 DB/.env/IDE 설정 변경 및 푸시 없음.
- 다음은 Worker DB 완료 후 offset 커밋 전 프로세스 종료와 재소비 검증.
- 로컬 main에 커밋으로 보관하며 사용자 검토 전에는 푸시하지 않는다.

## 2026-10-14: S5 세 번째 단계 — DB 완료 후 Worker 종료와 offset 재소비 검증

### 범위와 구현

- `WorkerProcessCrashTest`와 테스트 전용 `WorkerProcess` 진입점을 추가. 프로덕션 코드·스키마·설정 변경 없음.
- 전용 MySQL/Kafka에서 부모 앱은 API/Relay, 자식 JVM은 Worker를 실행.
  실제 `WorkerStore.complete()` 커밋 후 `NotificationService.recordCompletion()`에서만 첫 자식을 대기시켰다.
  활성 DB 트랜잭션이 없음을 검사하고 실제 리스너 반환 전 경계를 사용했다.
- 부모의 별도 DB 조회와 Kafka Admin offset 조회로 완료 결과가 영속화됐지만 offset은 미커밋임을 확인한 뒤 강제 종료.
- 중단 장치 없는 새 PID의 Worker를 동일 group으로 시작. offset reset/seek/DB 시각 변경이나 재발행 없이 정상 재할당을 기다렸다.
- 두 JVM의 공급자 시도 카운터를 각각 읽고 새 Worker replay, DB 전체 행, 토픽 레코드 수, offset/lag를 대조했다.
- README·PLAN 갱신과 [학습/재현 문서](S5_WORKER_OFFSET_CRASH.md) 추가. S5 세 번째 항목만 완료 표시.

### 실제 검증

- 최초 집중 테스트는 마지막 작업 행 비교에서 실패. DB VARBINARY `target_id`의 `byte[]`를 Map.equals로 비교해
  동일 내용에도 다른 객체로 판단한 테스트 문제였다. 재귀적인 내용 비교로 수정했다.
- 수정 후 `./gradlew --no-daemon --console=plain test --tests '*WorkerProcessCrashTest'`:
  **1개 통과 / 실패·오류·스킵 0**, BUILD SUCCESSFUL (1분 3초), 테스트 본체 50.373초.
  이후 시도 이력의 수락 상태/완료 시각 assertion을 보강해 전체 빌드에서 검증했다.
- `./gradlew --no-daemon --console=plain build --rerun-tasks`:
  **94개 통과 / 실패 0 / 오류 0 / 스킵 0**, BUILD SUCCESSFUL (4분 26초), 6개 작업 모두 실행.
  JUnit XML 12개 suite와 종료 코드 0 확인. 새 장애 테스트 본체 48.428초.
- Kafka 레코드 1개 유지, 기존 Worker 공급자 호출 1, 새 Worker 호출 0/replay 1.
  종료 직전 DB PROVIDER_ACCEPTED·시도 이력 완료, offset 없음 또는 0, lag 1.
  재시작 후 committed offset 합 1·lag 0. 알림/작업/시도/Outbox 전체 행 유지,
  발행·작업 시도 각각 1, Retry/DLQ 레코드 0, 미발행 0 확인.
- 실행 JAR 생성/ZIP 검사: 프로덕션 NotificationWorker 포함, 테스트 진입점/중단 장치 미포함.
- 문서 내부 링크·코드 블록 및 diff 공백 검사 통과.
- 종료 후 테스트 자식 JVM 및 컨테이너 잔존 없음. 기존 로컬 MySQL healthy 유지.
- Git 작성자 JinHyeon / tiemo0708@naver.com, GitHub tiemo0708 인증,
  원격 기본 main/HEAD c95a444 확인. 푸시하지 않음.

### 한계와 다음 단계

- 완료 결과가 이미 커밋된 최종 성공 1건의 경계 검증이다. 공급자 수락 후 DB 저장 전 중단은
  여전히 결과 불확실/재호출 가능 구간이며, 외부 발송 exactly-once나 단말 도착을 보장하지 않는다.
- API/Relay·DB·브로커는 유지했다. 새 consumer의 정상 재할당을 이용했지만 다중 Worker의
  동시 처리 중 명시적 리밸런스, 동시 장애, Graceful Shutdown까지 검증한 것은 아니다.
- 테스트용 classpath 진입점으로 실행했으며 배포 JAR 별도 기동, 1만 건 실험, 새 원격 CI는 실행하지 않았다.
- 프로덕션 코드·설정·DB migration·로컬 DB/.env/IDE 설정 변경 없음.
- 다음은 S5의 Relay/Worker 재시작 및 명시적 리밸런스 검증. S6는 아직 시작하지 않았다.
- 로컬 main에 커밋으로 보관하며 사용자 검토 전에는 푸시하지 않는다.

## 2026-10-14: S5 네 번째 단계 — Relay/Worker JVM 재시작과 처리 중 리밸런스

### 범위와 구현

- `WorkerRebalanceTest`와 테스트 전용 `RebalanceWorkerProcess` 추가. 프로덕션 코드·설정·스키마 변경 없음.
- API만 부모 앱에서 실행하고 Relay 및 Worker A/B는 독립 자식 JVM으로 시작.
  테스트 전용 MySQL/Kafka와 같은 group을 유지하고 역할을 한 번에 하나씩 변경했다.
- A의 첫 공급자 호출을 최대 25초 대기시키고 실제 B를 시작.
  A의 PROCESSING·offset 미커밋 중 Kafka PREPARING_REBALANCE를 확인한 뒤 대기를 해제해 실제 Mock을 호출했다.
- 안정된 group의 1 → 2 → 1 → 2명 전환과 요청/Retry 총 6개 파티션의 중복·누락 없는 배정을 대조.
- 처리/offset 완료 후 Relay 강제 종료, 그 사이 접수된 PENDING의 ID 확인, 다른 PID의 Relay로 복구.
  기존 taskId/eventId 목록이 유지됨을 확인했다.
- 처리 완료 후 B 강제 종료, A의 모든 요청 파티션 인계, 다른 PID의 B 복귀 후 새 요청 처리 검증.
- 매 구간 요청 파티션 세 곳을 포함하는 가상 API 데이터를 접수한다. UUID 키 매핑에 따라 건수가 달라지며
  한 구간 최대 60건으로 제한한다. 처리량 비교/성능 벤치마크가 아니다.
- 각 구간의 접수 ID·DB 결과/이력·Kafka 레코드 수·파티션별 offset을 확인.
  JVM별 공급자 호출은 종료 전 B의 값도 포함해 합산한다.
- README·PLAN 갱신과 [학습/재현 문서](S5_RESTART_REBALANCE.md) 추가. S5 네 번째 항목만 완료 표시.

### 실제 검증

- `./gradlew --no-daemon --console=plain test --tests '*WorkerRebalanceTest'`:
  **1개 통과 / 실패·오류·스킵 0**, BUILD SUCCESSFUL (1분 16초), 본체 60.646초.
  접수 17건, 공급자 호출 17회, Kafka 레코드/committed offset 합 각각 17, lag 0.
  최초 합류 단계까지 6건 처리, 이후 Relay 재시작과 Worker 이탈/복귀 구간에서 나머지 11건 처리.
- 집중 테스트 후 해제 직전 DB lease 유효 여부와 종료 전 B의 Worker 오류 0 검증을 보강했다.
- `./gradlew --no-daemon --console=plain build --rerun-tasks`:
  **95개 통과 / 실패 0 / 오류 0 / 스킵 0**, BUILD SUCCESSFUL (5분 37초), 6개 작업 모두 실행.
  JUnit XML 13개 suite와 종료 코드 0 확인. 새 리밸런스 테스트 본체 58.429초.
- 전체 빌드의 리밸런스 실험은 접수 26건, 최초 합류 단계까지 5건.
  공급자 호출·Kafka 레코드·committed offset 합 각각 26, 요청 각 파티션 committed=end offset.
  각 작업/Outbox 발행 시도 1, 수락 이력 26, Retry/DLQ·Worker 오류·미발행·lag 0.
- PROCESSING/committed 0/PREPARING_REBALANCE/lease 유효를 실제 관찰.
  안정된 그룹 1 → 2 → 1 → 2명, 각 단계 요청/Retry 파티션 6개 중복·누락 없음.
  종료한 B의 호출 값과 재시작 B의 호출 값을 구분해 전체 호출 수를 합산했다.
- 실행 JAR 생성/ZIP 검사: 프로덕션 Worker 포함, 테스트 진입점·공급자 대기 장치 미포함.
- 문서 링크·코드 블록·diff 공백 검사 통과. 테스트 JVM/컨테이너 잔존 없음, 기존 로컬 MySQL healthy 유지.
- Git 작성자 JinHyeon / tiemo0708@naver.com, GitHub tiemo0708 인증,
  원격 기본 main/HEAD c95a444 확인. 로컬 변경만 진행했으며 푸시하지 않음.

### 한계와 다음 단계

- 합류는 첫 호출을 lease/poll 제한 안에 끝내는 조건, Relay/Worker 종료는 해당 구간 처리와 offset 완료 후다.
  장시간 공급자 정지, lease/poll 만료 후 리밸런스, Retry backlog, 동시 다중 장애를 검증한 것은 아니다.
- 이번 실험의 호출 수 일치는 모든 장애에서의 외부 발송 exactly-once나 단말 도착 보장이 아니다.
- 테스트 classpath의 Spring Boot 진입점을 사용했다. 배포 JAR 별도 기동, 1만 건 실험, 새 원격 CI는 실행하지 않음.
- 로컬 DB/.env/IDE 설정과 기존 이력 변경 없음. S6는 시작하지 않았다.
- 다음은 S5 마지막 항목인 Graceful Shutdown: 신규 수신 중지, 진행 중 작업 정리, 유예 시간 초과 시 복구.
- 로컬 main에 커밋으로 보관하며 사용자 검토 전에는 푸시하지 않는다.

## 2026-10-14: S5 마지막 단계 — Graceful Shutdown과 유예 초과 복구

### 범위와 구현

- 사용 중인 Spring Boot 3.5.16 / Spring Kafka 3.3.16 소스 JAR에서 기본 종료 순서를 확인.
  Kafka의 기본 phase(MAX-100)가 HTTP graceful phase(MAX-1024)보다 높아 Worker 종료 대기 중 HTTP가 열려 있을 수 있었다.
- `AsyncConfiguration`에 container customizer를 추가해 HTTP graceful 다음 phase로 Worker를 이동하고
  `stopImmediate=true`로 현재 레코드 처리 뒤 종료한다. 기존 RECORD ack·poll 1·10초/phase 설정 유지.
- `OutboxRelay`는 ContextClosedEvent 이후 선점 루프를 중지한다. 이미 진행 중인 ACK/DB 기록은 마무리하되
  나머지 PENDING은 다음 프로세스에서 처리한다. 새로운 scheduler나 별도 종료 스레드를 만들지 않았다.
- `GracefulShutdownTest`의 4개 시나리오와 테스트 전용 `ShutdownProcess` 추가.
  각 시나리오는 전용 MySQL/Kafka, 부모 검사 앱, 실제 HTTP API/Relay/Worker를 가진 자식 JVM을 사용한다.
- Worker는 실제 Mock 수락 후 DB 기록 전, Relay는 실제 ACK 후 DB 트랜잭션 전에 대기한다.
  부모가 SIGTERM을 보내고 신규 HTTP POST 미접수·자식 생존·offset 미커밋을 확인한다.
- 유예 내 완료는 대기를 해제하고, 초과는 해제하지 않은 채 실제 10초 phase 유예에 따른 종료를 기다린다.
  통과시키기 위한 SIGKILL 대체 없이 SIGTERM 종료 코드 143을 검증한다. 실패/종료 후 정리만 별도로 강제 종료한다.
- 초과 시 DB의 실제 30초 lease 만료 후 새 PID로 시작. taskId/eventId, 공급자 호출 파일 기록,
  상태/시도 이력·Kafka 레코드/offset·lag를 대조한다. DB 시각/offset 변경 없음.
- README·PLAN 갱신과 [종료 정책/학습/재현 문서](S5_GRACEFUL_SHUTDOWN.md) 추가.
  S5의 정의된 다섯 항목을 로컬 완료로 표시하고 S6는 시작하지 않았다.

### 실제 검증

- `./gradlew --no-daemon --console=plain test --tests '*GracefulShutdownTest'`:
  **4개 통과 / 실패·오류·스킵 0**, BUILD SUCCESSFUL (2분 23초), suite 137.040초.
- 집중 테스트의 SIGTERM→프로세스 종료: Worker 정상 347ms / 초과 10,081ms,
  Relay 정상 284ms / 초과 10,084ms. 테스트 대기 장치를 해제하는 시점에 따른 관찰값이며 운영 성능 보장이 아니다.
- 각 시나리오 접수 2건. Worker 정상과 Relay 정상은 공급자 호출/레코드/offset 합 각각 2.
  Worker 초과는 공급자 호출 3, 레코드/offset 합 2, UNKNOWN 이력 1.
  Relay 초과는 공급자 호출 2, 레코드/offset 합 3. 모두 최종 수락 2·미발행/lag/Retry/DLQ 0.
- `./gradlew --no-daemon --console=plain build --rerun-tasks`:
  **99개 통과 / 실패 0 / 오류 0 / 스킵 0**, BUILD SUCCESSFUL (7분 48초), 6개 작업 모두 실행.
  JUnit XML 14개 suite와 종료 코드 0 확인. 종료 시나리오 suite 132.821초.
- 전체 빌드의 SIGTERM→종료: Worker 정상 332ms / 초과 10,074ms,
  Relay 정상 289ms / 초과 10,077ms. 네 경우 모두 SIGTERM 코드 143, 신규 접수 없음,
  후속 작업 미선점 상태 확인 후 새 JVM에서 두 요청의 최종 수락과 lag 0 확인.
- 유예 초과 Worker는 실제 Mock 수락을 두 번 받는 첫 요청을 재현했다(총 호출 3, UNKNOWN 1).
  유예 초과 Relay는 동일 이벤트 재발행을 재현했다(레코드/offset 3, 공급자 호출 2).
  기존 작업/이벤트 ID와 각 시도 이력은 보존했다.
- 실행 JAR 생성/ZIP 검사: 프로덕션 종료 변경 포함, 테스트 진입점·대기 장치 미포함.
- 문서 링크·코드 블록·diff 공백 검사 통과. 테스트 JVM/컨테이너 잔존 없음, 기존 로컬 MySQL healthy 유지.
- Git 작성자 JinHyeon / tiemo0708@naver.com, GitHub tiemo0708 인증,
  원격 기본 main/HEAD c95a444 확인. 푸시하지 않음.

### 한계와 다음 단계

- 10초는 각 lifecycle phase의 유예다. 모든 장애에서 전체 프로세스가 10초 이내 종료됨을 보장하지 않는다.
  Worker/Relay 동시 정지, 느린 HTTP 요청의 종료 유예 초과, DB/브로커 동시 장애는 이번 범위 밖이다.
- 실제 단말 도착이나 외부 발송 exactly-once를 보장하지 않는다. 공급자 수락 후 DB 미기록 상태의
  중복 가능성은 정책 설명에 그치지 않고 이번 테스트에서 실제 재현했다.
- 활성 lease가 남은 채 너무 빨리 재시작하면 Worker가 멈출 수 있다. 원인/lease 만료 확인 후 명시적으로 재시작한다.
- macOS에서 SIGTERM을 검증했다. Linux CI 대상 코드이지만 이번 원격 CI는 실행하지 않았고 Windows 신호 동작은 미검증이다.
- 테스트 classpath 진입점으로 실행했으며 배포 JAR 별도 기동·1만 건 실험은 이번에 하지 않았다.
- DB migration·로컬 DB/.env/IDE 설정 변경 없음. 다음은 S6의 별도 HTTP Mock(지연/timeout/429/5xx/요청 한도)이다.
- S5는 정의한 다섯 장애 실험 범위에서 로컬 완료. S6는 시작하지 않았고 사용자 검토 전 푸시하지 않는다.

## 2026-10-14: S6 첫 단계 — 독립 HTTP Mock 공급자 서버

### 범위와 구현

- `src/mockServer` source set과 별도 패키지/`mock-server.yml`을 추가.
  `mockServer` 실행 task, `mockServerJar` 별도 실행 JAR, build의 두 JAR 생성과 테스트 classpath 연결을 구성했다.
- `127.0.0.1:18081`에서 DB/Kafka/공급자 키 없이 실행한다.
  `POST /pushes`, `GET /mock/config`, `GET /mock/stats`, Actuator health 제공.
- SUCCESS 202, 강제 429/Retry-After, 강제 500, 가상 수신자 영구 실패 400과 최대 10초 응답 지연 지원.
- 프로세스 단위 고정 윈도우 quota, 최대 8개 처리 슬롯(기본 4), 포화 시 즉시 503을 구현.
  기본 서버 스레드 16/연결 32/accept backlog 16. 입력 검증과 Content-Length 유무에 무관한 8KiB 본문 상한 적용.
- 요청/수신자 ID 목록을 보관하지 않고 집계만 유지한다. 같은 ID의 재호출도 별도 수락하며 새 providerMessageId를 반환한다.
- 수락 후 응답을 지연하므로 클라이언트 timeout이 이미 기록한 수락을 취소하지 않는 불확실성을 재현한다.
- 기존 Worker는 여전히 내부 Mock을 사용한다. HTTP 어댑터는 다음 timeout/연결 풀 상한 항목에서 연결한다.
  Redis 전역 제한, Worker 보호, Circuit Breaker까지 구현한 것이 아니다.
- README·PLAN 갱신과 [설정/재현/한계 문서](S6_HTTP_MOCK.md) 추가. S6 첫 항목만 완료 표시.

### 실제 검증

- 최초 빌드는 사용자 정의 BootJar의 targetJavaVersion 미설정으로 테스트 실행 전에 실패.
  Java 21을 명시한 뒤 다시 빌드했다.
- `./gradlew --no-daemon --console=plain test --tests 'dev.jinhyeon.notificationmock.*' mockServerJar`:
  **11개 통과 / 실패·오류·스킵 0**, BUILD SUCCESSFUL (16초).
- 실제 HTTP 202/429/500/400, 20개 동시 요청에서 quota 3 수락·17개 429,
  슬롯 2의 동시 처리 상한/초과 503, 800ms 응답 지연/200ms client timeout 뒤 수락 유지 검증.
  잘못된 JSON·trailing token·빈 필드·8KiB 초과 본문(고정 길이/청크 모두), 설정 상한도 검증했다.
- 별도 quota 테스트로 monotonic 경계의 리필/미사용 예산 비누적과 40개 동시 시도 중 3개만 허용함을 확인.
- 첫 실행 JAR 기동 검증은 전체 빌드의 JAR 재생성과 겹쳐 클래스 로딩 오류로 실패했다.
  빌드 파일과 분리한 사본으로 재실행해 health UP, POST 202, requests/accepted 1, active 0을 확인했다.
  기동에 사용한 사본과 최종 Mock JAR의 SHA-256 일치를 확인하고 소유 프로세스만 SIGTERM 종료(143)했다.
- JAR ZIP 검사: 알림 앱 JAR에 HTTP Mock 클래스 미포함, Mock JAR에 알림 앱/테스트 진입점 미포함,
  Mock 설정 파일 포함 확인. 독립 HTTP Mock JAR 자체를 실제 기동한 검증이다.
- `./gradlew --no-daemon --console=plain build --rerun-tasks`:
  **110개 통과 / 실패 0 / 오류 0 / 스킵 0**, BUILD SUCCESSFUL (9분 23초), 9개 작업 모두 실행.
  JUnit XML 16개 suite와 종료 코드 0을 확인했다. 기존 99개와 신규 11개를 모두 실행했다.
- 문서 링크·코드 블록·diff 공백 검사 통과. 테스트/Mock JVM과 테스트 컨테이너 잔존 없음.
  기존 `notification-lab-mysql-1`은 healthy를 유지했다.
- Git 작성자 JinHyeon / tiemo0708@naver.com, GitHub tiemo0708 인증과
  원격 기본 main/HEAD c95a444를 확인했다. 사용자 검토 전 푸시하지 않는다.

### 한계와 다음 단계

- S6의 별도 HTTP Mock 항목만 완료했다. Worker HTTP 연동, 클라이언트 timeout/연결 풀,
  Redis 전역 Rate Limit, pause/resume와 Circuit Breaker는 다음 작업이다.
- 요청 한도는 서버 프로세스별 고정 윈도우다. 윈도우 경계의 순간 버스트와 다중 서버의 한도 합산을 막지 않는다.
  timeout 뒤 재호출의 중복 수락 가능성이 있으며 단말 도착이나 exactly-once를 보장하지 않는다.
- 1만 건 실험과 원격 CI는 이번에 실행하지 않았다. 로컬 HTTP/동시성 검증을 운영 성능으로 해석하지 않는다.
- DB migration·로컬 DB/.env/IDE 설정 변경 없음. 다음은 HTTP 어댑터와 connect/response timeout·연결 풀 상한이다.

## 2026-10-14: S6 두 번째 단계 — Worker HTTP 어댑터와 timeout/연결 풀 상한

### 범위와 구현

- `app.provider.mode=HTTP`로 별도 HTTP Mock을 호출하는 PushProvider를 추가했다. 기본값은 기존 내부 `MOCK`이다.
  알 수 없는 mode/무제한 timeout/외부 URL은 기동 또는 설정 구성 시 실패한다.
- Apache HttpClient 5.5.2 / HttpCore 5.3.6을 기존 Boot BOM으로 선택했다.
  STRICT 풀 전체/목적지별 기본 연결 2개, 추가 진입 4개, 풀 대기 200ms/연결 500ms/응답 1000ms로 제한한다.
- 응답이 조금씩 도착해도 전체 3000ms deadline에서 취소한다. 예약 작업은 진입 호출 수 이내이며 완료 시 제거한다.
  종료 시 진행 중 요청과 연결 풀/타이머를 정리한다. 설정 상한은 deadline 10초/연결 8개/추가 진입 16개다.
- 로컬 HTTP Mock 전용으로 `http://127.0.0.1:명시적포트` origin만 허용하고 DNS/TLS를 도입하지 않았다.
  자동 HTTP 재시도/redirect/cookie/압축을 끄고 정상 응답을 8KiB/Mock message ID 형식으로 제한한다.
- 429/5xx는 재시도 가능, 일반 4xx는 영구 실패, timeout/잘못된 응답/연결 오류는 `HTTP_UNKNOWN_*`로 구분한다.
  기존 DB 기반 최대 3회/Retry/DLQ 정책을 사용한다. lease 복구 이력의 UNKNOWN outcome과는 구분한다.
- DB 선점 → 트랜잭션 밖 HTTP → 결과/이력/후속 Outbox 저장 → Kafka offset 커밋 경계를 유지한다.
- README/PLAN과 [HTTP 연동 문서](S6_HTTP_CLIENT.md)를 갱신했다. S6 두 번째 항목까지 완료 표시한다.

### 실제 검증

- 의존성 확인 및 `compileJava` 성공. BOM 해석 결과 HttpClient 5.5.2 / HttpCore 5.3.6 확인.
- `./gradlew --no-daemon --console=plain test --tests '*HttpPushProviderTest'`:
  최초 집중 실행 **15개 통과**, BUILD SUCCESSFUL (19초). 이후 종료 중 호출 취소 테스트 1개를 추가하여 전체 빌드에 포함했다.
- 실제 HTTP 결과 분류/자동 재시도 없음, 풀 1개 + 추가 호출 1개에서 BUSY와 pool timeout,
  유휴 연결 재사용, 600ms 지연/150ms 응답 timeout 뒤 수락 유지, 느린 본문의 600ms 전체 deadline을 검증했다.
  redirect 미추적, 큰/잘못된/민감한 응답 거절, 닫힌 포트 연결 실패, 호출 전 interrupt 보존도 검증했다.
- `./gradlew --no-daemon --console=plain test --tests '*HttpWorkerTest'`:
  **5개 통과**, BUILD SUCCESSFUL (34초). 실제 MySQL/Kafka/HTTP를 사용했다.
- 정상: 호출/수락/시도 이력/offset 합 1, DLQ 0. 500·429: 호출/이력/offset 합 3, 수락 0, DLQ 1.
  영구 실패 400: 호출/이력/offset 합 1, 수락 0, DLQ 1.
- 응답 지연 700ms/클라이언트 timeout 200ms: 호출/수락/이력/offset 합 모두 3, DLQ 1.
  각 시도와 DB 최종 오류는 HTTP_UNKNOWN_TIMEOUT. 실제 수락 3회와 Worker의 응답 실패가 동시에 남는 것을 확인했다.
- 빌드된 두 실행 JAR를 별도 사본으로 기동해 실제 연동을 확인했다.
  memory 프로필 알림 API 201 → HTTP Mock → PROVIDER_ACCEPTED, Mock requests/accepted 1, active 0, 양쪽 health UP.
  검증 프로세스는 모두 SIGTERM 143으로 종료했다. 이 smoke는 패키징 검증이며 Kafka 흐름은 위 통합 테스트에서 확인했다.
- JAR 검사: 알림 앱에 HTTP 어댑터와 HttpClient 5.5.2 의존성 포함, HTTP Mock 서버 클래스/테스트 진입점 미포함.
  기동한 사본과 빌드 산출물 SHA-256 일치 확인.
- `./gradlew --no-daemon --console=plain build --rerun-tasks`:
  **131개 통과 / 실패 0 / 오류 0 / 스킵 0**, BUILD SUCCESSFUL (10분 12초), 9개 작업 모두 실행.
  JUnit XML 18개 suite와 종료 코드 0 확인. 기존 110개 + HTTP 어댑터 16개 + Worker 연동 5개다.
- 문서 링크/코드 블록/diff 공백 검사 통과. 테스트/검증 JVM과 테스트 컨테이너 잔존 없음.
  기존 notification-lab-mysql-1은 healthy 유지. 로컬 DB/.env/IDE 설정 변경 없음.
- Git 작성자 JinHyeon / tiemo0708@naver.com, GitHub tiemo0708 인증,
  원격 기본 main/HEAD c95a444 확인. 원격 푸시는 하지 않았다.

### 한계와 다음 단계

- TCP connect timeout은 설정했으나 패킷 드롭 네트워크에서 실제 만료 시간을 측정하지 않았다.
  실제 연결 실패 검증은 loopback의 닫힌 포트이며 DNS/TLS/원격 네트워크는 범위 밖이다.
- timeout/비정상 응답 이후 기존 재시도 정책은 중복 수락을 허용할 수 있다. 최종 실패/DLQ도 미발송 확정이 아니다.
  429 Retry-After는 아직 예약 시각에 반영하지 않으며 DB backoff를 사용한다.
- 연결 풀/진입 상한은 프로세스별이다. 전역 공급자 quota 준수나 Worker 병렬 처리/유한 대기열을 완료한 것이 아니다.
  다음은 Worker 동시성 제한과 유한 대기열이며 Redis 전역 제한/pause/resume/Circuit Breaker도 남아 있다.
- 1만 건 실험과 원격 CI는 이번에 실행하지 않았다. 테스트 수치를 운영 성능으로 제시하지 않는다.
- DB migration·기존 로컬 MySQL/.env/IDE 설정은 변경하지 않았다. 사용자 검토 전 원격에 푸시하지 않는다.

## 2026-10-15: S6 세 번째 단계 — Worker 동시성 제한과 유한 FIFO 대기열

### 문제·대안·선택

- 느린 공급자 호출에서 소비 스레드만 늘리면 DB/HTTP 처리 수도 증가한다. 별도 비동기 executor는
  미완료 offset/파티션 회수/종료 관리를 추가로 요구하므로 이번에는 소비 스레드의 동기 처리를 유지했다.
- `WorkerAdmission`의 프로세스 공통 처리 자리와 유한 FIFO를 DB 선점 앞에 배치했다.
  대기 작업은 lease/시도 횟수를 만들지 않고, 처리/결과 저장 후 리스너가 반환해야 RECORD offset을 커밋한다.
- `app.worker` 기본값: 소비 스레드 1/처리 1/대기 2/대기 시간 1000ms.
  범위는 소비 1~3, 처리 1~소비 수, 대기 0~3, 대기 시간 1~5000ms.
- 포화/timeout/interrupt는 예외로 소비를 중지하고 해당 offset을 보존한다. 자동 반복 재시작이나 DLQ 처리로 바꾸지 않았다.
  처리/대기 gauge 및 고정 reason label의 진입 거절 counter를 추가했다.
- 앱 종료는 신규 진입 차단/대기자 깨우기 후 이미 진입한 작업을 기존 종료 유예 안에서 마친다.
  종료 중 오류 처리기가 별도의 비정상 stop을 시작하지 않게 보완해 lifecycle 종료 대기가 진행 중 리스너를 기다리도록 했다.
- [문제·대안·선택 이유·설정·검증·한계](S6_WORKER_CONCURRENCY.md)를 추가하고 README/PLAN/HTTP 문서를 갱신했다.
  S6의 Worker 동시성/유한 대기열 항목까지 완료 표시한다. Redis와 pause/resume 등은 남겨 둔다.

### 실제 검증과 실패 수정

- `compileJava` 성공. `WorkerAdmissionTest` 최초 집중 실행 **6개 통과**, BUILD SUCCESSFUL (6초).
- 첫 `WorkerConcurrencyTest`는 4개 중 3개 통과/1개 실패(36초).
  timeout counter 생성 전에 `get`으로 조회한 테스트가 MeterNotFoundException으로 끝나서 `find`와 생성 대기로 수정했다.
- 종료/새 앱 복구 시나리오를 추가한 두 번째 집중 실행은 11개 중 10개 통과/1개 실패(1분).
  종료 중 대기자 거절과 오류 처리기의 별도 stop이 경합해 진행 중 알림이 PROCESSING에 남는 것을 재현했다.
  종료 중에는 추가 stop 대신 예외 전파로 offset을 보존하고 기존 lifecycle이 종료를 담당하도록 보완했다.
- `./gradlew --no-daemon --console=plain test --tests '*WorkerAdmissionTest' --tests '*WorkerConcurrencyTest'`:
  수정 후 **11개 통과 / 실패·오류·스킵 0**, BUILD SUCCESSFUL (38초).
- 동시 스레드 테스트: FIFO, 처리/대기 상한, 포화·timeout, interrupt 보존, 종료 시 대기 해제,
  예외 후 자리 반환과 잘못된 설정 거절을 검증했다.
- 실제 MySQL/Kafka/HTTP: 소비 3/처리 2/대기 1, HTTP 지연 1500ms에서 최고 HTTP 처리 2.
  대기 및 같은 파티션 후속 알림은 시도 0, 처리 중 세 파티션 offset 0 확인 후 최종 완료/호출/offset 합 4, 파티션별 2/1/1.
- 대기 timeout 100ms와 대기 용량 0 포화 각각에서 미선점/offset 0을 확인했다.
  진행 중 작업 완료 후 명시적으로 재시작해 두 요청을 총 호출 2회로 완료했다.
- DB 완료 실패는 해당 파티션 offset 0과 후속 알림 시도 0을 보존했다.
  테스트 trigger 제거/SQL로 lease 만료 후 첫 요청 UNKNOWN 이력 1, 총 호출 4/고유 완료 3/offset 합 3으로 복구했다.
- 대기 작업이 있는 앱 종료/새 context 재시작: 진행 중 작업 완료, 대기 작업 시도 0/offset 0,
  재시작 후 두 요청의 최종 완료/호출/offset 합 2를 확인했다.
- 통합 실험은 Relay를 끄고 API에 저장된 Outbox payload를 명시적 파티션으로 직접 발행한다.
  Outbox를 임의로 PUBLISHED로 변경하지 않았다. 기존 전체 회귀가 Relay 경로를 별도로 검증한다.
- 첫 전체 `build --rerun-tasks`는 **142개 중 141개 통과/1개 실패**(2시간 6분 2초).
  기존 GracefulShutdownTest의 Relay 정상 종료 후 Outbox 시도 횟수가 기대 1과 달리 5였다.
  실행 로그에 긴 시간 공백/clock leap 경고가 있었고 macOS 전원 로그에서 실행 시간대의 반복된 Sleep/DarkWake를 확인했다.
  환경 중단 영향을 분리하기 위해 테스트 기대값을 변경하지 않고 `caffeinate -i`로 유휴 잠자기를 억제한 전체 재실행을 시작했다.
- `caffeinate -i ./gradlew --no-daemon --console=plain build --rerun-tasks`:
  **142개 통과 / 실패 0 / 오류 0 / 스킵 0**, BUILD SUCCESSFUL (8분 50초), 9개 작업 모두 실행.
  JUnit XML 20개 suite와 종료 코드 0 확인. 기존 131개와 신규 11개를 모두 재실행했다.
  앞서 실패한 Relay 정상 종료 테스트도 코드/기대값 변경 없이 통과했다.
- 실행 JAR에 WorkerAdmission/WorkerProperties 포함 및 테스트 진입점 미포함 확인.
  이번 단계에서는 실행 JAR를 별도로 기동하지 않았고 테스트 classpath의 실제 앱/하위 JVM으로 검증했다.
- 문서 링크/코드 블록/diff 공백 검사 통과. 테스트 JVM/컨테이너와 임시 caffeinate 프로세스 잔존 없음.
  기존 notification-lab-mysql-1은 healthy 유지.
- Git 작성자 JinHyeon / tiemo0708@naver.com, GitHub tiemo0708 인증과 원격 기본 main/HEAD c95a444 확인.
  원격 푸시는 하지 않았다.

### 한계와 다음 단계

- 클라이언트 timeout 뒤 공급자 서버의 작업은 계속될 수 있다. 로컬 Worker 상한을 공급자 측 실행 수 보장으로 해석하지 않는다.
- 이 제한은 프로세스별 처리 수/FIFO 대기 수다. 여러 앱의 합산 처리 수나 초당 공급자 요청량은 제한하지 않는다.
  다음은 Redis Lua 전역 Rate Limit과 속도/허용 버스트 정책이다.
- 대기 중 소비 스레드는 poll하지 않는다. 대기 최대 5초/HTTP deadline 최대 10초와 별개로 DB 지연까지 더해지므로
  모든 장애에서 max.poll.interval 60초를 보장하지 않는다. poll/heartbeat 유지와 pause/resume는 남은 항목이다.
- 포화/timeout은 자동 pause/resume가 아니라 소비 중지와 명시적 복구 정책이다.
  공급자 429 Retry-After 반영, Circuit Breaker, Redis 장애 정책은 구현하지 않았다.
- 새 DB 실패 실험의 lease 만료는 테스트 SQL로 재현했다. 새 병렬 구성의 모든 다중 앱/장기 장애 조합을 검증한 것은 아니다.
  1만 건 실험·처리량 벤치마크·원격 CI는 이번에 실행하지 않았다.
- DB migration·기존 로컬 DB/.env/IDE 설정 변경 없음. 사용자 검토 전 원격에 푸시하지 않는다.

## 2026-10-16: S6 네 번째 단계 — Redis Lua 공유 토큰 버킷

### 문제·대안·선택

- 프로세스별 Worker/HTTP 상한만으로 여러 앱의 합산 초당 요청 수를 제한할 수 없다.
  메모리 제한기, 고정 윈도우, sliding window 로그와 비교해 속도/버스트를 분리하고 고정 크기 상태를 쓰는 token bucket을 선택했다.
- Redis TIME 기준 보충·확인·차감을 Lua 한 번으로 수행한다. 기본 r=10/초, B=10이며 각각 1~10000이다.
  토큰당 credit 1000의 정수 계산으로 거절 재확인 사이의 부분 보충을 보존한다.
- 공급자당 고정 키 하나를 모든 클라이언트가 공유한다. TTL을 두지 않아 만료로 예산/정책이 초기화되지 않는다.
  시계 역행 시 추가 보충 없이 기존 last_ms를 유지하고, 긴 유휴 시간에도 B개까지만 보충한다.
- 기존 rate/burst/version 충돌, 손상된 상태, Redis 오류/timeout/연결 풀 포화는 UNAVAILABLE로 반환한다.
  timeout은 차감 여부가 불확실하므로 내부 재시도/환불 없이 반환한다. 제한 결과의 재확인 시간은 예약이 아니다.
- Spring Boot BOM의 Jedis 6.0.0, 연결 풀 기본 2/최대 4, 추가 대기열 없음, 연결/읽기 timeout 각각 기본 250ms를 사용한다.
  outcome counter와 사용 중 연결 gauge를 등록하며 로그/label에 요청 식별자나 payload를 넣지 않는다.
- 선택적 Compose redis 프로필(7.4.11-alpine, loopback 16379, AOF/volume/noeviction)을 추가했다.
  이번 검증은 격리된 Testcontainers에서 진행하며 기존 Compose Redis를 기동하지 않았다.
- **이번 범위는 제한기 컴포넌트다. Worker는 아직 호출하지 않는다.** `client-enabled` 기본 false이며 true도 빈 생성만 활성화한다.
  최초 발송/재시도에 quota 연결, quota 대기와 DB 시도/offset 보존은 바로 다음 PLAN 항목으로 남겼다.
- [문제·알고리즘 비교·정책·실험·한계](S6_REDIS_RATE_LIMIT.md)를 추가하고 README/PLAN을 갱신했다.

### 실제 검증

- `compileJava`: 성공(11초). 처음 집중 실행 10개 통과(13초) 후 실제 시간 합산 상한과 연결 거절 사례를 추가했다.
- `./gradlew --no-daemon --console=plain test --tests '*RateLimitConfigurationTest' --tests '*RedisRateLimiterTest'`:
  **12개 통과 / 실패·오류·스킵 0**, BUILD SUCCESSFUL(11초).
- 실제 Redis에서 독립 연결 풀 4개/총 40회 동시 요청/B=7: 허용 7, 제한 33, 오류 0.
  저장 시각을 미래로 지정해 보충이 없는 원자적 차감 경합을 검증했다. 운영 코드에 테스트 시계를 주입하지 않았다.
- 별도 실제 TIME 실험: 4개 클라이언트/총 80회/r=10/B=3, 집중 실행에서 610ms/허용 8.
  `B + floor(r × Δms / 1000)` 이내였고 초기 버스트를 초과해 보충되는 것도 확인했다. 처리량 벤치마크가 아니다.
- 실제 보충(2/초, 최소 500ms), 반복 거절 시 부분 credit 보존, 긴 유휴 후 버스트 상한,
  클라이언트 재생성/스크립트 캐시 삭제 시 예산 유지, 역행 분기, 설정 충돌/잘못된 상태 무변경을 검증했다.
- CLIENT PAUSE 1500ms/읽기 timeout 250ms/연결 1개에서 사용 중 연결 1을 확인했다.
  추가 호출은 대기 없이 UNAVAILABLE, 기존 호출은 timeout으로 UNAVAILABLE이며 해제 후 재사용했다.
  닫힌 포트, context 종료, 기본 비활성, 잘못된 활성 설정도 검증했다.
- 첫 `caffeinate -i ./gradlew --no-daemon --console=plain build --rerun-tasks`: **154개 통과 / 실패·오류·스킵 0**,
  BUILD SUCCESSFUL(9분 36초), 22개 suite/9개 작업 모두 실행.
- 추가 경계 테스트에서 첫 연결 생성이 멈췄을 때 풀 설정만으로는 추가 호출을 즉시 거절하지 못하는 것을 재현했다.
  응답 없는 loopback TCP 연결/자리 1개 조건에서 200ms 이내 거절 기대가 실제 2046ms로 실패했다(단독 실행 1개 실패, 11초).
  연결 풀의 생성 경합이 소진 검사보다 먼저 실행되므로, 풀 진입 전 같은 크기의 세마포어 tryAcquire로 연결 생성까지 상한에 포함했다.
- 수정 후 `test --tests '*RateLimitConfigurationTest' --tests '*RedisRateLimiterTest'`: **13개 통과 / 실패·오류·스킵 0**(12초).
  첫 연결 경합 테스트의 200ms 이내 거절과 기존 공유 quota/timeout/보충 테스트 모두 통과했다.
- 최종 `caffeinate -i ./gradlew --no-daemon --console=plain build --rerun-tasks`: **155개 통과 / 실패 0 / 오류 0 / 스킵 0**,
  BUILD SUCCESSFUL(9분 24초), 22개 suite/9개 작업 모두 실행. 기존 142개 + 신규 13개를 최종 코드로 재실행했다.
  최종 실제 TIME 실험도 603ms/허용 8로 공유 rate/burst 상한 이내였다.
- 테스트 컨테이너/Java 21 검증 JVM/caffeinate 잔존 없음. 기존 notification-lab-mysql-1은 healthy 유지.
- 실행 JAR에 제한기/설정/Jedis 6.0.0과 Lua 원문 일치 확인. 테스트 진입점/HTTP Mock 클래스 미포함.
  이번 단계에서 JAR 별도 기동 smoke는 실행하지 않았다.
- Compose 구문 검사, 문서 상대 링크/코드 블록과 diff 공백 검사 통과.
- Git 작성자 JinHyeon / tiemo0708@naver.com, GitHub tiemo0708 인증과 원격 기본 main/HEAD c95a444 확인.
  원격 푸시는 하지 않았다.

### 한계와 다음 단계

- 아직 실제 공급자 발송을 제한하지 않는다. 다음은 최초 발송/재시도를 같은 quota에 연결하고
  부족한 quota 때문에 발송 시도 횟수/lease를 소모하거나 미완료 offset을 넘기지 않는지 검증하는 단계다.
- Redis 오류를 허용하지 않는 컴포넌트 계약까지 검증했다. 전체 파이프라인의 Redis 장기 장애/복구,
  pause/resume와 poll/heartbeat, Circuit Breaker 및 우선순위 배분은 남은 항목이다.
- 클라이언트 4개는 하나의 JVM에서 독립 연결 풀을 사용했다. 별도 앱 JVM 4개, failover/replication/Cluster,
  Redis 프로세스 재시작/AOF 복원과 메모리 상한 도달은 이번에 실험하지 않았다.
- 역행은 저장된 미래 시각으로 분기를 검증했으며 실제 OS 시계를 변경하지 않았다. 시계 전진/상태 손실은 버스트를 다시 허용할 수 있다.
  AOF everysec/noeviction도 모든 장애에서 예산 보존을 보장하지 않는다.
- 허가 시점과 실제 HTTP 호출 시점의 차이, 공급자의 별도 fixed-window 계약도 후속 통합에서 고려해야 한다.
- 1만 건 정합성 실험·운영 처리량 벤치마크·원격 CI는 이번에 실행하지 않았다.
- DB migration·기존 로컬 DB/.env/IDE 설정 변경 없음. 사용자 검토 전 원격에 푸시하지 않는다.

## 2026-10-16: S6 다섯 번째 단계 — 최초 발송/재시도 공통 quota

### 문제·대안·선택

- Redis 제한기를 Worker에 연결하되, DB 선점 후 기다리거나 quota 부족을 공급자 실패로 기록하면
  실제 발송 전 시도 횟수/lease/재시도 예산을 소모한다. 모든 레코드 앞에 무조건 적용하면 완료 재소비도 Redis에 의존한다.
- WorkerStore에 쓰기 없는 NEEDS_PERMIT 결과를 추가했다. 이벤트/세대/task/상태/due/lease 검증 후 발송 후보만 반환하고
  트랜잭션 밖에서 quota를 기다린다. 허가 뒤 새 트랜잭션으로 다시 검증한 다음 시도/lease를 만든다.
- 완료/이전 세대 재소비와 시도 상한 소진 처리는 허가를 요구하지 않는다.
  상한 소진은 기존 UNKNOWN/최종 실패/DLQ 저장을 유지하며 HTTP를 호출하지 않는다.
- `app.rate-limit.client-enabled=true`가 이제 비동기 Worker의 최초/재시도에 적용된다. 기본 false와 memory/sync 비교 프로필은 기준선이다.
  같은 공급자 키/rate/burst를 공유한다. API 계약과 DB migration은 변경하지 않았다.
- WorkerQuota는 처리 자리 안에서 기본 2000ms/최대 5000ms 동안 LIMITED를 재확인한다(최소 간격 25ms).
  새 executor/무제한 큐 없이 소비 스레드에서 기다린다. timeout/UNAVAILABLE/interrupt/종료는 예외로 미완료 offset을 보존한다.
  Redis 오류는 즉시 소비 중지, 원인 해결 후 명시적 재시작 정책이다. 자동 pause/resume는 다음 항목이다.
- Redis 명령 전후로 대기 예산을 확인하며 늦은 허가는 사용/환불하지 않는다. 명령 자체를 강제 취소하는 전체 deadline은 아니다.
  quota 중단/경합에서 소비한 허가와 실제 HTTP 호출 수는 다를 수 있다.
- 종료 시 WorkerAdmission 정지 표시 후 WorkerQuota 대기자를 깨워 기존 lifecycle 종료 대기와 오류 처리기가 경합하지 않게 했다.
- [문제·대안·선택·트랜잭션·복구·검증](S6_WORKER_QUOTA.md), README/PLAN과 관련 S6 문서를 갱신했다.

### 실제 검증과 테스트 구성 수정

- 첫 `test --tests '*WorkerQuotaTest'`: **6개 통과**(7초).
- 최초 집중 실행은 12개 중 3개 실패(46초). 테스트가 서로 다른 작업에 같은 lease token을 사용해 UNIQUE 제약에 걸렸고,
  CLIENT PAUSE 5000ms의 해제용 연결이 3000ms 읽기 timeout으로 실패했다. 같은 Redis를 쓰는 다음 Relay 사례도 대기 확인에 실패했다.
  중단 시간을 1500ms로 줄이고 해제 후 PING 복구를 확인하며, 테스트 작업마다 다른 token을 사용하도록 수정했다.
- 두 번째 집중 실행은 12개 중 1개 실패(36초). 수정한 테스트 token의 접두어+UUID가 실제 36자 컬럼 길이를 넘어 실패했다.
  테스트 token을 36자 UUID로 맞췄다. 운영 코드의 제약이나 검증 기대값을 완화하지 않았다.
- 늦게 도착한 ALLOWED 거절 단위 사례와 종료 직후 DB/offset 검증을 보강했다.
- `./gradlew --no-daemon --console=plain test --tests '*WorkerQuotaTest' --tests '*WorkerQuotaIntegrationTest'`:
  **13개 통과 / 실패·오류·스킵 0**, BUILD SUCCESSFUL(37초). 단위 7개 + 통합 6개다.
- 실제 MySQL/Kafka/Redis/HTTP, 자동 Relay, HTTP 500: 최초+재시도 허가/HTTP/이력/offset 각각 3, DLQ 1.
  재시도 quota 대기 중 WAITING_RETRY/시도 1을 보존하고 다른 DB 연결에서 task UPDATE를 500ms 이내 완료했다.
- 같은 group/Redis 키를 쓰는 앱 context 2개, r=1/B=1: 둘 다 quota 대기할 때 시도/HTTP/offset 0, worker_token 없음.
  이후 각 앱 허가 1/합산 HTTP·시도·offset 2로 완료했다. 대기 중 task UPDATE로 잠금 해제를 확인했다.
- quota wait 100ms: 같은 파티션의 2건 모두 시도/offset 0을 유지한 채 소비 중지.
  테스트 예산 복원/명시적 재시작 후 각각 1회 처리했다. 이후 잘못된 Redis 상태에서도 완료 재소비는 추가 허가/HTTP 없이 offset만 3으로 처리했다.
- Redis CLIENT PAUSE 1500ms/클라이언트 timeout 100ms: UNAVAILABLE로 미선점/미커밋 중지, 해제 후 재시작하여 시도/HTTP/offset 1.
- quota 대기 중 context 종료: 종료 후 DB QUEUED/시도 0/offset 0/HTTP 0 확인. 새 context에서 시도/이력/HTTP/offset 1로 복구했다.
- 만료 시도 probe: 기존 STARTED/시도/토큰 무변경. 상한 소진 작업은 허가/HTTP 0으로 UNKNOWN/최종 실패/DLQ를 저장한 후 offset 1.
- 각 사례는 격리 MySQL 컨테이너의 별도 schema를 쓴다. 자동 Relay 사례 외에는 파티션/중단 경계를 통제해 직접 Kafka 발행했고,
  원래 Outbox는 PENDING으로 유지했다. 테스트 Redis 미래 시각/예산 복원은 관찰·복구 준비용이며 운영 초기화 기능을 추가하지 않았다.
- 첫 전체 `build --rerun-tasks`: 168개 중 167개 통과, 기존 OutboxProcessCrashTest 1개 실패(9분 15초).
  해당 사례에서 KafkaAdmin listNodes timeout과 bootstrap broker 연결 끊김이 이어져 ACK 신호 파일이 생성되지 않았다.
  새 quota 13개를 포함한 다른 테스트는 통과했다. 이 결과를 전체 통과로 간주하지 않는다.
- 실패 사례만 `test --tests '*OutboxProcessCrashTest'`로 새 컨테이너에서 재실행: **1개 통과**, BUILD SUCCESSFUL(47초).
  코드/기대값/timeout 변경 없이 재현되지 않았다. 원래 브로커 연결 실패의 근본 원인은 확정하지 않았다.
- 최종 `caffeinate -i ./gradlew --no-daemon --console=plain build --rerun-tasks`:
  **24개 클래스 / 168개 테스트 통과 / 실패·오류·스킵 0**, BUILD SUCCESSFUL(Gradle 출력 24분 7초), 종료 코드 0.
  Outbox 강제 종료 사례와 새 quota 13개를 포함해 전체 XML 결과를 확인했다.
- 실행 JAR에 WorkerQuota/Disposition, Redis Lua/Jedis가 포함되고 테스트/Mock 서버 클래스는 제외됨을 확인했다.
  WorkerQuota의 JAR 바이트와 컴파일 산출물이 일치했다. 문서 상대 링크/코드 블록과 `git diff --check`를 확인했다.
- 테스트 후 컨테이너와 Java/caffeinate 자식 프로세스가 남지 않았다. 기존 로컬 MySQL 컨테이너는 healthy다.
- Git 작성자 JinHyeon / tiemo0708@naver.com, GitHub tiemo0708 인증과 원격 기본 main/HEAD c95a444 확인. 원격 푸시는 하지 않았다.

### 한계와 다음 단계

- 다음은 소비 pause/resume와 poll/heartbeat 유지 검증이다. 현재 quota 대기는 소비 스레드와 Worker 처리 자리를 사용한다.
  FIFO 대기/Redis 명령/HTTP/DB 지연까지 합산하므로 모든 장애에서 poll 간격을 보장하지 않는다.
- 짧은 Redis 응답 중단과 수동 재시작은 검증했다. 장기 장애, 상태 유실/예산 복원, Redis 재시작/AOF/failover는 별도 항목으로 남긴다.
- 앱 2개는 하나의 테스트 JVM 안의 독립 context/소비자/연결 풀이다. 별도 호스트/JVM 조합과 처리량 벤치마크는 이번 범위가 아니다.
- 앱 간 공정성/우선순위, Circuit Breaker, 429 Retry-After는 구현하지 않았다.
  허가 후 실제 HTTP까지의 지연과 불확실한 공급자 수락/중복 가능성도 그대로다.
- 1만 건 실험·실행 JAR 별도 기동 smoke·원격 CI는 이번에 실행하지 않았다.
- 기존 로컬 DB/.env/IDE 설정은 변경하지 않았다. 사용자 검토 전 원격에 푸시하지 않는다.


## 2026-10-17: S6 여섯 번째 단계 — quota pause/resume와 poll 유지

### 문제·대안·선택

- 앞 단계의 유한 동기 대기는 quota를 기다리는 동안 소비 스레드/Worker 처리 자리를 썼다.
  정상 한도 부족이 오래가면 대기 초과로 중지되며 대기 예산을 단순히 늘리면 max.poll.interval 위험이 생긴다.
- 별도 executor/비동기 offset 추적 대신 Spring Kafka의 `handleOne=false` 보관 레코드와 컨테이너 pause를 선택했다.
  실제 의존성 3.3.16 소스와 공식 문서에서 보관·pause poll·리밸런스 pending 정리 계약을 확인했다.
- `pause-enabled=true`(기본): DB 선점 전 Redis 한 번 확인 → LIMITED 안전 예외 → 처리 자리 반환 → 부모 컨테이너 pause.
  100~60000ms 이후 resume만 요청하며 같은 레코드/DB 상태/허가를 다시 검사한다. 최초/Retry 공통이다.
  `client-enabled=false` 기본은 유지한다. 이전 유한 대기는 `pause-enabled=false` 비교 모드로 보존한다.
- 오류 처리기의 seek/ack-after-handle을 false로 하고 미완료 레코드를 recover/skip하지 않는다.
  UNAVAILABLE/DB/이벤트/진입 제어 오류는 기존 중지 정책을 유지하며 종료 중 별도 stop 경합을 피한다.
- 앱 부모 컨테이너당 예약 하나, 내부 예약 맵 최대 3개, 재개 전용 스레드 1개다.
  기본 poll 1건/소비 스레드 최대 3개에서 보관 레코드도 앱당 최대 3개다. Kafka fetch buffer 전체 상한은 아니다.
- 중지 이벤트에서 예약 취소/큐 제거/pause 요청 해제, context 종료에서 예약 취소/스케줄러 종료를 구현했다.
  예약 객체를 비교해 오래된 취소 콜백이 새로운 pause를 해제하지 못하게 한다.
- poll timeout 500ms, pause poll timeout 100ms. 기본 max.poll.interval 60000ms는 유지한다.
  pause/resume 요청 수와 남은 재개 예약 수를 고정 이름의 메트릭으로 추가했다.
- [문제·대안·선택·처리 경계·설정·실험](S6_CONSUMER_PAUSE.md), README/PLAN과 이전 단계 문서를 갱신했다.

### 실제 검증

- 첫 집중 실행 21개 중 20개 통과, 기존 동기 비교 모드 다중 앱 사례 1개 실패(1분 28초).
  최종 PROVIDER_ACCEPTED 기대 시 QUEUED였고 처리 오류로 소비가 중지됐다. 원래 오류는 payload 보호를 위해 감춰져 있어
  원인을 단정하지 않았다. 상태 진단에 quota 메트릭을 추가했다. 구현/완료 조건/timeout을 완화하지 않았다.
- 같은 사례 단독 재실행: **1개 통과**, BUILD SUCCESSFUL(30초).
- pause/heartbeat 테스트에서 이름이 일치하는 Kafka 메트릭이 실제 존재하는지 검증을 추가했다.
  리밸런스 테스트는 두 번째 앱 할당 후 양쪽 pause/offset 0을 확인하도록 관찰 구간을 보강했다.
- `test --tests '*WorkerQuotaTest' --tests '*WorkerQuotaIntegrationTest'`: **21개 통과 / 실패·오류·스킵 0**, BUILD SUCCESSFUL(1분 28초). 단위 9개 + 통합 12개다.
- max.poll.interval=5000ms 상태에서 7초 이상 pause 유지, last-poll-seconds-ago<2, heartbeat 증가,
  rebalance 횟수와 consumer member 불변. DB 시도/HTTP/offset/Worker active=0을 유지하고 다른 DB 연결의 UPDATE도 완료했다.
  자동 재개 뒤 같은 파티션 2건의 시도/HTTP/offset=2를 확인했다.
- 실제 Relay/HTTP 500에서 최초+재시도 허가/시도/HTTP/offset=3, DLQ Outbox=1.
- pause 후 Redis 상태 오류는 UNAVAILABLE로 미선점/미커밋 중지 후 명시적 재시작으로 복구했다.
- 컨테이너 stop/start와 context 종료/재생성에서 예약 정리와 1회 처리 복구를 검증했다.
- pause 중 다른 앱 context 합류/재할당에서 미완료 작업이 각 1회 처리되고 허가/HTTP/offset 합계=2임을 확인했다.
- `caffeinate -i ./gradlew --no-daemon --console=plain build --rerun-tasks`:
  **24개 클래스 / 176개 테스트 통과 / 실패·오류·스킵 0**, BUILD SUCCESSFUL(12분 22초), 종료 코드 0.
  집중 실행에서 실패했던 다중 앱 비교 모드를 포함한 전체 XML 결과를 확인했다.
- 실행 JAR의 WorkerPauseErrorHandler/QuotaDeferred 바이트가 컴파일 산출물과 일치하며 테스트/Mock 서버 클래스는 제외됐다.
  문서 상대 링크/코드 블록과 `git diff --check`를 확인했다. JAR 별도 기동 smoke는 이번에 실행하지 않았다.
- 테스트 컨테이너와 Java/caffeinate 프로세스가 정리됐고, 기존 로컬 MySQL 컨테이너는 healthy다.
- Git 작성자 JinHyeon / tiemo0708@naver.com, GitHub tiemo0708 인증과 원격 main/HEAD c95a444 확인. 푸시하지 않았다.

### 한계와 다음 단계

- 검증 범위는 quota pause 동안의 poll 유지다. WorkerAdmission/DB/HTTP의 긴 동기 처리까지 poll 간격을 보장하지 않는다.
- 부모 컨테이너 전체 pause라 다른 파티션도 영향을 받는다. 앱 간 공정성/우선순위와 운영자 수동 pause 제어 조합은 구현하지 않았다.
- 정상 quota 부족의 전체 대기 시간은 제한하지 않으며 Redis 오류는 자동 재개하지 않는다.
  Redis 장기 장애/상태 유실·failover 정책은 별도 항목으로 남긴다.
- 리밸런스 사례는 같은 테스트 JVM의 독립 앱 context다. 별도 호스트/JVM·운영 처리량·1만 건 정합성 실험·원격 CI는 이번 범위가 아니다.
- DB migration, 사용자 로컬 DB/.env/IDE 설정을 변경하지 않았다. 다음은 Circuit Breaker와 half-open 복구다.

## 2026-10-17: S6 일곱 번째 단계 — 공급자 회로 차단과 half-open 복구

### 문제와 구현

- quota는 속도를 제한하지만 공급자 장애를 판단하지 않는다. 반복 5xx 중 새 알림까지 발송 시도를 소모하는 문제에 대응했다.
- 연속 관측 실패 기준의 프로세스별 Circuit Breaker를 선택했다. CLOSED/OPEN/HALF_OPEN과 세대별 permit을 직접 구현했다.
  라이브러리의 실패율/슬라이딩 윈도우나 전역 회로까지 구현한 것은 아니다.
- 기본은 꺼짐, 비동기 HTTP 모드에서 선택한다. 기본 임계값 3(1~20), cooldown 5000ms(100~60000).
  monotonic clock을 사용하고 HALF_OPEN에서는 앱당 한 permit만 허용한다.
- Worker는 DB 확인 뒤 회로 → quota → DB 재검증/선점 → HTTP → 회로 결과 → DB 완료 → offset 순서로 처리한다.
  OPEN 보류는 시도/lease/Redis 허가를 소모하지 않는다. 완료 재소비는 두 gate를 우회한다.
- 5xx와 통신/알 수 없는 응답은 실패로 집계한다. 429/개별 요청 오류/로컬 자원 오류/내부 앱 오류는 제외한다.
  늦은 이전 세대 결과는 현재 회로 상태를 바꾸지 않는다.
- HTTP 전 quota/DB 오류나 경합으로 permit을 쓰지 못하면 자동 반환한다. HALF_OPEN 탐침이 영구 점유되는 것을 막는다.
- 기존 부모 컨테이너 pause/poll/resume에 회로 보류를 연결했다. 예약 상한/종료 취소를 유지하고 회로별 메트릭을 추가했다.
- [문제·대안·선택·트랜잭션·설정·검증·한계](S6_CIRCUIT_BREAKER.md), README/PLAN과 이전 pause 문서를 갱신했다.

### 실제 검증

- `caffeinate -i ./gradlew --no-daemon --console=plain test --tests '*ProviderCircuitBreakerTest' --tests '*WorkerCircuitBreakerIntegrationTest'`:
  **2개 클래스 / 25개 테스트 통과 / 실패·오류·스킵 0**, BUILD SUCCESSFUL(40초), 종료 코드 0.
- 단위 21개: 실패 분류·성공 초기화, cooldown 경계, 단일 탐침, 탐침 실패 재차단,
  오래된 성공/실패 무시, 취소·제외 결과의 탐침 반환, 중복 complete/close, 비활성화와 잘못된 설정 거절.
- 통합 4개: 실제 MySQL 8.4.8/Kafka 4.0.2/Redis 7.4.11와 loopback HTTP 서버. 사례별 schema/토픽/group/키 격리.
  실제 Relay의 500 두 번 뒤 pause에서 시도/HTTP/허가/offset=2, Worker active=0, DB UPDATE 500ms 이내 완료.
  202로 바꾼 후 자동 탐침으로 이력=실패/실패/수락, HTTP/허가/offset=3을 확인했다.
- 소비 스레드 3개에서 HALF_OPEN HTTP 1개와 나머지 QUEUED/시도 0을 관찰했다.
  탐침 500에서 다시 OPEN, 다음 202에서 CLOSED로 복구했다.
- OPEN+잘못된 Redis 상태에서도 완료 재소비는 HTTP 없이 offset을 진행했다.
  새 작업의 HALF_OPEN permit 뒤 Redis 오류는 미선점/미커밋 중지하고 탐침 자리를 반환했다. 복원/명시적 재시작 후 수락했다.
- OPEN pause 중 context 종료에서 예약 0/기존 offset 보존, 새 context에서 미처리 작업 1회 수락을 확인했다.
- 집중 테스트 뒤 영구 실패/내부 실패의 실제 상태 enum 분류와 Redis quota 비활성+회로 활성 조합을 보강해 전체 빌드에 포함했다.
- `caffeinate -i ./gradlew --no-daemon --console=plain build --rerun-tasks`:
  **26개 클래스 / 201개 테스트 통과 / 실패·오류·스킵 0**, BUILD SUCCESSFUL(11분 36초), 종료 코드 0.
  최종 XML에서 보강한 회로 단위 21개/통합 4개와 기존 회귀를 모두 확인했다. 이번 집중/전체 실행에서 실패는 없었다.
- 실행 JAR의 회로/permit/보류 예외와 Worker/오류 처리기 바이트가 컴파일 산출물과 일치하며 테스트/Mock 서버 클래스는 제외됐다.
  문서 상대 링크/코드 블록과 `git diff --check`를 확인했다. JAR 별도 기동 smoke는 이번에 실행하지 않았다.
- MySQL/Kafka/Redis 테스트 컨테이너와 Java/caffeinate 프로세스 종료를 확인했다. 기존 로컬 MySQL은 healthy다.
- Git 작성자 JinHyeon / tiemo0708@naver.com, GitHub tiemo0708 인증과 원격 main/HEAD c95a444 확인. 푸시하지 않았다.

### 한계와 다음 단계

- 프로세스별 회로라 재시작하면 CLOSED이며 앱마다 별도 탐침을 허용한다. 전역 단일 탐침/영속 회로가 아니다.
- 기존 세대에서 이미 허가한 작업은 OPEN 뒤 HTTP를 시작하거나 끝낼 수 있다. 늦은 결과의 상태 변경만 막는다.
- HALF_OPEN에서 DB가 오래 멈추면 탐침 자리도 묶일 수 있다. 기존 호출과 새 탐침의 중첩을 피하려고 시간만으로 permit을 반환하지 않는다.
- 부모 전체 pause·전체 대기 시간 무제한, 공급자 수락 후 DB 저장 전 중단의 불확실성과 외부 중복 가능성은 남아 있다.
- 별도 호스트/운영 처리량/새 1만 건 정합성 실험/원격 CI는 이번에 실행하지 않았다.
- DB migration, 사용자 로컬 DB/.env/IDE 설정을 변경하지 않았다. 다음은 Redis 장기 장애/예산 유실·failover 정책이다.
  S6 전체 완료로 표시하지 않으며 우선순위/자원 분리는 시작하지 않았다.

## 2026-10-17: S6 여덟 번째 단계 — Redis 장애·예산 유실의 fail-closed 정책

### 문제와 구현

- 기존 연결 오류 중지는 유지하되, quota 키가 없을 때 초기 버스트가 재생성되는 빈틈을 막았다.
  가용성 우선 fail-open/앱별 fallback 대신, 예산을 확인하지 못하면 새 발송을 시작하지 않는 정책을 선택했다.
- Lua 상태 v2는 키 부재·예상 밖 TTL·구버전/정책 충돌·손상된 상태에서 UNAVAILABLE을 반환한다.
  자동 생성/TTL 제거/상태 덮어쓰기를 하지 않으며 Worker는 미선점·미커밋 상태로 중지한다.
- 운영자 전용 `initialize-provider-quota.lua`: 존재하지 않는 키만 현재 Redis 시각/credit=0으로 원자적 생성한다.
  기존 키는 타입/내용과 무관하게 거절하며 이름/rate/burst 범위를 검증한다. Worker가 이 스크립트를 자동 호출하지 않는다.
- **호환성 변경:** 최초 설치도 초기화가 필요하며 기존 version=1은 새 앱에서 사용할 수 없다.
  모든 발송 주체 중지·기존 허가/HTTP 정리·예산/증거 확인·B/r 대기 후 통제된 v2 초기화 절차를 문서화했다.
  단순 연결 장애에서 신뢰할 수 있는 기존 v2 예산은 유지하고 앱만 명시적으로 재시작한다.
- 정상 형식의 오래된 snapshot은 자동 감지하지 못한다. 실제로 소비한 credit을 되돌리면 다시 허가되는 한계를 재현했다.
  자동 Sentinel/Cluster failover를 지원한다고 주장하지 않으며, 전환 전에 전체 발송 중지와 통제된 복구가 필요하다.
- 기존 quota/회로 테스트는 초기 예산 fixture를 명시적으로 준비해 이전 관찰 조건을 유지했다.
  학습 문서 [문제·대안·정책·최초 초기화·v2 전환·장애 복구·검증 한계](S6_REDIS_RECOVERY.md), README/PLAN과 관련 S6 문서를 갱신했다.

### 실제 검증

- `caffeinate -i ./gradlew --no-daemon --console=plain test --tests '*RedisRateLimiterTest' --tests '*RateLimitConfigurationTest' --tests '*WorkerQuotaIntegrationTest' --tests '*WorkerCircuitBreakerIntegrationTest'`:
  **4개 클래스 / 38개 테스트 통과 / 실패·오류·스킵 0**, BUILD SUCCESSFUL(2분 14초), 종료 코드 0.
- 실제 Redis에서 부재/삭제 후 자동 생성 없음, 명시적 초기화 credit=0/TTL 없음, r=1의 실제 시간 1초 후 첫 허가를 확인했다.
  기존/잘못된 키 무변경, 초기화 동시 호출 4개 중 1개 생성, 잘못된 설정 거절, TTL 상태 무변경 거절을 검증했다.
- Docker Redis 안에서 문서와 같은 `redis-cli -e --eval /dev/stdin` 실행을 검증했다.
  최초 성공 응답과 기존 키 재호출의 0이 아닌 종료 코드를 확인했다.
- 실제 Redis SIGKILL 후 API 2건 접수/Relay 발행을 유지했다. 6초 관찰 중 새 시도/HTTP=0, 기존 offset=1 유지,
  Worker active/재개 예약=0, UNAVAILABLE 횟수 불변으로 자동 재시도/로컬 fallback 부재를 확인했다. 다른 DB UPDATE도 500ms 이내 완료했다.
- 동일 Redis 컨테이너 start 후 AOF의 예산 필드가 저장값과 같음을 확인했다. Redis 복구만으로 소비는 시작하지 않았다.
  명시적 Worker 재시작 뒤 초기 1건+장애 중 2건의 각 이력은 수락 1회, HTTP/offset 합계=3이었다.
- quota 키 삭제 후 앱 context를 재생성해도 QUEUED/시도·HTTP·offset=0, 키 부재를 유지했다.
  명시적 credit=0 초기화와 Worker 재시작 후 2건이 각각 1회 수락되고 HTTP/offset=2가 됐다.
- 집중 검증 후 장기 중지 관찰에 worker_token 부재 직접 검증도 보강해 전체 빌드에 포함했다.
- `caffeinate -i ./gradlew --no-daemon --console=plain build --rerun-tasks`:
  **26개 클래스 / 210개 테스트 통과 / 실패·오류·스킵 0**, BUILD SUCCESSFUL(10분 29초), 종료 코드 0.
  최종 XML에서 새 9개 사례와 기존 회귀를 확인했다. 이번 집중/전체 실행에서 실패는 없었다.
- 실행 JAR의 제한/초기화 Lua 바이트가 소스와 일치하며 테스트/Mock 서버 클래스는 제외됐다.
  문서 상대 링크/코드 블록과 `git diff --check`를 확인했다. JAR 별도 기동 smoke는 이번에 실행하지 않았다.
- 테스트 컨테이너/Java/caffeinate 프로세스 종료를 확인했고 기존 로컬 MySQL은 healthy다.
- Git 작성자 JinHyeon / tiemo0708@naver.com, GitHub tiemo0708 인증과 원격 main/HEAD c95a444 확인. 푸시하지 않았다.

### 한계와 다음 단계

- Redis 실패를 관측하기 전에 이미 발급한 허가와 진행 중 HTTP는 취소하지 않는다. 응답 유실 시 토큰 환불도 하지 않는다.
- 정상 형식의 snapshot rollback은 자동 차단하지 못한다. Sentinel/Cluster 자동 전환·실제 replica 승격·호스트/디스크 손실은 미검증이다.
  통합 테스트는 appendfsync=always의 동일 컨테이너 SIGKILL/start이며 Compose의 everysec에 대한 무손실 보장이 아니다.
- 6초 관찰은 중지·무재시도 계약 검증이다. 수시간 장애의 DB/Kafka 용량이나 운영 복구 시간을 측정하지 않았다.
  API 접수 유지에 따른 적체 증가와 알림 만료/보관 정책은 남아 있다.
- 별도 1만 건 정합성 실험/운영 처리량/원격 CI는 이번에 실행하지 않았다.
- DB migration, 사용자 로컬 DB/Redis 예산/.env/IDE 설정은 변경하지 않았다. 기존 v1 예산은 자동 전환하지 않았다.
- 다음은 실시간/캠페인 자원 분리와 공급자 한도 내 우선순위 배분이다. S6 전체 완료로 표시하지 않는다.

## 2026-10-18: S6 아홉 번째 단계 — 실시간·캠페인 자원과 예약 예산 분리

### 문제와 구현

- 캠페인이 공통 Worker/HTTP 자리와 quota를 점유하면 실시간 알림도 기다리는 문제에 대응했다.
  같은 JAR의 역할별 프로세스와 고정 예약을 선택했다. 단일 메모리 우선순위 큐/토픽만 분리/엄격 우선순위와의 비교는
  [학습 문서](S6_TRAFFIC_ISOLATION.md)에 정리했다. 캠페인 생성·대상자 확장·Batch는 S7에 남긴다.
- 기본 REALTIME의 기존 토픽/group과 v2 예산은 유지한다. 선택적 분리 모드의 CAMPAIGN은 `.campaign` 토픽/group을 사용한다.
  두 역할 모두 shared Redis/pause/비동기 실행을 요구하며 역할 간 토픽 이름 충돌과 잘못된 배분을 기동 시 거절한다.
- 접수한 프로세스의 역할을 notification/Outbox에 함께 저장한다. V7 migration은 기존 행을 REALTIME으로 보존하고
  복합 FK로 역할 불일치를 막는다. Relay는 역할/status/due 인덱스로 자기 역할만 선점한다.
- Worker는 DB 선점/회로/quota 전에 DB 역할을 검증한다. 잘못된 토픽에 들어온 메시지도 승격/자동 skip하지 않는다.
  최초/Retry/DLQ/선택 redrive의 역할을 유지하며 다른 역할의 Idempotency-Key 재사용과 redrive는 409다.
- 분리 모드는 하나의 Redis Hash/v3 Lua에서 두 credit을 원자적으로 보충하고 해당 몫만 소비한다.
  기본 전체 r=10/B=10에서 실시간 8/8, 캠페인 2/2. 여러 앱도 같은 몫을 공유하며 유휴 몫은 빌리지 않는다.
- v3 운영자 초기화는 없는 키에 두 credit=0만 생성한다. v2/v3/배분 충돌, 부재/TTL/손상은 fail-closed다.
  기존 버전의 snapshot rollback·이미 받은 허가·HTTP 결과 불확실성은 그대로 남는다.
- 각 역할의 Worker/HTTP/DB/Redis 풀과 pause/회로는 별도 앱 자원이며 MySQL/Kafka/Redis 서버·호스트는 공유한다.
  backlog/lag 지표도 역할별로 조회한다. 개별 앱 수 증가에 따른 자원 총량 증가는 운영자가 고려해야 한다.
- README/PLAN 및 기존 S6 문서의 후속 상태를 갱신하고, 명시적 v2↔v3 정책 전환과 구버전 혼합 금지를 문서화했다.

### 실제 검증

- 첫 compileTestJava는 기존 테스트 전용 Relay/종료 실험의 상속 생성자 2곳이 새 TrafficProperties 인자를 전달하지 않아 실패했다.
  두 fixture가 기존 REALTIME 역할을 명시하도록 수정했다.
- 최초 Redis/config 집중 실행: 2개 클래스/26개 테스트 통과, BUILD SUCCESSFUL(17초), 실패·오류·스킵 0.
- 최초 새 통합 실행: 5개 중 2개 통과/3개 실패. 테스트 HTTP 응답의 providerMessageId가 어댑터 계약에 맞지 않아
  성공 응답이 WAITING_RETRY로 분류됐다. 테스트 응답을 기존 `http-mock-UUID` 형식으로 수정했으며 공급자 검증은 완화하지 않았다.
- `caffeinate -i ./gradlew --no-daemon --console=plain test --tests '*TrafficIsolationIntegrationTest' --tests '*RedisRateLimiterTest' --tests '*RateLimitConfigurationTest'`:
  **3개 클래스/32개 테스트 통과, 실패·오류·스킵 0**, BUILD SUCCESSFUL(36초), 종료 코드 0.
- 실제 Redis의 독립 클라이언트 4개/동시 48회 호출에서 실시간 허가 8/캠페인 허가 2를 대조했다.
  실제 시간 보충의 역할별 상한과 차용 없음, v2/v3 상호 거절/초기화/TTL/잘못된 상태 무변경 거절을 확인했다.
  split 초기화도 문서의 redis-cli 표준입력 방식으로 성공과 기존 키 거절의 nonzero exit를 확인했다.
- 실제 MySQL 8.4.8/Kafka 4.0.2/Redis 7.4.11와 공통 loopback HTTP 공급자, 역할별 독립 앱 context 2개로 검증했다.
  캠페인 HTTP/Worker 슬롯 1개가 사용 중일 때 실시간 완료/offset=1, 캠페인 offset=0을 확인한 후
  응답을 해제해 캠페인 두 작업이 각 1회 시도로 완료되는 것을 대조했다.
- 캠페인 quota pause의 미선점/worker_token 없음/offset=0과 실시간 진행을 확인했다.
  집중 검증 후 캠페인 Relay의 미발행 1건을 실시간 Relay가 가져가지 않는 조건과 역할별 미발행 지표를 보강했다.
  이 보강 검증 결과는 아래 전체 빌드에 포함한다.
- 캠페인 500 두 번 뒤 Retry/DLQ, 같은 역할 redrive 성공, 최초·재시도·redrive 허가 합계3/실시간 허가0을 확인했다.
  다른 역할 Idempotency-Key/redrive 409, 잘못된 토픽의 미선점·미커밋 중지, FK와 실제 V6→V7 기존 행 보존을 검증했다.
- `caffeinate -i ./gradlew --no-daemon --console=plain build --rerun-tasks`:
  **27개 클래스/222개 테스트 실행, 220개 통과/2개 실패/오류·스킵 0**, BUILD FAILED(10분 41초), 종료 코드 1.
  실패 두 개는 기존 MySQL 테스트의 migration 개수 6 및 V7 이전 전체 행 비교 기대값이었다.
  적용 개수 7과 새 REALTIME 컬럼만 기대값에 추가하고 기존 모든 데이터/lease/시도 이력의 보존 비교를 유지했다.
- 위 전체 실행에서 보강한 캠페인 Relay 미발행/역할별 지표와 context 재시작 테스트까지 통과했다.
  캠페인 pause/종료 중 실시간 2건 완료, 미선점 캠페인 대기/미발행 2건은 재시작 후 각 1회 수락했다.
  기존 Kafka 중단/복구, JVM 종료/offset 복구, graceful shutdown, 회로, Redis 장애와 리밸런스 회귀도 통과했다.
- `caffeinate -i ./gradlew --no-daemon --console=plain test --tests '*MySqlNotificationTest' build`:
  **1개 클래스/16개 테스트 통과, 실패·오류·스킵 0**, BUILD SUCCESSFUL(16초), 종료 코드 0.
  전체 실행 이후 기능 코드는 변경하지 않았다. 실패한 두 사례를 포함한 MySQL 테스트군만 재실행했다.
  두 실행의 XML을 클래스/테스트 이름으로 대조했을 때 고유 222개 모두의 최신 결과는 통과다.
  **단일 전체 실행 222개 무실패로 보고하지 않는다.** 마지막 HTML 보고서는 MySQL 16개 재실행 결과다.
- 실행 JAR의 v3 초기화/제한 Lua, V7 migration, 역할/Worker/Relay/제한기 class 바이트가 빌드 산출물과 일치했다.
  테스트 및 별도 HTTP Mock 서버 클래스는 알림 JAR에서 제외됐으며 문서 링크/코드 블록/`git diff --check`도 확인했다.
- Git 작성자 JinHyeon / tiemo0708@naver.com, GitHub tiemo0708 인증과 원격 main/HEAD c95a444 확인. 푸시하지 않았다.

### 한계와 다음 단계

- 고정 몫은 양쪽의 quota를 예약하지만 실제 처리량/지연 SLO는 보장하지 않는다. 동적 차용/역할 내부의 전역 공정 순서는 없다.
- 독립 context 통합 검증이며 별도 OS 프로세스/호스트의 CPU·장애 격리를 측정하지 않았다.
  공유 DB/브로커/Redis/공급자의 장애와 전체 자원 포화는 양쪽에 영향을 줄 수 있다.
- 새 1만 건 정합성/대량 캠페인 부하/운영 처리량/원격 CI/별도 실행 JAR 기동 smoke는 이번에 실행하지 않았다.
- 새 migration 파일을 추가했지만 사용자 로컬 DB/Redis 예산/.env/IDE 설정에 직접 적용하지 않았다. 실제 테스트는 disposable schema/container만 사용했다.
- 테스트 컨테이너와 Gradle/테스트 Java/caffeinate 프로세스 종료를 확인했다. 기존 로컬 MySQL은 healthy다.
- S6를 정의한 로컬 실험 범위에서 완료 표시했다. 다음은 S7 캠페인 생성/시작/조회와 중복 시작 방지다.

## 2026-10-18: S7 첫 단계 — 캠페인 생성·조회와 영속 시작 요청

### 문제와 구현

- 중복 클릭·응답 유실·서로 다른 앱의 동시 시작에서 캠페인 정의와 시작 의도를 한 번 확정한다.
  [학습 문서](S7_CAMPAIGN_API.md)에 문제/대안/DB 유니크·조건부 갱신의 선택 이유와 실패 경계를 정리했다.
- 비동기 CAMPAIGN 역할에만 POST /campaigns, GET /campaigns/{id}, POST /campaigns/{id}/start를 추가했다.
  생성은 필수 Idempotency-Key와 정확한 정의 비교로 201/200/409를 구분한다.
  이름·제목·본문 길이와 targetCount 1~100만 JSON 정수를 검증한다. 소수/문자열의 암묵 변환도 거절한다.
- V8은 campaign 테이블만 추가한다. 키는 binary unique, audience는 SYNTHETIC_V1로 고정한다.
  DB CHECK가 대상 수/상태/시각 조합을 제한한다. 이전 migration이나 기존 알림·시도 이력은 변경하지 않았다.
- 시작은 별도 DB 트랜잭션에서 DRAFT를 START_REQUESTED로 바꾸고 최신 잠금 조회로 응답한다.
  최초 202, 중복 200이며 최초 시각을 유지한다. 생성 충돌도 실패 트랜잭션 롤백 후 기존 정의를 읽는다.
  호출자의 오래된 REPEATABLE READ 스냅샷과 롤백으로 접수 결과가 달라지지 않게 경계를 분리했다.
- START_REQUESTED는 시작 의도만 의미한다. 실제 Batch Job·대상자 확장·발송은 실행하지 않는다.
  README/PLAN을 이 범위에 맞게 갱신하고 S7 첫 체크박스만 완료했다.

### 실제 검증

- 집중 실행 1차: CampaignApiTest 9개 + MySqlNotificationTest 16개 중 **23개 통과/2개 실패**,
  BUILD FAILED(26초). 한 실패는 테스트용 트리거를 앱 계정으로 만들다가 MySQL SUPER 권한 부족으로 발생했다.
  다른 실패는 CHECK 위반이 DataIntegrityViolationException으로 번역된다는 테스트의 가정이었다.
  격리 컨테이너의 트리거 DDL에만 기존 테스트와 같은 root 연결을 사용하고,
  CHECK 검증은 DataAccessException의 실제 MySQL 오류 코드 3819를 대조하도록 고쳤다.
  앱의 권한이나 DB 제약은 완화하지 않았다.
- 잘못된 targetCount의 암묵 숫자 변환을 방지하도록 전용 필드 역직렬화와 HTTP 오류 검증을 추가했다.
- `caffeinate -i ./gradlew --no-daemon --console=plain test --tests '*CampaignApiTest' --tests '*MySqlNotificationTest'`:
  **2개 클래스/25개 테스트 통과, 실패·오류·스킵 0**, BUILD SUCCESSFUL(30초), 종료 코드 0.
- 실제 MySQL 8.4.8/Kafka 4.0.2와 CAMPAIGN 2개/REALTIME 1개 독립 앱 context를 사용했다.
  두 앱에 동시 시작 12건을 보내 202 1건/200 11건, 같은 결과/최초 시각/저장 행 1개를 대조했다.
  동시 동일 생성은 201 1건/200 11건, 상충 정의 생성은 201 1건/409 11건이며 각각 행 1개다.
- 앱 context 재시작 뒤 조회·중복 시작·생성 재요청이 기존 시작 상태를 유지했다.
  이전 caller 트랜잭션 스냅샷이 있어도 커밋된 결과를 관측하며 caller 롤백 후에도 시작 의도는 유지됐다.
- 테스트용 AFTER UPDATE 트리거로 시작 변경 중 실패를 주입해 503과 DRAFT/시각 null 롤백을 확인했다.
  트리거 제거 뒤 같은 ID로 202 수락·200 재요청을 확인했다. SQL/테스트 본문은 오류 응답에 노출되지 않았다.
- 잘못된 입력·키·중복 헤더·UUID·없는 캠페인·DB CHECK와 REALTIME 엔드포인트 404를 확인했다.
  생성·시작 후 notification/delivery_task/outbox_event/delivery_attempt 행, 캠페인 Kafka 토픽 end offset,
  앱의 Redis 허가 카운터는 모두 0이었다. 이번 캠페인 API 테스트는 Redis 컨테이너를 사용하지 않는다.
- `caffeinate -i ./gradlew --no-daemon --console=plain build --rerun-tasks`:
  **28개 클래스/231개 테스트 통과, 실패·오류·스킵 0**, BUILD SUCCESSFUL(11분 13초), 종료 코드 0.
  XML 합계를 대조했다. 기존 Kafka 중단/복구, 프로세스 종료/offset 복구, graceful shutdown,
  재시도/DLQ, Redis 장애·예산, 회로, 역할 분리와 리밸런스 회귀를 포함한 단일 전체 실행 결과다.
- 실행 JAR 안의 캠페인 클래스와 V8 migration 바이트가 빌드 파일과 일치했다.
  테스트/별도 HTTP Mock 서버 클래스는 알림 JAR에서 제외됐다. 문서 링크/코드 블록/`git diff --check`를 확인했다.
- Git 작성자 JinHyeon / tiemo0708@naver.com, GitHub tiemo0708 인증과 원격 main/HEAD c95a444를 확인했다.
  기존 main 이력 위에 로컬 커밋하며 푸시하지 않는다.
- 테스트 컨테이너와 Java/caffeinate 프로세스 종료를 확인했다. 기존 로컬 MySQL은 healthy다.

### 한계와 다음 단계

- START_REQUESTED는 현재 실행기를 기다리는 영속 의도다. Batch 실행·자동 재개·진행률·취소·대상자별 작업은 아직 없다.
- SYNTHETIC_V1은 후속 가상 대상 규칙의 버전 식별자다. 실제 대상자 집합이나 100만 건을 생성/발송한 결과가 아니다.
- 캠페인 테스트의 재시작은 앱 context 종료/재생성이며 별도 캠페인 OS 프로세스 강제 종료 실험은 하지 않았다.
  1만 건 정합성 재실행/대량 부하/운영 처리량/원격 CI/별도 실행 JAR 기동 smoke는 이번에 실행하지 않았다.
- 사용자 로컬 DB, Redis 예산, .env, IntelliJ 설정에는 직접 변경을 적용하지 않았다. V8은 다음 새 앱 기동 시 적용된다.
  검증은 격리된 Testcontainers DB/토픽과 가상 데이터만 사용했다.
- 다음 단계는 고정된 가상 대상자 집합과 keyset pagination이다. 그 뒤 Batch checkpoint와 결정적 작업 ID를 연결한다.

## 2026-10-18: S7 두 번째 단계 — 고정 가상 대상자와 keyset 조회

### 문제와 구현

- 재시작/재조회 시 대상과 순서가 달라지지 않는 가상 집합이 필요하다.
  기존 DB 정의의 SYNTHETIC_V1과 targetCount로 순번 1~N, 수신자 ID synthetic-v1-번호를 고정했다.
  별도 전체 목록이나 DB seed를 만들지 않고 요청한 페이지의 구성원만 계산한다.
- CAMPAIGN 앱에 GET /campaigns/{id}/audience?after=0&limit=100을 추가했다.
  숫자 ordinal을 키로 `after < ordinal <= targetCount`를 최대 1,000개 반환한다.
  다음 키 nextAfter가 null이면 종료이며 after=targetCount는 빈 페이지다.
  페이지 크기는 다음 요청에서 변경할 수 있고 동일 키/크기는 동일 대상을 반환한다.
- 잘못된 커서/크기는 400, 없는 캠페인은 404다. 지원하지 않는 버전/손상된 정의는 503으로 거절한다.
  DRAFT/START_REQUESTED에서 조회할 수 있으며 읽기가 캠페인 상태나 발송 진행 위치를 변경하지 않는다.
  after는 인증/캠페인 식별 토큰이 아니므로 campaignId와 함께 보관해야 한다.
- [학습 문서](S7_CAMPAIGN_AUDIENCE.md)에 전체 메모리 목록/DB 스냅샷/가상 집합의 대안과 선택 이유,
  numeric key와 문자열 정렬의 차이, 이어 읽기·checkpoint 경계, HTTP 예제와 고정 해시를 기록했다.
  PLAN의 두 번째 항목과 README/API 문서를 갱신했다. V8과 기존 migration은 수정하지 않았다.

### 실제 검증

- `caffeinate -i ./gradlew --no-daemon --console=plain test --tests '*CampaignApiTest' --tests '*SyntheticCampaignAudienceTest'`:
  **2개 클래스/18개 테스트 통과, 실패·오류·스킵 0**, BUILD SUCCESSFUL(20초), 종료 코드 0.
- 가상 대상 100,000명은 101페이지, 1,000,000명은 1,002페이지로 997/1,000 크기를 번갈아 순회했다.
  단조 증가 순번/총수와 UTF-8 ordinal:recipientId 줄 단위 SHA-256이 고정 기준값과 일치했다.
  깊은 키 999997부터 재개, 마지막 부분/빈 페이지, 크기 변경, 범위/오버플로/알 수 없는 버전 거절을 검증했다.
  반환 목록의 변경도 거절되어 호출자가 페이지 내용을 수정할 수 없다.
- 실제 MySQL 8.4.8/Kafka 4.0.2와 독립 앱 context로 2,003명 캠페인을 조회했다.
  기본 100개, 최대 1,000개와 마지막 3개/종료 빈 목록을 대조했다.
  다른 캠페인 생성과 시작 상태 전환 뒤 두 CAMPAIGN 앱의 동시 조회 12건이 같은 페이지를 반환했다.
  앱 context 재시작 후 after=1000에서 1001번부터 이어 읽었다.
- HTTP의 잘못된 값/UUID/없는 ID/REALTIME 404를 확인했다. 조회 전 DRAFT는 조회 후에도 DRAFT다.
  발송 관련 DB 행/Kafka end offset/Redis 허가 카운터 0을 대조했다.
- `caffeinate -i ./gradlew --no-daemon --console=plain build`:
  **29개 클래스/240개 테스트 통과, 실패·오류·스킵 0**, BUILD SUCCESSFUL(11분 27초), 종료 코드 0.
  test 태스크는 실행됐고 XML 합계를 대조했다. 기존 Kafka 복구·프로세스 종료·Redis·회로·역할 분리·리밸런스 회귀를 포함한다.
- 실행 JAR의 캠페인 class 바이트 일치와 테스트/별도 Mock 서버 클래스 제외를 확인했다.
  문서 링크/코드 블록과 `git diff --check`를 확인했다.
- Git 작성자 JinHyeon / tiemo0708@naver.com, GitHub tiemo0708 인증과 원격 main/HEAD c95a444를 확인했다.
  기존 main 위에 로컬 커밋하며 푸시하지 않는다. 테스트 컨테이너/Java/caffeinate는 종료됐고 기존 MySQL은 healthy다.

### 한계와 다음 단계

- 가상 집합의 keyset 열거를 검증했다. 실제 회원 테이블의 SQL keyset/인덱스 성능과 변경 중 스냅샷은 별도 범위다.
- 10만/100만 대상 열거는 정합성 검사이며 DB 저장·Kafka 발행·공급자 발송 부하 테스트가 아니다.
  이번에 별도 1만 건 정합성 태스크/원격 CI/실행 JAR 기동 smoke를 실행하지 않았다.
- 페이지 조회는 checkpoint를 저장하지 않으며 START_REQUESTED에서 실제 Batch 실행/발송은 아직 진행되지 않는다.
  재시작 검증은 앱 context 재생성이다. 실제 캠페인 프로세스 강제 종료 실험은 후속 Batch 연결 시 필요하다.
- 사용자 로컬 DB/Redis 예산/.env/IntelliJ는 변경하지 않았다. 신규 migration이나 대상자 전체 seed도 없다.
- 다음은 Spring Batch chunk checkpoint와 작업 인계/ACK, 재시작 시 동일 범위 중복 처리 검증이다.

## 2026-10-19: S7 세 번째 단계 — Batch checkpoint·Outbox 인계와 실패 재시작

### 문제와 구현

- chunk 일부만 저장되거나 checkpoint가 작업보다 앞서 커밋되면 재시작 때 누락/중복이 생길 수 있다.
  [학습 문서](S7_CAMPAIGN_BATCH.md)에 직접 Kafka 발행/단건 접수 API 호출/Outbox 인계의 대안과 선택 이유를 정리했다.
- 기존 Boot BOM의 Spring Batch 5.2.6을 사용한다. 동일 DataSource/transactionManager에 알림·발송 작업·Outbox·대상 매핑과
  ExecutionContext의 마지막 순번을 저장한다. Reader는 고정 집합의 마지막 커밋 순번 뒤를 읽는다.
  chunk 기본 100/상한 1,000, 실행 슬롯 1개, 대기열/skip/자동 retry는 없다.
- campaignBatch 명령은 START_REQUESTED UUID 하나만 받아 동기 실행하고 종료한다. DRAFT/없는 캠페인은 거절한다.
  같은 campaignId와 고정 Job 이름으로 FAILED/STOPPED 재시작, COMPLETED 재요청은 기존 실행을 반환한다.
  시간/run.id를 추가해 새 인스턴스로 중복 실행하지 않는다. 일반 앱 자동 실행은 비활성화했다.
- 명령은 전용 JVM을 UTC로 설정하고 Relay/Worker를 끈다. Batch 인프라 직접 활성화도 CAMPAIGN/UTC를 요구한다.
  다른 앱에서 Relay/Worker가 실행 중이면 앞 chunk의 Outbox부터 처리될 수 있다.
- V9는 공식 BATCH_* 메타데이터 스키마와 campaign_target을 추가한다. 기존 V1~V8/알림 이력은 변경하지 않는다.
  공식 프레임워크 식별자는 보존하고 업무 테이블은 snake_case/PK/unique/FK 규칙을 적용했다.
- 캠페인 ID/순번/kind로 notification/task/event ID를 고정한다. 매핑 PK와 각 ID unique가 중복을 제한한다.
  재생 시 기존 정의/ID를 확인하고 완료된 작업/시도/발행 상태를 초기화하지 않는다.
  DB 충돌은 조용히 무시하지 않고 chunk를 롤백한다. Writer 오류의 SQL/본문은 Batch 로그·실패 메시지에 붙이지 않는다.
- Batch COMPLETED는 DB 인계 완료다. Kafka 발행은 기존 Relay가 맡고 publish future 성공 후 PUBLISHED를 기록한다.
  ACK 결과 유실에 따른 Kafka 중복과 외부 발송 exactly-once 한계를 유지한다.
- README/API/대상자 문서와 PLAN을 갱신했다. checkpoint와 고정 ID 항목을 완료했고,
  강제 종료 후 소유권 확인·안전한 재시작 항목을 대량 부하 전에 추가했다.

### 실제 검증

- dependencyInsight로 Spring Batch 5.2.6 선택을 확인했다. Batch 코드 컴파일 성공.
- `caffeinate -i ./gradlew --no-daemon --console=plain test --tests '*CampaignBatchTest' --tests '*CampaignApiTest' --tests '*MySqlNotificationTest'`:
  **3개 클래스/33개 테스트 통과, 실패·오류·스킵 0**, BUILD SUCCESSFUL(1분 5초), 종료 코드 0.
- 실제 MySQL 8.4.8/Kafka 4.0.2/Redis 7.4.11의 격리 DB/토픽을 사용했다.
  대상 7명/chunk 3에서 5번 매핑 INSERT 실패를 주입해 알림/작업/Outbox/매핑 각 3행과 checkpoint 3을 확인했다.
  새 앱 context에서 같은 JobInstance/새 JobExecution으로 4건을 읽고 총 7건/checkpoint 7로 완료했다.
- checkpoint 테이블 갱신 실패에서도 해당 chunk 업무 행이 함께 롤백됐다. 실패 원인 제거 후 같은 캠페인이 완료됐다.
  두 번째 앱의 동일 캠페인 실행을 진행 중 거절했고 JobExecution 1개/대상 6개를 확인했다.
- 대상 2건을 DB에 인계한 뒤 Kafka ACK future를 보류했을 때 레코드 1개/PROCESSING 1개/PUBLISHED 0개였다.
  브로커 수락 후 ACK 불확실성 주입과 Relay 재시도로 Kafka 3레코드/Outbox PUBLISHED 2건을 확인했다.
  실제 Worker의 공급자 시도는 2회였고 offset 합계 3이었다. 완료 후 Writer 재생에서도 시도/작업/발행 상태가 유지됐다.
- 별도 JVM 명령 실행과 완료 재실행은 종료 코드 0, DRAFT 거절은 1이었다.
  Batch 명령의 Relay/Worker 비활성화로 명령만 실행한 토픽 레코드는 0이었다.
- UTC 메타데이터와 고정 ID 기준값/checkpoint/실행 조건 단위 검증을 보강했다.
  `caffeinate -i ./gradlew --no-daemon --console=plain test --tests '*CampaignBatchTest' --tests '*CampaignCheckpointTest'`:
  **2개 클래스/11개 테스트 통과, 실패·오류·스킵 0**, BUILD SUCCESSFUL(30초), 종료 코드 0.
  command JVM의 저장 CREATE_TIME이 UTC 현재 시각과 일치하는 범위인지 확인했다.
- `caffeinate -i ./gradlew --no-daemon --console=plain build`:
  **31개 클래스/251개 테스트 통과, 실패·오류·스킵 0**, BUILD SUCCESSFUL(11분 31초), 종료 코드 0.
  XML 합계를 대조했다. 일반 캠페인 앱에 JobRepository가 없고 자동 생성 JobInstance가 0개인 것도 확인했다.
  기존 비교 프로필/비동기/리밸런스/종료/Redis/회로/역할 분리 회귀를 포함한다.
- 실행 JAR의 Batch class·V9 바이트 일치, Batch 5.2.6 의존성, 기존 HTTP main 유지,
  테스트/별도 Mock 서버 클래스 제외를 확인했다. 문서 링크/코드 블록/`git diff --check`도 확인했다.
- Git 작성자 JinHyeon / tiemo0708@naver.com, GitHub tiemo0708 인증과 원격 main/HEAD c95a444를 확인했다.
  기존 main 위에 로컬 커밋하며 푸시하지 않는다. 테스트 컨테이너와 Java/caffeinate가 종료됐고 기존 MySQL은 healthy다.

### 한계와 다음 단계

- FAILED 뒤 새 앱 context에서 같은 인스턴스를 재시작하는 경로와 별도 JVM의 정상 명령을 검증했다.
  Batch JVM을 강제 종료해 STARTED/UNKNOWN이 남은 상태의 안전한 복구는 아직 구현/검증하지 않았다.
  진행 중으로 남은 실행을 자동 FAILED로 바꾸거나 새 인스턴스로 우회하지 않는다.
- 다음은 강제 종료 후 실제 소유권 종료 확인/안전한 재시작 절차다. 이후 10만→100만 영속 인계/발송 부하를 진행한다.
- 새 대량 부하/별도 1만 건 정합성/운영 성능/원격 CI/패키징 JAR 기동 smoke는 이번에 실행하지 않았다.
  Batch 명령은 테스트 런타임 classpath의 별도 JVM으로 실행했다.
- 사용자 로컬 DB/Redis 예산/.env/IntelliJ는 변경하지 않았다. V9은 다음 새 앱 실행 시 적용된다.
  모든 검증은 가상 데이터와 격리된 Testcontainers DB/토픽/예산만 사용했다.

## 2026-10-20 — S7 Batch 강제 종료 소유권 복구

### 변경과 선택 이유

- 강제 종료된 STARTED/UNKNOWN 실행을 시간만 보고 FAILED로 바꾸면, 잠시 멈췄던 이전 JVM이 다시 쓸 수 있다.
  [학습 문서](S7_CAMPAIGN_RECOVERY.md)에 문제 상황, 상태 변경/lease fencing/MySQL 세션 방식의 대안과 한계를 정리했다.
- 현재 단일 스레드 동기 Batch에 맞춰 캠페인별 MySQL GET_LOCK과 전용 물리 연결을 사용한다.
  실행 전용 JobRepository/JobExplorer/Writer/TransactionManager가 그 연결을 공유한다.
  연결이 끊긴 이전 JVM은 업무 행뿐 아니라 실행 상태/checkpoint도 새 연결로 쓰지 못한다.
- 전용 연결은 풀로 반환하지 않고 종료해 잠금을 해제한다. autoReconnect/autoReconnectForPools와 다중 호스트 프록시는 거절한다.
  Driver 종류와 재접속 설정을 확인하므로 Connector/J 의존성을 runtimeOnly에서 implementation으로 변경했다. BOM 버전은 유지한다.
- 내부에서 별도 커밋하는 MySQL 시퀀스 incrementer만 기존 풀을 사용한다. 실행 연결을 실수로 커밋하지 않도록 분리했다.
  시퀀스 번호의 빈 간격은 허용하며, 그 연결은 업무 행/실행 기록/checkpoint를 저장하지 않는다.
- `ownershipProtocol=mysql-session-v1`을 비식별 JobParameter로 실행 생성 시 함께 저장한다. campaignId만으로 식별하던 계약을 유지한다.
  잠금을 얻고 이 표시를 확인한 실행만 불확실한 Step/Job을 한 트랜잭션에서 FAILED로 정리한다.
  이전 checkpoint/완료 Step/대상 ID를 유지하고 동일 인스턴스에서 재개한다. 추가 migration은 필요 없다.
- 살아 있는 세션과 소유권 표시가 없는 구버전의 불확실한 실행은 거절한다. 상태 임의 변경/타이머 탈취/자동 세션 KILL은 하지 않는다.
  기존 campaignBatch 명령으로 명시적 재시작한다. README/PLAN과 Batch 문서를 갱신했다.

### 실제 검증

- 실제 MySQL 8.4.8/Kafka 4.0.2/Redis 7.4.11 격리 컨테이너를 사용했다.
- 별도 Batch JVM을 앞 3건 커밋 후 강제 종료했다. STARTED/checkpoint 3이 남았고,
  새 JVM 명령으로 같은 JobInstance에서 4건을 읽어 총 7건/checkpoint 7로 완료했다. 실행 기록은 2개, Kafka 레코드는 0개였다.
- 이전 JVM을 SIGSTOP했을 때 살아 있는 DB 세션 때문에 실행이 거절됐다.
  테스트 관리자 연결에서 그 세션을 종료하고 새 실행을 완료한 뒤, 이전 JVM을 SIGCONT했다.
  이전 JVM은 종료 코드 1, 최종 업무 행 7건과 Job/Step/양쪽 ExecutionContext 전체 조회 결과는 그대로였다.
- UNKNOWN 복구 시 Job 상태 저장을 실패시켜 Step 상태 변경까지 롤백되는 것을 확인했다. 원인 제거 후 3번 checkpoint부터 정상 재개했다.
- 실행 파라미터 저장 실패에서 JobInstance/JobExecution/파라미터/Job context가 부분 커밋되지 않았다. 재요청은 정상 완료했다.
- 서로 다른 두 캠페인은 다른 앱에서 각각 완료했고, 같은 캠페인의 두 번째 실행은 계속 거절했다.
  구버전 STARTING/UNKNOWN과 자동 재접속 연결도 거절했다.
- 기존 11개 Batch 테스트는 BUILD SUCCESSFUL(35초). 최초 새 시나리오 포함 16개는 BUILD SUCCESSFUL(54초).
  전체 행 비교 보강 중 VARBINARY의 byte[] 객체 참조 비교 때문에 테스트 1개가 실패했다. 실제 바이트를 비교하도록 수정했다.
- `./gradlew --no-daemon --console=plain test --tests '*CampaignBatchTest' --tests '*CampaignCheckpointTest'`:
  **2개 클래스/18개 테스트 통과, 실패·오류·스킵 0**, BUILD SUCCESSFUL(45초), 종료 코드 0. XML 합계와 실험 출력을 확인했다.
- `./gradlew --no-daemon --console=plain build`:
  **31개 클래스/258개 테스트 통과, 실패·오류·스킵 0**, BUILD SUCCESSFUL(11분 47초), 종료 코드 0.
  XML 합계와 강제 종료/이전 소유자/ACK 실험 출력을 대조했다. 기존 비동기/리밸런스/정상 종료/Redis/회로/역할 분리 회귀를 포함한다.
- 테스트 컨테이너가 정리됐고 기존 notification-lab-mysql-1(MySQL 8.4.8)은 healthy였다.
  사용자 로컬 DB/.env/Redis 예산/IntelliJ를 변경하지 않았다. 모든 장애 주입은 격리된 테스트 DB/프로세스에만 적용했다.
- 실행 JAR의 변경 클래스 3개가 빌드 출력과 일치하고, 기존 NotificationLabApplication main과 Connector/J 9.7.0이 포함되는지 확인했다.
  테스트/별도 Mock 서버 클래스는 알림 앱 JAR에 포함되지 않았다. 패키징 JAR 기동 smoke를 실행했다는 뜻은 아니다.
- Git 작성자 JinHyeon / tiemo0708@naver.com, GitHub tiemo0708 인증과 원격 main/HEAD c95a444를 읽기 전용으로 확인했다.
  기존 main의 로컬 이력을 유지하며 푸시하지 않는다. 문서 링크/코드 블록/`git diff --check`도 확인했다.

### 한계와 다음 단계

- 단일 MySQL 서버/DB, 같은 소유권 프로토콜, 동기 단일 스레드 Step 범위다. 다중 primary/자동 failover/병렬 Step은 지원하지 않는다.
  DB가 이전 세션을 살아 있다고 판단하면 네트워크 단절이 길어져도 탈취하지 않는다. 가용성보다 중복 실행 방지를 우선한다.
- 구버전의 불확실한 실행에는 자동 복구를 제공하지 않는다. 새 버전과 구버전 실행기를 동시에 운용하지 않는다.
- 전용 연결은 기본 spring.datasource URL/사용자/비밀번호를 사용한다. Hikari 전용 옵션은 적용되지 않는다.
- 강제 정지/재개 검증은 macOS/Linux POSIX 신호를 사용한다. Windows에서 같은 테스트를 직접 수행하는 경로는 검증하지 않았다.
- 다음 단계는 10만→100만 건 영속 인계/발송 부하 실험이다. 이번에는 대량 부하/별도 1만 건 정합성/운영 성능/원격 CI를 실행하지 않았다.
  명령 검증은 테스트 런타임 classpath의 별도 JVM이며 패키징 JAR 기동 smoke와 구분한다.

## 2026-10-20 — S7 캠페인 대량 DB 인계 실험

### 범위와 변경

- 기존 CAMPAIGN 발송 예산은 2건/초다. 100만 건 호출에는 약 5.8일이 필요하므로 이번 단계는
  실제 Batch가 MySQL Outbox까지 인계하는 10만→100만 부하를 분리했다. HTTP Mock 발송 전체 경로는 별도 미완료 항목이다.
- 운영 Writer/트랜잭션/소유권/발송 정책을 변경하지 않고, 별도 `campaignLoadTest` Gradle task와 campaign-load 태그를 추가했다.
  기본 회귀 테스트에는 대량 실험이 포함되지 않는다. 명시적으로 실행할 때마다 새 JVM/컨테이너로 재측정한다.
- chunk 100, Writer 1개, JVM heap 512 MiB, MySQL 2 CPU/2 GiB/buffer pool 512 MiB, Hikari 4개로 고정한다.
  전용 Batch 연결은 별도 1개다. 내구성 옵션은 flush_log_at_trx_commit=1/sync_binlog=1로 실제 값을 확인한다.
  Kafka 4.0.2는 1 CPU/1 GiB, heap 256~512 MiB다. Relay/Worker는 끄고 Redis는 사용하지 않는다.
- 5초 간격으로 마지막 커밋 순번, JVM heap/RSS/CPU/GC/스레드, MySQL cgroup 메모리/CPU를 기록한다.
  기존 PipelineMetrics 수집은 유지했다. 관측 최댓값을 순간 최대나 운영 성능 보장으로 표현하지 않는다.
- 네 업무 테이블 N행과 QUEUED/PENDING=N, 발송 시도 0, read/write/checkpoint=N을 검증한다.
  1,000행 keyset으로 모든 매핑의 연속 순번/결정적 ID/참조를 대조한다. 완료 재요청 후 같은 실행과 행 수를 다시 확인한다.
- 원본 JSON에는 결과/설정/주기별 자원 샘플을 저장한다. 실험 전용 가상 데이터이고 수신자/본문/비밀정보를 결과에 넣지 않는다.
  [학습 문서](S7_CAMPAIGN_LOAD.md)에 측정 경계, 재현 명령, 선택 이유와 한계를 정리한다.

### 실제 검증

- 환경: macOS 15.7.7 arm64, Java Amazon Corretto 21.0.6, Gradle 8.14.5, 호스트 10 logical CPU/16 GiB,
  Docker Desktop 10 CPU/약 7.65 GiB. 기존 로컬 MySQL/IDE와 호스트를 공유한다. 전용 성능 측정 서버가 아니다.
- `./gradlew --no-daemon --console=plain campaignLoadTest -PcampaignTargets=1000`:
  사전 검증 1개 통과, BUILD SUCCESSFUL(24초), 종료 코드 0. 이 실행은 대량 실행 JVM의 warmup이 아니다.
- `caffeinate -i ./gradlew --no-daemon --console=plain campaignLoadTest -PcampaignTargets=100000`:
  **1개 테스트 통과**, BUILD SUCCESSFUL(4분 10초), 종료 코드 0.
  DB 인계 198.029초/초당 504.976건, 전체 매핑 대조 34.994초, 완료 재요청 0.046초.
  checkpoint 100000, commit 1001, rollback 0, 네 업무 테이블 각각 100000행, Kafka 레코드/공급자 시도 0.
  자원 샘플 41개/샘플 오류 0, 관측 최대 heap 63.46 MiB/RSS 262.33 MiB/MySQL cgroup 메모리 712.94 MiB.
  [원본 JSON](experiments/s7-campaign-handoff-2026-10-20-100k.json)을 보존했다.
- 최초 100만 건 실행은 **실패**했다(BUILD FAILED, 48분 9초, 종료 코드 1).
  Batch는 COMPLETED/read·write·checkpoint 1000000, commit 10001/rollback 0으로 종료했고 인계 시간은 2779.705초였다.
  그러나 후속 전체 매핑 JOIN 조회가 JDBC socket timeout으로 실패하여 전체 정합성/완료 재요청 통과로 인정하지 않는다.
  [실패 원본 JSON](experiments/s7-campaign-handoff-2026-10-20-1m-verification-timeout.json)을 별도로 보존했다.
- 검증 SQL을 최대 1,000개 매핑을 먼저 읽는 derived keyset page와 기본 키 조회로 변경했다.
  소규모 재현의 EXPLAIN에서는 JOIN 대상 행 수 추정이 1이고 hash join/full scan이 선택됐다.
  최초 100만 건 실패 시 실행 계획을 수집하지 않았으므로 그 실행의 정확한 옵티마이저 선택은 단정하지 않는다.
  최종 검증 쿼리는 STRAIGHT_JOIN/FORCE INDEX(PRIMARY)/NO_BNL로 기본 키 lookup을 요구하고,
  실제 EXPLAIN의 n/t/o가 모두 eq_ref/PRIMARY인지 검사하여 계획이 달라지면 실패한다. 운영 SQL은 변경하지 않았다.
- 중간 derived-page 검증 smoke 1개도 시간 초과로 실패했다. 다른 smoke를 이전 종료 전에 시작해 JUnit XML 쓰기 충돌까지 발생했다.
  두 실행의 종료와 격리 컨테이너 정리를 확인하고 이후 실행은 순차 수행한다. 이 중간 smoke는 성능 비교에 사용하지 않는다.
- 최종 검증 SQL의 1,000건 smoke: **1개 통과**, BUILD SUCCESSFUL(26초), 종료 코드 0.
  세 테이블의 eq_ref/PRIMARY와 모든 1,000개 매핑/완료 재요청을 확인했다.
- 최종 검증 SQL의 10만 건 재실행: **1개 통과**, BUILD SUCCESSFUL(3분 20초), 종료 코드 0.
  DB 인계 180.395초/초당 554.339건, 전체 정합성 검증 1.654초, 완료 재요청 0.057초.
  checkpoint 100000/commit 1001/rollback 0, 100000개 매핑 전체 대조와 세 JOIN의 eq_ref/PRIMARY를 확인했다.
  [재실행 원본 JSON](experiments/s7-campaign-handoff-2026-10-20-100k-v3.json)을 별도로 보존했다.
  인계 시간 차이는 운영 코드 최적화 결과가 아니며, 실행 간 변동으로 취급한다.
- 최종 검증 SQL의 100만 건 재실행: **1개 통과**, BUILD SUCCESSFUL(44분 1초), 종료 코드 0.
  DB 인계 2545.391초/초당 392.867건, 전체 정합성 검증 67.739초, 완료 재요청 0.076초.
  read/write/checkpoint 1000000, commit 10001/rollback 0, 네 업무 테이블 각각 1000000행과 전체 매핑을 확인했다.
  완료 재요청 후 실행 1개/업무 N행이 유지됐고 Kafka 레코드/공급자 시도는 0이었다.
  세 JOIN의 eq_ref/PRIMARY를 확인했다. [통과 원본 JSON](experiments/s7-campaign-handoff-2026-10-20-1m-v3.json)을 별도로 보존했다.
- 최종 10만/100만 자원 샘플은 37/502개, 오류·unavailable은 모두 0이다.
  관측 최대 heap 63.97/64.03 MiB, RSS 271.47/269.34 MiB, MySQL cgroup 메모리 711.19/2047.95 MiB였다.
  100만 실행에서 MySQL은 2 GiB 상한에 근접했다. 두 실행의 대상 수는 10배, 시간은 약 14.11배다.
  가상 대상/단일 개발 호스트/소수 실행이며 운영 성능이나 병목 원인을 확정하는 결과가 아니다.
- `caffeinate -i ./gradlew --no-daemon --console=plain build`:
  **31개 클래스/258개 테스트 통과, 실패·오류·스킵 0**, BUILD SUCCESSFUL(11분 57초), 종료 코드 0.
  JUnit XML 합계와 콘솔 출력을 대조했다. 대량 실험은 별도 task이며 기본 회귀 합계에 포함하지 않는다.
  운영 소스/의존성은 동일하여 실행 JAR 생성 task는 UP-TO-DATE였고 전체 build는 성공했다.
- 새 실험 Java/Gradle 설정, 문서와 원본 JSON 4개를 검토했다. 문서 링크/코드 블록/JSON 파싱/`git diff --check`를 확인했다.
  최초 실패와 중간 검증 문제를 숨기지 않고 기록했으며, HTTP Mock 발송 부하 항목은 미완료로 유지한다.
- 모든 테스트 JVM/격리 컨테이너가 종료됐고 기존 notification-lab-mysql-1은 healthy였다.
  Git 작성자 JinHyeon / tiemo0708@naver.com, GitHub tiemo0708 인증과 원격 main/HEAD c95a444를 읽기 전용으로 확인했다.
  기존 main 이력을 유지해 로컬 커밋하며, 사용자 검토 전 푸시하지 않는다.

### 한계와 다음 단계

- 이번 완료 항목은 Batch DB 인계 부하다. HTTP Mock 발송 경로의 10만→100만 부하 항목은 계속 미완료다.
  S7 전체 완료나 100만 건 공급자 수락/단말 도착을 주장하지 않는다.
- 최종 검증 SQL의 각 크기 실행은 1회이며, 이전 쿼리의 성공/실패 실행도 별도로 보존했다.
  JVM/DB 시작을 새로 하더라도 호스트 캐시·IDE·로컬 MySQL 등의 영향은 완전히 통제하지 못한다.
- 다음은 별도 HTTP Mock과 명시적인 실험 예산을 사용한 발송 경로 부하 설계/검증이다.
  기존 사용자 Redis 예산을 자동 상향하지 않는다. 공급자 수락·lag·완료 지연·REALTIME 간섭을 측정한다.
- 사용자 로컬 DB/.env/IntelliJ 설정을 변경하지 않았으며, 원격 CI/클라우드/실제 FCM/APNs는 실행하지 않았다.

## 2026-10-20 — S7 HTTP 발송 사전 실험과 실시간 지연 측정

### 범위와 변경

- 10만→100만 발송 부하 전에 정상 경로/측정/전수 대조 도구를 확인하는 1천/1만 건 pilot을 추가했다.
  PLAN에는 사전 실험만 완료 표시하고 10만→100만 HTTP 발송 부하는 계속 미완료로 유지한다.
- `campaignDeliveryPilot` Gradle task와 별도 태그를 추가했다. 기본 build/test에서는 제외하고 명시적 실행마다 새 JVM/인프라로 실행한다.
  허용 대상 수 1000..10000, 기본 10000, JVM heap 512 MiB, 발송 15분/테스트 20분/task 25분 상한이다.
- 실제 Batch로 캠페인을 사전 저장한다. REALTIME 단독 50개를 완료한 뒤 CAMPAIGN 앱을 시작하고,
  캠페인이 처리되는 동안 단일 생성기가 실시간 HTTP 202 응답 뒤 200ms씩 대기하며 요청한다.
  전체 실시간 요청 상한은 5000개이며 무제한 제출 큐/재시도/자동 catch-up을 만들지 않는다.
- 실제 loopback HTTP Mock 앱을 사용한다. 테스트 전용 감사 필터는 bounded body와 BitSet/ID 배열로
  캠페인 번호/결정적 ID와 실시간 API 반환 ID를 대조한다. 결과에는 수신자·본문·ID를 저장하지 않는다.
- 정상 SUCCESS 시나리오에서 고유 HTTP 수신=수락=DB 완료=작업당 성공 이력 1회,
  모든 파티션의 end/committed offset, 역할별 quota 허가 수를 대조한다.
- 격리된 실험 quota는 전체 1000/s·burst100, 캠페인 200/s·burst20, 실시간 800/s·burst80이다.
  두 앱은 각 Worker3/HTTP3/Redis3/Hikari4, Worker 대기열0을 사용한다. Mock은 지연1ms/동시8/10000요청·초 창이다.
  운영 기본 예산이나 사용자 Redis 키/DB/.env/IntelliJ는 변경하지 않았다.
- MySQL 8.4.8(2CPU/2GiB/buffer pool512MiB), Kafka4.0.2(1CPU/1GiB), Redis7.4.11(1CPU/256MiB/maxmemory128MiB)을 사용했다.
  기존 앱·Mock·생성기·감사는 한 JVM에서 실행되며, 역할별 독립 context/풀을 사용한다. 별도 OS 프로세스 실험은 아니다.
- [학습/실험 문서](S7_DELIVERY_PILOT.md)에 선택 이유, 측정 경계, 실시간 표본의 순차 생성 한계, 실패/통과 원본을 정리했다.

### 실제 실행과 결과

- 환경: macOS15.7.7 arm64, Amazon Corretto21.0.6, Gradle8.14.5, 호스트10 logical CPU/16GiB,
  Docker Desktop10CPU/약7.65GiB. IDE와 기존 로컬 MySQL이 있는 공유 개발 환경이다.
- 최초 1천 건 smoke: **실패**, BUILD FAILED(38초). 캠페인 Worker 중지를 발견했다.
  진단 지표를 추가한 재현: **실패**, BUILD FAILED(35초).
  quotaUnavailable=1/quotaRejectedUnavailable=1/admissionFull=0을 확인했다.
  [실패 JSON](experiments/s7-delivery-pilot-2026-10-20-1000-pool-failure.json)은 FAIL로 보존했다.
  두 실행에서 소비자는3개, Redis 연결은 기본2개였다. 기본값을 그대로 조합한 실험 설정 문제를 수정했다.
- 실험용 Redis 연결 상한만3개로 맞춰 fail-closed 동작을 유지했다.
  도구 보완 중 실제 Gauge 이름에 맞게 샘플러를 수정하고, HTTP 감사의 ObjectMapper를 재사용하도록 했다.
- `caffeinate -i ./gradlew --no-daemon --console=plain campaignDeliveryPilot -PdeliveryTargets=1000`:
  **1개 테스트 통과**, BUILD SUCCESSFUL(46초), 종료 코드0.
  사전 인계2.483초, 발송 구간12.467초/80.211건·초, 실시간 단독50/혼합55건.
  HTTP1105건과 DB/offset/허가 수 일치, 중복·잘못된 매핑0.
  [통과 JSON](experiments/s7-delivery-pilot-2026-10-20-1000.json)을 보존했다.
- `caffeinate -i ./gradlew --no-daemon --console=plain campaignDeliveryPilot`:
  **1개 테스트 통과**, BUILD SUCCESSFUL(2분43초), 종료 코드0.
  사전 인계16.184초, 발송 구간115.394초/86.659건·초, 실시간 단독50/혼합514건.
  캠페인10000명+실시간564명=HTTP10564건의 고유 수락과 DB/각 파티션offset/역할별허가 수가 일치했다.
  Mock429/500/busy/영구실패·중복·잘못된매핑0, 최대 동시HTTP4.
  [1만 JSON](experiments/s7-delivery-pilot-2026-10-20-10000.json)을 보존했다.
- 1만 실행의 실시간 DB 완료 p95는 단독479.459ms/혼합485.969ms, p99는631.212ms/571.243ms다.
  단독50개는 p99=최대이고 순차 실행/warmup 차이가 있어 개선 효과나 지연 SLO를 주장하지 않는다.
- 53개 자원 샘플의 null/NaN은0, JVM heap 관측 최대102.37MiB, MySQL cgroup479.14MiB, 스레드98개였다.
  샘플 구간 GC651ms, JVM 평균CPU약0.440코어 상당이며 두 앱/Mock/도구를 합친 값이다.
  캠페인 lag 최대179/미발행Outbox10000, 실시간1/1을 관측했다. lag만으로 전체 적체를 판단할 수 없다.
- `caffeinate -i ./gradlew --no-daemon --console=plain build`:
  **31개 클래스/258개 테스트 통과, 실패·오류·스킵 0**, BUILD SUCCESSFUL(13분27초), 종료 코드0.
  종료 후 JUnit XML과 콘솔258개 PASSED를 대조했다. pilot은 이 합계에 포함하지 않는 별도 실행이다.
  운영 소스/의존성 변경이 없어 JAR 생성 task는 UP-TO-DATE였고 전체 build는 성공했다.
- 모든 테스트 JVM/격리 컨테이너가 정리됐고, 기존 notification-lab-mysql-1은 healthy였다.
  문서 링크/코드 블록/원본 JSON 파싱/`git diff --check`를 확인했다.
- Git 작성자 JinHyeon / tiemo0708@naver.com, GitHub tiemo0708 인증과 원격 main/HEAD c95a444를 읽기 전용으로 확인했다.
  기존 main 이력에 실험과 문서를 로컬 커밋하고 사용자 검토 전 푸시하지 않는다.

### 한계와 다음 단계

- 1천/1만 정상 발송 pilot이며 10만/100만 발송, 단말 도착, exactly-once, 운영 성능, 원격 CI를 검증한 결과가 아니다.
- Batch와 발송을 동시에 실행하지 않는다. 캠페인 created→completed 지연에는 사전 저장과 단독 구간 대기가 포함된다.
- 실시간은 응답 후 대기하는 순차 생성기라 제출률이 응답 속도에 의존한다. 일정 도착률/과부하 검증은 별도다.
- 다음은 10만 건으로 확대할 때 전수 대조의 메모리/조회 비용·시간 상한을 확정하고 실행하는 단계다.
  그 결과를 확인한 뒤100만으로 확대한다. 사용자 기존 Redis 예산을 자동 상향하지 않는다.

## 2026-10-31 — 기존 완료 구간 푸시와 CI 보완

- 사용자 요청으로 기존 main의27개 로컬 커밋을 일반 push했다. 원격 main을 직접 조회해
  c95a444 → 9cf178636edc4b03a5aa1d560f9cfe3c71f36568 반영을 확인했다. 강제 push/분기/병합은 하지 않았다.
- push 전 작업 폴더는 깨끗했고 마지막 JUnit XML은31개 클래스/258개, 실패·오류·스킵0이었다.
  이 확인은 저장된 검증 결과 확인이며 이번 시점의 신규 재실행과 구분한다.
- [GitHub Actions 실행](https://github.com/tiemo0708/Notification-Lab/actions/runs/41187656290)은
  15분 제한 초과로 cancelled였다. annotation의 원문은 "The job has exceeded the maximum execution time of 15m0s"다.
  취소 전 PASSED 로그245개와 WorkerConcurrencyTest의 admissionTimeoutLeavesNoClaimAndRecoversUncommittedRecordOnExplicitRestart
  실패1개를 확인했다. 전체258개 통과나 CI 성공으로 기록하지 않는다.
- 실패 위치는 timeout 거절 counter 존재 확인(기존96행)이었다. 이 테스트는 첫 HTTP 호출의1200ms 지연에 의존했다.
  첫 호출이 끝난 뒤 다음 Consumer가 실행되면 의도한 대기열 timeout이 생기지 않을 수 있는 시간 의존성을 확인했다.
  CI 실행의 세부 스케줄링을 직접 수집한 것은 아니므로 정확한 원인을 확정하지 않는다.
- 테스트 전용 스레드가 실제 WorkerAdmission 슬롯을 latch로 유지하고 timeout 거절을 확인한 뒤 해제하도록 변경했다.
  거절된 레코드의 DB 선점/offset/HTTP 호출0, 명시적 Consumer 재시작 뒤 성공1회를 검증한다.
  latch60초/관측 대기/스레드 join에도 상한을 두며 운영 코드는 바꾸지 않았다.
- 전체 CI 상한은15→30분으로 조정했다. 실제 원격 취소와 기존 로컬 전체 실행13분27초를 근거로 여유를 둔다.
  개별 검증을 스킵하거나 assertion을 완화하지 않는다. 변경된 workflow의 원격 통과는 아직 확인하지 않았다.
- `caffeinate -i ./gradlew --no-daemon --console=plain test --tests '*WorkerConcurrencyTest'`:
  **5개 통과**, BUILD SUCCESSFUL(38초), 종료 코드0. 새 슬롯 유지 방식으로 timeout/선점 전 거절/재시작을 검증했다.
- `caffeinate -i ./gradlew --no-daemon --console=plain build`:
  **31개 클래스/258개 테스트 통과, 실패·오류·스킵0**, BUILD SUCCESSFUL(11분39초), 종료 코드0.
  JUnit XML 합계와 콘솔258개 PASSED를 대조했다. 기본 build는 대량 발송/pilot을 포함하지 않는다.
  운영 소스/의존성 변경이 없어 실행 JAR 생성은 UP-TO-DATE였고 전체 build가 성공했다.
- workflow 변경은 timeout 값 한 줄임을 원본과 대조했고 `git diff --check`를 통과했다.
  CI 보완은 로컬 검증/커밋이며 사용자 검토 전 재푸시하지 않는다. 변경된 원격 CI 성공은 미확인이다.

## 2026-10-31 — S7 캠페인 10만 건 HTTP 발송과 실시간 간섭 검증

### 변경과 완료 조건

- 이전1천/1만 pilot의 정상 경로를 재사용하는 opt-in `campaignDeliveryLoad`를 추가했다.
  대상은 정확히100000명이며 기본 회귀와 분리한다. 운영 코드/스키마/의존성/사용자 DB·Redis 예산은 바꾸지 않았다.
- 원래pilot 대상1000..10000/실시간5000/발송15분/task25분은 유지하고, load 모드는 실시간20000/발송60분/task90분으로 제한한다.
  공유 JUnit 메서드 상한은80분이다. 완료 지연은1000행 keyset 조회+고정 double 배열로 전수 계산한다.
- HTTP 감사는 캠페인 BitSet12504바이트와 제한된 실시간 BitSet/ID 배열이다. 원문 수신자/본문/ID를 결과에 저장하지 않는다.
- 완료 후 campaign_target1000행 페이지와 PK JOIN으로 결정적 ID/대상/notification-task-outbox-attempt 참조를 전수 대조한다.
  n/t/o/a 모두eq_ref/PRIMARY인지 검사한다. 건수/상태/성공시도1회/HTTP 고유 수락/각 파티션offset/역할별quota도 함께 대조한다.
- 새 [실험 문서](S7_DELIVERY_LOAD.md)에 상한·재현 명령·측정 경계·실패 기준·결과·한계를 작성했다.

### 실제 실행과 결과

- macOS15.7.7 arm64 / Amazon Corretto21.0.6 / Gradle8.14.5, 호스트10 logical CPU/16 GiB,
  Docker Desktop10 CPU/약7.65 GiB에서 각1회 실행했다. 기존 IDE/로컬 MySQL과 호스트를 공유했다.
- JVM heap512 MiB, MySQL8.4.8(2CPU/2GiB, buffer pool512MiB), Kafka4.0.2(1CPU/1GiB),
  Redis7.4.11(1CPU/256MiB, maxmemory128MiB/noeviction)을 사용했다. 앱별Worker3/HTTP3/Redis3/Hikari4/대기열0이다.
  실험 전용quota1000/s·burst100 중 캠페인200/s·burst20, 실시간800/s·burst80으로 기존pilot 조건을 유지했다.
  Mock은SUCCESS/지연1ms/동시8/10000요청·초 창이다. 두 앱/Mock/도구는 같은 JVM의 별도context다.
- `caffeinate -i ./gradlew --no-daemon --console=plain campaignDeliveryPilot -PdeliveryTargets=1000`:
  **1개 통과**, BUILD SUCCESSFUL(52초), 종료 코드0. 사전 저장2.057초/발송11.390초/87.797건·초,
  실시간50+51개와 HTTP1101건, 전체참조1000개를 대조했다. [smoke 원본](experiments/s7-delivery-pilot-2026-10-31-1000-v2.json).
- `caffeinate -i ./gradlew --no-daemon --console=plain campaignDeliveryLoad`:
  **1개 통과**, BUILD SUCCESSFUL(25분30초), 종료 코드0. [10만 원본](experiments/s7-delivery-load-2026-10-31-100k.json).
  사전 저장151.762초, 발송1346.993초(약22분27초)/74.239건·초, 최종 대조2.305초였다.
- 캠페인100000+실시간5342(단독50/혼합5292)=HTTP105342건 수락과 DB/전체참조/파티션offset/quota가 일치했다.
  HTTP 중복/잘못된 매핑/Mock429·5xx·busy·영구실패는0이었다. 최대HTTP동시4, 모든작업의 성공시도1회였다.
- 실시간 DB 완료 p95는 단독553.657ms/혼합918.136ms, p99는616.772ms/1213.208ms, 혼합 최대1905.362ms였다.
  API 응답 p95도28.583ms/334.452ms였다. 혼합 지연 증가를 기록하며 완전한 자원 격리나 지연 SLO를 주장하지 않는다.
  단독50개/순차 측정/warmup/생성률 차이로 원인·통계적 효과를 확정하지 않는다.
- 606개 샘플의 null/비유한 값은0, heap 관측 최대116.32MiB/MySQL cgroup1307.98MiB/스레드97개였다.
  샘플 구간 GC3680ms, JVM 평균 CPU는 약0.222코어 상당이다. 캠페인 lag 최대215/미발행 Outbox100000, 실시간2/2였다.
  최종 offset은 캐시된 lag 대신 Admin으로 모든 요청/Retry/DLQ 파티션의 committed=end를 확인했다.
- `caffeinate -i ./gradlew --no-daemon --console=plain build`:
  **31개 클래스/258개 통과, 실패·오류·스킵0**, BUILD SUCCESSFUL(11분39초), 종료 코드0.
  JUnit XML과 콘솔258개 PASSED를 대조했다. 위 smoke/load2회는 기본258개와 별도 실행이다.
- 문서 링크/코드 블록/JSON 파싱/샘플 유효값/`git diff --check`를 확인했다.
  변경은 로컬 커밋으로 정리하고 사용자 검토 전 재푸시하지 않는다.
- 모든 테스트 JVM/격리 컨테이너가 종료됐고 기존 notification-lab-mysql-1은 healthy였다.
  사용자 로컬 DB/.env/IntelliJ 설정을 변경하지 않았다.


### 한계와 다음 단계

- 정상 SUCCESS의 사전 저장 후 발송1회다. 실제 FCM/APNs·단말 도착·외부 exactly-once·100만 HTTP 발송은 미검증이다.
- 실시간은 HTTP 응답 후200ms 대기하는 닫힌 루프다. 고정 도착률/과부하 SLO/독립 OS 프로세스 격리·저장 발송 동시 실행과 다르다.
- 공유 DB/호스트 간섭, 실시간 지연 증가와 실험별 변동을 남긴다. 운영 처리량·정확한 병목 원인을 확정하지 않는다.
- PLAN에서10만 건 HTTP 검증만 완료하고100만 건은 미완료로 유지한다.
  다음은100만 건의 시간·자원·실시간 표본 상한을 설계하는 단계다.
  CI 보완 커밋도 아직 미푸시이므로 변경된 원격 CI 성공은 후속 검증이 필요하다.

## 2026-10-31 — S7 100만 건 HTTP 발송 도구 준비

### 범위와 변경

- 사용자에게 동일 조건의 100만 건 실험이 약 4~6시간 걸릴 수 있음을 안내했다.
  사용자가 **도구와 검증까지만 준비하고 장시간 실행은 따로 진행**하기로 선택했으므로 실제100만 건 발송은 시작하지 않았다.
- 기본 test/check/build와 독립적인 `campaignDeliveryMillion` task를 추가했다.
  기존512MiB heap/인프라/발송 예산/동시 처리/Mock 조건을 유지하며 대상만 정확히1000000명으로 설정한다.
- `CampaignDeliveryBounds`로 pilot1000..10000과 load100000/1000000만 허용한다.
  pilot에서 대량 크기를 지정하거나 검증되지 않은 load 크기를 지정하면 인프라를 만들기 전에 거절한다.
- 100만 건은 실시간 최대150000개/발송8시간/자원 샘플14402개로 제한한다.
  감사 BitSet 데이터125008바이트, 지연 배열8000000바이트, 조회 페이지1000행이다.
  객체·문자열·연결 풀 등 전체 메모리 사용을 이 바이트 수로 보장하지 않는다.
- JUnit 메서드 제한을 각 task의 설정으로 분리했다. pilot20분/10만80분/100만11시간이며
  Gradle task 제한은 각각25분/90분/12시간이다. 초과 샘플을 추가하기 전에도 상한을 검사한다.
- 경계값 테스트6개로 대량 실행 거절·정확한 load 크기·생성기 시간창과 표본 수 관계를 확인한다.
  감사 도구에100만 번째 결정적 ID를 직접 넣어 검사하고 중복/잘못된 ID/HTTP 실패/범위 초과를 거절한다.
  이 검증은 실제100만 건 HTTP 호출이나1m DB 조회 실행과 다르다.
- [준비 문서](S7_DELIVERY_MILLION.md)에 시간 추정 근거, 조건과 상한, 별도 실행 명령/출력/통과 기준/한계를 정리했다.
  PLAN에는 준비 항목만 완료 표시하고 실제100만 건 발송 항목은 계속 미완료로 유지한다.

### 실제 검증

- 최종 설정으로 `caffeinate -i ./gradlew --no-daemon --console=plain test --tests '*CampaignDeliveryBoundsTest' campaignDeliveryPilot -PdeliveryTargets=1000`:
  **경계값6개 + 별도 HTTP 실험1개 통과**, BUILD SUCCESSFUL(42초), 종료 코드0.
  사전 저장1.902초/발송10.642초/93.964건·초, 실시간50+48개와 HTTP1098건을 대조했다.
  캠페인 참조1000개와 PK JOIN 계획, 각 파티션offset/quota/고유 수신이 일치했다.
  [최종 소규모 원본 JSON](experiments/s7-delivery-million-prep-2026-10-31-1000.json)을 보존했다.
  중간 설정의 작은 실행도 성공했으나 위 수치는 task별 시간 제한 적용 후 최종 실행의 값이다. 성능 개선을 주장하지 않는다.
- `./gradlew --no-daemon --console=plain campaignDeliveryMillion --dry-run`:
  BUILD SUCCESSFUL(2초), 종료 코드0. **모든 task가 SKIPPED인 실행 계획 확인**이며 실제 발송이나 테스트 통과가 아니다.
- 실제100만 건 결과 파일 `build/reports/campaign-delivery-load/load-1000000.json`은 생성되지 않았음을 확인했다.
- `caffeinate -i ./gradlew --no-daemon --console=plain build`:
  **32개 클래스/264개 테스트 통과, 실패·오류·스킵0**, BUILD SUCCESSFUL(11분27초), 종료 코드0.
  새 경계값6개는 이264개에 포함되며 HTTP pilot1개는 별도다. JUnit XML과 콘솔 합계를 대조했다.
  기본 build에서 campaignDeliveryMillion은 실행되지 않았다. 운영 소스/의존성이 같아 실행 JAR 생성은 UP-TO-DATE였다.
- 문서 링크/코드 블록/원본 JSON/샘플 유효값/`git diff --check`를 확인했다.
  실제100만 건의 HTTP 수락·DB 조회 비용·장시간 안정성과 시간 제한 발동은 미검증이다.
  원격 CI 재확인과 실제 장시간 실험은 후속 작업이며, 이번 변경은 로컬 커밋으로만 정리한다.

- 테스트 JVM과 실행 중 격리 컨테이너가 정리됐고 기존 notification-lab-mysql-1은 healthy였다.
  사용자 로컬 DB/.env/IntelliJ 설정을 변경하지 않았다.

## 2026-11-02 — S7 100만 건 첫 실행 실패와 진단 보강

### 첫 실제 실행

- 10월31일에는 준비만 했고 이번 후속 요청으로 campaignDeliveryMillion을 실행했다.
  시작 코드는71f6406, 원격 main은9cf1786이었다. 기존3개 로컬 커밋과 사용자 DB를 보존하고 push하지 않았다.
- `caffeinate -i ./gradlew --no-daemon --console=plain campaignDeliveryMillion`:
  **BUILD FAILED(2시간20분17초), 종료 코드1, 테스트1개 실패/오류·스킵0**.
  날짜는 KST2026-11-02, 원본 시각은 UTC다. [실패 JSON](experiments/s7-delivery-million-2026-11-02-attempt-1-failure.json)을 그대로 보존했다.
- 준비된 JVM512MiB/MySQL2CPU·2GiB/buffer pool512MiB/Kafka1CPU·1GiB/Redis1CPU·256MiB 조건을 유지했다.
  역할별 Worker3/HTTP3/Redis3/Hikari4, 실험 quota1000/s 중 캠페인200/s·실시간800/s, Mock SUCCESS/1ms도 유지했다.
- Batch COMPLETED와 발송 시도0 확인 후 발송 구간으로 넘어갔다. 사전 저장2631.136초(약43분51초),
  실시간 단독50개 완료 p95는583.858ms였다. 사전 저장 진행은 격리 DB의 PRIMARY 역방향 LIMIT1 조회로 약1분마다 확인했다.
- 직접 실패는 MIXED 실시간 접수 probe의 HttpTimeoutException이었다. 실험 클라이언트의 접수 응답 대기5초이며
  공급자 호출 timeout/8시간 발송 상한/11시간 JUnit 상한 실패가 아니다. 원본 코드의 probe242행에서 발생했다.
- 마지막 샘플은 발송5736.619초/캠페인 완료 카운터383801, 마지막 stdout은383805/혼합 실시간23306이었다.
  관측 시점이 다른 숫자이며 최종 고유 수락·DB 건수·단말 도착 수로 사용하지 않는다.
  최종 HTTP 감사/DB 참조·이력/offset/quota 전수 대조에 도달하지 못했고100만 건 통과는 미완료다.
- 2593개 샘플에서 heap 관측 최대102.37MiB/MySQL cgroup 최대2048MiB/스레드99개였다.
  DB 메모리가 상한99% 이상인 샘플은2333개였다. 페이지 캐시를 포함하므로 OOM이나 원인을 확정하지 않는다.
  초기2개 샘플에 결측·비유한 값10개가 있었고0으로 바꾸지 않았다. 샘플 구간 GC 증가27.109초/평균 JVM CPU약0.273코어 상당이었다.
- [Docker 이벤트 관측](experiments/s7-delivery-million-2026-11-02-mysql-events.json)의 최초 stdout에서는
  종료 시각과 겹치는 kill→die137→destroy를 확인했다. 후속 조회는 빈 결과였다. 유한 이력/exit137만으로 OOM을 확정하지 않는다.
- timeout 난 요청은 서버에서 이미 커밋했을 수도 있다. 접수 거절·유실·미발송을 확정하지 않으며 자동 재시도하지 않았다.
  첫 도구가 timeout 순번/응답 미관측/실패 전 Hikari·cgroup 사건을 저장하지 않아 원인 판별 자료가 부족했다.

### 진단 보강과 작은 검증

- 운영 코드·스키마·의존성·probe5초 대기·발송 예산·자원 상한은 바꾸지 않았다. 장시간 실행을 즉시 반복하지 않았다.
- 테스트 도구는 timeout 요청의 phase/순번/확인된202 수와 serverOutcome=UNKNOWN을 기록하고 같은 예외를 다시 던진다.
  본문/수신자/ID/URI/인증정보와 원본 예외 메시지를 결과에 넣지 않는다.
- 혼합 구간 실패는 context 종료 전에 시각/경과 시간, 두 앱의 Hikari active/idle/waiting·listener·지표,
  HTTP 감사/Mock 집계/확인된 응답 수를 저장한다. 추가 DB 조회를 하지 않고 수집 오류가 원래 실패를 가리지 않게 했다.
- 주기 샘플에도 Hikari 상태와 MySQL memory.events/cpu.stat 카운터를 추가했다.
  첫 실패에는 이 자료가 없었으므로 뒤의 성공한 소규모 자료로 원인을 소급해 확정하지 않는다.
- 최초 보조 테스트 컴파일은 SimpleMeterRegistry의 AutoCloseable 사용 오류로 실패했다(5초).
  finally에서 close하도록 수정한 뒤 아래 검증을 실행했다. 이 실패를 테스트 통과로 세지 않는다.
- `caffeinate -i ./gradlew --no-daemon --console=plain test --tests '*CampaignDeliveryDiagnosticsTest' campaignDeliveryPilot -PdeliveryTargets=1000`:
  **단위4개 + 별도 HTTP 실험1개 통과**, BUILD SUCCESSFUL(1분3초), 종료 코드0.
  사전 저장2.658초/발송20.420초, 캠페인1000+실시간130=HTTP1130건과 DB/참조/offset/quota를 대조했다.
  [소규모 진단 원본](experiments/s7-delivery-million-2026-11-02-diagnostics-1000.json)을 보존했다.
  단위 timeout 검증은150ms 제한과 latch 서버이며 원래100만 건의 지연 원인을 재현한 것은 아니다.
- `caffeinate -i ./gradlew --no-daemon --console=plain build`:
  **기본 회귀33개 클래스/268개 통과, 실패·오류·스킵0**, BUILD SUCCESSFUL(12분26초), 종료 코드0.
  새 진단 단위4개는 이268개에 포함되고 별도 HTTP pilot은 포함되지 않는다.
  JUnit XML과 콘솔 합계를 대조했다. 별도100만 건 task의1개 실패를 기본 회귀 성공으로 덮어쓰지 않는다.
  운영 소스/의존성 변경이 없어 실행 JAR 생성은 UP-TO-DATE였고 기본 build가 성공했다.
- 실패 JSON이 build 원본과 동일한지 SHA-256으로 확인했다.
  원본 해시는0f2752898e43efdba1594d97c4ac76881efabbd7f22a0d9112cae1bf8dbab79b다.
  문서 링크/코드 블록/JSON 파싱/`git diff --check`도 확인했다.
- 모든 테스트 JVM이 종료된 뒤 이번 저장소의 실행 로그·JUnit 자료에서 생성이 확인된 잔존 Ryuk만 정리했다.
  기존 사용자 DB/.env/IntelliJ와 무관한 컨테이너·volume은 변경하지 않았다.

### 한계와 다음 단계

- 첫 실험은 실패이며 S7의100만 건 발송 완료 표시는 보류한다. 기본 회귀 통과와 별도 장시간 실험 실패를 구분한다.
- DB 메모리 근접과 낮은 Kafka lag/큰 Outbox 적체만으로 병목이나 OOM을 확정하지 않는다.
  다음은 Relay 발행·DB 트랜잭션·연결 풀 대기·실시간 API 지연을 좁혀 확인하고 작은 재현을 만드는 단계다.
  원인과 재현을 검증한 뒤100만 건 재실행 여부·조건을 결정한다. 원격 CI 재확인은 별도로 남아 있다.

## 2026-11-02 — S7 접수 timeout의 작은 진단

- 시작 커밋05a94c8, main의 기존4개 로컬 커밋을 보존했다. 푸시와100만 건 재실행을 하지 않았다.
- 완료 조건은 기존1만 건 HTTP를 새 계측으로 확인하고, 격리 MySQL의 작은 적체에서
  실제 Outbox 선점 SQL과 집계 계획/비용을 관측해 다음 가설을 좁히는 것이다.
  이전5초 timeout의 재현·원인 확정과 S7 전체 완료를 이번 작은 실험의 성공 조건으로 대체하지 않는다.
- `caffeinate -i ./gradlew --no-daemon --console=plain campaignDeliveryPilot -PdeliveryTargets=10000`:
  **BUILD SUCCESSFUL2분53초, 종료 코드0, 별도 테스트1개 통과/실패·오류·스킵0**.
  사전 저장24.895초/발송109.671초/91.182건·초, 캠페인10000+실시간544=HTTP10544건을 전수 대조했다.
  혼합 API 응답p95 24.756ms/최대112.488ms, 단독p95 22.901ms였다.
  DB 완료p95는 단독498.399ms/혼합491.728ms로 별도 경계다. 이전1m 실패는 재현되지 않았다.
- Hikari/cgroup 진단51개 샘플에서 대기 최대0, active 최대 캠페인4/실시간2,
  MySQL 메모리 최대477.40MiB, 관측 시작~끝 oom/oom_kill/max/CPU throttle 카운터0이었다.
  순간 샘플과 이번 실행의 범위이며 과거1m의 OOM·대기 여부를 소급해 확정하지 않는다.
- 별도 outboxQueryProbe task를 추가했다. JUnit8분/task10분/JVM512MiB 상한과 기본 build 제외를 명시했다.
  MySQL8.4.8/2CPU/2GiB/buffer pool512MiB, 역할별 Hikari4로 SQL 사전 저장1만→10만 행을 구성한다.
  운영 OutboxStore 코드와 명시적 두 트랜잭션, 실제 SubmissionService의 DB 접수,
  PipelineMetrics SQL을 복사한 집계를 관측한다. Kafka/Worker/HTTP/단말 발송을 재현하지 않는다.
- 최초 compileTestJava는 AssertJ와 TransactionTemplate의 Boolean 타입 추론 충돌로 실패했다(10초, 종료 코드1).
  지역 변수로 타입을 명시한 뒤 다음 실제 task에서 컴파일과 실행을 검증했다.
  이 컴파일은1만 건 HTTP 실행 중 병행했으므로 해당 관측에는 추가 CPU 부하가 있다.
  DB 진단 task 자체는 HTTP 종료 후 실행했으며 회차 간 성능 개선을 주장하지 않는다.
- `caffeinate -i ./gradlew --no-daemon --console=plain outboxQueryProbe`:
  **BUILD SUCCESSFUL32초, 종료 코드0, 별도 테스트1개 통과/실패·오류·스킵0**.
  각 역할/작업20회, 동시 캠페인 선점100회/집계10회/실시간 접수40회를 크기별로 관측했다.
  기본 build나1m task가 이 진단을 자동 실행하지 않는다.
- 집계·PENDING/PROCESSING 선점은 역할별 복합 인덱스 대신 idx_outbox_status_available을 선택했다.
  rows 추정은 집계2/선점1이었고 특히 캠페인 집계2는 실제 약10만 행 적체와 달랐다.
  EXPLAIN 추정과 LIMIT1이 실제 읽은 행 수를 뜻하지 않는다.
  실시간 집계p95는1만17.291ms→10만79.304ms, 캠페인 집계는8.833→96.295ms였다.
  겹친 실시간 DB 접수 최대16.983/11.832ms였고 초 단위 지연은 관측하지 못했다.
- SQL UUID()·빠른 사전 저장·따뜻한 캐시·고정 실행 순서와 Worker 처리 부재가 있다.
  집계 간격200ms는 운영5초보다 촘촘하다. ANALYZE TABLE/인덱스 힌트/운영 인덱스·설정 변경은 하지 않았다.
  PUBLISHED 상태 갱신은 SQL 진단용이며 Kafka ACK나 실제 발송의 증거가 아니다.
- [HTTP 원본](experiments/s7-timeout-diagnosis-2026-11-02-http-10000.json)과
  [SQL 원본](experiments/s7-timeout-diagnosis-2026-11-02-outbox-query.json)을 보존했다.
  [측정 경계·결과·다음 가설](S7_TIMEOUT_DIAGNOSIS.md)에 표와 한계를 정리했다.
- `caffeinate -i ./gradlew --no-daemon --console=plain build`:
  **기본 회귀33개 클래스/268개 통과, 실패·오류·스킵0**, BUILD SUCCESSFUL12분21초, 종료 코드0.
  JUnit XML/콘솔 합계를 대조했다. 별도 HTTP1개/SQL1개는268개에 포함되지 않는다.
  새 태그는 기본 build에서 제외됐고1m 재실행도 없었다. 운영 JAR 생성은 UP-TO-DATE였다.
- 문서 링크/코드 블록/JSON 파싱과 원본 동일성/수신자·ID 미포함/`git diff --check`를 확인했다.
  진단용 집계 SQL은 현재 PipelineMetrics.refresh와 공백을 제외하고 일치했다.
  이전1m 실패 JSON의 SHA-256도 그대로였다.
- 실행이 종료된 뒤 이번 로그·JUnit에서 생성이 확인된 잔존 Ryuk만 정리했다.
  사용자 로컬 DB/.env/IntelliJ 설정과 무관한 컨테이너·volume은 변경하지 않았다.
- 다음은 같은 적체에서 ANALYZE TABLE 전후 계획·실제 읽은 행 수·지연의 비교다.
  통계 갱신만으로 달라지는지 먼저 확인하고, 필요할 때 진단용 인덱스 힌트를 별도 조건으로 비교한다.
  이전1m의 당시 계획/통계가 없어 후속 재현만으로 과거 원인을 확정할 수 없다. S7의100만 건 검증은 미완료다.

## 2026-11-02 — S7 Outbox 통계 갱신 전후 비교

- 시작 커밋108debe, main의 기존5개 로컬 커밋을 보존했다. 새 브랜치/푸시/100만 건 재실행은 하지 않았다.
- 같은 적체에서 통계 갱신만 개입하고 실행 계획·실행 노드 행 수·지연이 달라지는지 확인하는 단계다.
  이전1m 장애 원인과 진단에서 확인한 메커니즘을 구분한다.
- 새 opt-in outboxStatisticsProbe task를 기본 build와 분리했다. JUnit8분/task10분/JVM512MiB 상한을 둔다.
  기존 쿼리 진단과 seed/container helper를 공유하고 클래스 태그를 메서드별 태그로 분리했다.
- MySQL8.4.8/2CPU/2GiB/buffer pool512MiB/Hikari4/연결 대기3초/쿼리30초로 고정했다.
  격리 outbox_event만 STATS_AUTO_RECALC=0으로 두고 빈 테이블 통계→캠페인100000+실시간100행 적재로 오래된 통계를 만들었다.
  전역 auto_recalc/persistent는 ON/sample_pages20이었다. 운영 기본 자동 갱신 조건과 같다고 주장하지 않는다.
- BEFORE/AFTER 사이에 ANALYZE TABLE을 한 번 실행했다. 인덱스 힌트·운영 앱/스키마/설정 변경은 없다.
  각 단계에서 같은 PENDING100100/시도0/lease NULL을 확인했다. 측정 중 업무 DML은 하지 않는다.
- 실제 OutboxStore의 두 SELECT를 DB 호출 없이 캡처한 뒤 실 DB에서 실행했다.
  SELECT는 별도 트랜잭션 후 rollback하며 선점 UPDATE/published/Kafka/HTTP/Worker는 실행하지 않는다.
  EXPLAIN/EXPLAIN ANALYZE와 같은 트랜잭션 연결의 Handler_read* 차이를 타이밍20회와 별도로 보존했다.
  트리 노드 행 수와 카운터 연산 수를 합산해 rows_examined로 표현하지 않는다.
- `caffeinate -i ./gradlew --no-daemon --console=plain outboxStatisticsProbe outboxQueryProbe`:
  **BUILD SUCCESSFUL55초, 종료 코드0. 별도 통계 비교1개+기존 쿼리 진단1개 통과, 실패·오류·스킵0**.
  기존 task도 재실행해 공통 helper와 메서드 태그 변경을 확인했다.
- 실시간 집계/PENDING이 상태 인덱스에서 역할별 복합 인덱스로 바뀌었다.
  집계 스캔 노드는100100→100행, PENDING은100001→1행이었다. 반환은 계속1건이었다.
  PENDING의 별도 읽기 연산 차이는 Handler_read_next100000→0이었다.
  집계p95 81.364→2.420ms/PENDING p95 119.378→2.273ms였고 JDBC 왕복·트랜잭션/rollback을 포함한다.
- 캠페인 집계는 상태 인덱스에서 테이블 스캔으로 바뀌고 스캔100100행을 유지했다(p95 88.099→22.527ms).
  PROCESSING은0행으로 상태 인덱스를 유지했다. 모든 쿼리에 같은 인덱스를 강제하는 결론을 내리지 않는다.
- [통계 비교 원본](experiments/s7-timeout-statistics-2026-11-02.json)과
  [기존 쿼리 회귀 원본](experiments/s7-timeout-statistics-2026-11-02-query-regression.json)을 보존했다.
  [측정 방법과 표·공식 문서](S7_TIMEOUT_DIAGNOSIS.md)에 경계와 한계를 기록했다.
- `caffeinate -i ./gradlew --no-daemon --console=plain build`:
  **기본 회귀33개 클래스/268개 통과, 실패·오류·스킵0**, BUILD SUCCESSFUL12분11초, 종료 코드0.
  JUnit XML/콘솔 합계를 대조했다. 통계 비교1개/기존 쿼리 진단1개는268개에 포함되지 않는다.
  새 태그와 모든 opt-in task는 기본 build에서 제외됐고 운영 JAR 생성은 UP-TO-DATE였다.
- 문서 링크/코드 블록/JSON 파싱·원본 동일성·실행 스캔 수·민감 값 미포함/`git diff --check`를 확인했다.
  첫1m 실패 JSON은 기존 SHA-256과 같고 FAIL 상태를 보존했다.
- 실행 종료 후 이번 로그·JUnit에서 생성이 확인된 잔존 Ryuk만 정리했다.
  사용자 DB/.env/IntelliJ와 무관한 컨테이너·volume은 변경하지 않았다.
- 오래된 통계가 과도한 스캔을 만들 수 있는 경로를 확인했지만, 원래1m는 기본 자동 통계 조건의43분 저장이었다.
  과거 계획/통계/잠금 자료가 없고 이번에는 Worker·동시 쓰기·장시간 부하가 없어5초 timeout의 원인으로 확정하지 않는다.
  다음은 기본 자동 통계 조건의 선점 계획과 교차 역할 잠금 범위 확인이다. S7의100만 건 검증은 미완료다.

## 2026-11-02 — S7 교차 역할 선점 잠금 재현과 수정

- 시작 커밋3cb14fa, main의 기존6개 로컬 커밋을 보존했다. 브랜치 생성/푸시/100만 건 재실행은 하지 않았다.
- 기본 자동 통계 ON/MySQL8.4.8/2CPU·2GiB/buffer pool512MiB/SQL 사전 저장100000+100행에서 확인했다.
  실제 SELECT의 보유 잠금과 data_lock_waits를 연결하고 기존 행 갱신/새 실시간 접수/캠페인 선점을 구분했다.
  관측용 root는 격리 DB의 잠금·연결 메타데이터만 읽었으며 ID/LOCK_DATA/인증정보는 원본에 저장하지 않았다.
- 초기 EXPLAIN-only 계측은42초/종료 코드0이었으나 보유 잠금이 모두 비어 있어 부재 증거로 쓰지 않았다.
  실제 SELECT와 RECORD 잠금 존재 확인으로 보강했다. [초기 원본](experiments/s7-outbox-locks-2026-11-02-explain-only-preliminary.json)을 보존했다.
- 수정 전 실제 SELECT 계측과 기존 통계/쿼리 도구3개는1분27초/종료 코드0으로 통과했다.
  [수정 전 원본](experiments/s7-outbox-locks-2026-11-02-before.json)에서 적재 직후 상태 인덱스를 사용해
  PRIMARY 잠금100001건과 캠페인 기존 행 갱신 대기를 확인했다.20초 창 뒤 역할별 인덱스/PRIMARY1건으로 바뀌었다.
  두 인덱스 대조군도 같은 차이를 보였다. 새 실시간 접수의 해당 선점자 대기는 관측되지 않았다.
- 새 기본 회귀는32개 캠페인과1개 실시간, 오래된 통계를 고정해 다른 역할의 동시 선점 완료를 검사했다.
  최초 외부 트랜잭션 버전은20초/종료 코드1/테스트1개 실패였다(3초 Future timeout).
  역할별 인덱스만 명시한 뒤에도 같은 상한으로 실패했고, 실제 Spring interceptor와 생산 선언을 적용한 gate 버전도 실패했다.
- 인덱스만 고친 [중간 계측](experiments/s7-outbox-locks-2026-11-02-index-only.json)에서는
  캠페인 상태 변경에 X,GAP,INSERT_INTENTION 대기가 확인됐다. 상태 인덱스 음성 대조군의 빈 결과를 잘못 실패로 간주해
  task는42초/종료 코드1로 끝났다. 부분 관측을 보존하고 빈 결과도 기록하도록 계측을 고쳤다.
- 수정: 기존 V7 idx_outbox_traffic_status_available로 선점을 고정하고 OutboxStore.claim만 READ COMMITTED로 선언했다.
  반환1건=잠금1건이라는 잘못된 주석도 정정했다. 같은 행의 FOR UPDATE/lease token/시도 상한/조건부 기록은 유지한다.
  Relay는 바깥 DB 트랜잭션 없이 claim을 호출한다. 기존 외부 트랜잭션에 합류하는 REQUIRED 계약은 유지한다.
  API/Batch/Worker 전체 격리 수준이나 DB 전역 설정, ACK 후 PUBLISHED 경계는 바꾸지 않았다.
- 수정 후 선택 회귀1개+별도 잠금/통계/쿼리3개는 **BUILD SUCCESSFUL1분35초, 종료 코드0**, 실패·오류·스킵0.
  새 회귀는 실제 생산 선언의 READ-COMMITTED, 다른 역할의 선점 완료, 각각 lease/시도1과 이후 풀 연결의 REPEATABLE-READ를 확인했다.
  [실패→성공 요약](experiments/s7-outbox-locks-2026-11-02-regression.json)을 남겼다.
  [통계 도구](experiments/s7-outbox-locks-2026-11-02-statistics-after.json)/[쿼리 도구](experiments/s7-outbox-locks-2026-11-02-query-after.json)는 현재 강제된 선점 SQL을 캡처한다.
- 최종 v4 잠금 계측은 metadata/초기·최종 상태 기록을 정리한 버전이다.
  현재/20초 후/두 인덱스 대조군 모두 READ-COMMITTED/선택 행 RECORD 잠금을 확인하고,
  캠페인 갱신·선점·새 접수 대기 관계는 미관측, 각 probe는 holder 종료 전 완료였다.
  [최종 원본](experiments/s7-outbox-locks-2026-11-02-after.json)을 보존했다.
  범위는 짧은 SELECT를 의도적으로 유지한 SQL 진단이며1m HTTP 원인 재현이나 모든 부하의 무경합 보장이 아니다.
- `caffeinate -i ./gradlew --no-daemon --console=plain outboxLockProbe campaignDeliveryPilot -PdeliveryTargets=1000 build`:
  **BUILD SUCCESSFUL14분8초, 종료 코드0**, 기본33개 클래스/269개+별도 잠금1개/HTTP1개=합계271개 통과,
  실패·오류·스킵0. 새 교차 역할 회귀1개는269개에 포함된다. XML과 콘솔 합계를 대조했다.
  14분8초는 별도 실험을 포함한 전체 명령 시간이며 기본 test만의 시간으로 쓰지 않는다. 실행 JAR도 재생성했다.
- 수정 후 HTTP는 사전 저장2.251초/발송12.915초, 캠페인1000+실시간107=HTTP1107건을 전수 대조했다.
  중복·매핑 오류0, 혼합 APIp95 12.279ms/최대18.107ms(57개)였다.
  [HTTP 원본](experiments/s7-outbox-locks-2026-11-02-http-1000.json)을 보존했고 성능 개선/1m 검증으로 해석하지 않는다.
- 문서 링크/코드 블록/JSON 파싱·원본·민감 값 미포함/`git diff --check`를 확인했다.
  첫1m FAIL JSON의 SHA-256도 기존과 같았다. 종료된 실행 로그·JUnit과 컨테이너 상태를 대조했고 잔존 Ryuk는 없었다.
  사용자 DB/.env/IntelliJ와 무관한 컨테이너·volume은 변경하지 않았다.
- [설계 판단·위험·공식 문서·측정 경계](S7_OUTBOX_CLAIM_LOCKS.md)를 정리했다.
  다음은 수정된10만 건 HTTP 경로와 접수 지연 재검증이다. 원래5초 timeout과100만 건 완료 항목은 미완료다.

## 2026-11-02 — S7 선점 수정 후10만 건 HTTP 재검증

- 시작 커밋40f00e7/main, 기존7개 로컬 커밋을 보존했다. 다음 미완료 항목인10만 건 HTTP 재검증을 실행했다.
  이번에는 운영/테스트 소스·스키마·자원·예산·동시성·probe5초 대기를 추가 변경하지 않는다.
- 첫 샌드박스 실행은 Gradle wrapper 캐시 .lck 쓰기와 Docker 소켓 접근 권한에서 차단돼 시작되지 않았다.
  테스트 미실행이며 JUnit 실패가 아니다. 같은 격리 실험 명령에 필요한 추가 권한으로 다시 실행했다.
- 명령: `caffeinate -i ./gradlew --no-daemon --console=plain campaignDeliveryLoad`.
  격리 MySQL8.4.8/2CPU·2GiB/buffer pool512MiB/Kafka4.0.2/1CPU·1GiB/Redis7.4.11/256MiB,
  JVM512MiB/역할별 Worker3·HTTP3·Redis3·Hikari4/queue0/총1000/s 중 캠페인200/s 예산을 유지했다.
- 실제 부하 task는 **BUILD SUCCESSFUL34분49초, 종료 코드0, 테스트1개 통과/실패·오류·스킵0**이었다.
  원본 UTC 시작2026-11-02T01:07:06/종료01:41:49, 날짜는KST2026-11-02다.
  사전 저장201.748초/발송1840.512초(약30분41초)/54.333건·초/최종 대조7.338초였다.
- 캠페인100000+실시간7269(단독50+혼합7219)=HTTP107269건을 DB/참조/성공 시도1회/offset/quota와 전수 대조했다.
  중복·매핑 오류·Mock429/5xx/busy/영구실패0, 캠페인 참조100000개와PK JOIN 계획을 확인했다.
  [수정 후 원본](experiments/s7-delivery-load-2026-11-02-100k-after-claim-fix.json)은 build 출력과 동일하다.
  SHA-256은aeadb57242660c8fe03d215d4acf6c680986de2f2ecc662f3a667b67bf703be4다.
- 실시간 APIp95 단독37.636ms/혼합203.917ms, 혼합 최대1904.024ms였다.5초 timeout은 미재현이다.
  DB 완료p95는 단독536.310ms/혼합748.935ms로 다른 경계다. 공유 자원 간섭과 SLO 한계는 남아 있다.
- 809개 샘플에 결측·비유한 값0. heap최대101.36MiB/MySQL cgroup1328.40MiB/스레드98,
  Hikari active 최대양쪽4/waiting 캠페인1·실시간0이었다. max/oom/oom_kill0, CPU throttle17회/1.817초 증가였다.
  평균 CPU는 샘플 구간 JVM약0.251코어/MySQL약0.452코어 상당, GC 누적증가10.549초였다.
  CPU·메모리·풀의 순간 관측을 병목/잠금 부재/과거1m OOM의 확정 진단으로 쓰지 않는다.
- 이전10만 건은 발송1346.993초/74.239/s였다. 이번은 정합성을 통과했으나 처리량이 낮아지고 API최대도 커졌다.
  혼합 APIp95 334.452→203.917ms 감소만으로 성능 개선을 주장하지 않는다. 공유 호스트 각1회이며 계측/총 실시간량도 다르다.
- 운영/테스트 소스가40f00e7과 같아 이번에는 해당 load task만 실행했다.
  기본269개는 직전40f00e7의 실제 통과 기록이고, 이번에 재실행한 것으로 보고하지 않는다.
- 이번 부하의 잠금 관련 간접 관측은 Hikari 대기/진행 지표이며 직접 data_locks를 수집하지 않는다.
  cgroup OOM/CPU throttle 카운터는 별도로 읽는다. 과거1m 원인으로 소급하거나 잠금 부재를 보장하지 않는다.
- Docker 상태를 추가 권한으로 읽어 테스트 컨테이너가 정리됐고 기존 notification-lab-mysql-1이 healthy임을 확인했다.
  원격 main도 읽기 전용으로9cf1786임을 확인했고 push하지 않았다. 사용자 DB/.env/IntelliJ/volume은 변경하지 않았다.
- 100만 건 재실행은 시작하지 않았다. 다음에는 같은 자원/예산/5초 제한/상한을 유지하고 새 데이터로 실행한다.
  현재 속도 단순 외삽 발송약5시간7분+이전 사전 저장약44분으로 약6시간 규모이며 실제1m에서 더 느리거나 재실패할 수 있다.
  100만 건 원래5초 timeout의 원인과 S7 전체 완료는 계속 미완료다.


## 2026-11-02 — S7 Relay 계측과 선택적 완료 저장 묶음 비교

- 사용자 요청에 따라 속도 개선과 레퍼런스 검토를 진행했다. 시작은e6c7103/main/원격보다8개 앞선 깨끗한 상태였다.
  기존 커밋/파일/실패 원본을 보존하고 별도 브랜치·서브에이전트·푸시는 만들지 않는다.
- 9개 고정 stage timer를 추가했다. DB는 프록시 바깥에서 연결/격리 설정·SQL·commit·복원까지 포함한다.
  ACK는 send/future 대기, Worker handler는 offset commit 전 반환 경계다. 중첩/병렬 누적 시간을 wall time 비율로 합산하지 않는다.
- 첫1만 건의 Relay 누적 비용은 claim62.429초/완료 저장37.235초/ACK10.231초였다.
  ACK 비동기화나 Worker 증설 대신 ACK 성공분의 완료 저장만 선택적으로 최대10건/단일 조건부 UPDATE·commit으로 바꿨다.
  각 행은 PROCESSING+자기 lease token이 맞아야 하고 실제 변경 행 수만 카운터에 반영한다.
- 기본값1/범위1..10, 단건 선점/30초 lease/순차 send/ACK 확인/Worker offset/예산/자원은 유지한다.
  100ms는 blocking 호출 사이의 soft flush 기준이며 deadline 보장이 아니다.
  idle/오류/stop/loop 종료 시 남은 성공분을 flush하고 DB 실패/불확실한 commit은 lease 복구에 맡긴다.
  ACK 뒤 DB 저장 전 종료되면 재발행될 수 있는 묶음이 커진다. exactly-once를 주장하지 않는다.
- 격리 MySQL8.4.8/2CPU·2GiB/buffer pool512MiB/Kafka4.0.2/1CPU·1GiB/Redis7.4.11/256MiB,
  JVM512MiB/각 역할Worker3·HTTP3·Redis3·Hikari4/queue0/총1000/s 중 캠페인200/s·burst20/Mock1ms를 유지했다.
  캠페인 batch-size만1→10→1→10으로 변경하고 실시간은1을 유지했다. 각 실행은 새 JVM/새 격리 인프라다.
- 명령은 `caffeinate -i ./gradlew --no-daemon --console=plain campaignDeliveryPilot -PdeliveryTargets=10000 -PpublishBatchSize=1`
  또는 마지막 값을10으로 지정했다. 최초 계측 실행은 옵션 추가 전이므로 해당 인자 없이 같은 단건 경로였다.
  네 task 모두 BUILD SUCCESSFUL/종료 코드0/각1개 테스트 통과, 실패·오류·스킵0.
- 단건1차131.447초/76.076/s→최대10건1차100.122초/99.878/s,
  같은 최종 운영 소스의 단건2차109.349초/91.451/s→최대10건2차93.949초/106.440/s였다.
  시간 감소23.83%/14.08%, 처리량 증가31.29%/16.39%로 편차가 있다. 순서 고정/각2회/공유 호스트 결과다.
- 완료 저장 호출은10000→1036/1022회, 누적 시간37.235→4.402초/28.333→3.726초였다.
  선점과Worker DB 비용은 남아 있다. 타이머 count는 행 수가 아닌 트랜잭션 호출 수다.
- 캠페인40000+실시간2160=HTTP42160건을 전체 대상 참조/DB 성공 시도/HTTP/offset/quota로 대조했다.
  중복·잘못된 참조·Mock 오류0. 혼합 APIp95는23.484→21.700ms/18.635→21.790ms이므로 지연 개선을 주장하지 않는다.
- 네 원본은 `docs/experiments/s7-delivery-profile-2026-11-02-10k-publish-batch-{1,10,1-repeat,10-repeat}.json`에 수정 없이 보존했다.
  UUID/비유한 값 미포함을 확인한다. [경계·전체 결과·트레이드오프·레퍼런스](S7_RELAY_PUBLISH_BATCH.md)에 설명했다.
- Spring Kafka3.3.16/Kafka clients3.9.2/Spring JDBC6.2.19/Micrometer1.15.12 실제 캐시와 해당 버전 공식 문서,
  Spring JDBC sources의 트랜잭션 시작/commit/연결 복원 경계도 대조했다.
  AWS Outbox/중복 멱등성, Debezium CDC/Outbox SMT 대안을 검토했지만 새로운 connector는 도입하지 않는다.
- 100만 건과10만 건 task는 이번에 실행하지 않았다. 해당 task에도 `-PpublishBatchSize`를 전달할 수 있게 준비했다.
  기본값을10으로 올리기 전에 같은 옵션의10만 건과실시간 지연을 재검증한다.
  원래100만 건5초 timeout 원인과S7 전체 완료는 여전히 미완료다.
- 추가한 실제 MySQL 회귀4개(ACK 실패/개별 token fencing/묶음 SQL 롤백 후 재발행/stop의 부분 묶음 저장)와
  설정 범위 검증1개를 포함해 `caffeinate -i ./gradlew --no-daemon --console=plain build`를 실행했다.
  **BUILD SUCCESSFUL12분39초/종료 코드0/기본33개 클래스274개 통과/실패·오류·스킵0**을 최종 JUnit XML과 콘솔로 대조했다.
  별도1만 건 실험4회는 이274개와 구분한다. 실행 JAR도 새로 생성했고 BOOT-INF/lib의 실제 의존성 버전도 확인했다.
- 네 실험 명령 전체 시간은 각각3분9초/2분35초/2분42초/2분26초였으며, 표의 발송 시간과 다른 경계다.
  204개 자원 샘플 모두 MySQL cgroup max/oom/oom_kill0, Hikari 상태 결측0이었다.
  네 JSON의 UUID/비유한 값0과 로컬 문서 링크/코드 블록/`git diff --check`를 확인했다.
- 원격main을 읽기 전용으로 확인해9cf1786 그대로임을 확인했고 푸시하지 않았다.
  Git 기존 author identity를 유지한다. Docker에는 기존 notification-lab-mysql-1만 healthy로 남아 있다.
  사용자 DB/.env/IntelliJ/volume과 기존100만 건 FAIL 원본은 변경하지 않았다.


## 2026-11-02 — S7 완료 저장10만 건 비교와 전체 스캔 잠금 수정

- 시작37d4d3c/main/원격 추적보다9개 앞선 깨끗한 상태. AGENTS/CONTRIBUTING/PLAN/README/기존 기록을 읽고,
  운영·Java 부하 코드는 그대로 단건→구 OR10건 각1회를 실행했다. 두 실행 전후37d4d3c와 Java/Gradle diff가 없음을 확인했다.
- 첫 실행 명령의 자동 권한 검토는 deadline으로 프로세스 생성 전에 실패했다. 테스트 미실행이며 JUnit 실패가 아니다.
  도구 안내에 따라 같은 명령을1회 재시도해 시작했다. 사용자 MySQL/볼륨/IntelliJ/.env는 변경하지 않는다.
- 명령: `caffeinate -i ./gradlew --no-daemon --console=plain campaignDeliveryLoad -PpublishBatchSize=1`, 이후값10.
  실시간은1을 유지하고 Worker/HTTP/Redis/Hikari/queue/CPU/메모리/예산/5초 probe/상한을 고정했다.
  두 task는 각각 BUILD SUCCESSFUL28분47초/21분59초, 종료 코드0/각1개 테스트 통과/실패·오류·스킵0이었다.
- 단건 사전 저장177.923초/발송1512.978초·66.095/s/대조2.554초,
  구 OR10건 사전 저장152.264초/발송1132.741초·88.281/s/대조2.095초였다.
  캠페인200000+실시간10611=HTTP210611건의 대상 참조/성공 시도1회/Outbox/HTTP/전체 offset/quota를 전수 대조했고 중복·매핑·Mock 오류0.
- 구 OR 처리량은33.57% 증가했지만 혼합 APIp95 297.106→334.034ms/최대1231.243→1731.628ms로 악화했다.
  DB 완료p95 853.201→913.679ms도 다른 경계다. 단독 APIp95도16.143/24.658ms로 달랐다.
  각1회/고정순서/공유 호스트/closed-loop probe의 한계와 성능·지연의 트레이드오프를 함께 남긴다.
- 완료 저장 호출100000→10462/누적520.225→121.232초, 선점729.639→750.444초/ACK55.817→50.676초였다.
  각 구간은 중첩/병렬 caller 시간이며 wall time 비율로 모두 합산하지 않는다.
- 자원샘플686/515개에서 heap최대101.407/104.356MiB, MySQL cgroup1324.871/1268.355MiB,
  스레드97/97/Hikari waiting최대 C1·RT0으로 관측됐다. max/oom/oom_kill0과 상태 결측0을 확인했다.
- 새 읽기 전용 Python 도구로 해당 로그의 Testcontainers MySQL만 검증해60초마다 전역 row lock/순간 wait edge를 관찰했다.
  28/21회·오류0, 마지막 누적값3회18ms·최대11ms/2회1ms·최대0ms였다. 순간 대기/edge는 관측값0이었다.
  마지막 샘플은98391/95165건 시점이라 이후 구간을 포함하지 않는다. 관찰 오버헤드와 전역 지표의 귀속 한계를 기록했다.
- [전체 조건·원본·레퍼런스](S7_RELAY_100K_COMPARISON.md)에 네 HTTP/관찰 원본을 수정 없이 보존했다.
  Python 대상 검증/오류 미노출/기존 파일 보존/종료 후 미접근/예상 밖 필드 거절5개는0.006초/종료 코드0으로 통과했다.
- 별도100100행 SQL 진단1차는19초/실패1이었다. 구 OR10건의 PRIMARY/index 전체 스캔·추정rows1과104103개 레코드 잠금,
  실시간 완료/접수 약1초 대기 및 SKIP LOCKED 선점의 빈 결과를 관측했다. [첫 FAIL 원본](experiments/s7-relay-100k-2026-11-02-publish-locks-before.json)을 보존했다.
- `published(List)`를 같은 트랜잭션의 기본키 단건 UPDATE최대10번으로 바꿨다. 단건 경로와ACK/token/시도/offset/기본값1은 유지했다.
  UPDATE 개수 대신 연결·트랜잭션 관리/commit 횟수를 줄이고, 각 접근을 기본키 조건으로 좁히는 선택이다.
- 수정 경로의1차 가드는17초/실패1이었다. RT세 경로는 진행했지만 물리 잠금11개를10행과 비교해 실패했다.
  당시 추가1개의 LOCK_DATA 분류는 미수집이라 정확한 의미를 확정하지 않는다. [가드 FAIL](experiments/s7-relay-100k-2026-11-02-publish-locks-initial-guard-failure.json)도 보존했다.
- v3는 EXPLAIN을 별도 autocommit 연결로 분리하고 supremum/unknown을 집계했다. 키/행/연결/트랜잭션 ID는 출력하지 않는다.
  현재 단건/10건과구 OR대조군을 같은 fixture에서 비교해19초/종료 코드0/테스트1개 PASS였다.
  [최종 원본](experiments/s7-relay-100k-2026-11-02-publish-locks-after.json)은 현재 실제 PRIMARY레코드1/10·대기edge0·RT선점1·세 경로 holder 종료 전 완료를 확인했다.
  구 OR대조군은 실제100100+supremum4003/edge2/RT선점0/완료·접수 대기를 다시 관측했다. 이는 실제 UPDATE만으로의 결함 재현이다.
- 새 SQL 회귀를 기본 테스트에 포함하고 `outboxLockProbe --tests '*OutboxPublishProbeTest'`로도 선택할 수 있게 했다.
  수정판의 전체 빌드 후 추가10만 건을 재검증한다. 기존두 실험을 수정판 수치로 사용하지 않으며,
  단건 경로 무변경으로 첫 기준선을 재사용하는 한계도 구분한다. 원래100만 건 timeout 원인은 계속 미확정이다.
- 수정판 `caffeinate -i ./gradlew --no-daemon --console=plain build`는 **BUILD SUCCESSFUL11분56초/종료 코드0**.
  최종 JUnit XML과 콘솔에서 **34개 클래스275개 통과/실패·오류·스킵0**을 대조했다. 신규 SQL회귀1개가275개에 포함된다.
  실행 JAR도 생성했다. 기본 회귀 안의 재실행에서도 현재 PRIMARY 실제레코드1/10과RT세 경로 진행,
  구 OR대조군의100100개 실제 레코드 잠금/실시간 선점0을 확인했고 [별도 원본](experiments/s7-relay-100k-2026-11-02-publish-locks-build.json)을 보존했다.
- 원격main은 읽기 전용 확인에서9cf1786 그대로였다. 기존 author identity를 유지한다.
  수정 코드와 기본 회귀를 로컬 커밋으로 고정한 뒤 해당 버전의 추가10만 건을 실행한다. 푸시는 하지 않는다.
- 구 OR결함은 과거100만 건 실패 뒤37d4d3c에서 추가한 최적화의 별도 문제다. 원래 접수timeout 원인으로 소급하지 않는다.


## 2026-11-02 — S7 잠금 수정판557b2cf의 추가10만 건 재검증

- 수정/회귀 증거를557b2cf로 로컬 커밋한 뒤 깨끗한 main에서 실행했다. 명령은
  `caffeinate -i ./gradlew --no-daemon --console=plain campaignDeliveryLoad -PpublishBatchSize=10`.
  이후에도 Java/Gradle/scripts diff가 없음을 확인했다. Worker/연결/자원/예산/5초 제한과 실시간1을 유지했다.
- **BUILD SUCCESSFUL22분53초/종료 코드0/부하 테스트1개 통과/실패·오류·스킵0**이었다.
  원본 UTC2026-11-02T06:12:52~06:35:41. 사전 저장170.306초/발송1170.631초·85.424/s/최종 대조2.174초.
- 캠페인100000+실시간4639=HTTP104639건의 대상 매핑/DB 성공 시도1회/Outbox/HTTP/전체 offset/quota를 전수 대조했다.
  중복·매핑·Mock 오류0. [수정판 원본](experiments/s7-relay-100k-2026-11-02-point-batch-10.json)의 SHA-256은
  8a8c9b9f39571a35245edeac8504c409de7d9be66bcc8c62f61cd704ecc61c45다.
- 변경하지 않은 단건 경로의 첫 기준선1512.978초/66.095/s를 재사용하면 발송22.63% 감소/처리량29.24% 증가다.
  같은 최종 커밋으로 단건을 다시 실행한 결과와 구분한다. 구 OR10건보다는 시간3.34% 증가였다.
  각1회/순서 고정/공유 호스트/closed-loop 실시간 요청이라 통계적 유의성·운영 성능을 확정하지 않는다.
- 혼합 APIp95 364.875ms/최대1310.980ms는 기준선297.106/1231.243ms보다22.81%/6.48% 증가였다.
  단독 APIp95도21.338ms로 기준선16.143ms와 달랐다. DB 완료p95 936.484ms/최대2058.530ms는 다른 경계다.
  처리량 개선을 실시간 SLO 개선으로 표현하지 않고 기본값1/실시간1을 유지한다.
- 완료 저장 트랜잭션 호출10454회/누적156.363초, 선점753.411초/ACK52.936초였다.
  SQL UPDATE개수 대신 연결·트랜잭션 관리/commit을 묶는 구현이다. 중첩/병렬 시간은 wall time비율로 합산하지 않는다.
- 자원샘플528개/heap최대104.355MiB/MySQL cgroup1273.152MiB/스레드97/Hikari waiting최대 C1·RT0.
  max/oom/oom_kill0, Hikari 상태 결측0이었다. [외부 잠금 관찰](experiments/s7-relay-100k-2026-11-02-point-batch-10-locks.json)은22회/오류0,
  마지막 누적4회·5ms·최대2ms/순간대기·edge0을 관측했다. 마지막 샘플은96243건/1125.675초라 이후 구간은 포함하지 않는다.
- 이번 세100k 실험의 합계는 캠페인300000+실시간15250=HTTP315250건이며 연속30만 건 단일 부하가 아니다.
  전체275개 기본 회귀는557b2cf 직전 실제 통과 기록이다. 운영/테스트 코드가 같아 이번에는 부하 task만 실행했으며275개를 재실행한 것으로 기록하지 않는다.
- 세 실험/SQL 회귀의 성공·실패 원본을 모두 보존했다. UUID/비유한 값 미포함, 문서 링크/코드 블록과diff를 확인한다.
  Docker에는 기존 notification-lab-mysql-1만healthy로 남았고 사용자DB/.env/IntelliJ/volume은 변경하지 않았다.
  원래100만 건 FAIL 원본도 변경하지 않는다.100만 건 실행과 원래timeout 원인 항목은 계속 미완료이며 푸시하지 않는다.


## 2026-11-02 — S7 고정 자원에서 로컬 Relay1/2개 사전 비교

- 시작ada8b67/main/원격 추적보다11개 앞선 깨끗한 상태에서 AGENTS/CONTRIBUTING/PLAN/README/이전 기록을 확인했다.
  10만 건 선점 caller753.411초를 근거로 Worker 증설 전에 Relay 공급 경로의 병렬화 효과부터 확인한다.
- 운영 Java/resources와 기존 기본값은 변경하지 않았다. 테스트 전용 `relayScalingPilot`은 같은 앱의 OutboxStore/DB 풀/producer를
  공유하는 기존 Relay 루프를1..3개만 실행한다. 자동 캠페인 Relay bean을 끄고 중복 실행을 거부한다.
  1개 기준선도 같은 테스트 전용 스케줄러를 사용하므로 예전 자동 Relay 실험의 기준선을 재사용하지 않는다.
- 같은 루프는 겹치지 않으며 완료 뒤100ms마다 고정 작업만 등록한다. 개별 메시지 future/제출 대기열은 추가하지 않는다.
  예상 밖 주기 작업 종료를 감지하고 종료 시 신규 선점을 멈춰 실행 중 ACK/부분 완료 저장을 기다린다.
  정상30초+interrupt15초의 종료 상한을 두고 두 번 close해도 중복 stop하지 않는다.
- 첫1개 실행은44초/테스트1개 실패였다. KafkaTemplate의 Boot 제네릭 bean 조회 오류로 캠페인 Relay 시작 전에 실패했다.
  [원본](experiments/s7-relay-scaling-2026-11-02-initial-bean-failure.json)을 보존한 뒤 조회를 수정했다.
  이후 네 실행은 [동일 파일 SHA-256](experiments/s7-relay-scaling-2026-11-02-source.json)을 확인했다.
- 명령은 `caffeinate -i ./gradlew --no-daemon --console=plain relayScalingPilot -PcampaignRelays=N`이다.
  N을1→2→2→1로 바꾸고 매번 새 JVM/격리 MySQL/Kafka/Redis에서1만 건을 실행했다.
  완료 저장10건/실시간1건, Worker3/HTTP3/Redis3/Hikari4/queue0, 캠페인200/s·burst20, CPU/메모리와5초 접수 제한을 유지했다.
- 네 task는 BUILD SUCCESSFUL/종료 코드0/각1개 통과/실패·오류·스킵0이며 전체 시간2분7초/1분56초/2분30초/2분1초였다.
  발송82.887/72.809/101.600/76.114초, 처리량120.646/137.346/98.426/131.381/s였다.
  캠페인40000+실시간1711=HTTP41711건의 전체 매핑/성공 시도1회/PUBLISHED/HTTP/offset=end/quota를 대조했다. 중복·매핑·Mock 오류0.
- 혼합 APIp95는13.578/11.583/40.438/10.804ms, 최대29.703/22.956/1490.550/78.501ms였다.
  2개 조건이 일관되게 빠르지 않았고 원래100만 건 timeout 원인은 여전히 미확정이다. 앱 기본값과 배포 설정을 유지한다.
- 선점 호출10038/10554/10715/10048회·누적44.837/53.745/87.916/39.040초였다.
  병렬 타이머는 중첩되고 빈 폴링도 포함한다. 이 시간을 DB 내부 실행/잠금 대기로 단정하지 않는다.
  159개 자원샘플의 MySQL max/oom/oom_kill0, Hikari 결측0이었다. 캠페인 waiting최대0/2/1/0, 실시간0이었다.
  동시 루프1/2/2/1과 종료 후 active0/대기 주기 작업0/terminated=true를 확인했다.
- [문제·고정 조건·실제 수치·한계·버전별 공식 참고](S7_RELAY_SCALING_PILOT.md)에 네 PASS/첫 FAIL 원본을 보존했다.
  같은 JVM/공유 풀의 로컬 병렬 처리이며 여러 서버 자원 증가나 다중 프로세스 동시 강제 종료를 입증하지 않는다.
- 전체 기본 회귀 `caffeinate -i ./gradlew --no-daemon --console=plain build`는 **BUILD SUCCESSFUL12분8초/종료 코드0**이었다.
  최종35개 클래스 JUnit XML과 콘솔에서 **278개 통과/실패·오류·스킵0**을 대조했다. 기존275개+신규 실행기3개다.
  운영 코드가 같아 두 실행 JAR task는UP-TO-DATE였고 기본 회귀는 실제 재실행했다. 별도 발송4회와 구분한다.
- 실험/기본 회귀 뒤에도 소스 SHA-256과 원래100만 건 FAIL 원본이 같음을 확인했다. 새 JSON의 UUID/비유한 값/민감 필드가 없음을 확인했다.
  문서 링크/코드 블록/diff를 확인했다. Docker에는 기존 notification-lab-mysql-1만healthy로 남았고 사용자DB/.env/IntelliJ/volume은 변경하지 않았다.
- 원격main은 읽기 전용 확인에서9cf1786 그대로였다. Git identity를 유지하고 푸시는 하지 않는다.
  다음 작은 진단은 DB 연결 취득/SQL/commit과Kafka offset 저장 비용의 세분화다.100만 건과 유료/외부 서버 실행은 이번에 하지 않았다.


## 2026-11-02 — S7 JDBC와 Kafka offset 저장 비용 세분화

- 시작c64acf0/main/원격 추적보다12개 앞선 깨끗한 상태. AGENTS/CONTRIBUTING/PLAN/README/직전 결과를 확인했다.
  `deliveryTimingPilot`에서만 기존 Hikari/consumer를 감싸고 기존 트랜잭션 proxy 바깥에 계측 scope를 둔다.
  운영 Java/resources/DB 격리 수준/Worker/공유 예산/기본 RECORD ack는 변경하지 않는다.
- 역할별7개 scope×14개 고정 구간의 count/failures/total/mean/max만 저장한다. JDBC 연결 획득/prepare/execute/commit/rollback,
  autoCommit/격리 수준/read-only 확인·설정/반환과 실제 consumer.commitSync를 나눴다. SQL/인수/레코드/예외 메시지는 추가 저장하지 않는다.
  자체 offset 커밋·재시도나 무한 표본 버퍼를 만들지 않는다. JDBC 예외와 consumer 인수/예외를 그대로 전달한다.
- 첫 단위 검증은6초/8개 중7개 통과·1개 실패였다. 바인딩 인수 없는 update(String) 호출을 테스트 JDBC 대역에 맞는 PreparedStatement 경로로 수정했다.
  이후 단위/진단9개가4초/실패·오류·스킵0으로 통과했다. 기존 운영 코드 오류를 수정한 것으로 표현하지 않는다.
- 실제1천 건 v1/v2는42초/44초·각1개 테스트 통과/실패·오류·스킵0이었다.
  각각 캠페인1000+실시간97/98건을 전수 대조했다. v2는 연결 상태를 autoCommit/격리/read-only로 나누고 계측 count 가드를 추가했다.
- 후속 명령은 `caffeinate -i ./gradlew --no-daemon --console=plain deliveryTimingPilot -PcampaignRelays=N`이다.
  같은 [소스 SHA-256](experiments/s7-delivery-timing-2026-11-02-source.json)으로 N=1→2→2→1, 각1만 건을 새 JVM/격리 인프라에서 실행했다.
  완료 저장10/실시간1, Worker3/HTTP3/Redis3/Hikari4/queue0, 캠페인200/s·burst20, 자원과5초 probe 제한을 유지했다.
- 네 task는 BUILD SUCCESSFUL/종료 코드0/각1개 테스트 통과/실패·오류·스킵0이었다.
  task2분13초/2분19초/1분55초/2분1초, 발송83.733/93.196/71.916/78.354초·119.428/107.301/139.050/127.626/s였다.
  캠페인40000+실시간1717=HTTP41717건의 대상 매핑/성공 시도1회/PUBLISHED/HTTP/offset=end/quota를 전수 대조했고 중복·매핑·Mock 오류0.
- 혼합 APIp95 13.432/20.123/9.157/9.665ms, 최대57.451/145.725/22.851/69.546ms였다.
  조건별2회·공유 호스트·closed-loop 관찰이며 무계측 대비 개선/오버헤드와 운영 성능을 확정하지 않는다.
- 캠페인 Worker 각 단계의 call/acquire/commit은 각10000회, JDBC execute40000/80000/40000회였다.
  현재 permit 전후 claim을 쓰는 설정에서 **알림당 Worker JDBC16회/DB 커밋3회**다. 다른 프로필/재시도에 일반화하지 않는다.
  Kafka commitSync10000회씩/평균5.986/5.892/5.770/6.008ms, async0/계측 실패0이었다. 기존 handler 밖의 비용이다.
- Worker 세 단계의 연결 획득 합은 건당0.004/0.715/0.561/0.004ms, JDBC 실행 합8.232/10.055/6.796/7.124ms,
  DB 커밋 합3.639/4.338/2.981/3.020ms였다. caller 시간이며 DB 서버 CPU/잠금/fsync의 단일 원인으로 단정하지 않는다.
  Outbox 선점의 격리 상태 확인·설정/복원도 호출당1.033/1.344/1.114/0.910ms로 관측했고 기존 READ COMMITTED를 유지했다.
- 157개 자원 샘플의 Hikari 결측0/maximum4, MySQL max/oom/oom_kill0이었다.
  캠페인 waiting최대0/1/1/0·실시간0, 앱 종료 뒤 pool1개씩 종료/consumer3개 wrapping/activeCalls0을 확인했다.
  관찰 scope 밖의 API/지표 SQL과 구간 사이의 매핑/스케줄링/계측 오버헤드는 모두 귀속하지 않는다.
- [범위·수치·버전별 공식 소스·원본](S7_DELIVERY_TIMING.md)에1천 건2회와1만 건4회 원본을 수정 없이 보존했다.
  다음 후보는 DB 완료된 offset의 유한 개수·시간 기준 묶음 저장 비교다. 재소비 범위와 미완료 offset/오류/리밸런스 검증 전 기본값을 바꾸지 않는다.
- 전체 `caffeinate -i ./gradlew --no-daemon --console=plain build`는 **BUILD SUCCESSFUL11분36초/종료 코드0**이었다.
  최종36개 클래스 JUnit XML과 콘솔에서 **283개 통과/실패·오류·스킵0**을 대조했다. 기존278개와 신규 계측 검증5개다.
  두 실행 JAR task는UP-TO-DATE였고 기본 회귀는 실제 재실행했다. 별도1천/1만 건과 구분한다.
- 이후에도 계측 소스 해시와 원래100만 건 FAIL 원본이 같음을 확인했다. 새 원본의 UUID/비유한 값/민감 필드 미포함과 문서 링크/diff를 확인했다.
  Docker에는 기존 notification-lab-mysql-1만healthy로 남았고 사용자DB/.env/IntelliJ/volume은 변경하지 않았다.
  기존 Git identity를 유지하고 원격main은 읽기 전용 확인에서9cf1786이었다.
  100만 건 재실행/원래5초 timeout 원인 확정/다중 서버 배포/실제 공급자 호출/푸시는 하지 않았다.


## 2026-11-02 — S7 완료 offset의 COUNT_TIME 비교와 장애 경계

- 시작1029686/main/원격 추적보다13개 앞선 깨끗한 상태에서 AGENTS/CONTRIBUTING/PLAN/README/직전 진단을 확인했다.
  기본 RECORD와 운영 Java/resources를 유지하고 테스트에서만 COUNT_TIME(ackCount10/ackTime100ms)을 선택한다.
  동기 commit/asyncAcks=false/autoCommit=false/max.poll.records1/poll500ms와 기존 종료 customizer를 확인한다.
  시간 기준은 처리/poll 사이의 확인이며 강제100ms 마감이나 총 중복 횟수 상한이 아니다.
- 사전 정책/처리 중 offset/진입 timeout·큐 포화/DB 실패/종료·재시작/저유량 tail13개가1분8초에 통과했다.
  실제 프로세스 종료·재소비, 두 모드의7초 pause(5초 max.poll 초과)와 리밸런스·Worker/Relay 재시작6개도4분48초에 통과했다.
  최종 설정 가드를 보강한 소스는 전체 회귀에서 다시 확인한다. 사용자의 DB/volume을 초기화하지 않는다.
- [복수 완료 레코드의 사전 실제 JVM 종료](experiments/s7-offset-commit-2026-11-02-safety-pilot.json)는 warmup20+12건을 수동partition0에 발행했다.
  native commit RPC 전 broker22/DB 완료28에서 강제 종료했다. 완료6건을 재소비했으며 이6건의 공급자 재호출은0이었다.
  재시작 호출4건은 아직 처리 전인 작업이다. 전체32개 호출/성공 시도32개, 완료 행 fingerprint 불변/최종offset32를 확인했다.
  RPC 응답 유실 전체 시나리오나 공급자 성공 뒤 DB 저장 실패의 exactly-once를 증명하지 않는다.
- `offsetCommitPilot -PdeliveryTargets=1000 -PoffsetMode=COUNT_TIME`은1분1초/테스트1개 통과·실패/오류/스킵0이었다.
  캠페인1000+실시간98건 전수 대조, sync commit245회/누적1.921초였다. 이후 마지막 offset 관측을 보강했다.
- 명령은 `caffeinate -i ./gradlew --no-daemon --console=plain offsetCommitPilot -PoffsetMode=MODE`이다.
  [같은 최종 소스 SHA-256](experiments/s7-offset-commit-2026-11-02-source.json)으로 RECORD→COUNT_TIME→COUNT_TIME→RECORD, 각1만 건을 새 인프라에서 실행했다.
  캠페인Relay2/완료 저장10, 실시간Relay1/완료 저장1/RECORD, Worker3/HTTP3/Redis3/Hikari4/queue0과 캠페인200/s·burst20,
  CPU/메모리/Mock1ms/기존 계측/5초 probe 제한을 유지했다. 정책 외 독립 조건을 바꾸지 않는다.
- 네 task 모두 BUILD SUCCESSFUL/종료 코드0/각1개 통과/실패·오류·스킵0이었다. 전체2분38초/2분31초/2분16초/2분49초,
  캠페인 완료 관측101.078/86.897/81.048/111.512초·98.933/115.079/123.383/89.676건·초였다.
  캠페인40000+실시간1910=HTTP41910건을 전체 대상/성공 시도1회/PUBLISHED/HTTP/각partition offset=end/quota로 대조했다. 중복·매핑·Mock 오류0.
- commitSync10000/2163/2019/10000회·누적63.178/15.278/14.271/64.823초였다. 호출 수78.37%/79.81% 감소다.
  처리량 증가16.32%/37.59%를 관측했지만 각2회·공유 호스트·순차 probe이므로 통계적 유의성/운영 성능을 확정하지 않는다.
  캠페인 완료 관측→offset 확인0.294/0.410/0.403/0.274초에는 자원 샘플/실시간 완료 대기와 Admin 관측 비용이 포함된다.
- 혼합 APIp95는18.228/20.974/20.123/25.399ms, 최대55.399/115.243/184.559/264.187ms였다. 실시간 SLO 개선을 보장하지 않는다.
  COUNT_TIME b의 초기 read-only 확인3회 증가는 관측했으나 quota/재소비 분류 counter를 수집하지 않아 원인을 단정하지 않는다.
  시도 시작/완료/공급자 호출은각10000회였고 계측 실패와async commit은0이었다.
- [전체 조건/공식 레퍼런스/원본/한계](S7_OFFSET_COMMIT_PILOT.md)를 기록한다.178개 자원 표본의 Hikari 상태와 MySQL 자원 상한을 확인한다.
  기본 RECORD를 유지하고 다음은 같은 정책의10만 건/실시간 tail 검증과 적용 여부 판단이다.
- 최종 `caffeinate -i ./gradlew --no-daemon --console=plain build`는 **BUILD SUCCESSFUL15분22초/종료 코드0**이었다.
  최종37개 클래스 JUnit XML과 콘솔에서 **294개 통과/실패·오류·스킵0**을 대조했다. 기존283개+새 정책/모드11개다.
  최종 설정 가드와 새 모드 장애 경계를 실제 재실행했다. 두 실행 JAR task는UP-TO-DATE였고 운영 Java/resources diff0을 확인했다.
- [전체 회귀의 실제 중단 원본](experiments/s7-offset-commit-2026-11-02-safety-build.json)은 broker22/DB 완료27이었다.
  완료5건 재소비/해당5건 공급자 재호출0, 미처리5건 신규 호출/전체32건 호출32회, 완료 행 불변/최종offset32를 재검증했다.
- 이후 소스 해시/원래100만 건 FAIL 원본이 같고 새 원본에 UUID/비유한 값/민감 필드가 없음을 확인했다.
  문서 링크/diff를 확인한다. Docker에는 기존 notification-lab-mysql-1만healthy로 남았고 사용자DB/.env/IntelliJ/volume은 변경하지 않았다.
  기존 Git identity와 원격main9cf1786을 읽기 전용으로 확인했다.100만 건 재실행/유료 서버/실제 공급자/푸시는 하지 않았다.
  새 모드의 모든 HTTP 재시도/Retry·DLQ/commit RPC 장애 조합을 검증한 것은 아니며 후속 경계 보강과10만 건 확대 뒤 적용을 판단한다.


## 2026-11-02 — S7 COUNT_TIME HTTP/commit RPC 오류 경계와 10만 건 도구

- 시작651f8d7/main/원격 추적보다14개 앞선 상태. AGENTS/CONTRIBUTING/PLAN/README/직전 결과를 확인했다.
  운영 Java/resources/기본 RECORD를 유지하고 테스트 전용 검증을 보강한다. 새 브랜치/푸시는 하지 않는다.
- 실제 MySQL/Kafka/HTTP에서 성공·500·429·영구400·응답 timeout을 RECORD/COUNT_TIME 각각 검증한다.
  시도1/3/3/1/3회, Retry0/2/2/0/2개, DLQ0/1/1/1/1개와 DB 이력/Outbox PUBLISHED/각 partition offset=end를 대조했다.
  timeout은 Mock 수락3회/클라이언트 UNKNOWN이다. 외부 공급자의 exactly-once를 주장하지 않는다.
- 실제 native commitSync 전 미전송 오류와 실제 성공 후 호출자 응답 유실을 동일한 TimeoutException으로 주입했다.
  후속 commit을 계속 거절하고 consumer group 이탈 후 DB/브로커를 따로 읽는다. 물리 네트워크 단절 재현과 구분한다.
  동일 group/topic 새 JVM의 정상 재할당으로 복구하며 seek/reset/재발행을 하지 않는다.
- 사전 선택 실행21개는2분47초/종료 코드0/실패·오류·스킵0이었다.
  RPC 전 broker22/DB28/완료6건 재소비, RPC 후 broker29/DB29/재소비0, HOLD broker22/DB24/재소비2였다.
  각32건 공급자 호출/성공 시도32개, 완료 행 fingerprint 불변/최종offset32를 확인했다.
- 최종 전체 `caffeinate -i ./gradlew --no-daemon --console=plain build`는 **16분2초/종료 코드0**이었다.
  새37개 클래스 JUnit XML과 콘솔에서 **302개 통과/실패·오류·스킵0**을 대조했다. 기존294개+신규8개다.
  기본 회귀는 실제 재실행했고 두 실행 JAR task는UP-TO-DATE/운영 Java/resources diff0이었다.
  RPC 전 broker21/DB28/재소비7, RPC 후 broker27/DB27/재소비0, HOLD broker22/DB27/재소비5에서도 같은32건 불변 조건을 확인했다.
- [판단 기준/상세/실제 원본/공식 참고](S7_OFFSET_100K.md)를 보존했다. 코드15개 SHA-256을 고정했다.
  `offsetCommitLoad`는100000건만 허용하며 test/check/build와100만 건 task에 자동 연결하지 않는다.
  다음 실행은 같은 소스/자원/Relay2/Worker3/Hikari4/200/s/완료 저장10/5초 probe 제한의 RECORD→COUNT_TIME 각1회다.
  10만 건 발송 결과를 아직 완료로 기록하지 않는다.
- 기존 Git identity와 원격main9cf1786을 읽기 전용 확인했다. 원래100만 건 FAIL 원본 해시는 불변이다.
  사용자DB/.env/IntelliJ/volume, 기본 정책과 예산을 유지한다. 실제 공급자/유료 서버/100만 건/푸시는 실행하지 않는다.


## 2026-11-03 — S7 같은 코드의10만 건 offset 비교와 기본 정책 판단

- 직전 검증/도구를740d2ef로 로컬 커밋한 뒤 코드를 고정했다. 운영 Java/resources와 기본 RECORD는 바꾸지 않는다.
  같은 소스15개 SHA-256으로 새 JVM/격리 인프라에서 RECORD→COUNT_TIME 각1회 실행했다.
  원본/실행 명령/시각은UTC11월2일이며 한국 시간으로 첫 실행11월2~3일/둘째11월3일이다.
- 두 명령은 `caffeinate -i ./gradlew --no-daemon --console=plain offsetCommitLoad -PoffsetMode=RECORD` 및COUNT_TIME이다.
  두 task는 BUILD SUCCESSFUL/종료 코드0/각1개 통과/실패·오류·스킵0이었다. 전체25분45초/22분12초,
  사전 저장207.384/202.838초, 실제 발송1295.666/1083.862초·77.180/92.263건·초였다.
  사전 저장/단독50건/전수 대조 시간은 발송 처리량 계산에서 제외하고 캠페인 앱 기동은 포함한다.
- 캠페인Relay2/완료 저장10, 실시간Relay1/완료 저장1/RECORD, 역할별Worker3/HTTP3/Redis3/Hikari4/queue0,
  캠페인200/s·burst20/Mock1ms/기존 자원·세부 계측·5초 접수 제한을 고정했다. 정책 외 독립 조건을 바꾸지 않는다.
- 캠페인200000+실시간9563=HTTP209563건의 전체 대상 매핑/성공 시도1회/Outbox PUBLISHED/HTTP 감사/각partition offset=end/quota를 대조했다.
  HTTP 중복·매핑 오류·Mock 제한/오류0이었다. 두 독립10만 건이며 단일 연속20만 건이 아니다.
- 캠페인 commitSync100000→23891회(76.109% 감소), 누적625.088→162.836초/평균6.251→6.816ms였다.
  RPC가 빨라진 결과가 아니라 호출 횟수를 줄였다. 누적 병렬 시간과 wall time 감소를 동일시하지 않는다.
  마지막 완료 관측→offset 확인0.636/0.499초에는 자원 표본/실시간 대기/Admin polling이 포함된다.
- 처리량은19.54% 높고 발송시간은16.35% 짧게 관측됐다. 반면 실시간 접수p95가97.694→202.728ms,
  p99는657.172→746.529ms, 최대1410.648→1548.912ms, DB 완료p95는634.922→787.659ms로 높게 관측됐다.
  단독APIp95도21.292/23.440ms로 달랐고 조건별1회/공유 호스트/closed-loop/실시간 표본5219/4244개다.
  통계적 유의성/실시간 SLO 개선/지연 악화의 확정 원인을 주장하지 않는다. **기본 RECORD를 유지한다.**
- COUNT_TIME 초기 read-only 확인100001회 중 추가1회의 quota/재소비 원인은 분류하지 못했다.
  시도 시작/완료/공급자 호출은각100000회였다. 기존 Worker JDBC16회/DB 커밋3회의 비용도 남아 있다.
- 총1055개 자원 표본의 Hikari 결측0/maximum4/MySQL max·oom·oom_kill0, waiting 최대 C2/RT0을 확인했다.
  각 역할 pool1개 종료/consumer3개 wrapping/activeCalls0, Relay2개 종료/대기 작업0을 확인했다.
- [전체 수치·해석·공식 참고](S7_OFFSET_100K.md), 두 원본과 코드 커밋/해시를 담은 [실행 메타데이터](experiments/s7-offset-100k-2026-11-02-execution.json)를 기록했다.
  다음 작은 단계는 제한된 표본에서 실시간 지연의 공유 DB/연결/처리 구간을 분리 관찰하는 것이다.
  기본 정책 전환이나100만 건 재실행을 먼저 하지 않는다. 원래100만 건의5초 timeout은 이번에 재현되지 않았고 원인도 미확정이다.
- 소스15개와 원래100만 건 FAIL 원본 해시의 불변을 확인했다. 두 원본은 수정 없이 복사했다.
  실험 뒤 Docker에는 기존 notification-lab-mysql-1만healthy로 남았다. 사용자DB/.env/IntelliJ/volume을 변경하지 않았다.
  전체 기본 회귀302개 통과 이후 테스트 코드는 변경하지 않았다. 후속 변경은 실제 결과와 계획 문서뿐이다.
  실제 공급자/여러 서버/유료 자원/100만 건 재실행/푸시는 하지 않는다.


## 2026-11-03 — S7 실시간 접수 흐름과 요청별 JDBC 구간 진단

- 시작1e2307c/main/원격 추적보다16개 앞선 깨끗한 상태에서 AGENTS/CONTRIBUTING/PLAN/README/직전 결과를 확인했다.
  기본 비동기 POST는 NotificationService.send가 아니라 Controller→IdempotentSubmissionService.submit→SubmissionService.accept다.
  알림INSERT/역할UPDATE/작업INSERT/OutboxINSERT를한 연결/한 트랜잭션/SQL4회/DB commit1회로 처리한 뒤202를 반환한다.
  [호출 흐름·트랜잭션·측정 범위·학습 질문](S7_API_ADMISSION_TIMING.md)을 코드 링크와 함께 정리했다.
- 기존 계측에서 빠진 API 접수를 테스트 전용으로 보강했다. servlet 필터→accept 프록시 바깥→기존 DataSource 래퍼를 연결해
  같은 요청의 client/server/획득/prepare/개별SQL4회/commit/상태 확인·반환을 기록한다. 본문/수신자/ID/SQL/예외 메시지를 저장하지 않는다.
  순차 probe1개/trace최대5000행/고정 JDBC14구간/개별SQL4개/클라이언트5초/필터 완료 확인2초 상한을 둔다.
- apiAdmissionPilot은1000..10000건만 허용하며 기본build에 연결하지 않는다. 캠페인Relay2/완료 저장10,
  실시간Relay1/완료 저장1/RECORD, Worker3/Hikari4/HTTP3/Redis3/queue0/200/s와 기존CPU·메모리를 유지했다.
  운영 Java/resources/풀/격리 수준/기본 RECORD를 바꾸지 않는다.
- 첫 컴파일은 필터 제네릭 추론 오류로6초에 실패했다. 타입을 명시한 뒤 선택18개는5초/실패·오류·스킵0으로 통과했다.
  이후 개별SQL 시간과 상대시각을 보강하고 실험 소스9개 SHA-256을 고정했다.
- 1천 건COUNT_TIME 연결 검증은57초/종료 코드0/테스트1개 통과/실패·오류·스킵0이었다.
  캠페인1000+실시간94=HTTP1094건 전수 대조, 요청별SQL4회/획득·커밋·반환1회/rollback0과 구간 대응을 확인했다.
- 같은 소스로 새 JVM/격리 인프라에서1만 건RECORD→COUNT_TIME 각1회 실행했다. task2분36초/2분39초,
  사전 저장17.984/18.729초, 발송104.202/106.102초·95.968/94.249/s였다. 각1개 테스트 통과/실패·오류·스킵0/종료 코드0이었다.
  캠페인20000+실시간1034=HTTP21034건의 전체 대상 매핑/이력1회/Outbox PUBLISHED/HTTP 감사/각partition offset=end/quota를 대조했다.
  중복·매핑·Mock 제한/오류0, 실시간1034건 모두 요청별 계측이 대응됐다. 별도1천 건까지HTTP22128건이다.
- 혼합APIp95는21.373/26.318ms, 최대183.529/239.539ms, 서버 필터p95는18.939/22.439ms였다.
  commitSync10000→2612회로 줄었지만 이번1만 건의 처리량 개선은 미관측이다. 각1회/공유 호스트/새 계측 오버헤드가 있다.
  기존10만 건의97.694→202.728ms 수준의p95는 미재현이고 원래100만 건의5초 timeout 원인도 여전히 미확정이다.
- 혼합 응답 상위ceil(N×5%)24개씩의 같은 요청에서 평균SQL 합14.422/16.330ms·commit10.266/18.775ms,
  연결 획득2.578/0.254ms를 관측했다. 서로 다른p95를 빼서 원인을 계산하지 않는다.
  가장 느린COUNT_TIME 요청239.539ms에서는 SQL57.984ms/commit139.999ms/획득0.003ms였다.
  RECORD의다른 요청은획득59.933ms였지만실시간 pool waiting97개 표본은모두0이었다. 획득시간 전체를풀 대기로 단정하지 않는다.
- [원본·명령·시각·해시·가장 느린 요청 구간 요약](experiments/s7-api-admission-2026-11-03-analysis.json)을 보존했다.
  다음 작은 단계는 긴SQL/commit에서 DB 내부 잠금·I/O와 호출자 스케줄링을 구분할 증거를 제한적으로 수집하는 것이다.
  현재 증거만으로 풀/인덱스/격리 수준을 바꾸지 않고 기본 RECORD를 유지한다.
- 첫 전체 회귀는 새 테스트가 금지 문자열sql을 정상 숫자 필드sqlExecutionMillis에도 적용해1개 실패했다.
  156개 통과/1개 실패 확인 뒤중단/종료 코드130으로 보존했다. 전체 통과로 기록하지 않는다.
  검사식은실제SQL 원문 JSON 키로 좁혔다. 해당5개 테스트는4초/실패·오류·스킵0으로 통과했다.
  실험 소스와 최종 회귀 소스의차이는이테스트 검사식 한 줄뿐이다. 계측/운영 코드는변경하지 않았다.

- 최종 전체 `caffeinate -i ./gradlew --no-daemon --console=plain build`는 **16분58초/종료 코드0**이었다.
  새38개 클래스 JUnit XML과 콘솔에서 **308개 통과/실패·오류·스킵0**을 대조했다. 기존302개+신규6개다.
  기본 회귀는 실제 재실행했고 두 실행 JAR task는UP-TO-DATE/운영 Java/resources diff0이었다.
- 최종 소스 해시/원래100만 건 FAIL 원본 해시의 불변, 새 원본의 UUID·비유한 값·민감 필드 미포함과 문서 링크를 확인했다.
  기존 Git identity와 원격main9cf1786을 읽기 전용 확인했다. 사용자DB/.env/IntelliJ/volume과 기본 정책을 유지한다.
  실제 공급자/유료 자원/10만·100만 건 재실행/푸시는 하지 않았다.
- 최종 Docker에는 기존 notification-lab-mysql-1만healthy로 남았고 격리 검증 컨테이너는 정리됐다.


## 2026-11-03 — S7 DB 잠금과 커밋 후 호출자 대기 제어군 검증

- 시작3f4362b/main/원격 추적보다17개 앞선 깨끗한 상태에서 AGENTS/CONTRIBUTING/PLAN/README/직전 진단을 확인했다.
  이번에는 운영 SubmissionService/JdbcNotificationStore/DeliveryTaskStore와 실제 Spring 트랜잭션 프록시를 작은 별도context에서 실행한다.
  HTTP/Kafka/Relay/Worker/공급자 부하는 없으며 운영 Java/resources/기본 RECORD/풀·격리 설정을 변경하지 않는다.
- apiWaitProbe는 opt-in SQL 진단이다. MySQL8.4.8/2CPU/2GiB/buffer512MiB/JVM512MiB,
  앱풀4+별도blocker/observer 각1개, 단일 platform worker/큐1개, 워밍업3개+제어3개=접수6개로 제한했다.
  보류200ms/잠금5초/쿼리5초/관찰2초/gate3초/future8초 상한을 둔다. 본문·ID·SQL·예외 메시지는 결과에 저장하지 않는다.
- 정상 접수, 테스트 blocker의RR Outbox 기본키 전 범위 잠금, 실제native commit 반환 후 호출자 반환만 보류하는3조건을 비교했다.
  CPU시간은 ThreadMXBean의현재platform thread만 읽으며 DB 서버CPU나 순수스케줄링시간으로 해석하지 않는다.
- 첫 연결 검증은22초/종료 코드0/테스트1개 통과/실패·오류·스킵0이었다.
  테스트 설정을TestConfiguration으로 분리하고 연결proxy identity 처리를 보강한 뒤 최종6개 소스 해시를 고정했다.
  최종 새MySQL 재실행은 **17초/종료 코드0/테스트1개 통과/실패·오류·스킵0**이었다.
- 최종 정상 조건의wall12.210ms/threadCPU7.388ms/네번째SQL0.517ms/commit1.105ms였다.
  Outbox 잠금 조건은wall250.688ms/CPU3.050ms/네번째SQL240.601ms/commit3.796ms였고,
  실제Outbox 대기 관계1개와 미커밋 행의 비가시성을 확인했다. 해제 후 정상 완료했다.
- 커밋 후 호출자 보류 조건은wall216.580ms/CPU2.742ms/네번째SQL1.163ms/commit호출208.463ms였다.
  native commit이이미반환했고 별도연결에서 접수3개 테이블의새행이 보이지만future는미완료임을 확인했다.
  해당시점대기관계0/gate해제후정상완료였다. 호출자commit시간을DB fsync시간과같다고볼수없음을 확인했다.
- 각측정접수SQL4회/획득·commit·반환1회/rollback0, 전체알림·작업·Outbox각6개/시도이력0/공급자호출0을 확인했다.
  대기 관계0은 순간관찰이며 전체대기부재를 증명하지 않는다. latch는 합성 호출자 지연으로 실제OS선점/네트워크장애와 구분한다.
- [판단 기준·학습 질문·수치·공식 참고](S7_API_WAIT_CALIBRATION.md)와 두원본/소스해시/[실행 메타데이터](experiments/s7-api-wait-2026-11-03-execution.json)를 기록했다.
  제어군 구분은 통과했지만 기존10만 건p95악화/100만 건5초timeout 원인은 여전히 미확정이다.
  다음은 자연 발생 혼합 요청의 세션·시간과 별도DB관찰을 대응할 때의 정확성·오버헤드·상한을 검토하는 작은 단계다.
  풀/인덱스/격리 수준을 추측으로 바꾸거나10만·100만 건부하를 다시 실행하지 않았다.

- 최종 `caffeinate -i ./gradlew --no-daemon --console=plain build`는 **16분10초/종료 코드0**이었다.
  새38개 클래스 JUnit XML과 콘솔에서 **308개 통과/실패·오류·스킵0**을 대조했다.
  기본 회귀는 실제 재실행했고 두 실행 JAR task는UP-TO-DATE였다. 별도SQL 제어1개 테스트/3조건과 구분한다.
- 고정소스6개 해시/운영Java·resources diff0/원래100만건FAIL원본 불변을 확인했다.
  기존Git identity와원격main9cf1786을읽기전용으로확인했다. 사용자DB/.env/IntelliJ/volume과기본정책을변경하지 않았다.
  10만·100만건/실제공급자/유료자원/푸시는 실행하지 않았다.
- 새 원본의비유한값/UUID/민감필드미포함과문서링크를확인했다. Docker에는기존notification-lab-mysql-1만healthy로남았다.


## 2026-11-03 — S7 접수 물리 세션과 유효 호출 구간의 DB 관찰 대응

- 시작cfdab26/main/원격추적보다18개앞선깨끗한상태에서AGENTS/CONTRIBUTING/PLAN/README/직전제어군결과를확인했다.
  API연결에진단SQL을추가하지않고 Connector/J9.7.0의캐시된getId를읽어별도observer연결에서대상세션만관찰한다.
  원본드라이버소스의session.getThreadId반환을로컬동일버전sources와공식소스에서대조했다.
- 각native execute/commit 호출의새구간객체를전후확인한다. 조회중구간이끝나거나연결이재사용되면결과를버린다.
  같은연결번호를다음요청이재사용해도이전결과를붙이지않는단위검증을추가했다.
  SQL원문·digest·수신자·ID·예외메시지는보관하지않고고정statement분류/실행중여부/대기관계수만보관한다.
- 샘플러는별도연결1개/스레드1개/20ms fixed delay, 전체조회512회/요청당8표본/최대5000요청,
  쿼리1초/network2초/오류3회후조회중단/종료3+2초상한이다. 미관찰/세션부재/OFF를대기0과구분한다.
  시작시statement consumer와기본3개instrument활성상태만읽고서버계측설정은변경하지않는다.
- 요청대응/관찰상한·오류/기존트랜잭션선택15개는6초/실패·오류·스킵0으로통과했다.
  apiSessionProbe는기존SQL3조건의제어군에세션관찰을붙인다. 초기21초통과후관찰비용·종료상태를추가했고,
  최종17초/종료코드0/테스트1개통과/실패·오류·스킵0이었다. 접수6건/공급자호출0이다.
- 최종세션제어군조회19회/오류0/늦은표본2개버림/잠금표본8개였다. 잠금구간의대상접수세션을확인했다.
  정상은NOT_OBSERVED, 잠금/커밋후보류는OBSERVED였다. 관찰호출44.888ms/최대8.200ms,
  종료후queuedTasks0/samplerTerminated=true/openRequests0을확인했다.
- 이후자연실험출력경로를분리하고소스7개해시를고정했다. apiDbObservationPilot은정확히1000건/COUNT_TIME만실행한다.
  같은코드/자원/기존발송예산·pool상한으로새인프라에서OFF→ON각1회실행했다. observer연결/설정검증은동일하고주기조회만다르다.
  두task는47초/39초·각1개통과/실패·오류·스킵0/종료코드0이었다. 캠페인발송8.743/8.450초,
  혼합접수p95 16.774/16.524ms·최대22.338/17.130ms·표본38/37개였다.
- 캠페인2000+실시간175=HTTP2175건의전체대상/시도이력1회/Outbox PUBLISHED/HTTP감사/각partition offset=end/quota를대조했다.
  중복·매핑·Mock제한/오류0이었다. API배열/관찰배열의175개요청수와window순서가일치했다.
  각역할pool종료/consumer3개wrapping/activeCalls0을확인했다. 운영Java/resources와기본RECORD는변경하지않았다.
- OFF88개요청은전부DISABLED였다. ON87개중OBSERVED4개/NOT_OBSERVED83개, 혼합37개중관측2개/미관측35개였다.
  ON조회22회중18개는늦어버렸고오류0/보관4개/쿼리예산초과0이었다. 4개모두대상연결의commit구간에서COMMIT실행중이었고순간대기관계0이었다.
  전체관찰호출44.055ms/최대5.274ms, 종료후스레드/대기작업0을확인했다. 현재statement는겹친연결구간의맥락이며개별서버이벤트완전추적이아니다.
- [설계·실제수치·관찰범위·질문·공식참고](S7_API_DB_OBSERVATION.md)와원본/소스해시/[실행메타데이터](experiments/s7-api-db-2026-11-03-execution.json)를기록했다.
  짧은요청대부분은미관측이었다. 4개시점의대기0을전체요청·과거10만건의대기부재로해석하지않는다.
  각1회/공유호스트/적은표본/공통계측비용이있어ON의p95감소를오버헤드0이나성능개선으로주장하지않는다.
  다음은긴호출우선관찰의예산·커버리지검증이다. 과거10만건p95/100만건timeout 원인은계속미확정이다.
  풀/인덱스/격리수준/기본발송정책변경,10만·100만건재실행/실제공급자/유료자원/푸시는하지않았다.

- 최종 `caffeinate -i ./gradlew --no-daemon --console=plain build`는 **15분6초/종료코드0**이었다.
  새39개클래스JUnit XML과콘솔에서 **312개통과/실패·오류·스킵0**을대조했다. 기존308개+새단위4개다.
  기본회귀는실제재실행했고두실행JAR task는UP-TO-DATE였다. 별도세션제어/자연OFF·ON결과와구분한다.
- 고정소스7개해시와운영Java/resources diff0을확인했다. 드라이버캐시조회는CoreSession→NativeCapabilities.threadId반환까지대조했다.
  원래100만건FAIL원본불변과새원본의비유한값/UUID/민감필드미포함,문서링크를확인했다.
  기존Git identity와원격main9cf1786을읽기전용확인했다. 사용자DB/.env/IntelliJ/volume과기본정책을유지했다.
  10만·100만건/실제공급자/유료자원/푸시는실행하지않았다.
- 최종Docker에는기존notification-lab-mysql-1만healthy로남았고임시검증컨테이너는정리됐다.

## 2026-11-03 — S7 긴 호출 우선 관찰과 조회 예산 비교

- 시작03ac275/main에서테스트전용observer에호출나이문턱0/20ms를추가했다. 생략시기존0이며기본운영설정은그대로다.
  호출별단조시계기준을새로잡고20ms미만이면DB조회전건너뛴다. 기존동일호출객체재확인/늦은표본폐기와상한은유지한다.
  youngPollsSkipped, 설정별eligibleCalls/observedEligibleCalls, 공통20ms분모longCalls/observedLongCalls, 조회시작호출나이를기록한다.
  세션부재표본을관측성공분자에넣지않고, 주기없이끝난긴호출도분모에남긴다. 물리ID/SQL/메시지는보관하지않는다.
- 선택단위17개는6초/종료코드0/실패·오류·스킵0이었다. 새2개는가짜단조시계로600개짧은호출의조회0,
  정확히20ms경계의관찰, 표본없는긴호출분모, 다음호출나이초기화와동일세션재사용중늦은결과폐기를검증한다.
- 소스7개해시를고정하고새격리인프라에서즉시0→20ms를제어군/자연실험각1회씩순차비교했다.
  제어군apiSessionProbe각11초/테스트1개PASS/종료코드0/실패·오류·스킵0, 각각접수6건/공급자0이었다.
  양쪽모두20ms이상2호출/관측2호출, 잠금표본8개였다. 조회20→18회/짧은주기skip0→3/늦은폐기0→1,
  조회합계45.004→41.272ms/최대3.775→3.662ms였다. 독립잠금관계·미커밋행비가시성과commit후보류의행가시성을확인했다.
- 자연apiDbObservationPilot각1천건/COUNT_TIME/실시간RECORD는42초/45초, 각각테스트1개PASS/종료코드0/실패·오류·스킵0이었다.
  캠페인완료10.920/8.247초, 혼합접수p95 62.485/13.639ms·최대244.988/17.195ms·표본45/36개였다.
  캠페인2000+실시간181=HTTP2181건대상/시도이력1회/Outbox PUBLISHED/감사/partition별offset=end/quota를전수대조했다.
  중복/잘못된매핑/Mock제한·오류0, API·관찰행181개요청수/window순서일치, SQL4회/획득·commit·반환1회/rollback0을확인했다.
- 즉시방식95요청중관측6개, 조회27회중늦은폐기21개였다. 20ms방식86개는모두미관측이며짧은주기20개를skip하고조회0회였다.
  자연실험의20ms이상execute/commit은양쪽0개다. 0/0을100%커버리지나대기부재로표현하지않는다.
  조건밖조회예산은아꼈으나자연긴호출커버리지개선은미검증이다. 각1회/공유호스트/순차probe/관측편향으로p95차이의인과효과를주장하지않는다.
  observer오류0/열린요청0/대기작업0/스레드종료, 역할별pool종료/activeCalls0을확인했다.
- 즉시최대접수244.988ms는AUTOCOMMIT_STATE127.318ms(3회합계)/PREPARE32.544ms(4회합계),
  SQL각11.572/2.539/0.833/0.865ms/commit18.109ms였다. 집계항목만으로어떤상태변경호출/DB·네트워크·스케줄원인인지확정하지않는다.
  다음은상태변경을메서드·트랜잭션진입/복원별로분리하는작은검증이다. 기존10만건tail/100만건timeout원인은계속미확정이다.
- [설계·판단·원본수치](S7_API_SLOW_OBSERVATION.md)와[실행메타데이터](experiments/s7-api-db-slow-2026-11-03-execution.json)를기록했다.
  운영Java/resources/기본RECORD/풀·격리수준·예산, 사용자DB/.env/IntelliJ/volume을유지했다.
  10만·100만건재실행/실제공급자/유료자원/푸시는하지않았다.

- 최종 `caffeinate -i ./gradlew --no-daemon --console=plain build`는 **14분18초/종료코드0**이었다.
  새39개클래스JUnit XML과콘솔에서 **314개통과/실패·오류·스킵0**을대조했다. 기존312개+새단위2개다.
  기본회귀는재실행했고bootJar는UP-TO-DATE였다. 원격CI의실행결과가아니다.
- 고정소스7개해시/운영Java·resources diff0/기존100만건FAIL원본SHA불변/원본JSON과문서링크를확인했다.
  Git identity를유지했고원격main9cf1786을읽기전용확인했다. 푸시는하지않았다.
- 최종Docker에는기존notification-lab-mysql-1만healthy로남았고임시검증컨테이너는정리됐다.

## 2026-11-03 — S7 연결 상태 변경 메서드·복원 경계 진단

- 시작6edb1b1/main. 테스트전용AUTOCOMMIT_STATE를getAutoCommit/setAutoCommit(false)/setAutoCommit(true)로분리했다.
  기존합계·호출횟수는유지하며상세순번/메서드/관찰경계/wall millis/실패여부만보관한다.
  BEFORE_FIRST_SQL/BEFORE_COMPLETION/AFTER_COMMIT/AFTER_ROLLBACK은JDBC proxy에서관찰한순서이며서버트랜잭션시각이아니다.
  savepoint rollback을완료경계로취급하지않고기존delegate를한번만호출한다. 상세조회/재시도/관찰스레드는추가하지않는다.
- 상세최대8개/요청, 기존API순차1개/최대5000요청을유지한다. 성공접수의3개메서드·경계·합계횟수와상세합계≤바깥합계를검증한다.
  물리ID/SQL/인자전체/드라이버오류메시지를보관하지않는다. OFF는기존요청별합계계측을유지하고상세만끈다.
- Spring6.2.19/DataSourceTransactionManager와Connector/J9.7.0의버전고정공식·로컬소스를대조했다.
  진입에서auto-commit해제/완료후복원, 드라이버get의캐시상태조회와set의조건부내부SET명령을확인했다.
  기존EXECUTE4회는애플리케이션DML이며드라이버내부제어명령을포함한서버전체SQL횟수가아니다. wall time은순수서버시간이아니다.
- 선택20개는6초/종료코드0/실패·오류·스킵0이었다. 정상3메서드/rollback후복원/상태변경실패원래예외보존과본체미실행,
  상세누락·잘못된완료경계·8개상한을검증했다. 기존17개+새3개다.
- 고정소스9개에서apiConnectionProbe11초/테스트1개PASS/종료코드0/실패·오류·스킵0, 접수6건/공급자0이었다.
  진입전합성보류는SET_FALSE206.024ms/BEFORE_FIRST_SQL/행증가없음, 복원반환후보류는SET_TRUE209.287ms/AFTER_COMMIT/3개테이블행증가를확인했다.
  두조건모두관찰순간잠금대기관계0/호출자미완료였다. 합성보류로자연OS·네트워크원인을추정하지않는다.
- 공유제어도구의기존apiSessionProbe(min20)도11초/1개PASS/종료코드0/실패·오류·스킵0이었다.
  기존실제잠금/commit후보류3조건, 조회17회/잠금표본8개/오류0, 종료후열린요청0/대기작업0/스레드종료를확인했다.
- 새인프라의자연apiConnectionPilot은같은소스/1천건/COUNT_TIME/실시간RECORD에서상세OFF→ON각1회비교했다.
  각41초/39초·테스트1개PASS/종료코드0/실패·오류·스킵0, 캠페인완료8.594/8.257초였다.
  혼합접수p95 11.962/13.744ms·최대12.730/14.392ms·표본38/36개였다.
  HTTP2174건(캠페인2000+실시간174)의대상/시도이력1회/PUBLISHED/감사/partition별offset=end/quota를전수대조했다.
  중복/잘못된매핑/Mock제한·오류0, SQL4회/획득·commit·반환1회/rollback0/상태호출3회, pool종료/consumer3개/activeCalls0을확인했다.
- ON86요청/상세258개모두3메서드·경계가일치했고상세합계≤바깥합계를확인했다.
  get/setfalse/settrue최대0.239/2.765/1.585ms, 상태합계최대OFF3.506/ON3.265ms로127.318ms는미재현이다.
  ON전체최대접수86.389ms는서버41.025ms/store7.907ms/상태합계1.090ms로다른구간시간을이번계측만으로분해하지않았다.
  각1회/공유호스트/순차probe/적은표본으로p95차이를계측비용의인과효과나개선으로해석하지않는다. 과거10만건tail/100만건timeout원인은미확정이다.
- [호출흐름·공식참고·실제결과](S7_API_CONNECTION_TIMING.md), [고정소스·원본·실행메타데이터](experiments/s7-api-connection-2026-11-03-execution.json)를기록했다.
  다음은고정계측의제한된1만건반복에서긴상태호출표본을확보할수있는지판단하는단계다.
  운영Java/resources/기본정책/풀·격리수준·예산과사용자DB/.env/IntelliJ/volume을유지한다.
  10만·100만건재실행/실제공급자/유료자원/푸시는하지않았다.

- 최종 `caffeinate -i ./gradlew --no-daemon --console=plain build`는 **14분14초/종료코드0**이었다.
  새39개클래스JUnit XML과콘솔에서 **317개통과/실패·오류·스킵0**을대조했다. 기존314개+새단위3개다.
  기본회귀를재실행했고bootJar는UP-TO-DATE였다. 원격CI결과가아니다.
- 고정소스9개/운영Java·resources diff0/기존100만건FAIL원본SHA불변/원본JSON·문서링크를확인했다.
  기존Git identity와원격main9cf1786을읽기전용확인했다. 푸시는하지않았다.
- 최종Docker에는기존notification-lab-mysql-1만healthy로남았고임시검증컨테이너는정리됐다.

## 2026-11-03 — S7 고정 연결 상태 계측의1만 건2회 반복

- 시작bba081f/main. Java 계측·테스트8개 소스를 그대로 두고 apiConnectionPilot의대상 입력만1000/10000 두 값으로 제한했다.
  생략 시1000을 유지한다. 상세ON/캠페인COUNT_TIME/실시간RECORD, 기존pool·동시성·Relay·예산·infra·HTTP5초 제한을 유지했다.
  실행 전에새격리인프라에서1만 건2회/순차/실패원본보존/대형부하자동확대없음으로방법을고정했다.
- 동일소스각1회 task는1분54초/1분45초, 각테스트1개PASS/종료코드0/실패·오류·스킵0이었다.
  캠페인완료65.195/61.140초, 처리량153.387/163.560건/s, 혼합접수p95 11.759/11.485ms·최대23.911/22.582ms였다.
  API전체351/333개(혼합301/283개), 상태상세1053/999개를확인했다.
- 캠페인20000+실시간684=HTTP Mock 발송20684건을전수대조했다. 대상매핑/시도이력1회/Outbox PUBLISHED/감사/
  partition별offset=end/quota를검증했고중복/잘못된매핑/Mock제한·오류0이었다. 단말도착을검증한것은아니다.
  각접수의SQL4회/획득·commit·반환1회/rollback0/상태호출3회, 세메서드·경계·상세합계≤바깥합계를대조했다.
  pool종료/consumer3개/activeCalls0, Relay2 종료/대기작업0/활성실행0을확인했다.
- 상태합계최대5.358/5.681ms였다. get/setfalse/settrue최대는0.021/4.844/1.688ms와0.046/3.933/5.167ms였다.
  상세2052개에서20ms이상0개, 이전127.318ms 상태합계도0개였다. 과거10만건p95와100만건5초timeout의원인은계속미확정이다.
  전체최대API100.550/80.075ms는BASELINE이며서버43.453/40.409ms/store8.029/8.536ms/상태상세합계1.095/1.160ms였다.
  나머지구간의원인을이번계측으로분해하지않았다. 다른메서드최대를더해한요청의지연으로표현하지않는다.
- [실행방법·실제결과·판단](S7_API_CONNECTION_10K.md), [원본·실행메타데이터](experiments/s7-api-connection-10k-2026-11-03-execution.json),
  [요청대응분석](experiments/s7-api-connection-10k-2026-11-03-analysis.json)을보존했다. 메서드분위수는nearest rank이고API분위수는기존원본을사용했다.
  두실행/공유호스트/순차probe/짧은관찰로성능개선이나계측오버헤드의인과효과를주장하지않는다. 관측실패를해결이나대기부재로표현하지않는다.
- 계획한두번으로이번반복을마친다. 다음은장시간실행의실패직전구간에대한고정메모리관찰과중단기준을먼저정하는단계다.
  운영Java/resources/기본정책/풀·격리수준·예산, 사용자DB/.env/IntelliJ/volume과기존원본을유지한다.
  10만·100만건/실제공급자/유료자원/푸시는실행하지않았다.

- 최종 `caffeinate -i ./gradlew --no-daemon --console=plain build`는 **14분23초/종료코드0**이었다.
  새39개클래스JUnit XML과콘솔에서 **317개통과/실패·오류·스킵0**을대조했다.
  Java/compileTestJava는변경없음/UP-TO-DATE였으나기본회귀는실제로재실행했다. bootJar는UP-TO-DATE였다. 원격CI결과가아니다.
- 고정소스9개와이전Java8개, 기존100만건FAIL SHA, 원본·분석SHA와문서링크를확인했다.
  기존Git identity와원격main9cf1786을읽기전용확인했다. 푸시는하지않았다.
- 범위밖100000 입력의dry-run은기대대로설정단계에서2초/종료코드1로거부됐다. 발송·컨테이너는시작하지않았다.
  이는상한검증의예상거부이며실험·회귀실패와구분한다. 최종Docker에는기존notification-lab-mysql-1만healthy로남았다.

## 2026-11-03 — S7 장시간 실패 관찰·중단 기준과 고정 용량 프로토타입

- 시작392b130/main. 별도테스트용ApiFailureWindow를추가했다. 최근완료32건+진행1건/호출중첩2단계/고정Part배열/
  SQL시간4개/상태상세8개/전체probe1..150000으로제한한다. 내부보관상한이며기존harness전체메모리O(1)을주장하지않는다.
- 완료합계와진행중activePart·경과시간을분리한다. commit/rollback정상반환은관찰표시이며HTTP미관찰이면결과UNKNOWN을유지한다.
  첫중단을불변Map/List로동결하고늦은callback·202·다른중단이유가바꾸지못하게했다. 중단후새probe는거부한다.
  JDBC/HTTP/추가연결/진단SQL/새스레드를생성하지않는다. 서버실행을취소하거나timeout을미접수로간주하지않는다.
- [설계·상한·중단표·미구현범위](S7_LONG_RUN_OBSERVATION.md)를정의했다. HTTP5초/최대probe150000/drain8h/JUnit11h/task12h는변경하지않았다.
  HTTP202외응답/불완전경계/상한중단은프로토타입이처리하며, I/O·인프라·실행상한의실제장시간연결은별도미구현으로구분했다.
  메모리freeze는디스크보존이나SIGKILL/OOM후생존을보장하지않는다. 다음은작은실제HTTP/MySQL pilot의opt-in연결·즉시파일보존검증이다.
- 최초선택31개는5초/종료코드0/실패·오류·스킵0이었다. SQL원래예외/rollback증거보존1개를추가한최종선택32개도5초/동일하게통과했다.
  합성접수5000개후최근32개/evicted4968/마지막ordinal5000, 가짜단조시계의진행중COMMIT5초와완료횟수0,
  commit반환후에도HTTP결과UNKNOWN, 첫중단후늦은완료불변, 겹침/다른서버스레드/중첩상한/불완전202를검증했다.
- 실제Spring TransactionTemplate+합성JDBC의정상DML4회/commit과원래SQL예외/rollback을확인했다. 추가드라이버호출과민감문자열보관은없었다.
  실제loopback HTTP는warmup후500ms timeout/합성COMMIT마커를사용했다. 총HTTP2건(단독warmup1+실패probe1), 늦은응답이첫증거를덮지않음을확인했다.
  이fixture에는DB가없으며실제MySQL·HTTP결합실험이나장시간원인의재현으로주장하지않는다.
- [최종고정소스](experiments/s7-api-failure-window-2026-11-03-source-final.json)와[실행메타데이터](experiments/s7-api-failure-window-2026-11-03-execution.json)를기록했다.
  기존ApiRequestTiming/CampaignDeliveryPilotTest와장시간task는바꾸지않았다. 기본정책/풀·격리수준·예산/사용자DB/.env/IntelliJ/volume을유지한다.
  10만·100만건실행/실제공급자/유료자원/푸시는하지않았다. 기존timeout원인은계속미확정이다.

- 응답관찰여부보완전첫 `caffeinate -i ./gradlew --no-daemon --console=plain build`는 **14분10초/종료코드0**이었다.
  새40개클래스JUnit XML과콘솔에서 **325개통과/실패·오류·스킵0**을대조했다. 기존317개+새8개다.
  기본회귀는실제로재실행했고bootJar는UP-TO-DATE였다. 원격CI결과가아니다.
- 고정소스8개/기존100만건FAIL SHA불변/운영Java·resources diff0을확인했다.
- 최종검토에서202외응답의responseObserved=false표시를발견해status/관찰여부와서버결과UNKNOWN을분리했다.
  실제503과불완전202는응답관찰true/status를보관하며, timeout은미관찰상태를유지하고늦은응답이덮지못한다.
  수정후선택32개는6초/종료코드0/실패·오류·스킵0이었다. 수정전소스·첫전체회귀는별도보존했고최종고정소스는source-final.json이다.
- 응답관찰보완후최종build는 **14분23초/종료코드0**이었다. 새40개클래스XML/콘솔에서 **325개통과/실패·오류·스킵0**을다시대조했다.
  기본회귀를실제로재실행했고bootJar는UP-TO-DATE였다. 수정전첫전체회귀와구분하며원격CI결과가아니다.
- 기존Git identity/원격main9cf1786을읽기전용확인했다. Docker는기존notification-lab-mysql-1만healthy이며임시검증컨테이너는정리됐다. 푸시는하지않았다.

## 2026-11-03 — S7 실제 HTTP/MySQL 접수의 첫 실패 파일 보존

- 시작99ce59d/main. FailureSnapshotStore와opt-in apiFailurePersistenceProbe를추가했다. 운영코드/기존혼합pilot/장시간task는변경하지않았다.
  실제NotificationController→IdempotentSubmissionService→SubmissionService와JDBC/MySQL을사용하고Kafka/Relay/Worker/공급자발송은제외했다.
- 첫snapshot동결→같은디렉터리temp전체쓰기→FileChannel.force(true)→기존이름을바꾸지않는새hard link공개→temp정리순서다.
  256KiB쓰기상한과기록기32+1구조를유지하며첫저장후재호출은기존결과를반환한다. 실패후자동재시도나unsafe move/copy우회는없다.
  parent directory fsync는하지않아전원장애보존을보장하지않는다. 정리실패시주된결과를바꾸지않고temp가남을수있다.
- 파일저장실패는report에종류만기록하고내용·경로없는suppressed예외를붙여원래timeout객체를유지한다.
  기존파일미덮어쓰기/실패후재시도없음/동시저장자중정확히하나성공/원래예외보존을검증했다.
- 선택24개는초기7초/최종4초, 모두종료코드0/실패·오류·스킵0이었다.
  HTTP/MySQL제어는초기16초/최종15초, 각테스트1개PASS/종료코드0/실패·오류·스킵0이었다.
  첫실행후Java·heap/실제connector상한·파일관찰시각을추가했고초기·최종소스와원본은분리보존했다.
- 최종Java Amazon.com Inc.21.0.6/heap512MiB, MySQL8.4.8 2CPU/2GiB/buffer512MiB/pool4/외부관찰1,
  실제Tomcat maxThreads4/maxConnections16을확인했다. 각조건HTTP202 40건뒤제어요청1건, 총HTTP82건이다.
  일반요청5초/fixture timeout1초/gate최대10초, 공급자호출은0이다. 기존장시간HTTP5초제한을바꾼결과가아니다.
- BEFORE_COMMIT은active COMMIT/commitReturned=false/DB행40, AFTER_RESTORE는active AUTOCOMMIT_STATE/commitReturned=true/DB행82였다.
  두조건모두클라이언트결과UNKNOWN이며기록기는최근32건/진행1건/완료40/evicted8을보관했다.
  앱과pool이살아있고서버가gate에서대기중인동안첫파일68289/68282bytes를저장했다.
  gate해제후DB행41/82와세테이블일치, 늦은처리/재저장후파일SHA불변/앱종료후파일존재를확인했다.
- 최종DB접수82건/관찰HTTP202 80건/timeout2건/provider0/delivery_attempt0, Outbox82개는PENDING이었다.
  접수제어이며발송완료나단말도착의검증이아니다. 실제장시간timeout이나127ms지연의원인은계속미확정이다.
  result=PASS는주입한예상timeout의검증이며실제부하실패를PASS로바꾼것이아니다.
- [순서·상한·공식참고·실제결과](S7_FAILURE_PERSISTENCE.md)와[원본/파일명대응/실행메타데이터](experiments/s7-api-failure-persistence-2026-11-03-execution.json)를기록했다.
  다음은작은기존Kafka캠페인혼합pilot의opt-in연결과보존실패처리검증이다. 운영설정과사용자DB/.env/IntelliJ/volume을유지했다.
  10만·100만건/실제공급자/유료자원/푸시는실행하지않았다.

- 최종 `caffeinate -i ./gradlew --no-daemon --console=plain build`는 **15분3초/종료코드0**이었다.
  새41개클래스JUnit XML과콘솔에서 **329개통과/실패·오류·스킵0**을대조했다. 기존325개+새단위4개다.
  기본회귀를실제로재실행했고bootJar는UP-TO-DATE였다. 별도opt-in제어와구분하며원격CI결과가아니다.
- 고정소스8개/운영Java·resources diff0/기존100만건FAIL SHA불변을확인했다.
- 기존Git identity/원격main9cf1786을읽기전용확인했다. Docker는기존notification-lab-mysql-1만healthy로남았고임시검증컨테이너는정리됐다. 푸시는하지않았다.


## 2026-11-03: S7 1천 건 Kafka 혼합 실험의 첫 실패 보존 연결

시작 cdf21d9/main. 직전 접수 전용 제어의 ApiFailureWindow/FailureSnapshotStore를 기존 혼합 harness에 opt-in 연결했다.

- 새 `apiFailurePilot`은 캠페인1000건만 허용하고 task5분/JUnit240초/JVM512MiB다.
  HTTP5초·연결2초/순차 요청/최근32건+진행1건/파일256KiB/기존probe5000개 상한을 유지한다.
  기본 test/build와 기존 pilot은 새 제어 tag/옵션을 사용하지 않는다. 100만 건 task는 미연결이다.
- 앱과pool을 닫거나 추가 진단하기 전에 첫 오류를 동결·파일 보존하고, 중첩된 실패 처리의 재저장을 막았다.
  AssertionError도 원래 객체를 유지한다. 저장 실패는 정제된 suppressed 오류1개이며 원래 HTTP 오류를 덮지 않는다.
- 초기 컴파일 오류3개(타입 추론/예외 생성자/지역 변수 중복)를 수정한 후 관련 단위27개가7초에 통과했다.
- NONE/TIMEOUT/STORAGE_FAILURE 각1회: Gradle40/35/38초·종료코드0, 제어 검증3개 모두통과.
  NONE은 캠페인1000+실시간87=1087건 HTTP·DB·전체 참조·offset 대조를 통과했다. 중복/잘못된 참조0이다.
  두 주입 조건의 원래 harness 결과는 FAIL/HttpTimeoutException이며 성공으로 바꾸지 않았다.
  첫MIXED(51번째) 요청이5초 timeout일 때 완료50/evicted18/최근32+진행1개가 남았다.
  active AUTOCOMMIT_STATE/commitReturned=true/responseObserved=false/클라이언트결과UNKNOWN이었다.
  앱active/pool open/gate 대기 중 저장을 확인했다. TIMEOUT은68422bytes 파일과 frozen snapshot이 정리 후 byte 일치했다.
  STORAGE_FAILURE는 기존35bytes 합성 증거를 보존하고 FileAlreadyExistsException/suppressed1개를 남겼다.
  중도 실패한 두 실행은 최종 발송 정합성 완료로 집계하지 않는다.
- 새옵션을 끈 기존 `apiConnectionPilot -PdeliveryTargets=1000 -PconnectionCalls=OFF`도39초에 통과했다.
  캠페인1000+실시간88=1088건 전수 대조/중복0을 확인했다. NONE과 합쳐 정상 HTTP2175건이다.
- [순서·상한·결과·한계](S7_FAILURE_MIXED_PILOT.md), [고정 소스](experiments/s7-api-failure-mixed-2026-11-03-source.json),
  [원본 SHA·실행 메타데이터](experiments/s7-api-failure-mixed-2026-11-03-execution.json)를 보존한다.
  이는 오류 보존 제어 실험이며 성능 개선 또는 기존100만 건 timeout 원인 확정이 아니다.
  장시간 도구 연결 검토와 실제 실행은 별도다. 10만/100만·실제공급자·유료자원·푸시는 실행하지 않았다.

- 최종 `caffeinate -i ./gradlew --no-daemon --console=plain build`: **14분45초/종료코드0**.
  새42개 클래스의 XML과 콘솔에서 **332개통과/실패·오류·스킵0**을 대조했다. 기존329개+새단위3개다.
  기본 회귀는 재실행했고bootJar는UP-TO-DATE였다. 별도pilot4회와 원격CI 결과를 구분한다.
  고정소스10개/보존원본10개의SHA 일치, 운영Java/resources diff0, 기존100만건FAIL SHA 불변을 확인했다.
- 기존 Git identity와 원격main9cf1786을 읽기 전용 확인했다. Docker에는 기존notification-lab-mysql-1만healthy로 남았다.
  사용자DB/.env/IntelliJ/volume을 유지했고 푸시는 하지 않았다.


## 2026-11-03: S7 장시간 도구의 선택적 첫 실패 보존과 1천 건 리허설

시작67269e3/main. `campaignDeliveryMillion -PfailureObservation=ON` 연결을 추가했으며 기본OFF를 유지한다.
이번에 실제10만/100만 건 발송은 실행하지 않았다.

- LONG_RUN은 대상100만/load=true/주입NONE만 허용한다. 전체 세부계측·Relay 병렬·offset비교·다른API계측과 혼용을 거절한다.
  RECORD/기본 Relay1/publishBatchSize1/Worker3/역할별Hikari4/예산을 유지하며 실시간 앱에만 기록용 계측을 붙인다.
  기존 publishBatchSize 옵션은 유지한다. 테스트 전용이며 운영Java/resources는 변경하지 않았다.
- 기존 상한 probe150000/drain8h/JUnit11h/task12h/JVM512MiB/페이지1000/자원표본14402개를 유지한다.
  기록기는최근32+진행1/첫파일256KiB다. 다른 전체 자료구조도 남아 있어 전체 프로세스 상수 메모리를 주장하지 않는다.
- 새 REHEARSAL은 기존 장시간 기본 구성의1천 건만 허용한다. 오류 주입은 이 작은 프로필에서 검증한다.
  관련 단위35개가6초에 실패·오류·스킵0으로 통과했다. 합성 callback5000/150000회 모두 최근32개 보관,
  상한 다음open 거절/PROBE_LIMIT 동결/직렬화 크기상한을 검증했다. 실제15만 건 HTTP 발송이 아니다.
- REHEARSAL NONE/TIMEOUT/STORAGE_FAILURE 각1회가45/36/36초·종료코드0으로 제어 검증을 통과했다.
  NONE은 캠페인1000+실시간104=1104건 HTTP·DB·전체 참조·offset 대조, 중복/잘못된참조0이었다.
  두 제어의 원래 harness는 FAIL/HttpTimeoutException을 유지한다. 첫MIXED(51번째)5초 timeout에서
  완료50/evicted18/최근32+진행1, active AUTOCOMMIT_STATE/commitReturned=true/응답미관찰/클라이언트UNKNOWN이었다.
  앱active/pool open/gate 대기 중 첫 저장을 확인했다. TIMEOUT 파일68395bytes는 정리 후 동결자료와 byte 일치했다.
  STORAGE_FAILURE는 FileAlreadyExistsException/suppressed1개, 기존35bytes 보존을 확인했다.
- 실제 장시간 task의 기본OFF/명시적ON 설정을 임시Gradle검사로 읽고 각각dry-run3/2초에 확인했다.
  대상100만/RECORD/주입NONE/전체세부계측OFF/병렬Relay OFF/heap512m/fork1/JUnit11h/task12h였고 task는SKIPPED다.
  실제 실행이나 장시간 보존 능력 검증이 아니다. 기존 PILOT도39초/1086건 전수대조를 통과했다.
- [구성·실제 결과·미보장 경계](S7_LONG_RUN_WIRING.md), [고정 소스](experiments/s7-long-observation-2026-11-03-source.json),
  [원본 SHA·명령](experiments/s7-long-observation-2026-11-03-execution.json)을 보존한다.
  기존100만 건 timeout 원인은 여전히 미확정이며 성능 개선 주장도 하지 않는다.
  초기화/preload/종료 중 오류·외부 강제종료의 종료 전 파일은 보장하지 않는다. 실제 장시간 실행은 별도다.

- 최종 `caffeinate -i ./gradlew --no-daemon --console=plain build`: **14분23초/종료코드0**.
  새43개 클래스 XML과 콘솔에서 **338개통과/실패·오류·스킵0**을 대조했다. 기존332개+프로필5개+가상15만 회1조건이다.
  기본회귀를 재실행했고bootJar는UP-TO-DATE였다. 별도pilot4회/설정확인2회와 원격CI 결과를 구분한다.
  고정소스12개/보존원본13개 SHA일치, 운영Java/resources diff0/기존100만 건FAIL 원본SHA 불변을 확인했다.
- 기존 Git identity/원격main9cf1786을 읽기 전용 확인했다. Docker에는 기존notification-lab-mysql-1만healthy로 남았다.
  사용자DB/.env/IntelliJ/volume을 유지했고 실제장시간·유료자원·실제공급자·푸시는 실행하지 않았다.


## 2026-11-03: S7 최종 보고서 실패의 원래 오류 보존

시작0b47a1c/main. 앞선 단계에서 남긴 finally의 최종 report 쓰기 오류가 원래 timeout을 가리는 결함을 보완했다.
실제100만 건/10만 건 발송은 시작하지 않았다.

- 바깥 catch의 원래 실패 객체를 보관하고 마지막 진단·report 처리를 PilotFinalization으로 감쌌다.
  마지막 Exception/AssertionError는 기존 실패에 정제된 suppressed IOException1개로 더한다.
  기존 실패가 없으면 마지막 오류 자체를 전파하고 메모리 결과를FAIL로 바꾼다. 자동 재시도나PASS 변환은 없다.
- 관련 단위20개가7초에 실패·오류·스킵0으로 통과했다. 새 단위3개는 실제 파일 쓰기 실패/원래 timeout 객체 동일성,
  기존 실패 없는 보고서 오류 전파, AssertionError와 기존 suppressed 오류 보존을 검증했다.
- 1천 건 REHEARSAL의 REPORT_FAILURE는37초/종료코드0으로 제어 검증을 통과했다.
  첫MIXED(51번째)5초 timeout에서 완료50/evicted18/최근32+진행1, active AUTOCOMMIT_STATE/commit반환true/응답미관찰이었다.
  앱active/pool open/gate 대기 중 첫파일68414bytes를 저장하고 정리 후 동결 snapshot과 byte 일치를 확인했다.
  그 뒤 result.json 경로의 제어 디렉터리 때문에 실제 마지막 쓰기가FileNotFoundException으로 실패했다.
  원래 harness는FAIL/HttpTimeoutException과 suppressed1개를 유지했으며 최종 report 파일은 생성되지 않았다.
  검증기만 별도 control-observed-report.json에 메모리 결과를 복사했다. 실제 자동복구/report 재시도가 아니다.
- NONE도45초/1105건(캠페인1000+실시간105) HTTP·DB·참조·offset 전수대조를 통과했다. 중복/잘못된참조0이다.
  실패 제어의 부분 발송은 최종 성공 집계에 넣지 않는다. LONG_RUN은 새 주입도 거절하며 운영Java/resources는 변경하지 않았다.
- [변경·실제 결과·미보장 경계](S7_FINAL_REPORT_FAILURE.md), [고정 소스](experiments/s7-final-report-2026-11-03-source.json),
  [원본 SHA·실행 메타데이터](experiments/s7-final-report-2026-11-03-execution.json)를 보존한다.
  강제종료/치명적VM Error/전원 장애의 파일 보존과 실제100만 건 timeout 원인 확정은 보장하지 않는다.

- 최종 `caffeinate -i ./gradlew --no-daemon --console=plain build`: **14분19초/종료코드0**.
  새44개 클래스 XML과 콘솔에서 **341개통과/실패·오류·스킵0**을 대조했다. 기존338개+새단위3개다.
  기본회귀를 재실행했고bootJar는UP-TO-DATE였다. 별도제어2회/원격CI와 구분한다.
  고정소스10개/보존원본7개 SHA일치, 운영Java/resources diff0/기존100만 건FAIL 원본SHA 불변을 확인했다.
  사용자 선택에 따라 이번 범위는 최종 보고서 저장 실패 보완까지다. 실제 장시간 실행과 푸시는 하지 않았다.
- 기존 Git identity/원격main9cf1786을 읽기 전용 확인했다. Docker에는 기존notification-lab-mysql-1만healthy로 남았다.
  사용자DB/.env/IntelliJ/volume을 유지했다.


## 2026-11-04: S7 관찰 실험의 예산 조회 실패 분석과 유한 복구

71688a8의승인된100만건관찰실험은11월3일21:46~23:13 KST/Gradle1시간27분50초에FAIL로종료했다.
사전저장2774.790초/발송실패관찰2448.709초, Mock 캠페인190131+실시간10438건 수락이었다.
quotaUnavailable/거절각1회와캠페인Worker정지를확인했다. API10438개는완료했고최근32개는202/최대483.995ms였다.
첫파일66708bytes와최종결과를보존했다. 종료코드1/JUnit1실패/자동재실행없음이며완료로표시하지않는다.

- 작은원인대조4개를31초에재실행해통과했다. 현재UNAVAILABLE는timeout/연결/상태오류를구분하지못하며,
  Worker의원문cause도보존하지않아당시의정확한저수준원인은미확정이다.
  MySQL상한접근max730/관측OOM0/일시적인풀대기는인과관계증거가아니다. [상세분석](S7_OBSERVED_MILLION_FAILURE.md)을남겼다.
- 사용자요청에따라진단과복구를보완했다. 고정FailureReason/단계/소요시간/실효timeout/pool을첫오류에만보존한다.
  키/주소/본문/원문예외는저장하지않고원인별counter는고정label이다. 정상허가는연결반환성공후에만집계한다.
- 새옵션은기본OFF다. ON은TIMEOUT/CONNECTION/SLOT_BUSY만250/500/1000ms로최대3번pause후새허가를얻는다.
  네번째실패·상태오류·닫힌pool·미분류오류는중단한다. 허가전발송/offset처리/token환불·재사용은없다.
  예산은공유부모컨테이너의topic/partition/offset별최대6개,예약3개/스케줄러스레드1개로제한한다.
- 초기집중검증은정리용admin연결timeout으로46초에실패했다. 제어연결만15초로조정하고서비스설정은유지했다.
  후속Redis/Worker/진단41개가51초에통과했다. 공유부모로인한예산상쇄를고쳐파티션분리후Worker14개도55초에통과했다.
  일시정지복구/지속장애시유한중단/상태오류즉시중단/두record와명시적재시작의예산초기화/3Consumer3파티션을검증했다.
- 복구ON의1천건REHEARSAL은54초/1116건(캠페인1000+실시간116) 전수대조를통과했다. 중복/잘못된참조0이었다.
  양쪽실효Redis timeout250ms/연결3개를확인했다. 기본RECORD/Relay1/Worker3/완료저장1과역할별예산은유지했다.
- [복구계약·참고자료](S7_QUOTA_RECOVERY.md)와[고정소스](experiments/s7-quota-recovery-2026-11-04-source.json),
  [검증원본](experiments/s7-quota-recovery-2026-11-04-validation.json)을보존한다. 전체회귀후수정커밋에서승인된100만건을재실행한다.

- 최종 `caffeinate -i ./gradlew --no-daemon --console=plain build`: **16분34초/종료코드0**.
  새44개 클래스 XML과 콘솔에서 **347개통과/실패·오류·스킵0**을대조했다. 기존341개+새6개다.
  기본회귀를재실행하고실행JAR도생성했다. 별도1천건/집중검증과구분하며원격CI결과가아니다.
  고정소스160개SHA일치를확인했다. 수정커밋과실제재실험시각은새실행기록에남긴다.
- 재실행 전dry-run에서복구ON/관찰ON/RECORD/Relay기본1/완료저장1/최대12h를읽어확인했다.
  기존Git identity/원격main9cf1786을읽기전용확인했고기존사용자MySQL만healthy로남았다. 푸시는하지않았다.
