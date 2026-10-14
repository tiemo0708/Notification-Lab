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
