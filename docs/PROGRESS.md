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
