# 진행 기록

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
