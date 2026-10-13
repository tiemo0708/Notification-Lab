# Notification Lab

Kafka 기반 알림 시스템을 단계별로 만드는 Java/Spring 학습 프로젝트.
현재 v0.2(S2)는 **MySQL에 처리 결과를 저장하는 동기식 Mock API**이다.
Outbox 테이블은 준비했으며, 이벤트 저장·Kafka 발행/소비는 S3에서 구현한다.

## 시작

Java 21, Docker(Compose 포함), 인터넷 연결이 필요하다.
Gradle Wrapper가 최초 실행 시 Gradle과 의존성을 받는다. AWS/FCM 키는 필요하지 않다.

테스트는 임시 MySQL 컨테이너를 직접 생성하고 종료한다. `.env`나 Compose 기동은 필요 없다.
Docker가 없거나 실행되지 않으면 실제 DB 테스트는 실패하며 자동으로 스킵하지 않는다.

```bash
./gradlew test
```

애플리케이션 실행:

1. `.env.example`을 `.env`로 복사하고 두 비밀번호를 각각 임의의 로컬 값으로 채운다. `.env`는 커밋하지 않는다.
2. Compose로 MySQL을 시작한다. 기본 DB 포트는 `127.0.0.1:13316`, 데이터는 named volume에 저장한다.
3. 같은 터미널에 `.env` 값을 로드하고 애플리케이션을 실행한다. Compose는 `.env`를 읽지만 Spring Boot는 자동으로 읽지 않는다.

```bash
cp .env.example .env
# .env의 MYSQL_PASSWORD / MYSQL_ROOT_PASSWORD를 채운 뒤 실행
docker compose up -d --wait
set -a
. ./.env
set +a
./gradlew bootRun
```

위 환경변수 로딩 명령은 bash/zsh 기준이다. `.env` 값은 셸에서도 읽을 수 있게 작성한다.
Windows에서는 같은 값을 환경변수로 설정한 뒤 `gradlew.bat test`, `gradlew.bat bootRun`을 사용한다.
별도 Gradle 설치 없이 저장소의 Wrapper를 사용한다. 빌드 설정은 `build.gradle`(Groovy DSL)이다.
Java 버전이 다르면 로컬의 JAVA_HOME을 JDK 21로 설정한다.

전체 빌드와 실행 JAR 생성:

```bash
./gradlew build
java -jar build/libs/notification-lab-0.2.0-SNAPSHOT.jar
```

`build`는 테스트와 실행 JAR 생성을 함께 수행한다. JAR 실행 시에도 앞에서 설정한 DB 환경변수가 필요하다.
테스트 HTML 보고서는 `build/reports/tests/test/index.html`, XML 결과는 `build/test-results/test/`에 생성된다.
이미 테스트를 통과한 코드의 JAR만 다시 만들 때는 `./gradlew bootJar`를 사용한다.

`MYSQL_PORT`를 바꾸면 Compose와 앱의 기본 JDBC 주소에 함께 반영된다.
별도 DB 연결은 `DB_URL`로 재정의할 수 있다. 운영 자격증명이나 외부 DB를 사용하지 않는다.
`docker compose stop` 또는 `docker compose down`은 DB volume을 보존한다.
`docker compose down -v`는 저장된 알림까지 삭제하므로 데이터 초기화가 목적일 때만 사용한다.

기본 주소: `http://127.0.0.1:8080`. 외부 공개용 인증은 없으므로 바인딩을 임의로 0.0.0.0으로 바꾸지 않는다.

## 실행 흐름

HTTP 요청 -> 입력 검증 -> PROCESSING 저장/커밋 -> Mock Provider 동기 호출 -> 결과 저장/커밋 -> HTTP 201.
Mock은 실제 네트워크 요청이나 푸시를 전혀 보내지 않는다.
공급자 호출 중에는 DB 트랜잭션을 유지하지 않는다. DB와 공급자 호출은 원자적이지 않다.

```bash
curl -i http://127.0.0.1:8080/notifications \
  -H 'Content-Type: application/json' \
  -d '{"recipientId":"user-100","title":"테스트 알림","body":"알림 처리 확인"}'
```

응답의 `notificationId` 또는 `Location` 헤더를 이용한다.

```bash
curl http://127.0.0.1:8080/notifications/응답에서_받은_UUID
curl http://127.0.0.1:8080/actuator/health
curl http://127.0.0.1:8080/actuator/prometheus
```

### 응답 의미

- `201 Created`: 처리 결과 리소스를 생성했다. 성공한 푸시를 뜻하지 않는다. `status`를 반드시 확인.
- `PROVIDER_ACCEPTED`: Mock이 요청을 수락. 단말 도착 아님.
- `RETRYABLE_FAILURE`: 재시도 가능한 실패로 분류. 현재 버전에서는 자동 재시도하지 않음.
- `PERMANENT_FAILURE`: 영구 실패로 분류.
- `INTERNAL_FAILURE`: 예상 밖 provider 오류. 상세 메시지를 응답/로그에 노출하지 않음.
- `PROCESSING`: 초기 DB 저장 후 처리 중. 중단/결과 저장 실패 시 이 상태로 남을 수 있음.
- `400`: 유효하지 않은 요청/UUID. `404`: 알림 없음.
- `503`: DB 접근/트랜잭션 실패. `memory` 프로필에서는 저장 공간 한도 초과도 포함.

503이나 응답 유실이 공급자 미호출을 뜻하지는 않는다. 결과 저장이 실패하면 이미 호출했을 수 있다.
현재는 요청 멱등성과 자동 복구가 없어 재요청 시 중복 발송 시도가 가능하다.

Outbox 단계에서는 API를 비동기 접수(202)로 변경한다. 지금 202를 반환하지 않는 것은 의도적이다.

## 실패 모드

서버를 정상 종료한 다음 다음 중 하나로 시작한다. 현재 설정은 런타임 동적 변경이 아니다.

```bash
./gradlew bootRun --args='--app.mock.outcome=TRANSIENT_FAILURE'
./gradlew bootRun --args='--app.mock.outcome=PERMANENT_FAILURE'
./gradlew bootRun --args='--app.mock.delay-ms=1000'
```

기본값: 지연 100ms, SUCCESS. 허용 지연 0~5000ms.
DB 연결 풀은 최대 8개, 연결 대기 3초로 제한한다. 레코드 보관/정리 정책은 아직 없다.

## S1 비교 실행

```bash
./gradlew bootRun --args='--spring.profiles.active=memory'
```

이 프로필은 Docker/DB 없이 실행하며 메모리 최대 10,000건, 재시작 시 데이터 삭제라는 S1 특성을 유지한다.
전체 테스트 명령 `./gradlew test`에는 여전히 MySQL 테스트가 포함된다.
특정 단위 테스트만 실행하려면 `./gradlew test --tests '*NotificationServiceTest'`를 사용하고,
그 결과를 S2 전체 검증 통과로 기록하지 않는다.

| 항목 | S1 `memory` | S2 기본 실행 |
| --- | --- | --- |
| 생성 응답 | 동기 처리 후 201 | 결과 DB 커밋 후 201 |
| 저장 | 용량 제한 메모리 | MySQL |
| 재시작 후 조회 | 불가 | 저장된 레코드 조회 가능 |
| 미완료 작업 복구 | 없음 | PROCESSING은 남지만 자동 복구 없음 |
| Outbox/Kafka | 없음 | 스키마만 준비, 발행·소비 없음 |

## 지표

- `notification_provider_attempts_total`: provider 호출 시도.
- `notification_completed_total{status=...}`: 상태별 처리 결과.
- `notification_processing_seconds_*`: 초기 저장 이후 provider 호출과 결과 저장 시간 histogram.
- `http_server_requests_seconds_*`: HTTP 요청 시간.

recipient/notification ID 또는 payload는 metric label로 쓰지 않는다.
Outbox age/Kafka lag/실제 비동기 종단 지연은 해당 단계에서 추가한다.

## 이번 버전의 한계

- DB에 커밋된 데이터는 앱 재시작 후 조회 가능하다. 공급자 호출과 결과 저장 사이 중단 시 결과는 불확실하다.
- PROCESSING 자동 복구/재시도는 없다. DB 장애, 디스크/volume 손실 및 운영 고가용성을 보장하지 않는다.
- 동일한 POST를 반복하면 서로 다른 알림이 생성된다. 요청 멱등성은 다음 단계.
- Kafka/Outbox 발행·소비/Redis/Retry/DLQ/FCM/APNs는 미구현.
- 인증, 대량 대상자 조회, 다중 인스턴스 처리의 장애 복구는 미구현.
- 실제 외부 공급자 timeout, 연결 풀, quota를 시뮬레이션하지 않는다. 나중에 HTTP Mock으로 분리.
- 1만/100만/1천만 건 부하 테스트 결과를 아직 보유하지 않는다.

## 다음 단계와 문서

- `PLAN.md`: 전체 순서/완료 조건/체크리스트
- `docs/PROGRESS.md`: 실제 검증 결과와 이어서 할 작업
- `AGENTS.md`: 작업 규칙. 별도 에이전트를 생성하는 파일이 아님
- [CONTRIBUTING.md](CONTRIBUTING.md): 커밋 메시지, 브랜치, PR, 검증 규칙
- [.github/pull_request_template.md](.github/pull_request_template.md): 변경 이유와 검증 결과 체크리스트
- [docs/S2_PERSISTENCE.md](docs/S2_PERSISTENCE.md): DB 스키마, 트랜잭션 경계, 실패 시 상태

다음 구현은 S3: 요청/Outbox 원자적 저장 + Kafka 발행/소비 + HTTP 202 전환.
