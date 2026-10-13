# Notification Lab

Kafka 기반 알림 시스템을 단계별로 만드는 Java/Spring 학습 프로젝트.
현재 v0.1은 **동기식 Mock 발송 기준선**이다. DB, Outbox, Kafka는 아직 연결되지 않았다.

## 시작

Java 21과 인터넷 연결이 필요하다. Maven Wrapper가 최초 실행 시 Maven과 의존성을 받는다.
이번 버전에는 Docker, Redis, AWS, FCM 키, Claude Code가 필요하지 않다.

```bash
./mvnw test
./mvnw spring-boot:run
```

Windows: `mvnw.cmd test`, `mvnw.cmd spring-boot:run`.
Maven이 설치되어 있다면 `mvn test`와 `mvn spring-boot:run`도 가능하다.
Java 버전이 다르면 로컬의 JAVA_HOME을 JDK 21로 설정한다.

기본 주소: `http://127.0.0.1:8080`. 외부 공개용 인증은 없으므로 바인딩을 임의로 0.0.0.0으로 바꾸지 않는다.

## 실행 흐름

HTTP 요청 -> 입력 검증 -> 메모리 공간 확보 -> Mock Provider 동기 호출 -> 결과 저장 -> HTTP 201.
Mock은 실제 네트워크 요청이나 푸시를 전혀 보내지 않는다.

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
- `400`: 유효하지 않은 요청/UUID. `404`: 알림 없음. `503`: 로컬 저장 공간 한도 초과.

Outbox 단계에서는 API를 비동기 접수(202)로 변경한다. 지금 202를 반환하지 않는 것은 의도적이다.

## 실패 모드

서버를 정상 종료한 다음 다음 중 하나로 시작한다. 현재 설정은 런타임 동적 변경이 아니다.

```bash
./mvnw spring-boot:run -Dspring-boot.run.arguments="--app.mock.outcome=TRANSIENT_FAILURE"
./mvnw spring-boot:run -Dspring-boot.run.arguments="--app.mock.outcome=PERMANENT_FAILURE"
./mvnw spring-boot:run -Dspring-boot.run.arguments="--app.mock.delay-ms=1000"
```

기본값: 지연 100ms, SUCCESS, 저장 최대 10,000건. 허용 지연 0~5000ms.
용량 제한은 메모리 폭증을 막는 로컬 보호 장치이지 Rate Limiter가 아니다.

## 지표

- `notification_provider_attempts_total`: provider 호출 시도.
- `notification_completed_total{status=...}`: 상태별 처리 결과.
- `notification_processing_seconds_*`: 저장 슬롯 확보 이후 provider 처리 시간 histogram.
- `http_server_requests_seconds_*`: HTTP 요청 시간.

recipient/notification ID 또는 payload는 metric label로 쓰지 않는다.
Outbox age/Kafka lag/실제 비동기 종단 지연은 해당 단계에서 추가한다.

## 이번 버전의 한계

- 재시작하면 메모리 데이터가 사라진다. 유실 방지 보장 없음.
- 동일한 POST를 반복하면 서로 다른 알림이 생성된다. 요청 멱등성은 다음 단계.
- Kafka/DB/Redis/Retry/DLQ/FCM/APNs는 미구현.
- 인증, 다중 인스턴스 공유 저장, 대량 대상자 조회는 미구현.
- 실제 외부 공급자 timeout, 연결 풀, quota를 시뮬레이션하지 않는다. 나중에 HTTP Mock으로 분리.
- 1만/100만/1천만 건 부하 테스트 결과를 아직 보유하지 않는다.

## 다음 단계와 문서

- `PLAN.md`: 전체 순서/완료 조건/체크리스트
- `docs/PROGRESS.md`: 실제 검증 결과와 이어서 할 작업
- `AGENTS.md`: 작업 규칙. 별도 에이전트를 생성하는 파일이 아님
- [CONTRIBUTING.md](CONTRIBUTING.md): 커밋 메시지, 브랜치, PR, 검증 규칙
- [.github/pull_request_template.md](.github/pull_request_template.md): 변경 이유와 검증 결과 체크리스트

다음 구현은 S2: MySQL + Flyway + Testcontainers.
그 다음 S3에서 API 저장/Outbox/Kafka 흐름을 연결한다.
