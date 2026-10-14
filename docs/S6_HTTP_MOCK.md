# S6 첫 단계: 별도 HTTP Mock 공급자

## 현재 범위

`src/mockServer`에 독립 Spring Boot HTTP 서버를 추가했다. DB/Kafka 없이 실행하며 실제 FCM/APNs를 호출하지 않는다.
기존 알림 앱과 Worker는 아직 JVM 내부 `MockPushProvider`를 사용한다.
다음 단계에서 connect/response timeout·연결 풀 상한을 가진 HTTP 어댑터를 연결한다.
이번 서버의 요청 한도는 공급자 측 장애 재현용이며, 아직 Redis 전역 Rate Limit이나 Worker 보호 장치가 아니다.

기본 주소는 `127.0.0.1:18081`. 별도 source set·패키지·설정 파일을 사용하며 알림 앱 JAR에는 포함하지 않는다.
HTTP Mock JAR에도 알림 앱의 API/Worker/Relay 클래스는 포함하지 않는다. 의존 라이브러리는 기존 Boot BOM을 공유한다.

## 실행

Java 21만으로 실행할 수 있다. 이 서버 실행에는 Docker, DB, Kafka, `.env`, 공급자 키가 필요 없다.

```bash
./gradlew mockServer
# 별도 실행 파일 생성/기동
./gradlew mockServerJar
java -jar build/libs/notification-lab-0.3.0-SNAPSHOT-http-mock.jar
```

`./gradlew build`도 두 JAR와 일반 테스트를 함께 만든다. 일반 테스트 전체는 기존처럼 Docker가 필요하다.

```bash
curl -i http://127.0.0.1:18081/pushes \
  -H 'Content-Type: application/json' \
  -d '{"notificationId":"00000000-0000-0000-0000-000000000001","recipientId":"synthetic-user","title":"테스트","body":"가상 데이터"}'
curl http://127.0.0.1:18081/actuator/health
curl http://127.0.0.1:18081/mock/config
curl http://127.0.0.1:18081/mock/stats
```

202 응답은 `{"providerMessageId":"http-mock-..."}`다. Mock 수락이며 단말 도착이 아니다.
같은 notificationId를 다시 보내도 별도 수락하며 새 providerMessageId를 반환한다. 공급자 멱등성을 제공하지 않는다.
요청 데이터/ID 목록을 저장하지 않고 집계만 유지한다. 재시작하면 집계와 요청 한도가 초기화된다.

## 실패·지연 설정

설정은 기동 시 적용하며 바꾸려면 서버를 종료한 뒤 새 설정으로 실행한다.

```bash
./gradlew mockServer --args='--mock.http.mode=TOO_MANY_REQUESTS'
./gradlew mockServer --args='--mock.http.mode=SERVER_ERROR'
./gradlew mockServer --args='--mock.http.mode=INVALID_RECIPIENT'
./gradlew mockServer --args='--mock.http.response-delay-ms=2000'
./gradlew mockServer --args='--mock.http.requests-per-window=3 --mock.http.window-ms=1000'
```

| 설정 | 기본값 | 허용 범위/의미 |
| --- | --- | --- |
| `server.port` | 18081 | 개발용 포트. 테스트는 임의 포트 0 |
| `mock.http.mode` | SUCCESS | SUCCESS / TOO_MANY_REQUESTS / SERVER_ERROR / INVALID_RECIPIENT |
| `mock.http.response-delay-ms` | 100 | 0~10,000ms. 수락/실패 결정을 기록한 뒤 응답을 지연 |
| `mock.http.requests-per-window` | 100 | 1~10,000. 서버 한 프로세스 전체의 유효 요청 한도 |
| `mock.http.window-ms` | 1000 | 100~60,000ms. monotonic clock으로 측정하는 고정 윈도우 |
| `mock.http.max-in-flight` | 4 | 1~8. 응답 지연 중인 처리 수 상한 |

| 조건 | 응답 | errorCode |
| --- | --- | --- |
| 정상 수락 | 202 | 없음, providerMessageId 반환 |
| 강제 429 또는 윈도우 한도 초과 | 429 + Retry-After | MOCK_RATE_LIMITED |
| 강제 서버 실패 | 500 | MOCK_UNAVAILABLE |
| 강제 수신자 영구 실패 | 400 | MOCK_INVALID_RECIPIENT |
| 동시 처리 슬롯 소진 | 503 | MOCK_BUSY |
| 잘못된 JSON/필수 값·길이 오류 | 400 | MOCK_INVALID_REQUEST |
| 8KiB 초과 JSON 본문 | 413 | MOCK_PAYLOAD_TOO_LARGE |

강제 429의 Retry-After는 1초, 한도 초과는 현재 윈도우 잔여 시간을 초 단위로 올림한다.
수신자 100자, 제목 100자, 본문 500자 상한과 UUID 형식을 검사한다. Content-Length가 없는 요청도 8KiB 상한을 적용한다.
잘못된 요청은 한도를 소비하지 않는다. 유효 요청은 먼저 윈도우 한도를 소비하고 이후 슬롯/설정 결과를 적용하므로
500·강제 429·busy도 한도 안에 들어온 시도라면 예산을 소비한다.

## timeout은 응답 실패와 다르다

SUCCESS + 응답 지연 2000ms로 실행한 서버에 아래처럼 0.2초 제한을 두면 클라이언트가 timeout을 경험할 수 있다.

```bash
curl --max-time 0.2 -i http://127.0.0.1:18081/pushes \
  -H 'Content-Type: application/json' \
  -d '{"notificationId":"00000000-0000-0000-0000-000000000001","recipientId":"synthetic-user","title":"timeout","body":"가상 데이터"}'
curl http://127.0.0.1:18081/mock/stats
```

서버는 응답 전에 수락을 기록한다. 클라이언트 timeout이나 연결 종료가 이미 기록한 수락을 취소하지 않는다.
따라서 timeout을 미발송 확정으로 해석하거나 무조건 재시도하면 중복 호출이 생길 수 있다.
실제 테스트는 800ms 응답 지연과 200ms 클라이언트 deadline으로 이 구간을 재현한다.
무한히 응답하지 않는 서버 대신 최대 10초의 유한 지연을 사용한다.

## 상한과 집계 해석

Tomcat 스레드 16, 연결 32, accept backlog 16으로 설정했다. Mock 처리 슬롯은 기본 4이며 슬롯 대기는 하지 않고 503을 반환한다.
연결 한도까지 초과하는 부하에서는 HTTP 응답 전에 연결 거절/timeout이 생길 수 있다. 모든 과부하에 503을 보장하지 않는다.

`/mock/stats`의 requests는 유효한 호출 수, accepted/rateLimited/serverErrors/permanentFailures/busy는 서버의 결정 횟수다.
active는 지연을 포함한 현재 처리 수, maxActive는 기동 후 관찰한 최고 동시 처리 수다.
수락된 응답이 클라이언트에 도달했다는 뜻은 아니다. 카운터는 요청/수신자 ID를 label이나 목록으로 보관하지 않는다.
동시 실행 중 snapshot의 각 필드는 순간적으로 어긋날 수 있으므로 정확한 대조는 요청이 끝난 뒤 한다.

고정 윈도우 경계 양쪽에는 짧은 시간에 두 윈도우의 한도가 몰릴 수 있다. 장기 미사용 예산을 누적하지 않는다.
서버를 여러 개 띄우면 각각 별도 한도를 가진다. 실제 FCM/APNs 쿼터나 전역 속도 제어 보장이 아니다.
이후 Redis/Worker 보호 실험에서 공급자 한도를 지키는지 측정할 대상이다.

## 검증과 다음 단계

```bash
./gradlew test --tests 'dev.jinhyeon.notificationmock.*'
./gradlew build --rerun-tasks
```

실제 HTTP의 202/429/500/400, 동시 한도, 처리 슬롯, timeout 후 수락 유지, 잘못된 입력/큰 본문,
설정 상한과 monotonic 윈도우 리필을 검증한다. 상세 실행 결과는 [PROGRESS](PROGRESS.md)에 기록한다.
현재 Worker HTTP 연동·연결 풀·Redis 제한·Circuit Breaker는 미구현이다. 다음은 HTTP 어댑터와 timeout/연결 풀 상한이다.
