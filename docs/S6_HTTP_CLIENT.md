# S6: Worker HTTP 연동과 호출 자원 상한

## 현재 구성과 실행

기존 PushProvider 경계에 `HttpPushProvider`를 추가했다. `app.provider.mode=HTTP`이면 Worker가 별도 HTTP Mock을 호출한다.
기본 `MOCK`은 이전 단계의 JVM 내부 Mock을 유지한다. 선택한 공급자는 sync/memory 프로필의 동기 호출에도 적용된다.
`MOCK`과 `HTTP` 외의 값은 기동 실패로 드러난다.

터미널 두 개에서 실행한다. 알림 앱은 기존 MySQL/Kafka 및 DB 환경변수 설정이 필요하다.

```bash
# 터미널 1: 별도 공급자 시뮬레이터
./gradlew mockServer
# 터미널 2: 기존 알림 앱의 공급자만 HTTP로 전환
./gradlew bootRun --args='--app.provider.mode=HTTP'
```

다른 Mock 포트를 쓸 때는 양쪽을 맞춘다.

```bash
./gradlew mockServer --args='--server.port=18082 --mock.http.mode=SERVER_ERROR'
./gradlew bootRun --args='--app.provider.mode=HTTP --app.provider.http.base-url=http://127.0.0.1:18082'
```

알림 접수/조회 API는 그대로다. Worker는 짧은 DB 선점 후 HTTP를 호출하고, 결과/시도 이력/후속 Outbox를
DB에 저장한 뒤 Kafka offset을 커밋한다. HTTP 대기 중 DB 트랜잭션을 유지하지 않는다.

HTTP 목적지는 명시적 포트를 가진 `http://127.0.0.1` origin만 허용한다. DNS, TLS, 실제 FCM/APNs 연동은 이번 범위 밖이다.
사용자 정보·경로·query·fragment가 있는 base URL은 거절하고, 요청 경로는 `/pushes`로 고정한다.

## 왜 timeout을 여러 개 두는가

| `app.provider.http` 설정 | 기본값 | 범위 | 제한 대상 |
| --- | --- | --- | --- |
| `max-connections` | 2 | 1~8 | 프로세스 전체 및 목적지별 연결 수 |
| `max-pending` | 4 | 0~16 | 연결 수에 추가로 허용하는 호출 수 |
| `acquire-timeout-ms` | 200 | 1~1000 | 연결 풀에서 연결을 기다리는 시간 |
| `connect-timeout-ms` | 500 | 1~2000 | 새 TCP 연결을 맺는 시간 |
| `response-timeout-ms` | 1000 | 1~5000 | 응답/데이터를 기다리는 I/O 비활성 시간 |
| `deadline-ms` | 3000 | 1~10000 | 풀 획득부터 응답 본문 읽기까지 전체 HTTP 교환 시간 |

전체 deadline은 각 개별 timeout보다 짧게 설정할 수 없다. 운영체제 스케줄링까지 포함한 엄밀한 실시간 보장은 아니다.
HTTP 교환 종료 후의 DB 저장 시간은 포함하지 않는다. Worker lease 30초 안에 유한한 호출 예산을 두려는 설정이다.

Apache HttpClient 5.5.2(Boot BOM)를 사용하며 STRICT 연결 풀의 전체/목적지별 상한을 동일하게 설정했다.
호출 진입은 대기 없는 Semaphore로 `max-connections + max-pending`개까지만 허용한다.
연결 풀 대기 또한 acquire timeout으로 끝난다. 별도 작업 executor에 요청을 무한히 쌓지 않는다.
HTTP 풀과 별도로 [Worker 처리/대기 상한](S6_WORKER_CONCURRENCY.md)을 설정할 수 있다.
기본 Worker는 단건 순차 처리이며 Kafka pause/resume는 아직 구현하지 않았다.

응답 timeout만 있으면 조금씩 계속 도착하는 데이터 때문에 호출이 길어질 수 있다.
단일 deadline 스레드가 제한 시간 후 요청을 취소하고 연결을 닫는다. 예약 작업 수는 진입한 호출 수 이내이며
완료된 예약은 즉시 제거한다. 종료 시 진행 중 요청과 풀/스레드도 정리한다.

자동 HTTP 재시도·redirect·cookie·응답 압축을 비활성화했다. 공급자 호출 하나에 최대 한 번의 HTTP 시도만 한다.
재시도는 기존 DB 기반 최대 횟수/backoff/jitter/Retry 토픽을 통해서만 실행한다.
정상 응답은 최대 8KiB, providerMessageId는 Mock이 생성한 UUID 형식만 수용한다.
오류 본문은 저장하거나 로그에 남기지 않고, 응답 헤더/오류 본문에 포함된 주소로 이동하지 않는다.

## 결과 분류와 불확실성

| HTTP/클라이언트 결과 | PushResult | 저장하는 errorCode |
| --- | --- | --- |
| 유효한 202 + Mock message ID | ACCEPTED | 없음 |
| 429, 5xx | RETRYABLE_FAILURE | HTTP_STATUS_429 / HTTP_STATUS_5xx |
| 408·429를 제외한 4xx | PERMANENT_FAILURE | HTTP_STATUS_4xx |
| 연결 풀 대기 초과 | RETRYABLE_FAILURE | HTTP_POOL_TIMEOUT |
| 진입 상한 초과 | RETRYABLE_FAILURE | HTTP_CLIENT_BUSY |
| 응답/연결 timeout | RETRYABLE_FAILURE | HTTP_UNKNOWN_TIMEOUT |
| 전체 deadline 취소 | RETRYABLE_FAILURE | HTTP_UNKNOWN_DEADLINE |
| 연결/읽기/JSON I/O 오류 | RETRYABLE_FAILURE | HTTP_UNKNOWN_IO |
| 미지원 status(408 포함), 잘못된/큰 응답 | RETRYABLE_FAILURE | HTTP_UNKNOWN_STATUS / HTTP_UNKNOWN_RESPONSE |
| 호출 전 interrupt, 종료된 클라이언트 | RETRYABLE_FAILURE | HTTP_INTERRUPTED / HTTP_CLIENT_CLOSED |

`HTTP_UNKNOWN_*`는 미발송 확정이 아니다. 이번에는 기존 재시도 정책에 따라 최대 3회 시도한다.
`delivery_attempt.outcome`은 RETRYABLE_FAILURE이며 오류 코드로 불확실성을 구분한다.
기존 `UNKNOWN` outcome은 프로세스 종료 후 lease 복구 이력에 사용하는 의미를 유지한다.
3회 소진 후에도 최종 상태는 RETRYABLE_FAILURE이고 DLQ로 인계한다. 이미 공급자가 수락했을 가능성을 없애지 않는다.

실제 연동 테스트의 결과는 다음과 같다.

| Mock 조건 | HTTP 호출 | Mock 수락 | DB 최종 상태 | 요청/Retry offset 합 | DLQ Outbox |
| --- | --- | --- | --- | --- | --- |
| 정상 | 1 | 1 | PROVIDER_ACCEPTED | 1 | 0 |
| 500 | 3 | 0 | RETRYABLE_FAILURE | 3 | 1 |
| 429 | 3 | 0 | RETRYABLE_FAILURE | 3 | 1 |
| 영구 실패 400 | 1 | 0 | PERMANENT_FAILURE | 1 | 1 |
| 지연 700ms / response timeout 200ms | 3 | 3 | RETRYABLE_FAILURE | 3 | 1 |

마지막 행은 공급자 수락 후 응답을 잃었을 때의 중복 가능성을 실제 HTTP로 보여준다.
단말 도착이나 exactly-once를 보장하는 결과가 아니다. [불확실성 정책](S4_UNCERTAIN_OUTCOMES.md)을 함께 읽는다.

## 검증과 남은 작업

```bash
./gradlew test --tests '*HttpPushProviderTest'
./gradlew test --tests '*HttpWorkerTest'
./gradlew build --rerun-tasks
```

실제 HTTP의 분류/응답 지연, 풀 대기 초과/진입 거절/연결 재사용, 느린 본문의 전체 deadline,
redirect 거부·응답 상한·연결 거절·종료 취소를 확인한다.
MySQL/Kafka 연동에서는 API 202부터 DB 이력, Mock 호출/수락 수, DLQ와 offset까지 대조한다.
개별/전체 실행 결과는 [PROGRESS](PROGRESS.md)에 기록한다.

connect timeout 설정은 적용했으나 패킷을 버리는 네트워크에서 실제 TCP connect 만료 시각을 측정하지 않았다.
이번 연결 실패 검증은 loopback의 닫힌 포트에 대한 연결 거절이다. DNS/TLS/원격 네트워크 동작은 미검증이다.

현재 429의 Retry-After는 재시도 시각에 반영하지 않는다. 기존 DB backoff를 사용한다.
Worker 병렬 처리/유한 FIFO 대기열은 별도 단계에서 추가했다.
후속 단계에서 [Redis 공유 quota](S6_WORKER_QUOTA.md), [pause/resume](S6_CONSUMER_PAUSE.md),
[프로세스별 Circuit Breaker](S6_CIRCUIT_BREAKER.md)를 연결했다. [Redis 장애/예산 유실 정책](S6_REDIS_RECOVERY.md)도 추가했다.
연결 풀은 프로세스별이며 Worker를 늘리면 합산 연결 수도 증가한다. S6 전체 완료 또는 전역 보호 달성을 뜻하지 않는다.

설정의 근거: Apache의 [RequestConfig 소스](https://github.com/apache/httpcomponents-client/blob/rel/v5.5.2/httpclient5/src/main/java/org/apache/hc/client5/http/config/RequestConfig.java)와
[연결 풀 설명](https://hc.apache.org/httpcomponents-client-5.6.x/connection-pooling.html).
