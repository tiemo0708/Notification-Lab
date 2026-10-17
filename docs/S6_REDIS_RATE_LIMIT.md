# S6: Redis Lua 전역 Rate Limit

## 문제와 이번 범위

Worker 동시 처리 수와 HTTP 연결 수는 프로세스별 상한이다. 앱을 2대 실행하면 합산 호출 수가 늘어난다.
또한 같은 동시성이라도 공급자가 빨라지면 초당 요청량이 증가한다. 동시 처리 수와 시간당 요청 예산은 별도 제어가 필요하다.

이번에는 여러 클라이언트가 **동일 Redis의 동일 공급자 키**를 사용해 한 예산을 공유하는 제한기를 구현했다.
이 문서는 제한기 컴포넌트 자체의 설계와 검증을 설명한다.
후속 [Worker 연동](S6_WORKER_QUOTA.md)에서 DB 선점 전 대기와 최초/재시도 공통 quota를 추가했다.

## 대안과 선택 기준

| 방식 | 장점 | 이번 문제에서의 비용/한계 |
| --- | --- | --- |
| 앱마다 메모리 제한기 | 외부 왕복이 없음 | 앱 수에 따라 공급자 합산 예산이 늘어남 |
| Redis 고정 윈도우 카운터 | 간단하고 저장량이 작음 | 윈도우 경계에서 요청이 몰리며 속도/버스트를 별도 정의하기 어려움 |
| Sliding window 로그 | 이동 구간의 요청 수를 엄격하게 계산 | 요청별 기록과 만료/정리 비용이 필요함 |
| Redis token bucket + Lua | 고정 크기 상태, 지속 속도와 버스트를 분리, 여러 앱이 공유 | Redis 왕복/장애 의존, 버스트를 허용하므로 임의의 1초 구간 요청 수가 rate와 같지 않음 |

학습용 공급자의 지속 요청량과 순간 몰림을 별도로 설정하려고 token bucket을 선택했다.
클라이언트에서 GET 후 SET하면 동시에 같은 잔량을 쓸 수 있으므로 보충·확인·차감을 Lua 한 번으로 실행한다.
Redis는 스크립트를 원자적으로 실행하며 실행 중 다른 명령이 끼어들지 않는다.
스크립트는 키 1개와 고정된 필드 5개만 처리하고 반복문/스캔을 사용하지 않는다.
후속 [장애 정책](S6_REDIS_RECOVERY.md)에서 v2로 변경해 키 부재/TTL도 거절하고 명시적인 빈 초기화를 사용한다.
근거: [Redis 스크립팅 계약](https://redis.io/docs/latest/develop/programmability/eval-intro/).

## 속도와 버스트 계약

- `rate-per-second = r`: 초당 보충하는 토큰 수. 기본 10, 허용 1~10000.
- `burst = B`: 최대 보관 토큰 수(v2 초기 토큰은 0). 기본 10, 허용 1~10000.
- 한 번의 `tryAcquire()` 허용은 토큰 1개를 소비한다. 사용하지 않은 허가나 실패한 호출의 토큰을 반환하지 않는다.
- 동일 Redis 시간 기준 구간 Δms의 허용 수는 `B + floor(r × Δms / 1000)` 이하이다.
  기본값은 충분히 보충된 예산에서 순간 10개를 허용하고 이후 초당 10개를 보충한다. 최초 빈 초기화 직후에는 보충을 기다린다. 모든 1초 구간을 반드시 10개 이하로 제한하는 정책은 아니다.
- 공급자 fixed-window 한도와 token bucket은 서로 다른 계약이다. 실제 공급자 문서/허용 버스트에 맞춰 별도로 설정해야 한다.
  이 수치를 실제 FCM/APNs quota로 제시하지 않는다.

Lua는 Redis `TIME`을 밀리초로 변환한다. 앱 서버 시간은 인자로 보내지 않는다.
토큰 1개를 정수 credit 1000으로 나타내므로 `r` 토큰/초는 `r` credit/ms이다.
거절될 때에도 소수 토큰에 해당하는 credit을 보존해, 자주 재확인한다고 보충 진행이 사라지지 않는다.
오래 쉬어도 최대 B개만 쌓이며, 큰 시간 차이는 보충에 필요한 길이로 제한한 뒤 곱한다.
시간 출처: [Redis TIME](https://redis.io/docs/latest/commands/time/).

서버 시간이 뒤로 이동하면 `last_ms`를 뒤로 되돌리지 않고 추가 보충을 멈춘다.
LIMITED의 재확인 시간에는 시계가 따라잡는 시간도 포함한다.
앞으로 급격히 이동하면 최대 B개까지 빨리 보충될 수 있다. wall clock 기반이며 시계 급변을 완전히 제거하지 않는다.

## 키와 설정 일관성

기본 키는 `notification-lab:provider:mock:quota`이며 recipient/request/instance ID를 붙이지 않는다.
모든 참여자가 같은 Redis DB 0, 같은 키, 같은 rate/burst를 사용해야 한다.
별도 Redis/키를 쓰는 앱은 별도 예산을 만들므로 전역 제한에 참여하지 않는다.

Hash 필드는 `version`, `rate`, `burst`, `credit`, `last_ms`다. 기존 rate/burst/version과 다르면
`UNAVAILABLE`로 반환하며 잔량을 덮어쓰거나 키를 삭제하지 않는다. 잘못된 타입/불완전한 상태/범위 밖 credit도 거절한다.
정책 변경을 혼합 배포로 처리하지 않는다. version=2만 허용하며 [전환/복구 절차](S6_REDIS_RECOVERY.md)를 따른다.

**TTL을 두지 않는다.** 사용자별 키가 아니라 공급자당 고정 키 하나라 요청량에 따라 키 수가 늘지 않는다.
만료에 의한 예산 초기화나 설정 충돌의 자동 해제를 피한다. 앱 클라이언트 재시작과 `SCRIPT FLUSH`도 잔량을 초기화하지 않는다.
v1에서는 외부 키 삭제/상태 유실로 초기 버스트가 재생성될 수 있었다. v2는 키 부재·예상 밖 TTL에서 UNAVAILABLE을 반환한다.
최초 설치를 포함해 명시적인 초기화가 필요하며, 정상 형식의 오래된 snapshot 복원은 자동 감지하지 못한다.

## Java 호출과 자원 상한

`RedisRateLimiter.tryAcquire()`는 대기/자동 재시도 없이 한 번 확인한다.

| 결과 | 의미 | 호출자가 지켜야 할 계약 |
| --- | --- | --- |
| `ALLOWED` | 토큰 1개 차감 완료 | 공급자 호출을 진행할 수 있음 |
| `LIMITED` | 현재 토큰 부족 | `retryAfterMs` 이후 다시 확인; 예약/미래 허가가 아님 |
| `UNAVAILABLE` | Redis 오류, timeout, 풀 포화/종료, 정책 충돌/상태 오류/키 부재/TTL | 허가로 취급하지 않음; retryAfterMs=0도 즉시 호출 허가가 아님 |

timeout이면 Redis에서 이미 토큰을 소비했을 수도 있다. 응답을 못 받았다고 자동 재시도하거나 환불하지 않는다.
따라서 장애 중 예산을 덜 사용할 수 있다. Worker의 유한 대기/소비 중지/명시적 재시작과 offset 보존은
[연동 문서](S6_WORKER_QUOTA.md)에서 다룬다. 이 문서의 제한기 단독 테스트와 구분한다.

Spring Boot BOM의 Jedis 6.0.0을 직접 사용한다. Spring Data Redis 자동 설정/health 의존성을 추가하지 않았다.
연결 풀은 기본 2개(1~4), 추가 대기열 없음(`blockWhenExhausted=false`).
연결 생성 중 경합도 포함하려고 풀 진입 전에 같은 크기의 세마포어를 `tryAcquire`한다.
처음에는 풀 설정만 사용했으나 응답 없는 첫 연결 중 추가 호출이 약 2초 지연되는 경계를 재현해 보완했다.
풀의 생성 경합이 소진 여부 검사보다 먼저 진행되는 점은
[Commons Pool 2.12.1 소스](https://github.com/apache/commons-pool/blob/rel/commons-pool-2.12.1/src/main/java/org/apache/commons/pool2/impl/GenericObjectPool.java)와 실제 테스트로 확인했다.
연결/소켓 읽기 timeout은 각각 기본 250ms(1~1000)다.
이는 네트워크 단계별 timeout이며 호출 전체의 엄격한 wall-clock deadline 보장은 아니다.
풀은 앱 context 종료 시 닫고, 예외 상세/수신자/payload는 로그나 메트릭 label에 넣지 않는다.
단일 고정 Lua 본문을 `EVAL`로 전송한다. 스크립트 캐시 유무에 의존하지 않는 대신 본문 전송 비용이 있다.
연결 풀 API는 [사용 중인 Jedis 6.0.0 소스](https://github.com/redis/jedis/blob/v6.0.0/src/main/java/redis/clients/jedis/JedisPool.java)를 기준으로 확인했다.

## 설정과 실행 범위

`application.yml`의 `app.rate-limit`:

```yaml
client-enabled: false
wait-timeout-ms: 2000 # Worker 대기 예산, 제한기 자체의 네트워크 timeout과 별개
host: 127.0.0.1
port: 16379
key: notification-lab:provider:mock:quota
rate-per-second: 10
burst: 10
max-connections: 2
timeout-ms: 250
```

`client-enabled=true`는 제한기 빈을 만들며 현재는 비동기 Worker의 최초/재시도 quota에도 적용된다.
기본값 false와 memory/sync 비교 프로필은 기존 기준선을 유지한다.

필요한 경우에만 로컬 Redis를 실행한다. Compose 예시는 README의 `.env` 준비가 된 경우이며,
자동 테스트에는 `.env`나 Compose 사전 실행이 필요 없다.

```bash
docker compose --profile redis up -d --wait redis
# 최초 초기화와 기존 v1 전환은 S6_REDIS_RECOVERY.md를 따른다.
```

Redis 7.4.11-alpine, loopback 16379, 메모리 상한 32MB/컨테이너 128MB, `noeviction`, AOF와 redis-data volume을 사용한다.
환경변수 `REDIS_PORT`로 포트를 바꿀 수 있다. `.env`/기존 MySQL/Kafka 데이터는 변경하지 않는다.
인증/TLS/Cluster/Sentinel은 구성하지 않았다. 학습용 로컬 실행 범위다.
AOF 기본 everysec는 모든 ACK의 영구 보존이나 failover 시 예산 보존을 보장하지 않는다.

## 실제 검증

```bash
./gradlew --no-daemon --console=plain test \
  --tests '*RateLimitConfigurationTest' --tests '*RedisRateLimiterTest'
caffeinate -i ./gradlew --no-daemon --console=plain build --rerun-tasks
```

최초 제한기 단계의 집중 테스트는 13개 통과했다. 아래는 당시 기본 알고리즘 검증이며 v2에서는 기존 예산 조건을 명시적인 fixture로 만든다. Testcontainers Redis 7.4.11을 loopback 임의 포트로 사용하며 Docker 부재 시 자동 스킵하지 않는다.

| 시나리오 | 확인한 결과 |
| --- | --- |
| 독립 Jedis 클라이언트 4개, 요청 40회, 공유 B=7 | 허용 7 / 제한 33 / 오류 0. 저장 시각을 미래로 둬 보충 없이 원자적 차감만 검사 |
| 실제 TIME, 클라이언트 4개, 요청 80회, r=10/B=3 | 최종 전체 실행에서 603ms 동안 허용 8. B+rΔ 예산 이내이며 초기 B를 넘어 보충되는 것도 확인 |
| r=2/B=1, 거절을 20ms마다 재확인 | 실제 서버 시간 최소 500ms 후 다음 토큰 허용; 소수 credit 보존 |
| 긴 유휴 상태, 클라이언트 재생성 | 버스트까지만 보충, 새 클라이언트도 기존 잔량 사용, TTL 없음/필드 수 5 |
| 미래 last_ms | 토큰 보충 없음, 저장 시각 보존. 실제 OS 시계 조작이 아니라 저장 상태를 통한 역행 분기 검증 |
| rate/burst/version 충돌 및 손상된 Hash/타입 | UNAVAILABLE, 기존 상태 그대로 유지 |
| SCRIPT FLUSH | 예산을 초기화하지 않고 다음 호출 정상 판정 |
| CLIENT PAUSE 1500ms / 클라이언트 timeout 250ms / 연결 1개 | 사용 중 연결 1 확인, 추가 호출 즉시 UNAVAILABLE, 진행 중 호출도 timeout으로 UNAVAILABLE. 해제 후 다시 사용 가능 |
| 닫힌 포트 / context 종료 | 허가를 내주지 않음 |
| TCP 연결만 받고 응답하지 않는 서버 / 연결 자리 1개 | 첫 연결 생성 중 추가 호출이 200ms 이내 UNAVAILABLE. 진행 중 연결도 읽기 timeout 이후 UNAVAILABLE |
| 기본 비활성 / 잘못된 설정 | 기본 빈 없음, 활성 설정 검증/종료 확인 |

클라이언트 4개는 한 테스트 JVM 안의 독립 연결 풀이다. 별도 4개 앱 JVM/네트워크 장애/Redis failover를 검증한 것은 아니다.
값은 correctness 실험 결과이며 처리량 벤치마크가 아니다. 전체 회귀 결과는 [PROGRESS](PROGRESS.md)에 기록한다.

## 다음 단계와 남은 한계

최초 발송과 재시도는 이제 같은 limiter를 통과한다. quota 부족을 공급자 실패로 기록하지 않으며
대기 중 시도 횟수/lease/offset 보존을 별도 통합 테스트로 검증했다.
정상 LIMITED의 [소비 pause/resume](S6_CONSUMER_PAUSE.md)와 [프로세스별 Circuit Breaker](S6_CIRCUIT_BREAKER.md)도 연결했다.
[Redis 장애/예산 유실·통제된 복구 정책](S6_REDIS_RECOVERY.md)도 추가했다. 다음 PLAN 항목은 우선순위 배분/자원 분리다.

허가를 받은 뒤 스레드가 멈췄다가 여러 개 동시에 HTTP를 호출하면 허가 시점과 실제 요청 시점이 달라진다.
이 컴포넌트 검증만으로 공급자에서 관측되는 도착률/모든 장애 조합의 quota 준수를 보장하지 않는다.
공급자 수락은 단말 도착이 아니며 중복 수락/exactly-once 한계도 그대로다.
