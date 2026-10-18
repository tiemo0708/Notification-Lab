# S6: 공급자 Circuit Breaker와 half-open 복구

## 문제와 선택 기준

공급자가 계속 500을 반환해도 기존 Worker는 새 요청과 예약된 재시도를 계속 보낸다.
Redis quota는 요청 속도를 제한하지만 공급자가 정상인지 판단하지 않는다.
개별 알림의 최대 3회 재시도 역시 전체 공급자 장애 동안 다른 알림까지 실패하는 것을 막지 못한다.
이번 단계는 **실패가 반복되면 새 발송을 잠시 차단하고, 한 요청으로 복구를 확인**한다.

| 대안 | 장점 | 비용과 한계 |
| --- | --- | --- |
| 기존 retry/backoff와 quota만 사용 | 구성과 상태가 단순하다 | 장애 중에도 허용 속도로 발송하고 개별 시도 횟수를 소모한다 |
| 프로세스별 Circuit Breaker | 네트워크 저장소 없이 장애 요청을 줄이고 상태 전이를 학습하기 쉽다 | 앱마다 차단 시점과 복구 탐침이 다르다 |
| 공유 저장소의 전역 회로 | 모든 앱의 탐침을 한곳에서 제어할 수 있다 | 저장소 장애·원자성·lease·복구 정책을 추가로 설계해야 한다 |
| Resilience4j 등의 라이브러리 | 실패율/느린 호출/슬라이딩 윈도우 등 다양한 정책을 제공한다 | 이번 단계에서 필요한 최소 상태 전이보다 설정과 통합 범위가 넓다 |

이번 구현은 **프로세스별 연속 관측 실패 기준의 작은 상태 머신**이다. 라이브러리를 추가하지 않았다.
직접 구현한 상태 머신에는 테스트·유지보수 비용이 있으며, 운영 기능이 늘면 검증된 라이브러리 사용을 재검토한다.
[Resilience4j 공식 설명](https://resilience4j.readme.io/docs/circuitbreaker)은 비교 참고 자료다.
이 프로젝트가 Resilience4j의 슬라이딩 윈도우나 모든 상태를 구현한 것은 아니다.

## 상태와 실패 분류

| 상태 | 동작 | 전환 |
| --- | --- | --- |
| CLOSED(0) | 기존 처리 상한과 quota 안에서 발송 허용 | 관측 실패가 임계값에 도달하면 OPEN |
| OPEN(1) | DB 선점·quota 소비 전에 발송 보류 | cooldown이 지난 다음 진입에서 HALF_OPEN |
| HALF_OPEN(2) | 앱당 한 개 permit만 허용, 나머지는 보류 | 수락 성공이면 CLOSED, 공급자 실패면 새 cooldown의 OPEN |

기본 임계값은 3, cooldown은 5000ms다. 경과 시간에는 wall clock 대신 `System.nanoTime()`을 쓴다.
OPEN의 시간이 끝나도 유입이 없으면 상태 gauge는 OPEN이다. 다음 진입에서 전환한다.
단일 탐침이 아직 진행 중이면 시간이 더 흘러도 두 번째 탐침을 허용하지 않는다.

- 수락 성공: `PROVIDER_ACCEPTED`. CLOSED 실패 수를 0으로 만들거나 HALF_OPEN을 닫는다.
- 공급자 장애로 집계: HTTP 5xx, IO/응답 timeout/deadline, 알 수 없는 응답·상태.
- 집계 제외: 429, 개별 요청의 영구 실패(4xx), 로컬 풀/진입 포화·종료·인터럽트, 앱 내부 오류.
- 제외 결과는 CLOSED 실패 수를 올리거나 초기화하지 않는다. 따라서 “연속”은 **집계 대상 관측 결과 사이**의 연속이다.
  HALF_OPEN에서는 탐침 자리를 반환하되 복구 성공으로 보지 않고 HALF_OPEN을 유지한다.
- 429는 기존 DB 재시도로 처리한다. Retry-After 반영은 이번 범위가 아니다.

CLOSED에서 이미 permit을 받은 작업은 quota/DB 확인을 거쳐 OPEN 전환 뒤 HTTP를 시작하거나 끝낼 수 있다.
permit이 기억한 상태 세대(epoch)가 현재 세대와 다르면 늦게 도착한 결과는 회로를 바꾸지 않는다.
이는 오래된 성공이 OPEN을 닫는 문제를 막지만, 기존 permit을 회수하거나 이미 시작된 HTTP를 취소하지는 않는다.
HALF_OPEN의 한 개 제한은 새 복구 탐침에 적용된다. 이전 세대에서 허용한 작업까지 포함한 전체 동시 호출 상한은 Worker/HTTP 풀에서 정한다.

## Worker와 트랜잭션 경계

```text
Kafka 레코드
  → Worker 처리 자리
  → 짧은 DB 트랜잭션: 작업/세대/시도/lease 확인
      ├─ 완료·오래된 이벤트·시도 소진: 필요한 영속 처리 후 반환
      └─ 발송 필요: 변경 없이 NEEDS_PERMIT 반환
  → Circuit permit (트랜잭션 밖)
      ├─ OPEN / HALF_OPEN 사용 중: 처리 자리 반환 → pause/poll → resume
      └─ 허용: Redis quota (선택) → 새 DB 트랜잭션으로 재확인·선점
  → HTTP 호출 (트랜잭션 밖)
  → 회로에 HTTP 결과 반영
  → 짧은 DB 트랜잭션: 결과/이력/재시도 Outbox 저장
  → Kafka 원본 offset 커밋
```

회로 차단은 알림의 새로운 실패 상태가 아니다. QUEUED/WAITING_RETRY와 기존 시도 횟수를 유지한다.
새 lease·delivery_attempt를 만들지 않으며 Redis 허가도 소비하지 않는다.
완료된 이벤트의 재소비는 OPEN과 Redis 장애 중에도 추가 발송 없이 완료할 수 있다.
시도 상한 소진 처리는 기존 WorkerStore의 영속 복구 경로를 유지한다.

회로 permit 뒤 quota 부족/Redis 오류/DB 경합/예외로 HTTP를 하지 못하면 try-with-resources가 permit을 닫는다.
HALF_OPEN 탐침 자리를 반환하며 이를 공급자 실패나 성공으로 집계하지 않는다.
회로 결과 반영은 HTTP 결과 직후, DB 완료 저장 전이다. DB 오류를 공급자 장애로 세지 않기 위해서다.
따라서 회로가 CLOSED라고 해서 모든 알림의 DB 기록이나 offset 커밋까지 성공했다는 뜻은 아니다.

## pause와 종료

기존 `WorkerPauseErrorHandler`의 부모 컨테이너 pause를 재사용한다.
회로가 열렸으면 남은 cooldown, 탐침 사용 중이면 100ms 뒤 다시 확인한다(지연 범위 100~60000ms).
Spring의 보관 레코드를 성공 처리하지 않고 poll을 유지하며, pending 예약은 부모당 하나이고 전체 상한은 3이다.
서로 다른 소비 스레드가 quota/회로 보류를 동시에 요청하면 먼저 예약한 타이머가 유지된다.
재개 때 모든 gate를 다시 검사하므로 이른 재개가 발송 허가를 우회하지 않는다. 반대로 복구 뒤 재개가 조금 늦을 수 있다.
메트릭의 pause 사유는 그 예약을 만든 첫 번째 사유다.

Redis 오류/DB 오류는 기존 중지 정책을 유지한다. 정상 quota 부족과 회로 차단만 자동 재개한다.
컨테이너 중지 시 예약 취소, 앱 종료 시 스케줄러 종료를 수행한다. 세부 poll/종료 계약은
[소비 pause 문서](S6_CONSUMER_PAUSE.md)를 함께 읽는다.

## 실행과 관찰

기본은 비활성화이며 비동기 HTTP Worker에서만 사용한다. `memory`/`sync` 비교 프로필에는 연결하지 않는다.
HTTP Mock을 먼저 실행하고 기존 DB/Kafka 환경변수를 준비한 뒤 다음과 같이 선택한다.

```bash
./gradlew bootRun --args='--app.provider.mode=HTTP --app.circuit-breaker.enabled=true'
```

Redis quota와 독립적으로 활성화할 수 있다. Redis도 사용할 때는 [기존 실행 설정](S6_WORKER_QUOTA.md)에
`--app.circuit-breaker.enabled=true`를 추가한다. 회로를 켠 상태에서 내부 MOCK 모드를 선택하면 기동을 거절한다.

| 설정 | 기본값 | 허용 범위 |
| --- | --- | --- |
| `app.circuit-breaker.enabled` | false | true/false |
| `app.circuit-breaker.failure-threshold` | 3 | 1~20 |
| `app.circuit-breaker.open-ms` | 5000 | 100~60000ms |

Prometheus 메트릭:

- `notification_provider_circuit_state`: CLOSED=0, OPEN=1, HALF_OPEN=2
- `notification_provider_circuit_failures`: 현재 집계한 실패 수(임계값 상한)
- `notification_provider_circuit_probe_active`: HALF_OPEN permit 점유 0/1. quota/DB 재확인 구간도 포함
- `notification_provider_circuit_decisions_total{outcome=allowed|deferred}`: permit 허용/보류
- `notification_provider_circuit_results_total{outcome=SUCCESS|FAILURE|IGNORED}`: 현재 세대에서 반영한 결과
- `notification_provider_circuit_transitions_total{state=CLOSED|OPEN|HALF_OPEN}`: 상태 전환
- `notification_worker_circuit_pauses_pending`, `notification_worker_circuit_pauses_total`, `notification_worker_circuit_resumes_total`: 회로 사유의 재개 예약/요청 수

카운터는 해당 사건이 처음 발생한 후 노출된다. 허용 permit 수는 실제 HTTP 수와 다를 수 있으며,
늦은 이전 세대 결과와 미사용 permit 취소는 결과 카운터에서 제외한다. 알림 ID나 수신자 값을 label로 쓰지 않는다.

## 검증 방법

`ProviderCircuitBreakerTest`는 가짜 monotonic clock으로 cooldown 경계, 실패 분류, 성공 초기화,
단일 탐침, 재차단, 이전 세대 결과 무시, permit 취소/중복 반환과 설정 범위를 검증한다.
`WorkerCircuitBreakerIntegrationTest`는 실제 MySQL/Kafka/Redis와 loopback HTTP 서버를 이용한다.
각 사례의 DB schema/토픽/group/Redis 키는 격리하며 사용자 로컬 DB를 수정하지 않는다.

1. 실제 Relay: HTTP 500 두 번 뒤 세 번째 재시도는 선점 전 pause. 이때 시도/HTTP/허가/offset=2,
   Worker active=0이며 다른 DB UPDATE가 가능하다. 공급자 202 전환 후 자동 탐침으로 최종 값=3, 수락 성공.
2. 소비 스레드 3개: 첫 장애 뒤 두 작업이 경쟁해도 HALF_OPEN에서 HTTP는 하나만 진행한다.
   나머지는 QUEUED/시도 0/미커밋이다. 탐침 500이면 다시 OPEN, 이후 202이면 복구한다.
3. 완료 재소비: OPEN+Redis 상태 오류에서도 추가 HTTP 없이 offset이 진행한다.
   새 작업은 HALF_OPEN에서 Redis 오류로 미선점 중지하지만 탐침 자리는 반환한다. Redis 복원·명시적 재시작 후 수락 성공.
4. Redis quota를 끈 조건의 OPEN pause 중 앱 종료: 예약 정리와 미완료 offset을 확인한다. 새 context가 남은 작업을 한 번 처리한다.

위는 테스트의 검증 계약이다. 실제 실행 집계와 실패 이력은 [PROGRESS](PROGRESS.md)에 기록한다.
수동 발행 사례는 원본 Outbox를 PENDING으로 두며 Relay의 PUBLISHED 기록을 위조하지 않는다.

## 한계와 다음 단계

- 회로는 프로세스별 메모리 상태다. 재시작은 CLOSED에서 시작하고, 앱 N개면 HALF_OPEN 탐침도 최대 N개다.
  Redis quota만 앱 간 공유한다. 전역 단일 탐침이나 재시작 후 차단 유지는 보장하지 않는다.
- 5xx 등 하나의 연속 실패 기준이다. 실패율/느린 호출/구간별 회로/운영자 강제 상태 변경은 구현하지 않았다.
- HALF_OPEN permit 중 DB가 오래 멈추면 탐침도 붙잡힐 수 있다. 임의 timeout으로 탐침을 반환하면
  이전 HTTP와 새 탐침이 겹칠 수 있어 추가 permit을 자동 발급하지 않는다.
- 부모 전체 pause는 다른 파티션에도 영향을 준다. OPEN 중 전체 대기 시간이나 알림 만료 정책은 없다.
- 공급자 수락 뒤 DB 저장 전 중단의 불확실성, 외부 중복 가능성은 기존과 같다. 단말 도착을 의미하지 않는다.
- 별도 호스트/운영 처리량/새 1만 건 정합성 실험/원격 CI는 이번 검증 범위가 아니다.
- 후속 [Redis 장애/예산 유실 정책](S6_REDIS_RECOVERY.md)은 fail-closed와 명시적 복구를 적용한다.
  [역할별 자원·예약 예산](S6_TRAFFIC_ISOLATION.md)을 추가했다. 회로는 여전히 프로세스별이며 자동 failover는 미지원이다.
