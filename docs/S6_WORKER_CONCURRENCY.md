# S6: Worker 동시성 제한과 유한 FIFO 대기열

## 문제와 선택 기준

이전 Worker는 Kafka 소비 스레드 하나에서 순차 처리했다. 느린 HTTP 호출 동안 다음 알림은 기다린다.
소비 스레드만 늘리면 DB와 공급자 호출도 함께 증가한다. 작업을 별도 무제한 executor로 넘기면
메모리 대기열이 커지고, 리스너가 먼저 반환하면서 아직 끝나지 않은 레코드의 offset을 처리할 위험이 있다.

이번 단계의 목표는 **처리 중/대기 중 작업 수를 제한하면서 DB 완료 전 offset을 넘기지 않는 것**이다.
운영 처리량 개선을 수치로 입증하거나 전역 공급자 quota를 준수했다는 목표는 아니다.

| 대안 | 장점 | 비용/한계 |
| --- | --- | --- |
| 소비 스레드 1개 유지 | 순서·offset 관계가 단순함 | 느린 호출에 전체 소비가 묶임 |
| 소비 스레드만 늘림 | 파티션 사이 병렬 처리가 쉬움 | 동시 처리 자원도 함께 증가 |
| 별도 비동기 executor + 수동 ack | poll과 발송을 분리할 수 있음 | 완료 순서·파티션 회수·종료·미완료 offset 관리가 복잡함 |
| 소비 스레드 + 공통 유한 FIFO 진입 제어 | 동기 반환/RECORD ack를 유지하며 처리 수와 대기를 제한 | 대기 중 소비 스레드의 다음 poll이 지연됨 |

마지막 방식을 선택했다. 파티션 안에서는 순차 처리하고, 다른 파티션은 정해진 수만큼 동시에 처리한다.
별도 발송 executor나 background future를 만들지 않는다. HTTP 호출과 DB 완료까지 원래 소비 스레드에서 수행한다.

## 처리 순서

1. Kafka가 레코드를 리스너에 전달한다. 소비자당 `max.poll.records=1` 기본값을 유지한다.
2. `WorkerAdmission`이 처리 자리를 배정한다. 자리가 없으면 크기가 제한된 FIFO에 대기 티켓을 넣는다.
3. 자리를 얻은 뒤에만 DB에서 작업/lease를 선점한다. 대기 중에는 시도 횟수가 늘지 않는다.
4. DB 트랜잭션 밖에서 공급자를 호출한다.
5. 결과·시도 이력·후속 Retry/DLQ Outbox를 DB에 저장한다.
6. 처리 자리를 반환하고 리스너가 정상 반환한 뒤, Spring Kafka가 RECORD offset을 커밋한다.

FIFO는 이 프로세스에서 진입 자리를 기다리는 순서다. 전체 알림의 도착/완료 순서를 보장하지 않는다.
같은 파티션의 후속 레코드는 앞 레코드의 리스너 처리가 끝나기 전에 실행하지 않는다.
다른 파티션의 offset은 독립적으로 진행할 수 있으며, 요청/Retry 토픽 사이의 전역 순서는 없다.

## 설정과 실행

| `app.worker` 설정 | 기본값 | 허용 범위 | 의미 |
| --- | --- | --- | --- |
| `consumers` | 1 | 1~3 | 한 앱의 Kafka 소비 스레드 수 |
| `max-active` | 1 | 1~consumers | DB 선점부터 결과 저장까지 동시 처리 수 |
| `queue-capacity` | 2 | 0~3 | 자리 배정을 기다릴 최대 티켓 수. 0은 대기 없이 거절 |
| `queue-wait-ms` | 1000 | 1~5000 | 대기열에서 자리를 기다리는 최대 시간 |

기본 설정은 이전 순차 동작을 유지한다. 요청/Retry 토픽은 각각 3개 파티션이며 기본 RangeAssignor를 유지하므로
이 단계에서는 consumers를 최대 3개로 제한한다. 프로세스별 설정이며 여러 앱을 띄우면 처리 상한이 합산된다.

```bash
# 터미널 1: HTTP 공급자 시뮬레이터
./gradlew mockServer --args='--mock.http.response-delay-ms=500'
# 터미널 2: 기존 DB/Kafka 환경을 사용하는 알림 앱
./gradlew bootRun --args='--app.provider.mode=HTTP --app.worker.consumers=3 --app.worker.max-active=2 --app.worker.queue-capacity=1 --app.worker.queue-wait-ms=5000'
```

이 예시는 소비 스레드 3개 중 최대 2개가 처리하고 1개가 기다린다. HTTP 풀 기본 연결 수도 2개다.
Worker 처리 수보다 HTTP 연결 수가 적으면 HTTP 풀 대기가 추가로 발생하므로 두 설정을 함께 확인한다.
처리 자리 제한은 요청 토픽과 Retry 토픽에 공통 적용한다. 초당 요청량/전역 quota를 제한하는 것은 아니다.

대기열은 티켓만 저장한다. 레코드는 호출 중인 소비 스레드가 보유한다. 메모리 발송 작업을 따로 무한히 복제하지 않는다.
미소비 backlog는 Kafka에 남지만 Kafka의 fetch buffer, 브로커 보관 기간/디스크는 별도 자원이다.
이 설정이 JVM 전체 메모리나 브로커 저장량의 절대 상한은 아니다.

## 포화·실패·종료 정책

대기열 포화/대기 timeout/interrupt는 DB 선점 전에 예외로 끝난다. 리스너가 성공 반환하지 않으므로 해당 offset은 커밋하지 않는다.
기존 CommonContainerStoppingErrorHandler가 컨테이너를 멈추며 자동 무한 재시작하지 않는다.
이는 알림의 최종 실패/DLQ가 아니다. 아직 시도하지 않은 알림은 QUEUED와 미커밋 레코드로 남는다.
다른 파티션에서 이미 진행 중인 작업은 완료될 수 있다.

원인을 해결하고 명시적으로 재시작한다. 이미 처리 중이었다가 끊긴 작업은 lease/불확실성 정책도 확인해야 한다.
풀/대기 설정이 그대로인데 단순 재시작만 반복하면 같은 이유로 다시 멈출 수 있다.
공급자 장애에 따른 HTTP 결과는 기존 최대 시도 횟수/DB backoff/Retry/DLQ 정책을 그대로 사용한다.

ContextClosedEvent가 오면 새 진입을 막고 대기 스레드를 깨운다. 이미 처리 자리를 받은 작업은 기존 Kafka 종료 유예 안에서 완료한다.
종료 중 거절 예외가 별도의 비정상 stop을 시작하면 Kafka 컨테이너가 먼저 stopped로 표시되어
Spring lifecycle이 진행 중 작업의 종료 대기를 건너뛸 수 있다. 실제 종료 테스트에서 PROCESSING 잔존을 재현했다.
이를 막기 위해 앱 종료 중 오류 처리기는 추가 stop을 시작하지 않고 예외를 전파하여 기존 lifecycle이 종료를 담당하게 했다.
이때도 거절된 레코드를 성공 처리하거나 offset을 넘기지 않는다.

## 관찰 지표

- `notification_worker_active`: 현재 진입한 처리 수. HTTP 대기와 결과 DB 저장을 포함한다.
- `notification_worker_waiting`: 현재 FIFO 대기 수.
- `notification_worker_admission_rejected_total{reason=full|timeout|interrupted|stopping}`: 거절 사유별 횟수.
- 기존 `notification_worker_failures_total`, `notification_worker_running`, consumer lag와 함께 확인한다.

label에 알림 ID나 payload를 넣지 않는다. 거절 카운터는 실제 해당 거절이 발생한 뒤 생성된다.
동시 변화 중 여러 지표의 조회 값은 하나의 원자적 snapshot이 아니다.

## 실제 검증과 해석

```bash
./gradlew test --tests '*WorkerAdmissionTest' --tests '*WorkerConcurrencyTest'
./gradlew build --rerun-tasks
```

단위/동시 스레드 테스트 6개: FIFO, 처리/대기 상한, timeout 제거, interrupt 보존,
종료 시 대기 해제/기존 처리 유지, 예외 시 자리 반환, 설정 범위 검증.

MySQL·Kafka·HTTP 통합 시나리오 5개:

- 소비 스레드 3/처리 2/대기 1, HTTP 지연 1500ms에서 실제 최고 HTTP 처리 2 확인.
  대기 중 알림과 같은 파티션의 후속 알림은 시도 0이며, 진행 중 세 파티션 offset도 0을 확인했다.
  최종 호출 4·완료 4·offset 합 4, 파티션별 2/1/1로 대조했다.
- 대기 timeout 100ms: 거절된 알림은 시도 0·QUEUED·해당 offset 0.
  진행 중 작업 완료 후 명시적 컨테이너 재시작으로 두 알림을 추가 중복 호출 없이 완료했다.
- queue-capacity=0 포화: DB 선점 없이 거절되고 명시적 재시작으로 복구했다.
- DB 완료 저장 실패: 같은 파티션의 후속 알림은 시도 0, 해당 offset 0을 유지했다.
  실패 trigger를 제거하고 테스트에서 lease를 만료시킨 뒤 재시작해 복구했다.
  첫 요청은 UNKNOWN 이력을 남기며 다시 공급자를 호출하므로 총 호출 4/고유 완료 3/offset 합 3이다.
- 대기 작업이 있는 상태에서 앱 종료: 진행 중 작업 완료, 대기 작업 시도 0/offset 0 확인.
  같은 group의 새 앱이 남은 작업을 처리해 최종 호출/완료/offset 합 2를 확인했다.

이 통합 테스트는 파티션을 고정하기 위해 Relay를 끄고 API가 저장한 실제 Outbox payload를 Kafka에 직접 발행한다.
Outbox는 수동으로 PUBLISHED로 꾸미지 않는다. Relay 발행을 포함한 전체 흐름은 기존 회귀 테스트에서 검증한다.
DB 실패 복구의 lease 만료는 SQL로 재현했다. 실제 시간 경과/프로세스 종료는 기존 S5 테스트 범위다.
실행 환경과 전체 결과/실패 기록은 [PROGRESS](PROGRESS.md)에 남긴다.

## 남은 한계와 다음 단계

대기 중에는 소비 스레드가 poll하지 않는다. 최대 대기는 5초, HTTP deadline은 최대 10초지만 DB 지연까지 합친
모든 상황에서 `max.poll.interval.ms=60000`을 지킨다는 보장은 없다. Kafka 설정을 바꾸면 이 예산도 다시 검증해야 한다.
장시간 대기 중 poll/heartbeat를 유지하는 pause/resume는 다음 계획에 별도로 남아 있다.
Worker 처리 자리는 로컬 호출과 DB 처리의 상한이다. 클라이언트 timeout 뒤에도 공급자가 계속 처리할 수 있으므로
공급자 서버의 전체 실행 수까지 같은 값으로 제한한다는 보장은 없다.
다중 앱 전역 제한·초당 Rate Limit·429 Retry-After·Circuit Breaker는 아직 구현하지 않았다.
다음 미완료 항목은 Redis Lua 전역 Rate Limit과 속도/버스트 정책이다.

근거: Spring Kafka 3.3의 [컨테이너·동시성·offset 설명](https://docs.spring.io/spring-kafka/reference/3.3/kafka/receiving-messages/message-listener-container.html),
[listener thread safety](https://docs.spring.io/spring-kafka/reference/3.3/kafka/thread-safety.html).
