# S8: 파티션 간 완료 순서와 미완료 offset 보존

## 문제와 선택

파티션 0의 첫 작업이 아직 처리 중인데 파티션 1의 작업이 먼저 끝날 수 있다.
완료 건수만 합쳐서 offset을 전진시키면 미완료 작업을 복구할 기회를 잃는다.
검증 단위는 **토픽·파티션별 committed offset과 DB 작업 상태**다.

현재 `NotificationWorker.receive`는 Consumer 스레드에서 동기적으로 실행한다.
여러 Consumer가 서로 다른 파티션을 병렬 처리하지만 한 파티션에서는 앞 작업이 반환된 뒤 다음 작업으로 넘어간다.
이번에는 이 구현을 유지하고 **파티션 간 완료 순서가 바뀌는 경우**를 검증했다.
같은 파티션 안에서 future를 병렬 실행하고 순서 없이 ACK하는 구현을 추가한 것은 아니다.
그 구조로 바꾸려면 연속 완료 구간 추적·pause/resume·종료/재할당 경계를 별도로 검증해야 한다.

Kafka의 committed offset은 재시작 시 읽을 다음 위치다. 레코드 offset 0과 1을 마쳤다면 committed=2다.
새 그룹에 저장된 offset이 없는 상태는 이 테스트의 `auto.offset.reset=earliest`에서 처음부터 읽는다.
비교 helper는 이 상태를 논리적 0으로 취급하지만, 원본에는 `committed: null`, `commitPresent: false`로 보존한다.
**offset 0이 실제 커밋됐다는 뜻으로 해석하지 않는다.**

## 제어 방법

[WorkerConcurrencyTest](../src/test/java/dev/jinhyeon/notificationlab/worker/WorkerConcurrencyTest.java)에
두 parameterized 시나리오를 추가해 `RECORD`와 테스트 전용 `COUNT_TIME`에서 각각 실행했다.

- 실제 MySQL 8.4.8, Kafka 4.0.2와 HTTP Mock을 Testcontainers/앱 컨텍스트로 실행한다.
- Consumer 3개, Worker 활성 상한 2, 대기 큐 0, `max.poll.records=1`을 고정한다.
- auto commit OFF, sync commit ON, async ACK OFF다. 테스트 전용 COUNT_TIME은 10건/100ms, poll은 500ms다.
  실제 생성된 factory/container 설정을 `OffsetCommitPolicy.requireApplied`로 대조한다. 운영 기본은 RECORD다.
- API로 합성 알림 4개를 만든 후 요청 토픽의 파티션을 명시해 `[0,0,1,1]`로 직접 발행한다.
  Relay는 꺼 둔다. Outbox의 PUBLISHED 전이나 Relay 성능을 검증하는 실험은 아니다.
- 파티션 0의 첫 작업만 공급자 진입 직전 latch로 잡고 파티션 1의 두 작업은 실제 HTTP까지 진행시킨다.
  gate에서 DB 트랜잭션이 열려 있지 않은지도 검사한다. gate 상한 45초, 각 시나리오 JUnit 상한 120초다.
- 파티션 1의 두 DB 완료와 committed=2를 기다린 뒤, 1200ms 연속으로 파티션 0의 미완료와 미선점을 확인한다.
  실제 HTTP timeout을 길게 늘리는 방식 대신 호출 전 gate를 사용해 호스트 속도 의존성을 줄였다.
- 단계별 브로커 log end/committed, 작업 상태와 시도 이력, HTTP 집계, Worker 활성/대기 수를 JSON으로 저장한다.
  실패 시에도 수집된 단계와 오류 종류를 보존하고 저장/정리 오류가 원래 실패를 덮지 않게 한다.

## 실제 관측

정상 해제와 완료 저장 실패를 두 모드에서 모두 검증했다.
표의 `—`는 **저장된 offset 없음**이며, 파티션 2에는 메시지를 넣지 않았다.
요청 토픽 log end는 네 경우 모두 `[2,2,0]`이다.

| 단계 | 파티션 0 committed | 파티션 1 committed | HTTP 수락 누적 | 막힌 작업 / 같은 파티션 다음 작업 |
| --- | ---: | ---: | ---: | --- |
| 첫 작업 유지, 다른 파티션 완료 | — | 2 | 2 | PROCESSING·시도1 / QUEUED·시도0 |
| 정상 해제 후 완료 | 2 | 2 | 4 | 둘 다 PROVIDER_ACCEPTED·시도1 |
| 완료 저장 실패로 Consumer 중단 | — | 2 | 3 | PROCESSING·STARTED1 / QUEUED·시도0 |
| 실패 제거·새 컨텍스트 복구 완료 | 2 | 2 | 5 | 수락·시도2·UNKNOWN1 / 수락·시도1 |

완료 실패는 첫 작업의 `PROVIDER_ACCEPTED` UPDATE를 MySQL trigger로 거절해 주입한다.
HTTP는 이미 수락했지만 DB 완료 트랜잭션은 롤백된다. 오류를 성공 처리하지 않고 컨테이너가 중단되는지,
Consumer 그룹 멤버가 빠지고 활성/대기가 0인지 확인한 뒤 offset을 읽는다.
독립 파티션의 committed=2는 유지되고 실패 파티션은 미커밋 상태다.

trigger를 제거하고 기존 앱을 닫은 뒤 테스트 SQL로 해당 worker lease를 DB 현재 시각보다 과거로 옮긴다.
같은 토픽/그룹의 새 앱 컨텍스트에서 미커밋 작업을 재소비하고, 이전 STARTED 시도는 UNKNOWN으로 남긴다.
최종적으로 네 작업의 DB 수락 이력은 각각 하나이며, 실패 작업만 시도2/UNKNOWN1이고 나머지는 시도1이다.
두 요청 파티션 committed=log end=2를 각각 대조한다.
이것은 **DB 시각을 제어한 lease 만료 복구**이며 실제 만료 시간 대기나 SIGKILL 실험은 아니다.

복구 사례는 **알림 4개에 HTTP 수락 5회**다. 첫 HTTP 수락 이후 DB 저장이 실패했기 때문에 재호출이 발생했다.
미완료 offset 보존은 재처리 기회를 유지하지만 외부 부작용을 원자적으로 되돌리지는 못한다.
공급자 멱등키 계약이 없는 현재 Mock 경로에서 외부 exactly-once나 단말 도착을 주장하지 않는다.

## 재현과 증거

```bash
./gradlew --no-daemon --console=plain test \
  --tests 'dev.jinhyeon.notificationlab.worker.WorkerConcurrencyTest.parallelCompletion*'
./gradlew --no-daemon --console=plain test \
  --tests 'dev.jinhyeon.notificationlab.worker.WorkerConcurrencyTest'
./gradlew --no-daemon --console=plain build
```

실제 명령에는 macOS 절전 방지용 `caffeinate -i`를 붙였다.
새 시나리오 4개 집중 실행은 34초에 통과했다. 공용 gate 회귀를 위해 기존 사례를 포함한 클래스 전체도 실행했다.
[고정 소스 167개와 프로토콜](experiments/s8-offset-order-2026-11-06-source.json),
[클래스 집중 검증·원본 4개 SHA](experiments/s8-offset-order-2026-11-06-focused.json)를 보존한다.
같은 소스의 전체 build는 **16분30초/종료코드0**, **48개 클래스·388개 통과/실패·오류·스킵0**이었다.
기존384개에 새4개가 추가됐고 WorkerConcurrencyTest15개도 모두 다시 통과했다.
소스167개와 빌드 설정의 SHA가 집중 검증 시점과 같음을 대조했다.
[전체 로컬 검증·JUnit/원본/소스/JAR SHA](experiments/s8-offset-order-2026-11-06-validation.json)를 남겼다.
알림/HTTP Mock0.3.0 JAR task는 UP-TO-DATE이며 기존0.2.0 산출물과 구분한다.
실행할 때마다 `build/reports/worker-offset-order` 아래에 고유 폴더를 만들어 기존 결과를 덮어쓰지 않는다.
기록에는 역할별 상태만 담고 알림 ID, 수신자, 본문을 내보내지 않는다.

운영 소스/설정은 변경하지 않았다. 이 검증은 처리량 비교, 다중 호스트 장애 내성, Retry/DLQ의 전체 경계,
원래 100만 건 실행의 접수 timeout 원인 해결을 입증하지 않는다.
다음 작은 단계는 기존 메트릭과 Grafana 대시보드의 연결·회귀 점검이다.
가상 스레드 비교는 외부 I/O가 주병목이라는 근거가 확보될 때의 조건부 후보로 유지한다.

## 공식 참고

- [Kafka 3.9.2 Consumer: offsets, group assignment, multithreaded processing](https://kafka.apache.org/39/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html): 읽기 위치와 committed 위치의 구분, 파티션별 소유권과 병렬 처리 주의 사항.
- [Spring Kafka 3.3 Message Listener Containers](https://docs.spring.io/spring-kafka/reference/3.3/kafka/receiving-messages/message-listener-container.html): 자식 컨테이너를 통한 동시 소비와 RECORD/COUNT_TIME 커밋 정책. COUNT_TIME은 처리 완료 뒤 count 또는 time 조건에 따른 커밋이며 작업 중 강제로 성공 커밋하는 설정이 아니다.
