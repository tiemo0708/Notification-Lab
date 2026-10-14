# S5: Relay/Worker 재시작과 처리 중 리밸런스

## 목적

같은 consumer group에 Worker가 합류하거나 빠지면 Kafka가 파티션을 다시 배정한다.
이를 리밸런스라고 한다. Worker 수가 늘었다는 사실만으로 정상 처리를 판단하지 않고,
배정된 파티션·DB 결과·공급자 호출·committed offset을 함께 확인한다.

이번 실험은 다음 세 가지 상황을 순서대로 만든다. 브로커/DB/API는 계속 실행한다.

| 구간 | 변경하는 조건 | 확인할 내용 |
| --- | --- | --- |
| 처리 중 합류 | Worker A의 첫 호출을 잠시 대기시키고 B 시작 | PROCESSING·미커밋 중 PREPARING_REBALANCE 관찰, 해제 후 2명에게 배정 |
| Relay 재시작 | 처리 완료 후 Relay 종료, 새 요청 접수, 새 Relay 시작 | PENDING 요청·기존 작업/이벤트 ID 보존, 재시작 후 발행/처리 |
| Worker 이탈·복귀 | 처리 완료 후 B 종료, A 인계, 새 B 시작 | 2 → 1 → 2명 배정, 각 구간의 새 요청 처리, 호출/offset 대조 |

## 구성과 실행

- `WorkerRebalanceTest`: 실제 Testcontainers MySQL 8.4.8, Kafka 4.0.2.
- 부모 Spring Boot는 API만 실행한다. Relay 한 대와 Worker A/B는 독립 자식 JVM이다.
- 토픽/consumer group은 테스트마다 고유하며 요청·Retry 토픽은 각각 3개 파티션이다.
- 실제 `NotificationWorker`, `WorkerStore`, `OutboxRelay`, `MockPushProvider`를 사용한다.
- 테스트 전용 `RebalanceWorkerProcess`에서 A의 첫 공급자 호출만 파일 표식으로 대기시킨다.
  부모가 Kafka Admin에서 PREPARING_REBALANCE를 확인하면 해제하고 실제 Mock으로 위임한다.
  대기는 최대 25초이며 기본 DB lease 30초와 max.poll.interval 60초를 바꾸지 않는다.
- 파티션 세 곳에 모두 요청이 생기도록 API UUID 키의 기본 Kafka murmur2 매핑을 확인한다.
  각 구간은 최대 60건으로 제한한다. UUID에 따라 전체 접수 수는 실행마다 다를 수 있다.
- 강제 종료는 이 테스트가 생성한 Process 객체에만 수행한다. 재시작 시 PID가 달라야 한다.
  consumer group·DB·토픽을 유지하며 offset reset/seek, DB lease 시각 변경을 하지 않는다.

```bash
./gradlew --no-daemon --console=plain test --tests '*WorkerRebalanceTest'
./gradlew --no-daemon --console=plain build --rerun-tasks
```

`REBALANCE_ASSIGNMENT`와 `WORKER_REBALANCE` 출력은 JUnit XML의 system-out에도 남는다.
실제 실행 횟수·건수·테스트 결과는 [PROGRESS](PROGRESS.md)에 기록한다.

## 판정 기준

1. A가 PROCESSING 상태이고 첫 offset이 미커밋인 동안 B의 합류로 PREPARING_REBALANCE를 관찰한다.
2. 안정된 그룹이 1 → 2 → 1 → 2명으로 바뀐다. 요청/Retry 총 6개 파티션이 중복·누락 없이 배정되고,
   두 Worker가 있을 때 각자 요청 파티션을 받는다.
3. Relay가 없는 동안 접수된 요청은 PENDING으로 남고, 새 Relay가 기존 taskId/eventId를 유지해 발행한다.
4. B가 없을 때 A만으로 세 요청 파티션의 새 요청을 처리한다. 새 B 복귀 후에도 두 Worker가 처리한다.
5. 각 단계가 끝나면 접수 ID 전체가 PROVIDER_ACCEPTED이며 작업 시도·이력·Outbox 발행 시도는 각각 1회다.
6. Kafka 레코드 수와 committed offset 합이 접수 수와 맞고, **각 요청 파티션별** committed offset이 end offset과 같다.
7. JVM별 공급자 호출 카운터를 합산한다. 종료할 B의 값은 종료 전에 저장하므로 재시작 초기화로 호출 수가 사라지지 않는다.
8. 최종 공급자 호출 합 = 고유 접수 수, Retry/DLQ 0, 미발행 0, lag 0이다.

## 해석과 한계

처리 중 합류 시 리밸런스가 시작되는 것을 관찰하되, 대기 중인 첫 호출은 기존 lease와 poll 제한 안에 끝낸다.
장시간 공급자 정지·lease 만료·max.poll.interval 초과로 소유권을 잃는 상황을 검증한 것은 아니다.

Relay와 B의 종료는 해당 단계 작업의 완료/offset 커밋을 확인한 뒤 실시한다.
처리 도중의 강제 종료 경계는 앞선 [Relay ACK 실험](S5_OUTBOX_ACK_CRASH.md)과
[Worker DB/offset 실험](S5_WORKER_OFFSET_CRASH.md)에서 따로 검증했다.
이번 실험에서 공급자 호출 수가 유지되더라도 모든 장애의 exactly-once나 단말 도착을 보장하지 않는다.

테스트 classpath로 Spring Boot 자식 JVM을 실행했다. 배포 JAR 자체 기동, 다중 브로커 고가용성,
Retry backlog 리밸런스, 처리량 비교, 동시 다중 장애까지 검증하지 않는다.
강제 종료를 사용하므로 **Graceful Shutdown 완료를 뜻하지 않는다**. 다음 S5 항목에서 종료 유예와 복구를 다룬다.
