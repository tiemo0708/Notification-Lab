# S5 첫 실험: Kafka 중단 중 접수와 복구 후 발행

## 확인할 질문

Kafka가 중단되었을 때도 API가 요청을 접수할 수 있는가? 접수했다면 어디에 남으며,
브로커 복구 후 무엇이 자동으로 처리되고 무엇은 조사 대상으로 남는가?

API의 `202 Accepted` 기준은 Kafka 발행이 아니라 **MySQL의 요청·작업·Outbox 커밋**이다.
이 실험은 해당 경계를 실제 브로커 중지/시작으로 확인한다. 기본 비동기 프로필을 대상으로 하며,
앱이 정상 기동하고 토픽이 만들어진 뒤 발생한 장애만 다룬다.

## 실험 구성과 재현

```bash
# Docker와 Java 21 필요. Compose 사전 실행 및 .env 불필요.
./gradlew --no-daemon --console=plain test --tests '*KafkaRecoveryTest'

# 기존 회귀 테스트와 함께 실행
./gradlew --no-daemon --console=plain test
```

테스트: [KafkaRecoveryTest](../src/test/java/dev/jinhyeon/notificationlab/outbox/KafkaRecoveryTest.java)

- Java 21 / Gradle Wrapper 8.14.5 / Spring Boot 3.5.16.
- Testcontainers 전용 MySQL 8.4.8와 Kafka 4.0.2. 사용자 Compose DB·volume을 사용하지 않는다.
- Kafka 단일 KRaft 브로커. 요청/Retry/DLQ 토픽은 각각 파티션 3개, 복제본 1개.
- Spring Boot를 임의 HTTP 포트로 실행하고 실제 HTTP POST/GET을 사용한다. 요청 timeout은 5초이며,
  이는 테스트의 유한 대기 상한이지 성능 SLO 측정값이 아니다.
- Relay 스케줄러와 Worker를 켠 상태로 유지한다. 수동 `relay()` 호출이나 DB 예약 시각 변경을 하지 않는다.
- 기본 producer timeout/backoff를 유지한다. 내부 Mock은 SUCCESS, 지연만 0ms로 설정한다.
- OS에서 선택한 빈 loopback 포트를 명시적으로 바인딩한다. 같은 컨테이너를 다시 시작할 때
  Docker의 임의 포트 재배정으로 기존 앱과 Kafka advertised listener 주소가 달라지는 것을 방지한다.
  선택과 Docker 바인딩은 원자적이지 않으므로 최초 컨테이너 준비만 최대 3회 시도하며 매번 새 포트를 고른다.
  모두 실패하면 테스트도 실패한다. 테스트 assertion을 재실행하는 기능이 아니며, 중단·복구 중에는
  기존 컨테이너 ID/포트를 유지한다. 고정 공용 포트를 요구하지 않는다.
- Docker API로 **이 테스트가 생성한 Kafka 컨테이너 ID만** stop/start한다. stop의 종료 유예는 10초다.
  컨테이너 삭제·재생성이나 pause/unpause를 사용하지 않는다. 재시작 시 기존 컨테이너 데이터가 유지된다.
- 복구 중 앱, producer, consumer를 다시 만들지 않는다. 테스트 종료 시 컨테이너는 Testcontainers가 정리한다.

## 실험 A: 시도 예산이 남은 요청의 자동 복구

`acceptsWhileBrokerIsStoppedAndAutomaticallyDrainsAfterRecovery`

1. 기준 요청 1건을 정상 처리해 producer 연결과 Worker offset 커밋을 확인한다.
2. Kafka 컨테이너를 멈추고 실제 정지 상태를 확인한다.
3. 서로 다른 멱등 키로 요청 5건을 접수한다. 모두 202와 Location을 반환해야 한다.
4. 실제 Kafka 발행 실패를 기다린다. Outbox는 `PENDING`으로 돌아가며 발행 시도 횟수가 남는다.
5. 각 요청의 GET 결과는 `QUEUED`, 작업 시도 횟수와 발송 이력은 0, `published_at`은 null인지 확인한다.
6. 브로커가 중단된 동안 미발행 지표는 5이며 consumer lag는 측정 불가인 `NaN`이어야 한다.
7. 같은 Kafka 컨테이너를 시작한다. 기본 발행 예산 10회 안에서 Relay와 Worker가 자동으로 재연결한다.
8. 기준 요청을 포함한 6건의 결과, 작업·이벤트 ID, Outbox, 공급자 호출 횟수, committed offset을 대조한다.

2026-10-14 실행에서 확인한 복구 후 대조값:

| 항목 | 값 |
| --- | --- |
| 고유 요청 / 작업 / 최초 Outbox | 각각 6개, 기존 ID 유지 |
| 공급자 수락 / 발송 시도 이력 / Mock 호출 | 각각 6개 |
| PUBLISHED Outbox / 요청 토픽 committed offset 합계 | 각각 6 |
| 미발행 / FAILED Outbox / consumer lag | 각각 0 |
| Retry / DLQ 토픽 레코드 | 각각 0 |

Broker publish 재시도는 공급자 발송 재시도와 다르다. Kafka에 도달하지 못한 요청은 아직
Worker가 선점하지 않았으므로 발송 시도 예산을 사용하지 않는다.

## 실험 B: 발행 예산을 소진하면 자동 복구하지 않음

`exhaustedOutboxRemainsVisibleAfterBrokerRecoversWhileNewRequestsComplete`

장시간 장애의 경계를 짧게 재현하기 위해 `app.async.max-publish-attempts=1`만 변경한다.
기본값 10회의 전체 대기 시간을 측정하는 실험은 아니다.

1. 정상 기준 요청 1건을 처리한 후 Kafka를 중단한다.
2. 장애 중 접수한 요청 1건이 202를 반환하고, 실제 발행 실패 후 Outbox가 `FAILED`가 되는지 확인한다.
3. Kafka를 복구하고 새 요청 1건을 접수한다. 기준 요청과 새 요청은 정상 처리된다.
4. 장애 중 요청은 계속 `QUEUED`이며 작업 시도 0, Outbox `FAILED`/발행 시도 1로 보존되어야 한다.
5. 기존 ID를 보존하고 미발행 1, FAILED 1, consumer lag 0을 확인한다. DLQ 토픽은 비어 있어야 한다.

여기서 **lag 0만으로 모든 접수 건이 완료되었다고 판단할 수 없다.** 이 요청은 아직 Kafka에
발행되지 않았기 때문에 lag에 포함되지 않는다. Outbox 미발행 수·실패 수·대기 시간도 함께 봐야 한다.

Outbox `FAILED`는 공급자 최종 실패를 뜻하는 DLQ와 다르다. 현재 DLQ 선택 재발행 API의 대상도 아니다.
발행 상한을 소진한 Outbox는 DB와 지표를 통해 조사해야 하며, 이번 실험에서 자동 재설정 기능을 추가하지 않는다.

## 검증 범위와 한계

- 로컬 단일 앱/브로커, 소량의 가상 요청을 검증한다. 처리량 벤치마크나 고가용성 검증은 아니다.
- 앱을 시작하기 전부터 Kafka가 없는 경우, volume 손실, 토픽 삭제, 메시지 보관 기간 만료를 다루지 않는다.
- stop/start는 브로커 중단·복구 실험이다. 앱 강제 종료, ACK와 DB 저장 사이 종료,
  Worker 결과와 offset 사이 종료, 다중 인스턴스 리밸런스는 후속 S5 항목이다.
- 이 조건에서 관측한 정확한 건수만 보고한다. 외부 푸시 exactly-once나 모든 장애의 무조건적인 유실 방지를 뜻하지 않는다.
- 실제 실행 결과는 [PROGRESS.md](PROGRESS.md)에 기록한다. 실패한 실행을 통과로 집계하지 않는다.
