# S5 두 번째 실험: Kafka ACK 후 Outbox 기록 전 프로세스 종료

## 확인할 질문

Kafka는 이벤트를 받았는데 Relay가 DB에 `PUBLISHED`를 기록하기 전에 종료되면 어떻게 되는가?
복구 후 같은 이벤트가 또 발행된다면 Worker가 공급자를 다시 호출하는가?

이 실험은 **이벤트 중복 발행**과 **완료된 작업의 중복 발송 차단**을 각각 확인한다.
Kafka producer의 `enable.idempotence=true`가 새 프로세스의 애플리케이션 재발행까지 제거한다고 가정하지 않는다.

## 실행 구성

```bash
./gradlew --no-daemon --console=plain test --tests '*OutboxProcessCrashTest'
```

- 테스트: [OutboxProcessCrashTest](../src/test/java/dev/jinhyeon/notificationlab/outbox/OutboxProcessCrashTest.java)
- 테스트 전용 진입점/중단 장치: [RelayProcess](../src/test/java/dev/jinhyeon/notificationlabtest/RelayProcess.java)
- Java 21 / Spring Boot 3.5.16 / Gradle Wrapper 8.14.5 / MySQL 8.4.8 / Kafka 4.0.2.
- 별도 Testcontainers DB와 단일 KRaft 브로커를 사용한다. 사용자 Compose DB·volume·앱을 중지하지 않는다.
- 요청/Retry/DLQ 토픽은 각각 파티션 3개, 복제본 1개. 데이터는 가상 요청 1건이다.
- 부모 테스트 JVM의 앱은 API/Worker를 실행하고 Relay를 끈다. 자식 Spring Boot JVM은 Relay를 실행하고 Worker를 끈다.
  기존 설정으로 역할을 나눈 실험이며, 운영 구조를 별도 서비스로 변경한 것은 아니다.
- 자식 프로세스는 테스트와 같은 Java 실행 파일, `-Xmx256m`, 임의 HTTP 포트를 사용한다.
  Gradle `test` 태스크가 테스트 runtime classpath를 전달한다. IntelliJ에서도 Gradle 테스트 실행을 사용한다.
- DB 비밀번호는 테스트가 생성한 값이며 자식의 환경변수로 전달한다. `.env`를 읽거나 수정하지 않는다.

## 중단 지점을 정확히 잡는 방법

실제 `OutboxRelay`는 `KafkaTemplate.send(...).get(...)`이 성공한 다음 `OutboxStore.published()`를 호출한다.
첫 자식 JVM에서만 테스트용 `@Primary` 저장소가 이 메서드에 진입했음을 임시 파일에 기록하고 대기한다.

이 지점에서는 Kafka ACK를 이미 받았지만 `PUBLISHED` 갱신 SQL은 아직 시작하지 않았다.
부모는 이 표식뿐 아니라 Kafka 레코드와 Worker의 완료/offset도 별도로 확인하고,
**자신이 만든 자식 Process만 `destroyForcibly()`로 강제 종료**한다. 정상 종료 훅은 실행하지 않는다.

중단 장치는 `src/test` 및 애플리케이션 컴포넌트 스캔 밖의 패키지에 있다.
기능 코드에 장애용 설정이나 종료 API를 추가하지 않으며, 테스트 장치는 실행 JAR에 포함하지 않는다.
복구용 자식에서는 이 저장소를 등록하지 않고 원래 저장소를 사용한다.
표식 대기는 최대 90초, 테스트 전체 제한은 180초이며 종료 시 남은 자식 프로세스를 정리한다.

## 검증 순서

1. 실제 HTTP API로 요청 1건을 접수한다. 요청·작업·Outbox를 DB에 저장하고 202를 받는다.
2. 첫 Relay 프로세스가 이벤트를 선점하고 실제 Kafka ACK를 받는다. 상태 기록 직전의 테스트 장치에서 대기한다.
3. 살아 있는 Worker가 첫 레코드를 처리한다. 공급자 수락 결과와 시도 이력이 저장되고 offset 합계는 1이 된다.
4. 동시에 Outbox는 `PROCESSING`, 발행 시도 1, `published_at=null`, lease token 보유 상태인지 확인한다.
   아직 30초 lease가 만료되지 않은 시점에 첫 Relay JVM을 강제 종료한다. 종료 후 DB 행이 같은지 확인한다.
5. 새 PID의 Relay JVM을 시작한다. `available_at`을 수정하지 않고 실제 lease 만료를 기다린다.
6. 같은 이벤트가 재발행되어 Outbox 발행 시도가 2가 되고 `PUBLISHED`가 저장된다.
   DB의 발행 완료 시각이 원래 lease 만료 시각 이후인지 확인한다.
7. Kafka 레코드 2개의 key/payload/eventId/taskId가 같은지, 같은 파티션의 연속 offset인지 직접 읽어 대조한다.
   감사용 consumer는 수동 파티션 할당/자동 커밋 해제로 Worker의 offset에 영향을 주지 않는다.
8. Worker가 두 번째 레코드까지 처리한 뒤에도 요청 결과와 첫 발송 시도 이력이 그대로인지 확인한다.

## 대조 결과와 의미

선택 테스트 실행에서 확인한 값이며 전체 실행 이력은 PROGRESS에 남긴다.

| 항목 | 확인값 | 의미 |
| --- | --- | --- |
| notification / delivery_task / Outbox | 각각 1개 | 새 요청이나 새 작업을 만들지 않음 |
| Outbox 발행 시도 | 2 | 중단 전 발행 + lease 복구 후 재발행 |
| Kafka 요청 토픽 레코드 | 2 | 이벤트는 실제로 중복 기록됨 |
| 서로 다른 eventId / taskId | 각각 1 | 재발행해도 원래 식별자를 유지 |
| 공급자 호출 / 작업 시도 / 시도 이력 | 각각 1 | 완료된 작업은 공급자를 다시 호출하지 않음 |
| Worker replay 카운터 | 1 | 두 번째 이벤트에서 완료 상태를 재확인 |
| 요청 토픽 committed offset 합계 | 2 | 중복 레코드까지 처리 위치에 반영 |
| Retry / DLQ 레코드 | 각각 0 | 공급자 실패 재시도와 다른 상황 |
| Outbox 미발행 / consumer lag | 각각 0 | 이 실험의 두 처리 경로 모두 종료 |

`Outbox PROCESSING + notification PROVIDER_ACCEPTED`는 이 중단 구간에서 가능한 조합이다.
Outbox 상태만 보고 공급자 호출이 없었다고 판단하면 안 된다. Kafka의 수신 확인과 DB의 발행 기록은
서로 다른 완료 지점이며, Worker는 Outbox의 PUBLISHED 저장을 기다린 뒤에만 처리하는 구조가 아니다.

## 한계와 다음 단계

- 첫 이벤트의 DB 결과와 offset이 완료된 뒤 Relay를 중단한다. 처리 중인 Worker를 함께 종료하는 실험은 아니다.
- API/Worker 프로세스와 MySQL/Kafka는 살아 있다. 전체 앱 동시 장애, 다중 Relay 경쟁, 리밸런스,
  네트워크 응답 유실, 저장소 손실을 검증했다고 표현하지 않는다.
- 공급자 수락 뒤 Worker의 DB 결과 저장 전 종료는 별도의 불확실성 구간이다.
  이번 공급자 호출 1회 결과를 외부 푸시 exactly-once 보장으로 확대하지 않는다.
- 다음 S5 항목은 Worker DB 완료 후 offset 커밋 전 프로세스 종료와 재소비이다.
- 실제 실행 결과와 미검증 항목은 [PROGRESS.md](PROGRESS.md)에 기록한다.
