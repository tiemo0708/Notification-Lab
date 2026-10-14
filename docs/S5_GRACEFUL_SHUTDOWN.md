# S5: Graceful Shutdown과 유예 초과 복구

## 종료를 두 단계로 생각하기

종료 신호를 받으면 먼저 새로운 일을 받지 않고, 이미 진행 중인 일은 일정 시간 기다린다.
기다리는 동안 완료하지 못한 일은 영속 상태와 Kafka offset을 근거로 재시작 후 복구한다.
종료 순간 모든 큐를 비우는 정책은 아니다.

현재 설정은 `server.shutdown=graceful`, `spring.lifecycle.timeout-per-shutdown-phase=10s`다.
10초는 **각 lifecycle phase의 대기 시간**이며 프로세스 전체의 단일 10초 제한이 아니다.
HTTP 서버의 신규 접수 중지와 종료 유예는 [Spring Boot 3.5 문서](https://docs.spring.io/spring-boot/3.5/reference/web/graceful-shutdown.html)를 따른다.

## 보완한 동작

Spring Boot 3.5.16 / Spring Kafka 3.3.16의 사용 중인 소스 JAR에서 종료 phase를 확인했다.
SmartLifecycle은 종료 시 높은 phase부터 처리한다.
기본 Kafka 컨테이너 phase는 `Integer.MAX_VALUE - 100`, HTTP graceful phase는 `Integer.MAX_VALUE - 1024`라서
그대로 두면 Kafka 작업을 기다리는 동안 HTTP가 새 요청을 받을 수 있다.

- Kafka 컨테이너 phase를 `WebServerGracefulShutdownLifecycle.SMART_LIFECYCLE_PHASE - 1`로 지정했다.
  HTTP 신규 접수를 먼저 중지하고 진행 중 HTTP 요청의 종료 단계를 거친 뒤 Kafka Worker를 멈춘다.
- `stopImmediate=true`로 현재 레코드 처리 뒤 멈추게 했다. 기존 poll 크기 1과 RECORD ack 정책은 유지한다.
  해당 옵션의 의미는 [Spring Kafka 3.3 컨테이너 설정](https://docs.spring.io/spring-kafka/reference/3.3/kafka/container-props.html)을 참고한다.
- Relay는 ContextClosedEvent를 받으면 선점 루프를 종료하도록 표시한다.
  이미 시작한 선점/발행은 마무리하되 다음 루프로 넘어가 새 작업을 선점하지 않는다.
- Spring scheduler의 기존 종료 관리를 사용한다. 진행 중 Relay의 ACK/DB 기록과 Worker의 결과/offset 처리가
  끝날 때까지 해당 단계의 유예를 주고, 이후 자원을 종료한다. DB/Kafka 쓰기를 하나의 트랜잭션으로 묶지 않는다.

## SIGTERM 실험

`GracefulShutdownTest`는 네 시나리오를 실행한다. 각 시나리오는 전용 MySQL/Kafka 컨테이너와
새 자식 Spring Boot JVM을 사용한다. 부모 앱은 검사 역할만 하며 Relay/Worker는 비활성화한다.
실제 HTTP 접수는 종료 대상 자식에게 보낸다.

1. 첫 요청을 접수하고 정해진 경계에서 자식을 대기시킨다.
2. 두 번째 요청도 접수해 아직 처리하지 않은 작업을 남긴다.
3. macOS/Linux의 `Process.destroy()`로 SIGTERM을 보낸다.
4. ContextClosedEvent, HTTP 신규 접속 거절, 새 POST의 미접수를 확인한다.
   진행 중 작업 때문에 자식은 아직 살아 있고, 이 시점 offset은 미커밋이어야 한다.
5. 정상 사례는 대기를 해제한다. 초과 사례는 해제하지 않고 설정된 종료 유예가 지나도록 둔다.
6. 자식이 SIGTERM 종료 코드 143으로 끝나는지 확인한다. 검증 통과를 위해 SIGKILL로 대신 종료하지 않는다.
7. 초과 사례는 DB의 실제 30초 lease 만료를 기다린 뒤 새 PID를 시작한다. DB 시각/offset을 조작하지 않는다.
8. 남은 요청과 결과, 호출 이력, 토픽/offset을 대조한다.

테스트 전용 `ShutdownProcess`는 앱 컴포넌트 스캔 및 실행 JAR 밖에 있다.
Worker 대기는 실제 Mock이 수락한 뒤 `WorkerStore.complete()` 호출 전이다.
Relay 대기는 실제 Kafka ACK 이후 `OutboxStore.published()`의 DB 트랜잭션 전이다.
해제 시 실제 저장소 프록시로 진입해 기존 트랜잭션을 사용한다.
유예 초과 사례는 취소되지 않는 호출을 모사하며, 임시 파일에 남긴 공급자 호출 기록으로 JVM 재시작 전후 호출을 합산한다.

| 시나리오 | 종료 직후 첫 작업 | 두 번째 작업 | 복구 후 공급자 호출 | Kafka 레코드 / offset 합 |
| --- | --- | --- | --- | --- |
| Worker 유예 내 완료 | PROVIDER_ACCEPTED, offset 커밋 | QUEUED, 시도 0 | 2 | 2 / 2 |
| Worker 유예 초과 | PROCESSING, offset 미커밋 | QUEUED, 시도 0 | 3 | 2 / 2 |
| Relay 유예 내 완료 | Outbox PUBLISHED | Outbox PENDING | 2 | 2 / 2 |
| Relay 유예 초과 | Outbox PROCESSING, ACK는 받음 | Outbox PENDING | 2 | 3 / 3 |

Worker 유예 초과에서는 첫 Mock 수락 사실이 DB에 저장되지 않았다. 복구 시 첫 시도는 UNKNOWN,
두 번째 시도는 PROVIDER_ACCEPTED가 된다. 따라서 첫 요청의 Mock 호출은 두 번이다.
Relay 유예 초과에서는 같은 이벤트가 재발행되지만 완료 상태 확인으로 공급자 추가 호출을 막는다.
네 경우 모두 두 요청의 기존 taskId/eventId를 유지하고, 복구 후 미발행·lag·Retry/DLQ 레코드는 0이다.
실제 측정 시간과 회귀 테스트 결과는 [PROGRESS](PROGRESS.md)에 남긴다.

```bash
./gradlew --no-daemon --console=plain test --tests '*GracefulShutdownTest'
./gradlew --no-daemon --console=plain build --rerun-tasks
```

## 로컬에서 종료와 재시작

종료할 앱 프로세스의 PID를 확인하고 SIGTERM을 보낸다. 여기서는 실제 명령을 실행하지 않고 절차만 제시한다.

```bash
kill -TERM <앱_PID>
```

IDE의 Stop 버튼은 신호 방식에 따라 즉시 종료할 수 있으므로 IDE 종료를 SIGTERM 검증과 동일하게 보지 않는다.
이는 [Spring Boot 종료 문서](https://docs.spring.io/spring-boot/3.5/reference/web/graceful-shutdown.html)에서도 구분한다.

프로세스 종료 후 DB/Kafka volume을 유지하고 기존 설정으로 다시 시작한다.
PROCESSING이 남았다면 lease 만료 여부와 [불확실한 결과 정책](S4_UNCERTAIN_OUTCOMES.md)을 먼저 확인한다.
현재 Worker는 살아 있는 lease를 새로 선점하려 하면 오류로 멈추므로, 실험도 실제 lease 만료 후 재시작한다.
너무 일찍 재시작해 Worker가 멈췄다면 원인/lease를 확인한 후 명시적으로 재시작해야 한다.

## 한계

단일 인스턴스에서 Worker와 Relay의 종료 경계를 각각 검증한다. 두 역할의 동시 정지,
느린 HTTP 요청의 유예 초과, 브로커/DB 장애 동반, 모든 라이브러리의 무한 정지까지 보장하지 않는다.
외부 프로세스 관리자의 전체 종료 제한은 phase별 시간과 자원 정리 시간을 고려해 별도로 정해야 한다.
Compose에는 앱 서비스가 없으므로 앱의 Docker 종료 유예를 설정했다고 주장하지 않는다.

실험은 테스트 classpath의 Spring Boot 진입점을 실행한다. SIGTERM을 지원하는 macOS/Linux 대상이며
Windows의 `Process.destroy()` 동작은 검증하지 않았다. 단말 도착과 외부 발송 exactly-once도 보장하지 않는다.
특히 유예 초과 후 공급자 재호출 가능성은 실제로 재현한 한계다.
S5는 정의한 장애 실험 범위의 완료이며 다음은 S6 HTTP Mock과 공급자 보호 장치다.
