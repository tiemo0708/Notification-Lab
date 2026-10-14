# S5: DB 완료 후 offset 커밋 전 Worker 종료

## 확인할 문제

MySQL 결과 저장과 Kafka consumer offset 커밋은 별개다. 공급자 호출과 결과 저장이 끝났어도
offset 커밋 전에 Worker가 종료되면 Kafka는 같은 레코드를 다시 전달할 수 있다.
이때 DB의 완료 상태를 확인해 공급자 호출을 건너뛰는지 검증한다.

```text
Kafka 레코드 0 → Worker 선점 → Mock 수락 → DB 결과 커밋
                                             ↓
                                       여기서 JVM 종료
                                             ↓
동일 group의 새 Worker → 레코드 0 재소비 → 완료 상태 확인 → 공급자 호출 생략 → offset 1 커밋
```

이전 [Relay ACK 경계 실험](S5_OUTBOX_ACK_CRASH.md)은 Kafka 레코드가 2개로 늘었다.
이번에는 레코드 1개를 Worker가 두 번 읽는 상황이다.

## 실험 구성

- Java 21 / Gradle Wrapper 8.14.5, 전용 Testcontainers MySQL 8.4.8·Kafka 4.0.2.
- 부모 Spring Boot 앱은 API/Relay만 실행하고, Worker는 테스트가 생성한 자식 JVM에서 실행한다.
  같은 코드·DB·토픽을 사용한다. 운영 서비스를 분리한 변경은 아니다.
- `WorkerProcess`는 `src/test` 및 앱 컴포넌트 스캔 밖에 있는 테스트 전용 진입점이다.
- 첫 자식만 테스트용 `NotificationService.recordCompletion()`을 사용한다.
  실제 `NotificationWorker`가 실제 `WorkerStore.complete()`의 트랜잭션 프록시를 통과한 후 호출하는 지점이다.
  기존 완료 지표를 기록하고 표식을 남긴 뒤 최대 90초 대기한다. 활성 DB 트랜잭션이 있으면 즉시 실패한다.
- 아직 Kafka 리스너가 반환하지 않았으므로 RECORD ack 커밋 전이다. 부모는 표식만 믿지 않고
  별도 DB 연결로 완료 행을 읽고 Admin API로 offset이 미커밋인지 확인한다.
- 부모는 자신이 시작한 첫 자식만 `destroyForcibly()`로 종료하고 비정상 종료를 확인한다.
  Spring 종료 훅을 이용한 Graceful Shutdown 실험이 아니다.
- 새 PID의 Worker에는 중단 장치를 등록하지 않는다. 같은 group으로 브로커의 재할당을 기다린다.
  offset reset/seek, DB 시각 변경, 메시지 추가 발행 없이 복구한다.
- 두 JVM의 실제 Prometheus 공급자 호출 카운터를 따로 조회한다. 카운터는 재시작으로 초기화되므로
  새 프로세스의 0만으로 전체 호출 수가 0이라고 판단하지 않는다. 새 Worker의 replay 카운터도 확인한다.

## 실행과 판정

```bash
./gradlew --no-daemon --console=plain test --tests '*WorkerProcessCrashTest'
./gradlew --no-daemon --console=plain build --rerun-tasks
```

| 확인 대상 | 종료 직전 | 새 Worker 처리 후 |
| --- | --- | --- |
| 알림 결과 | PROVIDER_ACCEPTED | 같은 행·결과 유지 |
| 작업 시도 / 이력 수 | 1 / 1 | 1 / 1, 전체 행 동일 |
| Outbox | PUBLISHED, 발행 시도 1 | 전체 행 동일 |
| 요청 토픽 레코드 수 | 1 | 1 |
| 요청 토픽 committed offset 합 | 없음 또는 0 | 1 |
| Worker group lag | 1 | 0 |
| 기존 Worker 공급자 호출 | 1 | 프로세스 종료 |
| 새 Worker 공급자 호출 / replay | 기동 전 | 0 / 1 |
| Retry / DLQ 레코드 | 없음 | 0 / 0 |

일반 회귀 테스트에 포함한다. 실제 실행 결과는 [PROGRESS](PROGRESS.md)에 기록한다.
종료 후 자식 JVM과 전용 컨테이너를 정리하며 기존 Compose DB와 `.env`는 수정하지 않는다.

## 해석과 한계

이 완료 경계에서는 재소비해도 공급자 호출을 추가하지 않는다. Kafka 처리 전체나 외부 발송의
exactly-once를 증명하지 않는다. Mock 수락은 실제 단말 도착이 아니다.

공급자가 수락했지만 **DB에 결과를 저장하기 전** 종료되는 경우는 별도의 불확실성 구간이다.
그때는 lease 복구 후 공급자를 다시 호출할 수 있다. [S4 불확실성 정책](S4_UNCERTAIN_OUTCOMES.md)을 따른다.

부모 API/Relay, DB, 브로커는 계속 살아 있다. 단일 가상 요청의 최종 성공 경계를 검증하며
재시도 예약/최종 실패 경계 종료, 여러 Worker의 동시 리밸런스, 동시 장애, 데이터 손실은 이 실험의 범위 밖이다.
테스트 classpath의 Spring Boot 진입점을 실행했으며 배포 JAR 자체를 기동한 실험은 아니다.
다음 항목은 Relay/Worker 재시작과 명시적 리밸런스, 이후 Graceful Shutdown이다.
