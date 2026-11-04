# S8 Kafka 파티션 3→6 변경 계약 검증

2026-11-04. 기준 커밋 c6cccc1. 실제 Kafka 4.0.2 / 클라이언트 3.9.2에서 **집중 테스트 5개를 22초에 통과**했다.
기존 토픽·DB·기본 설정을 보존하고 Testcontainers의 별도 토픽/group만 사용했다.
이 단계는 브로커 계약 검증이다. 알림 앱·MySQL·HTTP 공급자를 실행한 처리량 실험과 구분한다.

## 왜 처리량보다 이 검증을 먼저 하나

파티션 수 변경은 key의 배정을 바꿀 수 있지만 기존 레코드를 새 파티션으로 옮겨 주지는 않는다.
같은 key의 예전/새 레코드가 서로 다른 파티션에 남을 수 있다.
새 파티션에는 기존 group의 커밋 offset이 없으므로 초기화 정책도 확인해야 한다.
이는 Kafka의 [토픽 변경 안내](https://kafka.apache.org/40/operations/basic-kafka-operations/#modifying-topics)와
[auto.offset.reset 설명](https://kafka.apache.org/40/configuration/consumer-configs/#auto.offset.reset)에서 다루는 동작이다.

## 실제 결과

| 검증 | 통제한 조건 | 집중 실행 결과 |
| --- | --- | --- |
| key 배정과 기존 데이터 | 60개 합성 key마다 변경 전/후 각 2건, ACK 확인 후 metadata 6개 확인 | 27개 key 이동 / 33개 유지. 전체 240건의 key·값·파티션·offset 대조와 파티션 내부 append 순서 일치 |
| 같은 key의 처리 순서 | 변경 전 1번 레코드의 파티션만 의도적으로 pause, 새 파티션의 2·3번 처리 | 처리 순서 2→3→1. pause 중 이전 파티션 commit=0, 완료 후=1. 새 파티션은 2 |
| 기존 offset + earliest | 3개 기존 파티션 각 1건 처리/커밋 후 잔여 데이터·신규 파티션 데이터 저장, 같은 group 재시작 | 기존 commit [1,1,1] 유지. 신규 파티션의 사전 기록 6건 모두 읽음. 전체 15건 처리 |
| 기존 offset + latest 제어군 | 위와 같은 데이터, 초기화 정책만 latest | 기존 파티션 잔여는 읽지만 신규 파티션의 사전 기록 6건은 건너뜀. 전체 15건 중 9건 처리 |
| 실행 중 재할당 | 동일 group의 Consumer 2개를 계속 poll하면서 토픽 증가 | 소유 개수 [1,2]→[3,3], 중복 소유 0, 추가 할당 callback 2회. 기존 commit 유지 및 후속 포함 9건 처리 |

`earliest/latest` 비교에서는 prefix 3건을 실제로 읽고 offset 1을 커밋한다. 그 뒤 기존 파티션에 3건,
신규 파티션에 6건을 저장한 상태로 재시작하고, 초기 위치가 결정된 뒤 새 marker 3건도 발행한다.
두 설정 모두 기존 커밋이 있던 파티션에서 prefix를 다시 읽지 않고 잔여 레코드부터 이어 갔다.
커밋이 없던 새 파티션만 초기화 정책의 영향을 받았다.

**두 설정의 최종 commit은 모두 [2,2,2,3,3,3]이다.** 이는 이 fixture에서 각 파티션에 ACK된 기록 수와 같아
최종 lag는 0이지만, latest의 실제 처리 수는 6건 적다. 최종 offset/lag 대조만으로 전수 처리를 증명할 수 없다는 제어 사례다.
`latest`의 6건 누락은 의도적으로 확인한 위험 동작이며 발송 성공으로 세지 않는다. 앱 설정에 적용하지 않았다.

## Notification Lab에 적용할 판단

- 현재 앱은 `auto-offset-reset: earliest`, auto-commit OFF, RECORD를 유지한다.
  기존 offset의 보존과 새 파티션 초기화는 서로 다른 문제다. 전체 offset 초기화나 새 group 생성으로 해결했다고 취급하지 않는다.
- [Relay](../src/main/java/dev/jinhyeon/notificationlab/outbox/OutboxRelay.java)는 notificationId를 Kafka key로 사용한다.
  수신자 ID 기준 key가 아니므로 서로 다른 알림 ID를 가진 같은 고객의 모든 알림 순서를 보장하는 계약은 아니다.
- 파티션이 나뉘면 같은 key라도 이전/이후의 처리 순서를 통합해 가정할 수 없다.
  2→3→1은 이전 파티션을 의도적으로 막아 만든 반례다. 자연 부하에서 역전 비율을 측정한 결과는 아니다.
- 이 테스트는 앱의 lease·DB 중복 제약·공급자 호출과 함께 파티션을 늘린 검증이 아니다.
  파티션 수를 바꾸는 운영 절차나 안전한 무중단 전환을 완료했다고 주장하지 않는다.
- 현재 앱 토픽 선언은 계속 3이다. 토픽 이름/group/기본 Consumer 수와 Redis 예산도 변경하지 않았다.
  다음 단계는 테스트 전용 1만 건 HTTP Mock 경로에서 Consumer·pool·quota를 고정한 파티션 3/6 비교다.
  Consumer가 그대로인 조건의 처리량 변화를 실제로 측정한 뒤 판단한다.
- 그 전에 [WorkerPauseErrorHandler](../src/main/java/dev/jinhyeon/notificationlab/worker/WorkerPauseErrorHandler.java)의
  `MAX_TRANSIENT_RECORDS=6`을 확인해야 한다. 현재 주석/설계는 요청·Retry 각각 3개 파티션을 전제한다.
  둘 다 6개가 되면 소비 대상은 12개로 늘어나므로, 일시 오류가 여러 파티션에 분산되는 경우의 복구 budget을 별도로 검증해야 한다.
  이 코드 점검만으로 데이터 유실을 관측했다는 뜻은 아니다. 기존 cap은 유지하며 앱 확대 실험 전에 오류 경계를 확인한다.

## 재실행과 경계

[테스트 코드](../src/test/java/dev/jinhyeon/notificationlab/outbox/KafkaPartitionExpansionTest.java):

```bash
./gradlew --no-daemon --console=plain test --tests '*KafkaPartitionExpansionTest'
```

- 기본 회귀에도 포함되는 테스트 5개(파라미터 2개 포함)다. 기본 회귀 외 별도 1만 건 발송을 자동 실행하지 않는다.
- 브로커 1개, CPU 1/메모리 1024MiB/JVM heap 최대 512MiB, 바인딩은 loopback, 테스트마다 새 토픽/group이다.
- 60개 key/240건이 최대 데이터 집합이다. 그 외 순서 3건, offset 비교 각 15건, live 재할당 9건이다.
- 메서드 90초, poll 수집 20초, 할당 25초, metadata 10초, Admin/send 10초 상한이다.
  테스트의 poll 상한은 300이며, 앱의 poll 1건/Worker 설정과 별개다.
- keyed 테스트는 실제 기본 String key partitioner를 사용하고 producer metadata가 6개임을 확인한 뒤 발행한다.
  producer 간 metadata 전파 시점이 섞이는 레이스는 별도 미검증이다.
- offset 제어군은 각 파티션의 입력 수를 고정하기 위해 명시적 파티션을 사용한다.
  순서 제어는 수동 assign/pause, 재시작·live 재할당은 subscribe와 실제 group 할당을 사용한다.
- 출력은 `build/reports/partition-expansion/이름-고유번호.json`으로 구분한다. 합성 key·topic/group UUID·payload는 기록하지 않는다.
  [고정 소스와 집중 실행 원본 SHA](experiments/s8-partition-expansion-2026-11-04-source.json)에 161개 소스 해시와 원본을 보존했다.

첫 시도는 fixture Map.of의 최대 항목 수를 넘겨 컴파일 단계에서 6초에 실패했다(실행 테스트 0개).
초기화 방식을 수정한 뒤 위 5개가 22초에 통과했다. 전체 회귀는 집중 검증 뒤 따로 실행한다.

전체 `caffeinate -i ./gradlew --no-daemon --console=plain build`는 **16분 39초/종료코드 0**이었다.
기본 test를 새로 실행했고 콘솔/JUnit XML 45개 클래스에서 **354개 통과, 실패·오류·스킵 0**을 대조했다.
새 브로커 테스트 5개도 전체 회귀에서 다시 통과했다. bootJar는 UP-TO-DATE다.
[최종 검증](experiments/s8-partition-expansion-2026-11-04-validation.json)에 원본 SHA와 결과를 보존한다.

별도로 이전 c6cccc1의 [원격 CI](https://github.com/tiemo0708/Notification-Lab/actions/runs/41757502204)는
349개 중 WorkerConcurrencyTest의 RECORD 1개가 active=2 관찰 대기로 실패했다.
이번 로컬 PASS로 원격 CI를 통과했다고 해석하지 않는다. 기존 동시성 fixture의 관찰 경계는 별도 수정/검증한다.
