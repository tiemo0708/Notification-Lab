# S8 요청·Retry 파티션 6+6의 복구 예산 검증

2026-11-04. 기준 커밋 9a4ceec. [브로커 계약 검증](S8_PARTITION_EXPANSION.md) 뒤,
실제 앱 Worker/MySQL/Kafka/Redis/HTTP Mock으로 파티션 확대의 오류 경계를 확인했다.
집중 테스트 7개는 **1분 8초/종료코드 0**이었다. 기존 사용자 토픽과 DB는 변경하지 않았다.

## 확인한 문제와 변경

기존 handler는 부모 컨테이너·topic·partition별 현재 offset의 복구 횟수를 메모리에 보관한다.
현재 offset이 바뀌면 해당 파티션의 횟수를 새로 시작하고, 컨테이너 중단/종료 시 이력을 비운다.
정상 복구 뒤에도 해당 파티션의 마지막 이력 항목은 남으므로, map 크기는 동시 장애 수와 같지 않다.
기존 고정 상한 6은 요청/Retry 각각 3개인 구성을 수용했지만, 각각 6개이면 최대 12개 항목이 필요하다.

수정 전 실제 6+6 구성에서 기본 상한의 안전 중단은 통과했으나,
요구한 12개 용량 경로는 **일곱 번째 파티션에서 중단되어 실패**했다(2개 중 1개 실패, 34초).
당시에는 새 설정을 읽는 구현이 없었다. [수정 전 결과](experiments/s8-partition-recovery-2026-11-04-before.json)를 보존했다.

[WorkerPauseErrorHandler](../src/main/java/dev/jinhyeon/notificationlab/worker/WorkerPauseErrorHandler.java)에
`app.rate-limit.transient-recovery-max-records`를 추가했다. **기본 6 / 허용 1~12 / 초과 설정은 시작 실패**다.
metadata에 따라 용량을 자동 확대하지 않는다. 이 값은 복구 이력 map의 슬롯 수이며 Consumer/HTTP 동시성 수가 아니다.

| 경계 | 변경 후 계약 |
| --- | --- |
| 복구 활성화 | 기존 `transient-recovery-enabled=false` 유지 |
| 이력 용량 | 기본 6, 명시적으로 최대 12 |
| 같은 record의 일시 오류 | 기존 최대 3번 pause(250/500/1000ms), 네 번째 실패에 중단 |
| 새 파티션의 용량 초과 | 유효 허가 확인·새 DB 시도·HTTP 호출·offset 진행 없이 중단 |
| 잘못된 Redis 예산/상태 | 기존 즉시 중단 유지 |
| 스케줄러 / 예약 | 기존 스레드 1개 / 예약 최대 3개 유지 |
| 상태 초기화 | 컨테이너 중단/종료 시 map과 예약 정리, 명시적 재시작으로 영속 작업 재개 |

복구 횟수는 현재 컨테이너의 topic/partition/offset 기준이다. 여러 앱·재시작 전체에 걸친 영속 횟수 제한을 새로 보장한 것이 아니다.
발송 작업의 DB attempt 상한과도 구분한다.
timeout 난 Redis EVAL이 서버에서 token을 소비하지 않았다고 보장하지 않는다.
불확실한 허가를 환불하거나 재사용하지 않는 기존 규칙을 유지한다.

## 실제 검증 결과

| 경우 | 관찰한 결과 |
| --- | --- |
| 기본 용량 6, 파티션 6+6 | 첫 6개 복구 후 일곱 번째에서 capacity 중단. 추가 시도/HTTP/offset 진행 없음. 명시적 재시작 뒤 일곱 번째까지 완료 |
| 명시적 용량 12, 파티션 6+6 | 요청 6개와 실제 Retry 이벤트 6개를 각각 일시 장애 후 처리. 이력 12개 유지, 용량/횟수 소진 0, HTTP 12회/offset 합 12 |
| 용량 12, 13번째 파티션 제어군 | 요청 7+Retry 6 구성에서 12개 처리 뒤 용량 중단 1회. 막힌 Retry의 기존 attempt=1 유지. 재시작 뒤 attempt=2로 처리하고 HTTP/offset 합 13 |
| 용량 12, 지속 장애 | 3개 Consumer의 요청 파티션 0/3/5에서 중단. record별 pause 상한 3 유지, 중단 전 시도·HTTP·offset 0. 정상화/재시작 뒤 3개 처리 |
| 용량 12, 예산 유실 | transient pause 없이 즉시 중단. 새 시도·HTTP·offset 0 |
| 설정 검증 2개 | 설정 생략 시 실제 기본 6, 명시적 12 적용. -1/0/13/정수 최댓값은 시작 실패 |

앱의 실제 Consumer/Worker 활성 상한은 3/3, RECORD·동기 commit이었다.
토픽 증가는 테스트용 Admin으로 수행했고, 앱의 기본 토픽 선언은 계속 3이다.

Retry 경로는 첫 HTTP 500 뒤 DB에 생성된 정상 generation 1 Retry 이벤트를 사용했다.
테스트에서만 발송 attempt 상한을 2로 두어 두 번째 500 뒤 최종 실패와 DLQ Outbox 저장까지 확인했다.
**의도적인 HTTP 500이므로 공급자 수락은 0건**이다. 한 알림의 두 시도는 정상 Retry이며 별개 성공 발송으로 세지 않는다.
Relay는 꺼 둔 채 이벤트를 수동으로 Kafka에 발행했다. Outbox PUBLISHED나 DLQ Kafka ACK를 완료했다고 표현하지 않는다.

## 테스트 제어를 수정한 이유

초기 제어는 `CLIENT PAUSE ALL 10000` 뒤 즉시 UNPAUSE로 정상화하려 했으나,
해제 자체가 9670/9768ms 걸려 같은 record의 복구 횟수를 먼저 소진했다(2개 실패).
이 결과는 용량 비교로 사용하지 않고 [fixture 실패](experiments/s8-partition-recovery-2026-11-04-fixture-failure.json)로 보존했다.

새 제어는 `PAUSE WRITE`로 EVAL을 막고 첫 오류 관찰 뒤 UNPAUSE한다. 수정 전 비교의 해제 시간은 0~11ms였다.
[Redis 공식 PAUSE 설명](https://redis.io/docs/latest/commands/client-pause/)은 WRITE에서 EVAL을 막는 동작과 명시적 해제를 설명한다.
[UNPAUSE 설명](https://redis.io/docs/latest/commands/client-unpause/)도 참고한다.
첫 compile은 테스트용 Admin 호출의 checked exception 처리 누락으로 6초에 실패했고 테스트는 실행하지 않았다.

## 사용과 계측

파티션 6+6을 별도로 준비한 실험 앱에서만 아래처럼 명시한다. 이 옵션 자체가 토픽을 늘리지는 않는다.

```bash
--app.rate-limit.client-enabled=true
--app.rate-limit.pause-enabled=true
--app.rate-limit.transient-recovery-enabled=true
--app.rate-limit.transient-recovery-max-records=12
```

- `notification_worker_quota_transient_capacity`: 설정된 슬롯 상한(새 gauge)
- `notification_worker_quota_transient_records`: 보관 중인 파티션 이력 수(현재 장애 수와 다름)
- `notification_worker_quota_transient_capacity_exhausted_total`: 새 슬롯 부족 중단(새 고정 counter)
- `notification_worker_quota_transient_exhausted_total`: 동일 record의 횟수 소진

수신자·key·topic/partition ID를 새 metric label에 넣지 않는다.
Spring의 보류 record/오류 처리와 이벤트는
[3.3.16 listener 원본](https://github.com/spring-projects/spring-kafka/blob/v3.3.16/spring-kafka/src/main/java/org/springframework/kafka/listener/KafkaMessageListenerContainer.java)과
[공식 예외 처리 설명](https://docs.spring.io/spring-kafka/reference/kafka/annotation-error-handling.html)을 함께 참고했다.

```bash
./gradlew --no-daemon --console=plain test \
  --tests '*WorkerPauseErrorHandlerTest' \
  --tests '*WorkerQuotaIntegrationTest.sixPartition*' \
  --tests '*WorkerQuotaIntegrationTest.thirteenth*'
```

[고정 소스 162개·집중 검증·원본 SHA](experiments/s8-partition-recovery-2026-11-04-source.json)를 보존한다.
각 통합 시나리오의 최대 입력은 13개 record이며, 메서드 90초/할당 20초/복구 15초/Admin 10초 상한이다.

다음은 1만 건 HTTP Mock 경로의 파티션 3/6 반복 비교다. 용량은 두 조건 모두 12로 고정해 파티션 효과와 분리한다.
이번 오류 경계 검증을 처리량 개선, 모든 rebalance/장애 조합, 무중단 운영 확장의 증거로 해석하지 않는다.
전체 `caffeinate -i ./gradlew --no-daemon --console=plain build`는 **18분 14초/종료코드 0**이었다.
기본 test를 새로 실행했고 콘솔/JUnit XML **46개 클래스/361개 통과, 실패·오류·스킵 0**을 대조했다.
기존 354개와 새 7개이며, 다섯 통합 시나리오도 전체 회귀에서 다시 통과했다.
알림 앱 bootJar를 새로 생성했고, 고정 소스 162개 SHA는 실행 중 변하지 않았다.
[최종 검증·추가 원본 SHA](experiments/s8-partition-recovery-2026-11-04-validation.json)를 보존한다.
이 단계는 로컬 검증이다. 새 원격 CI 실행 결과를 확인한 것은 아니다.
