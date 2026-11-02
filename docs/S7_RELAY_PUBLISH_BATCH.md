# S7 Relay 구간 계측과 ACK 후 완료 저장 묶음 실험

2026-11-02, 시작 커밋 `e6c7103`. 목적은 100만 건 재실행 전에 작은 HTTP 부하에서 비용을 구분하고,
한 가지 변경의 효과와 장애 시 비용을 비교하는 것이다. 100만 건 완료나 원래 접수 timeout 원인 확정이 아니다.

## 측정과 선택 근거

`notification.pipeline.stage`에 고정된 9개 `stage`만 사용한다. ID/본문/수신자를 label에 넣지 않는다.
DB 타이머는 저장소 프록시 바깥에서 재므로 연결 획득, 트랜잭션 시작/격리 설정, SQL, commit/rollback과 연결 설정 복원까지 포함한다.
서버 SQL 자체의 시간, 네트워크 왕복, fsync 비용을 각각 분해한 결과는 아니다.

| stage | 경계 |
| --- | --- |
| relay.claim | 빈 선점을 포함한 `OutboxStore.claim` 호출 전체 |
| relay.ack | `KafkaTemplate.send` 호출과 future 대기, 실패도 포함 |
| relay.mark.published | 단일/묶음 완료 저장 트랜잭션 호출 전체; count는 행 수가 아닌 호출 수 |
| relay.run | scheduled callback 전체, 내부 Relay 타이머와 중첩 |
| worker.handler | admission과 처리 전체, listener 반환 뒤 Kafka offset commit은 제외 |
| worker.claim.initial | 공급자 허가 전 최초 DB 검증/선점 |
| worker.claim.admitted | quota/circuit 허가 후 재검증과 시도 시작 |
| worker.provider | 공급자 호출 경로, DB 트랜잭션 밖 |
| worker.complete | 결과/시도 이력/후속 Outbox 저장 트랜잭션 |

count/totalMillis/meanMillis를 보존한다. 실행 중 레지스트리 스냅샷이므로 동시 샘플은 원자적이지 않고,
진행 중인 callback은 종료 때 기록된다. Worker 3개의 시간은 서로 겹친다.
중첩 타이머와 병렬 누적 시간을 모두 합해 wall time의 비율로 해석하지 않는다.
Micrometer max/p95는 시간 창 값일 수 있어 이 보고서에서는 누적 최대/p95처럼 사용하지 않는다.
API 응답 지연은 기존 `*ApiLatencyMs`, DB 완료 지연은 `*LatencyMs`로 별도 경계다.

첫 1만 건에서 Relay claim 62.429초, 완료 저장 37.235초, ACK 대기 10.231초였다.
따라서 ACK 비동기화/Worker 증설을 함께 적용하지 않고 완료 저장의 왕복/커밋 횟수부터 줄이는 후보를 만들었다.
가장 큰 claim 비용과 Worker DB 비용은 이 변경 뒤에도 남는다.

## 구현과 트레이드오프

- 기본 `app.async.relay.publish-batch-size=1`은 기존 단건 완료 저장 경로다.
- 선택 값 2..10은 ACK 성공한 claim만 메모리에 최대 10개 보관한 뒤 한 조건부 UPDATE/트랜잭션으로 완료한다.
  JDBC batchUpdate를 호출하는 방식이 아니라, `(event_id=? AND lease_token=?) OR ...`를 바인딩하는 단일 SQL이다.
  모든 조건에 `status='PROCESSING'`을 적용하고 실제 변경 행 수만 `outbox.published`에 더한다.
- 선점은 기존 READ COMMITTED/역할 인덱스/단건 선점이다. Kafka 전송과 ACK는 순차이며 DB 트랜잭션 밖이다.
  producer acks/idempotence/max-in-flight, Worker 수/offset 처리/공급자 예산은 바꾸지 않는다.
- 10건 도달, 다음 반복 시작 때 첫 보관 ACK 이후 100ms 경과, 빈 선점, 50회 loop 종료, stop,
  발행 실패나 선점 예외 시 남은 성공분을 저장한다. 100ms는 blocking call 사이에서 검사하는 기준이지 강제 완료 deadline이 아니다.
- 묶음 SQL 실패/commit 불확실성에서는 메모리 목록을 재시도하지 않고 durable lease 만료로 복구한다.
  오래된 token은 새 소유자의 행을 완료시킬 수 없다. 일부 token이 오래됐으면 유효한 행만 완료한다.
- ACK 뒤 DB commit 전 강제 종료 시 재발행될 수 있는 성공분은 기존 단건보다 늘어 최대 묶음 크기다.
  현재 전송 중인 불확실한 1건까지 포함하면 메모리에 보관한 ACK 성공분은 최대 9건 + 전송 중 1건이다.
  반복 장애/lease 만료/다중 Relay에 걸친 총 중복 횟수의 상한이나 exactly-once 보장이 아니다.
- 긴 blocking/DB 장애로 lease 30초가 지나면 다른 Relay가 재선점할 수 있다. fencing은 오래된 완료 저장을 차단하지만 Kafka 중복은 막지 않는다.
  큰 부하 재검증 전 기본값을 10으로 바꾸지 않는다. 공급자 수락은 단말 도착과 다르다.

## 재현 조건

Java 21/Boot 3.5.16/Gradle 8.14.5, MySQL 8.4.8(2 CPU/2GiB/buffer pool512MiB),
Kafka broker4.0.2(1 CPU/1GiB), Redis7.4.11(256MiB), test JVM512MiB.
각 역할 Worker3/HTTP3/Redis3/Hikari4/queue0, 총 예산1000/s 중 캠페인200/s·burst20,
Mock SUCCESS/1ms/max-in-flight8, 실시간 단독50개 후 순차 probe(각 응답 뒤200ms)를 유지했다.
두 역할은 같은 JVM의 별도 app context이며 DB/브로커/호스트를 공유한다. 실제 여러 서버 결과가 아니다.

```bash
caffeinate -i ./gradlew --no-daemon --console=plain campaignDeliveryPilot -PdeliveryTargets=10000 -PpublishBatchSize=1
caffeinate -i ./gradlew --no-daemon --console=plain campaignDeliveryPilot -PdeliveryTargets=10000 -PpublishBatchSize=10
```

`publishBatchSize`는 캠페인 역할만 바꾸고 실시간은 1로 유지한다.
같은 선택 인자는 `campaignDeliveryLoad`/`campaignDeliveryMillion`에도 전달할 수 있으나 이번에는 해당 대형 task를 실행하지 않는다.
일반 앱은 `--app.async.relay.publish-batch-size=10`으로 선택한다. 0 또는 11 이상은 시작 검증에서 거절한다.

## 비교 결과 (각 조건 2회)

각 값은 task 전체 시간이 아닌 `campaignDrainSeconds` 경계이며 캠페인 앱 시작을 포함하고 사전 저장/단독 probe/최종 검증은 제외한다.

| 조건 | 발송 시간 | 처리량 | 완료 저장 호출 / 누적 시간 | 혼합 실시간 API p95 / 최대 | HTTP 전수 대조 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 단건 1차 | 131.447초 | 76.076/s | 10,000회 / 37.235초 | 23.484 / 293.640ms | 10,638건 |
| 최대10건 1차 | 100.122초 | 99.878/s | 1,036회 / 4.402초 | 21.700 / 185.555ms | 10,497건 |
| 단건 2차 | 109.349초 | 91.451/s | 10,000회 / 28.333초 | 18.635 / 150.215ms | 10,548건 |
| 최대10건 2차 | 93.949초 | 106.440/s | 1,022회 / 3.726초 | 21.790 / 69.349ms | 10,477건 |

네 실행 모두 PASS, 캠페인 매핑 10,000개/성공 시도/HTTP 참조/offset/quota 대조를 통과했고 중복·잘못된 참조·Mock 오류는 0이었다.
첫 비교에서 발송 시간은 23.83% 감소하고 처리량은 31.29% 증가했다.
같은 최종 운영 소스의 두 번째 비교는 시간 14.08% 감소/처리량 16.39% 증가였다.
합계 캠페인40,000+실시간2,160=HTTP42,160건을 검증했다.
완료 저장 비용은 줄었지만 선점은 최대10건 조건에서 여전히54.568~59.075초였고 Worker DB 비용도 남는다.
실시간 API p95는 두 번째 비교에서18.635→21.790ms로 커졌다. 지연 개선을 주장하지 않는다.
각 조건2회/고정된1→10→1→10 순서/공유 호스트이므로 통계적 유의성, 일반 운영 처리량이나 다른 크기의 효과를 확정하지 않는다.
단건 1차는 구간 계측을 먼저 추가한 소스이고, 후보 구현 뒤의 1/10 반복 비교는 같은 운영 소스와 설정 인자만 사용한다.
1차 원본의 `publishBatchSize` 필드는 계측 당시 아직 없었으며 단건 경로였다. 원본은 수정하지 않았다.
모든 성공 발행 경로의 ACK 계측 범위는 같다. 후보에서 실패 유지보수 DB 호출을 ACK 시간 밖으로 분리했으나 이 네트워크 성공 실험에는 해당 오류가 없었다.

- [단건 1차 원본](experiments/s7-delivery-profile-2026-11-02-10k-publish-batch-1.json)
- [최대10건 1차 원본](experiments/s7-delivery-profile-2026-11-02-10k-publish-batch-10.json)
- [단건 2차 원본](experiments/s7-delivery-profile-2026-11-02-10k-publish-batch-1-repeat.json)
- [최대10건 2차 원본](experiments/s7-delivery-profile-2026-11-02-10k-publish-batch-10-repeat.json)


| 원본 조건 | 자원 샘플 | heap 최대 MiB | MySQL cgroup 최대 MiB | Hikari waiting 최대 C / RT |
| --- | ---: | ---: | ---: | ---: |
| 1 | 61 | 102.29 | 480.82 | 0 / 0 |
| 10 | 47 | 101.66 | 513.66 | 0 / 0 |
| 1-repeat | 52 | 105.30 | 479.79 | 0 / 0 |
| 10-repeat | 44 | 109.19 | 475.03 | 0 / 0 |

네 실행의 cgroup max/oom/oom_kill 카운터는 모두0이었다. 표는 샘플 최댓값이며 순간 대기/잠금 부재를 보장하지 않는다.
기존100만 건 실패 JSON의 SHA-256 `0f2752898e43efdba1594d97c4ac76881efabbd7f22a0d9112cae1bf8dbab79b`도 그대로 유지했다.

작은 부하 결과를 10만/100만 건으로 선형 외삽해 완료 시간을 약속하지 않는다.
API tail latency와 공유 자원 간섭, 긴 lease/DB 지연, 실제 여러 Relay의 지속 부하는 별도 검증이 필요하다.
묶음 UPDATE의 대형 backlog에서의 실행 계획/역할 간 잠금도 이번1만 건 결과만으로 보장하지 않으며 다음10만 건 검증에서 함께 확인한다.

## 실패 경계 검증

기본 회귀의 `AsyncNotificationTest`에 실제 MySQL을 사용하는 다음4개 검증을 추가했다.

- 다음 send의 future가 미완료/실패여도 이전 ACK 성공분만 저장하고 실패분은 PENDING으로 되돌린다. send 중 DB 트랜잭션이 없고 ACK 타이머가 성공/실패를 각각 기록하는지도 확인했다.
- lease 만료 후 새 token으로 재선점된 행은 이전 token의 묶음 완료로 바뀌지 않으며, 같은 묶음의 유효한 다른 행은 완료된다.
- 두 이벤트의 Kafka ACK 뒤 MySQL trigger로 묶음 UPDATE를 실패시키면 두 행 모두 PROCESSING/published_at NULL을 유지한다. trigger 제거/lease 만료 후 Relay로 다시 발행해 각 payload의2회 전송과 최종 완료를 확인한다. Worker는 꺼져 있어 공급자 호출은0이다.
- 두 번째 send에서 stop을 알리면 ACK 성공한 두 행은 저장하고 나머지 한 행은 선점/시도0의 PENDING으로 남긴다.

`AsyncPropertiesTest`에는 publish-batch-size의1/10 허용과0/11 거절을 추가했다.
실제 부하 task에서 기본값1과 선택값10의 앱 시작/주입도 검증했다.
전체 검증 명령 `caffeinate -i ./gradlew --no-daemon --console=plain build`는12분39초/종료 코드0으로 성공했다.
기본33개 클래스274개가 통과했고 실패/오류/스킵은0이었다. 최종 JUnit XML과 콘솔을 대조했고 실행 JAR도 생성했다.
별도1만 건 task4회는 이 기본274개에 포함하지 않는다. 해당 task 전체 시간은 순서대로3분9초/2분35초/2분42초/2분26초다.
테스트 컨테이너는 정리됐고 기존 사용자 MySQL만 healthy로 남아 있다.

외부 공급자의 실제 발송 성공, 단말 도착, 다중 Relay의 장시간 장애 확률을 검증한 것은 아니다.

## 대안 검토

[AWS Transactional Outbox](https://docs.aws.amazon.com/prescriptive-guidance/latest/cloud-design-patterns/transactional-outbox.html)는
업무 데이터와 Outbox를 같은 DB 트랜잭션으로 저장한 뒤 별도 전송하는 경계와 중복 소비에 대한 멱등성 필요를 설명한다.
이 문서를 우리 프로젝트에서의 exactly-once나 성능 보장으로 해석하지 않는다.
[Debezium Outbox Event Router](https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html)
(열람 시 3.7 문서)는 Outbox 변경을 connector로 수집하고 SMT로 메시지를 변환하는 CDC 대안이다.
현재 프로젝트에는 Debezium이 없고 이 대안의 설정/스키마 호환성이나 처리량을 검증하지 않았다.

우리 프로젝트에서 내린 판단은 다음과 같다. 처음부터 CDC로 전환하면 connector/소스 로그 위치/이벤트 변환/운영 복구를 함께 검증해야 하므로,
작은 조건부 UPDATE 변경으로 비용을 줄일 수 있는지 먼저 확인한다. CDC가 항상 더 빠르거나 현재 규모에 불필요하다는 결론은 아니다.
ACK 비동기화는 첫 측정에서 더 작은 비용 구간이었고, Worker 증설/DB 자원 증설은 이번 비교의 독립 변수를 늘리므로 적용하지 않았다.

## 참고한 공식 자료

실제 Gradle 캐시와 새 실행 JAR(`BOOT-INF/lib`)에서 확인한 의존성은 Spring Kafka3.3.16/Kafka clients3.9.2/Spring JDBC6.2.19/Micrometer1.15.12였다.
브로커4.0.2와 클라이언트3.9.2를 구분해 아래 버전 문서와 대조했다.

- [Spring Kafka 3.3: Sending Messages](https://docs.spring.io/spring-kafka/reference/3.3/kafka/sending-messages.html): send의 future와 동기 대기 경계. 여기서는 ACK 대기 방식을 유지했다.
- [Kafka 3.9: Producer Configs](https://kafka.apache.org/39/configuration/producer-configs/): ACK/idempotence/max-in-flight의 의미. DB와 Kafka의 원자적 commit을 보장하는 설정으로 해석하지 않는다.
- [Spring Framework 6.2: Transaction Propagation](https://docs.spring.io/spring-framework/reference/6.2/data-access/transaction/declarative/tx-propagation.html): REQUIRED의 외부 트랜잭션 참여와 REQUIRES_NEW의 연결 풀 비용. Relay 바깥에 트랜잭션을 추가하지 않는다.
- [Spring Framework 6.2: JDBC Batch Operations](https://docs.spring.io/spring-framework/reference/6.2/data-access/jdbc/advanced.html): 왕복을 줄이는 묶음 처리와 반환 행 수의 제약을 검토했다. 최종 구현은 조건부 단일 UPDATE로 선택했다.
- [MySQL 8.4: UPDATE](https://dev.mysql.com/doc/refman/8.4/en/update.html): WHERE에 맞는 행의 변경과 변경 행 수. 각 lease 조건과 실제 MySQL 실패/롤백 테스트를 함께 사용한다.
- [Micrometer 1.15: Timers](https://docs.micrometer.io/micrometer/reference/1.15/concepts/timers.html): count/total 기록과 time-window max/percentile 한계.

로컬 Spring JDBC6.2.19 sources JAR의 `DataSourceTransactionManager.doBegin/doCommit/doCleanupAfterCompletion`과
`DataSourceUtils.prepareConnectionForTransaction/resetConnectionAfterTransaction`도 확인했다.
격리 설정/복원과 commit을 호출자 시간에 포함해야 한다는 근거이며 각 비용의 비중을 소스만으로 추정하지 않는다.
