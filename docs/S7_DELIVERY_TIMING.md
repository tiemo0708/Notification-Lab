# S7 JDBC와 Kafka offset 저장 비용 세분화

2026-11-02. 시작c64acf0/main. [앞선 Relay1/2개 비교](S7_RELAY_SCALING_PILOT.md)에서 일관된 처리량 증가를
확인하지 못했고 캠페인 Hikari 대기가 관측됐다. 기존 저장소 caller timer가 포함한 비용을 더 나눠 확인한다.
운영 Java/resources, 발송 한도, DB 격리 수준, ACK/offset 정책을 변경하지 않는 **테스트 전용 관찰**이다.

## 관찰 방법과 경계

`deliveryTimingPilot`만 `DeliveryTiming.Wiring`을 명시적으로 추가한다. 사전 저장 앱에는 적용하지 않는다.
라이브 역할별 실제 Hikari DataSource를 감싸되 같은 pool/transaction manager를 사용하며 풀 unwrap과 종료를 보존한다.
컨슈머 생성기의 post processor는 실제 consumer의 메서드/인수를 한 번 그대로 전달한다.
자체 offset 커밋이나 재시도는 하지 않는다. 실제 컨테이너의 RECORD ack와 syncCommits=true를 확인한다.

기존 트랜잭션 proxy의 바깥쪽에 테스트 advice를 추가해 아래 범위에만 JDBC 작업을 귀속한다.

- OUTBOX_CLAIM / OUTBOX_PUBLISHED / OUTBOX_FAILED
- WORKER_INITIAL: permit 확인 전의5인수 claim
- WORKER_ADMITTED: permit 후 재검증/시도 시작 claim
- WORKER_COMPLETE: 결과/시도 이력 저장
- CONSUMER: Kafka commit API만 관찰

API 접수와 지표 갱신 등 이 scope 밖의 SQL은 세부 JDBC 집계에서 제외한다. 전체 DB 부하를 모두 귀속한 결과는 아니다.

STORE_CALL은 트랜잭션 시작/종료를 포함한 전체 호출이다. 같은 트랜잭션의 내부 self-invocation은 별도 scope가 아니다.
세부 계측 이름은 enum으로 고정하고 SQL/파라미터/수신자/토큰/본문/레코드/commit 인수의 offset map/예외 메시지를 저장하지 않는다.
기존 최종 정합성 검증의 토픽·파티션별 end/committed 집계는 유지한다.
역할당7×14개 집계 칸에 count/failures/total/mean/max만 보관한다. 호출별 표본이나 무한 버퍼는 없다.
ThreadLocal scope는 finally에서 복구/제거하며 예외는 원래 객체를 호출자에게 전달한다.

| 구간 | 포함하는 시간과 한계 |
| --- | --- |
| ACQUIRE | Hikari getConnection 호출. checkout 대기 외에 검증/연결 준비가 포함될 수 있어 순수 queue wait로 단정하지 않음 |
| PREPARE | JDBC prepare/createStatement 호출. 서버 SQL parse 시간과 동일하지 않음 |
| EXECUTE | JDBC execute 계열. 클라이언트/네트워크/서버·잠금/결과 버퍼링을 포함할 수 있으며 DB CPU 시간이 아님 |
| COMMIT / ROLLBACK | JDBC 메서드가 반환할 때까지. commit 시간만으로 fsync 병목을 확정하지 않음 |
| AUTOCOMMIT_STATE | get/setAutoCommit. 확인·시작·복원을 함께 집계하며 설정 값은 저장하지 않음 |
| ISOLATION_STATE | get/setTransactionIsolation. 기존 READ COMMITTED 설정과 복원 비용을 포함 |
| READ_ONLY_STATE | is/setReadOnly 확인·설정 |
| CONNECTION_STATE | 위 분류 외 connection API |
| RELEASE / STATEMENT_CLOSE | 연결 반환 / statement 종료 |
| COMMIT_SYNC | consumer.commitSync의 원래 overload가 반환할 때까지 |
| COMMIT_ASYNC_SUBMIT | 호출될 경우 제출 시간만 의미. 이 실험은0이어야 하며 비동기 완료 시간으로 해석하지 않음 |

STORE_CALL과 내부 구간은 중첩되므로 함께 합산하지 않는다. 전체에서 내부 합을 뺀 값에는 결과 매핑/CPU,
미계측 API/프레임워크 처리/스케줄링과 관찰 오버헤드가 남는다. 서로 다른 스레드의 시간도 wall time 비율로 합산하지 않는다.
구간 p95는 측정하지 않으며 mean/max와 기존 실시간 API p95를 구분한다.

세부 집계 범위는 **라이브 앱 생성부터 종료까지**이며 실시간 단독50건/idle/마지막 종료 경로도 포함한다.
사전 저장은 제외한다. 기존9개 pipeline timer의 마지막 스냅샷과 종료 경계가 다를 수 있다.
컨슈머 커밋에는 할당/리밸런스 시 호출도 포함될 수 있어 count가 항상 발송 건수라고 가정하지 않는다.
앱 종료 뒤 active0/각 pool 종료/역할별 consumer3개 wrapping을 확인하고 집계한다.
Worker 각 단계의 call/acquire/commit 수, Outbox 완료 SQL 수와 consumer commit 수를 작업 수와 대조한다.

## 실험 조건과 명령

- 1000건으로 연결 검증 후10000건을 Relay1→2→2→1, 조건별2회 실행한다. 매번 새 JVM/격리 인프라다.
- 완료 저장 CAMPAIGN10/REALTIME1, Worker3/HTTP3/Redis3/Hikari4/queue0, 역할별 요청·Retry 각3파티션.
- 총1000/s·burst100, 캠페인200/s·burst20, HTTP Mock SUCCESS/1ms/max-in-flight8.
- 실시간 단독50건 뒤 응답+200ms 순차 probe. 접수5초/발송900초/표본5000개/task25분 상한.
- Java21/Boot3.5.16/Gradle8.14.5/test JVM512MiB, MySQL8.4.8(2CPU/2GiB/buffer pool512MiB),
  Kafka broker4.0.2(1CPU/1GiB)/client3.9.2, Redis7.4.11(1CPU/256MiB).
- 전체 대상/성공 시도1회/HTTP 감사/Outbox PUBLISHED/offset=end/quota를 기존 방식대로 대조한다.

```bash
caffeinate -i ./gradlew --no-daemon --console=plain deliveryTimingPilot -PdeliveryTargets=1000 -PcampaignRelays=1
caffeinate -i ./gradlew --no-daemon --console=plain deliveryTimingPilot -PcampaignRelays=1
# 원본을 별도 보존한 뒤2,2,1을 각각 실행
```

출력은 `build/reports/delivery-timing-pilot/relays-N-targets-T.json`이다.
같은 조건을 재실행하기 전 `docs/experiments/`에 별도 이름으로 복사한다.
기존 task와 test/build에는 계측을 자동 적용하지 않으며10만/100만 실행은 허용하지 않는다.

관찰을 켠 결과이므로 이전 무계측 처리량과 직접 비교해 성능 개선/오버헤드를 확정하지 않는다.
조건별2회/공유 호스트/closed-loop probe로 실시간 SLO나 여러 서버의 성능을 증명하지 않는다.

## 연결 검증과 원본

- 첫 단위 검증은6초/8개 중7개 통과·1개 실패였다. SQL 실패를 검증할 테스트 JDBC 대역이 지원하지 않는
  바인딩 인수 없는 update(String)의 Statement 경로를 호출했다. 대역에 맞는 PreparedStatement 경로로 수정한 뒤9개 모두4초/실패·오류·스킵0이었다.
  실제 운영 코드의 커밋/롤백 수정은 아니다.
- [1천 건 v1](experiments/s7-delivery-timing-2026-11-02-smoke-v1.json):42초/테스트1개 통과,
  캠페인1000+실시간97=HTTP1097건 전수 대조. 발송10.687초. 연결 상태를 한 묶음으로 관찰했다.
- [1천 건 v2](experiments/s7-delivery-timing-2026-11-02-smoke-v2.json):44초/테스트1개 통과,
  캠페인1000+실시간98=HTTP1098건 전수 대조. 발송10.671초.
  autoCommit/격리 수준/read-only를 나누고 계측 count의 누락 가드를 추가했다.
- 후속1만 건은 v2의 [동일 소스 SHA-256](experiments/s7-delivery-timing-2026-11-02-source.json)을 고정한다.
  기준c64acf0에 아직 커밋하지 않은 테스트 계측을 포함하므로c64acf0 자체의 결과라고 표현하지 않는다.

## 1만 건 실제 결과

아래 네 task는 BUILD SUCCESSFUL/종료 코드0/각1개 테스트 통과/실패·오류·스킵0이다.
소스 SHA-256은 실행 전후 동일했다. 캠페인40000+실시간1717=HTTP41717건을 전수 대조했고 중복·매핑·Mock 오류0이다.
네 독립1만 건의 합계이며 단일 연속4만 건을 뜻하지 않는다. 별도1천 건 두 번은 이 합계에 포함하지 않는다.

| 순서 / Relay | 사전 저장초 | 발송초 / 처리량 | 혼합 API p95 / 최대ms | DB 완료 p95 / 최대ms | 실시간 수 | task 전체 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 / 1개 a | 17.215 | 83.733 / 119.428/s | 13.432 / 57.451 | 475.278 / 644.722 | 440 | 2분13초 |
| 2 / 2개 a | 14.267 | 93.196 / 107.301/s | 20.123 / 145.725 | 509.584 / 693.763 | 476 | 2분19초 |
| 3 / 2개 b | 14.445 | 71.916 / 139.050/s | 9.157 / 22.851 | 488.970 / 632.409 | 387 | 1분55초 |
| 4 / 1개 b | 14.751 | 78.354 / 127.626/s | 9.665 / 69.546 | 483.965 / 599.884 | 414 | 2분1초 |

단독 API p95는19.112/20.063/20.484/26.067ms였다. 혼합 요청 표본390/426/337/364개 외 각50개 단독 요청이 있다.
Relay1 범위119.428~127.626/s, Relay2 범위107.301~139.050/s이며 일관된 개선은 확인하지 못했다.
5초 접수 timeout은 발생하지 않았지만 원래100만 건 장애 원인을 확정하거나 해소한 결과가 아니다.

### Worker 한 건에 드는 비용

각 실행에서 캠페인 WORKER_INITIAL/ADMITTED/COMPLETE의 STORE_CALL·ACQUIRE·COMMIT은 각각10000회,
ROLLBACK0회였다. JDBC EXECUTE는 각각40000/80000/40000회로 **이 설정에서 알림당16회**였다.
Kafka commitSync는10000회, commitAsync는0회였다. 세부 계측 실패 수는 두 역할 모두0이었다.
이 수치는 Redis permit 전후 두 번 claim하는 현재 정상 HTTP 발송 설정에 해당한다.
다른 프로필/재시도/완료 재소비까지 같은 횟수라고 일반화하지 않는다.

아래 값은 Worker 세 단계의 누적 caller 시간/완료 알림10000개다. 전체 wall time 비율이 아니다.
STORE_CALL은 내부 열을 포함한다. offset 저장은 기존 handler 종료 뒤의 별도 구간이다.

| 조건 | Worker 저장소 전체ms/건 | 연결 획득 합ms/건 | JDBC 실행 합ms/건 | DB 커밋 합ms/건 | autoCommit 확인·설정 합ms/건 | offset 저장 평균ms/회 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1개 a | 14.545 | 0.004 | 8.232 | 3.639 | 2.349 | 5.986 |
| 2개 a | 18.420 | 0.715 | 10.055 | 4.338 | 2.886 | 5.892 |
| 2개 b | 12.695 | 0.561 | 6.796 | 2.981 | 2.029 | 5.770 |
| 1개 b | 12.491 | 0.004 | 7.124 | 3.020 | 2.039 | 6.008 |

연결 획득은2개에서 늘었지만 전체 비용을 연결 대기 하나로 설명할 수 없다.
건별 SQL/트랜잭션 왕복과 **기존 handler timer 바깥의 offset 저장 약6ms**도 관측했다.
네 실행에서 offset 저장 평균이 비슷하게 관측됐으며 broker CPU/fsync/네트워크 중 단일 원인을 확정하지 않는다.
157개 자원 샘플이나 네 번의 실행으로 성능 차이의 통계적 유의성을 주장하지 않는다.

### Outbox 선점의 연결·트랜잭션 상태 비용

아래는 빈 폴링을 포함한 선점 호출당 누적 시간/호출 수다. READ COMMITTED를 설정하고 복원하는 비용이 나타난다.
교차 역할 잠금 방지에 필요한 격리 수준을 제거하거나 SQL 인덱스를 되돌리지 않는다.

| 조건 | 호출 수 | 전체ms/호출 | 획득 | JDBC 실행 | 커밋 | autoCommit 상태 | 격리 수준 상태 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1개 a | 10035 | 4.549 | 0.002 | 1.451 | 1.321 | 0.663 | 1.033 |
| 2개 a | 10822 | 5.963 | 0.429 | 1.710 | 1.550 | 0.841 | 1.344 |
| 2개 b | 10550 | 4.985 | 0.351 | 1.423 | 1.323 | 0.700 | 1.114 |
| 1개 b | 10063 | 3.878 | 0.002 | 1.236 | 1.083 | 0.566 | 0.910 |

PUBLISHED 저장은1008/1042/1002/1001번의 트랜잭션에 각각10000개의 기본키 단건 UPDATE를 수행했다.
UPDATE 호출은 줄이지 않고 같은 트랜잭션 안에서만 commit을 묶는 기존 구현을 유지했다.

| 조건 | 자원샘플 | heap / MySQL cgroup 최대MiB | 스레드최대 | Hikari waiting 최대 C / RT |
| --- | ---: | ---: | ---: | ---: |
| 1개 a | 40 | 103.237 / 474.211 | 99 | 0 / 0 |
| 2개 a | 44 | 103.615 / 513.266 | 100 | 1 / 0 |
| 2개 b | 35 | 103.573 / 477.828 | 100 | 1 / 0 |
| 1개 b | 38 | 101.371 / 478.016 | 99 | 0 / 0 |

MySQL cgroup max/oom/oom_kill0, Hikari 상태 결측0/maximum4를 확인했다.
각 역할 pool1개 종료와 consumer3개 wrapping, 최종 activeCalls0도 확인했다.
Hikari 표본0을 모든 순간의 대기 부재로 해석하지 않는다.

### 원본

- [1개 a](experiments/s7-delivery-timing-2026-11-02-1a.json): `bb2f5681330f7459e721e1e3f73906172ff0b91cf7ad92601c597ce914e97808`
- [2개 a](experiments/s7-delivery-timing-2026-11-02-2a.json): `262f318c337fe9a56f8d35596b59e2bd290b98f37aefeab6036b39f690170c4c`
- [2개 b](experiments/s7-delivery-timing-2026-11-02-2b.json): `d23e19b022338bf6f4eb044b3900404a8d057a6c65659fcb71fbf59f6fdd0b43`
- [1개 b](experiments/s7-delivery-timing-2026-11-02-1b.json): `2e8c5c00807f7574c5304f660a160333ba61306b0fbd784c4bbc6ab4a29d1005`

원본은 build 출력에서 수정 없이 복사했다. UUID/비유한 값/민감 필드 미포함을 확인했다.
앞선100만 건 실패 원본은 변경하지 않으며100만 건/유료 서버/실제 공급자 호출/푸시는 이번 범위에 포함하지 않는다.

## 다음 작은 실험과 한계

후보는 **DB 완료된 레코드의 offset 저장을 개수·시간 기준으로 제한해 묶는 선택적 비교**다.
RPC 횟수를 줄일 가능성과 장애 시 이미 완료한 레코드의 재소비 범위 증가를 함께 검증해야 한다.
미완료 offset을 건너뛰지 않는지, 오류·재시작·리밸런스에서 공급자 중복 호출을 차단하는지 확인하기 전 기본 RECORD를 변경하지 않는다.
현재 결과는 해당 기능을 구현하거나 속도 향상을 증명한 결과가 아니다. DB SQL/commit 비용도 계속 남는다.

## 기본 회귀

전체 `caffeinate -i ./gradlew --no-daemon --console=plain build`는 BUILD SUCCESSFUL11분36초/종료 코드0이었다.
최종36개 클래스 JUnit XML과 콘솔을 대조해 **283개 통과/실패·오류·스킵0**을 확인했다.
기존278개와 새 계측 검증5개이며 별도1천 건2회/1만 건4회와 구분한다.
운영 코드가 같아 bootJar/mockServerJar는UP-TO-DATE였고 기본 회귀는 실제 재실행했다.
종료 뒤에도 소스 해시와 원래100만 건 FAIL 원본이 같음을 확인했다.
격리 컨테이너는 정리됐고 기존 사용자 MySQL만healthy로 남았다. Git identity와 원격main을 읽기 전용으로 확인했으며 푸시하지 않았다.

## 참고

- [Spring Kafka3.3.16 listener container](https://docs.spring.io/spring-kafka/reference/3.3/kafka/receiving-messages/message-listener-container.html):
  RECORD ack와 syncCommits. handler 완료와 offset commit은 다른 경계다.
- [실제3.3.16 ConsumerFactory 소스](https://github.com/spring-projects/spring-kafka/blob/v3.3.16/spring-kafka/src/main/java/org/springframework/kafka/core/DefaultKafkaConsumerFactory.java):
  consumer post processor. 로컬 동일 버전 sources JAR의 commitSync 호출 경로도 확인했다.
- [Spring6.2.19 JDBC transaction manager](https://github.com/spring-projects/spring-framework/blob/v6.2.19/spring-jdbc/src/main/java/org/springframework/jdbc/datasource/DataSourceTransactionManager.java):
  연결 획득/설정/commit/복원/반환 경계.
- [Spring6.2 Advised API](https://docs.spring.io/spring-framework/reference/6.2/core/aop-api/advised.html):
  기존 비고정 proxy의 advice 추가. 새 트랜잭션이나 다른 전파 속성을 만들지 않는다.
