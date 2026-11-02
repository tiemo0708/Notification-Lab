# S7 완료 offset 묶음 저장 비교

2026-11-02. 시작1029686/main. [직전 진단](S7_DELIVERY_TIMING.md)에서 기존 handler 밖의 건별 동기 offset 저장이
평균5.770~6.008ms였다. 완료된 레코드의 저장 횟수를 줄이는 선택적 테스트를 진행한다.
운영 Java/resources와 기본 RECORD 정책은 변경하지 않는다.

## 선택한 방식과 경계

테스트 전용 `OffsetCommitPolicy.Wiring`은 listener factory template의 ackMode만 RECORD/COUNT_TIME으로 비교한다.
두 조건 모두 ackCount10/ackTime100ms, syncCommits=true, asyncAcks=false, enable.auto.commit=false,
max.poll.records1이다. 기존 종료 customizer/stopImmediate/pollTimeout500ms를 유지한다.
COUNT_TIME은 record listener이며 비동기 handler나 batch listener로 전환하지 않는다.
Spring의 완료 offset 집계/리밸런스/종료 처리를 사용하고 자체 커밋 스레드·offset 계산·메모리 대기열을 추가하지 않는다.

100ms는 **처리/poll 사이에서 확인하는 기준**이다. 별도 timer의 강제 마감이 아니며 idle poll500ms와 처리/RPC/스케줄링 지연이 더해질 수 있다.
시간·개수·리밸런스·종료에 따라10개보다 작게 커밋할 수 있다. 정상 경로의 count 조건은 consumer별로 적용된다.
consumer3개인 실험의 총 재소비 범위나 반복 장애의 중복 횟수를10개로 보장하지 않는다.
offset 저장 전에 DB 완료한 레코드는 중단 후 재소비될 수 있다. 완료 작업의 DB 멱등성을 검증한다.

DB 완료 전 공급자 수락 뒤 DB 저장이 실패하면 기존 UNKNOWN/유한 재시도 정책에 따라 공급자를 다시 호출할 수 있다.
이 실험은 외부 공급자/단말의 exactly-once를 구현하지 않는다.

## 사전 안전성 검증

사전 선택 실행은 실제 격리 MySQL/Kafka/HTTP/Redis와 별도 JVM을 사용한다.
기존 RECORD 검증을 유지하고 COUNT_TIME을 함께 검증한다. 전체 회귀에서는 최종 설정 확인 가드까지 재실행한다.

- 정책 범위/비동기 설정 거절, 처리 중 offset0 유지, 진입 timeout·큐 포화의 미선점/미커밋,
  DB 완료 쓰기 실패의 미완료 레코드 보존, 종료 뒤 새 context 복구와3건 저유량 tail 반영:
  **13개 통과/실패·오류·스킵0, 1분8초**.
- DB 완료/offset 전 실제 JVM 종료, 복수 완료 레코드의 실제 JVM 종료·정상 재할당,
  두 모드의7초 quota pause(5초 max.poll보다 길게 heartbeat/poll 유지), 두 모드의 처리 중 합류·Relay/Worker 재시작·이탈:
  **6개 통과/실패·오류·스킵0, 4분48초**.
- 리밸런스와 프로세스 중단 검증은 각 case의 새 MySQL 컨테이너를 사용해 다른 모드의 DB 행이 섞이지 않게 했다.
  사용자 DB/volume을 초기화하지 않는다.

### 실제 미커밋 완료 구간 종료

warmup20건 뒤12건을 수동으로 partition0에 발행했다. Relay를 끈 Worker offset 경계 검증이며 Outbox 발행 성공을 대신 검증한 결과가 아니다.
consumer가 이미 완료한 여러 레코드의 commitSync를 호출하는 시점에 **native RPC 직전** 막고 실제 JVM을 강제 종료한다.
DB 트랜잭션 밖임을 확인하고, broker offset과 완료 행을 별도 연결로 읽는다. marker에는 집계 offset 두 숫자만 보관한다.

[사전 원본](experiments/s7-offset-commit-2026-11-02-safety-pilot.json)에서 broker22/DB 완료28이었다.
미커밋 완료6건을 정상 재할당으로 재소비했고 이6건의 공급자 재호출은0이었다.
재시작의 공급자 호출4건은 아직 처리하지 않았던 작업이며 전체32건의 호출/성공 시도는 각각32건이었다.
완료28건의 notification/task/시도 행 fingerprint는 재소비 전후 같았고 최종 offset32였다.
이는 RPC가 아직 전송되지 않은 경계다. broker가 커밋했지만 응답이 유실된 모든 장애를 재현한 결과는 아니다.

[최종 전체 회귀 원본](experiments/s7-offset-commit-2026-11-02-safety-build.json)에서는 broker22/DB 완료27에서 종료했다.
완료5건 재소비/해당5건 재호출0, 미처리5건 신규 호출/전체32건 호출32회, 완료 행 불변/최종offset32를 다시 확인했다.
시점에 따른 부분 묶음 크기의 차이이며 임의로 항상10건이라고 고치지 않는다.

## 정상 발송 비교 조건

`offsetCommitPilot`은1000..10000건만 허용한다. 기본1만 건을 RECORD→COUNT_TIME→COUNT_TIME→RECORD로 각각 실행한다.
각 조건2회/새 JVM·격리 인프라다. 과거1029686의 결과를 기준선으로 재사용하지 않는다.

- 캠페인 Relay2/완료 저장 최대10건, 실시간 Relay1/완료 저장1건을 고정한다.
- 역할별 Worker3/HTTP3/Redis3/Hikari4/queue0, 요청·Retry 각3파티션을 유지한다.
- 총1000/s·burst100 중 캠페인200/s·burst20, HTTP Mock SUCCESS/1ms/max-in-flight8을 유지한다.
- 실시간 단독50건 뒤 응답+200ms 순차 probe, 접수5초/발송900초/task25분·표본/배열 상한을 유지한다.
- Java21/Boot3.5.16/Gradle8.14.5/JVM512MiB, MySQL8.4.8(2CPU·2GiB/buffer pool512MiB),
  Kafka broker4.0.2(1CPU·1GiB)/client3.9.2, Redis7.4.11(1CPU·256MiB)을 유지한다.
- 두 모드 모두 직전 세부 JDBC/consumer 계측을 사용한다. 캠페인 ack 정책만 바꾸며 실시간은 항상 RECORD다.

```bash
caffeinate -i ./gradlew --no-daemon --console=plain offsetCommitPilot -PdeliveryTargets=1000 -PoffsetMode=COUNT_TIME
caffeinate -i ./gradlew --no-daemon --console=plain offsetCommitPilot -PoffsetMode=RECORD
# 원본을 별도 보존하고 COUNT_TIME, COUNT_TIME, RECORD를 각각 실행
```

`build/reports/offset-commit-pilot/MODE-targets-T.json`을 매 실행 뒤 별도 이름으로 복사한다.
전체 대상 매핑/HTTP 감사/DB 성공 시도1회/Outbox PUBLISHED/각 partition offset=end/quota를 대조한다.
COUNT_TIME에서는 quota 보류나 완료 재소비로 초기 read-only claim 수가 늘 수 있으므로
초기 call/acquire/commit의 일치와 작업 수 이상 여부를 확인한다. permit 후 시도 시작/완료는 각각 작업 수와 같아야 한다.
단순히 초기 claim 수가 늘었다고 공급자 중복으로 집계하지 않는다.

### 측정 범위

- `campaignDrainSeconds`: 앱 시작부터 캠페인 완료 counter를 전부 관측할 때까지. offset 반영 완료 시간과 다르다.
- `campaignOffsetWaitSeconds`: 최종 offset 확인에 실제 기다린 구간. Admin RPC와100ms 기본 관측 polling 비용을 포함한다.
- `campaignCompletionToOffsetsSeconds`: 캠페인 완료 관측부터 캠페인 offset 완료 확인까지. 최종 자원 샘플/실시간 완료 대기도 포함한다.
- 세부 시간은 [직전 진단의 범위](S7_DELIVERY_TIMING.md)를 따른다. 중첩/병렬 누적 시간을 wall time 비율로 합산하지 않는다.
- 공유 호스트/각2회/closed-loop probe이며 통계적 유의성·실시간 SLO·여러 서버 성능을 보장하지 않는다.

[1천 건 연결 검증](experiments/s7-offset-commit-2026-11-02-smoke.json)은1분1초/테스트1개 통과/실패·오류·스킵0이었다.
캠페인1000+실시간98=HTTP1098건을 대조했고 동기 커밋245회/누적1.921초였다. 항상10건씩 저장된다는 의미가 아니다.
후속1만 건 직전에 offset tail 관측을 보강하고 [소스 SHA-256](experiments/s7-offset-commit-2026-11-02-source.json)을 고정했다.
1029686에 테스트 수정이 추가된 코드이며1029686 자체의 결과로 표현하지 않는다.

## 실제1만 건 결과

네 task 모두 BUILD SUCCESSFUL/종료 코드0/각1개 테스트 통과/실패·오류·스킵0이었다.
전체 시간은2분38초/2분31초/2분16초/2분49초다. 실행 전후 고정 소스 해시를 확인했다.
캠페인40000+실시간1910=HTTP41910건의 전체 매핑/성공 시도1회/PUBLISHED/HTTP/각 partition offset=end/quota를 대조했다.
중복·매핑·Mock 오류0이었다. 네 독립1만 건 결과이며 단일 연속4만 건이 아니다.1천 건은 별도다.

| 순서 / 모드 | 사전 저장초 | 발송초 / 처리량 | 혼합 API p95 / 최대ms | DB 완료 p95 / 최대ms | 실시간 수 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 / RECORD a | 19.855 | 101.078 / 98.933/s | 18.228 / 55.399 | 507.384 / 620.522 | 505 |
| 2 / COUNT_TIME a | 25.225 | 86.897 / 115.079/s | 20.974 / 115.243 | 510.531 / 636.371 | 442 |
| 3 / COUNT_TIME b | 17.718 | 81.048 / 123.383/s | 20.123 / 184.559 | 515.150 / 688.855 | 416 |
| 4 / RECORD b | 23.021 | 111.512 / 89.676/s | 25.399 / 264.187 | 513.077 / 952.890 | 547 |

단독 APIp95도19.923/19.279/24.319/24.038ms로 달랐다. 혼합 요청 표본은455/392/366/497개다.
이번 각2회 범위에서 RECORD89.676~98.933/s, COUNT_TIME115.079~123.383/s였다.
a끼리 비교하면 발송14.03% 감소/처리량16.32% 증가, b끼리는27.32% 감소/37.59% 증가였다.
순서와 공유 호스트의 편차가 있고 반복2회이므로 통계적 유의성이나 운영 성능을 확정하지 않는다.
실시간 APIp95와 최대는 두 모드에서 범위가 겹친다. 실시간 지연/SLO 개선을 보장하지 않는다.

### 커밋 호출과 마지막 반영

| 모드 | 동기 커밋 횟수 | 누적초 | 평균ms/호출 | 누적ms/캠페인1건 | offset 확인 대기초 | 완료 관측→offset 확인초 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| RECORD a | 10000 | 63.178 | 6.318 | 6.318 | 0.109 | 0.294 |
| COUNT_TIME a | 2163 | 15.278 | 7.063 | 1.528 | 0.122 | 0.410 |
| COUNT_TIME b | 2019 | 14.271 | 7.068 | 1.427 | 0.233 | 0.403 |
| RECORD b | 10000 | 64.823 | 6.482 | 6.482 | 0.109 | 0.274 |

커밋 호출 수는78.37%/79.81% 감소했다. RPC 하나가 빨라진 결과가 아니라 건별 호출을 줄인 결과다.
누적 시간은 consumer3개의 동시 호출을 합한 것이므로 wall time의 감소량과 동일하지 않다.
마지막 offset 확인까지도 전수 검증했지만,100ms 설정이100ms 안에 반영됐다는 주장을 하지 않는다.
표의 대기 시간은 Admin 관측/polling 비용을 포함하며 broker 내부 지연이 아니다.

### 추가 초기 확인과 자원

초기 Worker claim은10000/10000/10003/10000회였다. COUNT_TIME b에서 read-only 초기 확인3회가 추가됐지만
permit 후 시도 시작/완료는 각각10000회, 공급자 호출/성공 시도도10000회였다.
초기 확인 증가는 quota 보류나 완료 재소비와 양립하지만 원인 분류 counter를 별도로 수집하지 않아 어느 쪽인지 단정하지 않는다.
추가 공급자 발송으로 해석하지 않는다. 세부 JDBC/consumer 계측 실패와 async commit은 두 역할 모두0이었다.

| 모드 | 자원샘플 | heap / MySQL cgroup 최대MiB | 스레드최대 | Hikari waiting 최대 C / RT |
| --- | ---: | ---: | ---: | ---: |
| RECORD a | 47 | 104.030 / 476.711 | 100 | 1 / 0 |
| COUNT_TIME a | 41 | 104.549 / 512.332 | 99 | 1 / 0 |
| COUNT_TIME b | 38 | 103.374 / 476.594 | 100 | 1 / 0 |
| RECORD b | 52 | 100.180 / 482.582 | 100 | 1 / 0 |

178개 표본에서 Hikari 상태 결측0/maximum4와 MySQL max/oom/oom_kill0을 확인했다.
각 역할 consumer3개 wrapping, pool1개 종료, 최종 activeCalls0을 확인했다.
표본 waiting0을 모든 순간의 대기 부재로 해석하지 않는다.

### 원본과 판단

- [RECORD a](experiments/s7-offset-commit-2026-11-02-record-a.json): `ada9d2f37a09f68cae4234462a81458f8ff81c8ade78504cf63622ae5a412e67`
- [COUNT_TIME a](experiments/s7-offset-commit-2026-11-02-count-a.json): `532e4333e7d2eff54bf00d9dd67c01a4ee1e925886cb4adc4bb32f7f7975f031`
- [COUNT_TIME b](experiments/s7-offset-commit-2026-11-02-count-b.json): `53d932cbaca8fdc49437b4ccdfc5d48c90a031100dbfb8e90a959e7ed62f03b5`
- [RECORD b](experiments/s7-offset-commit-2026-11-02-record-b.json): `4c972d0a76f7c801e4011b10d448fa144803d3c36080d739ed9f8810dca18c05`

원본은 build 출력에서 수정 없이 복사했다. UUID/비유한 값/민감 필드 미포함을 확인했다.
이번 범위에서는 호출 수 감소와 처리량 증가를 관측했다. 운영 기본값은 RECORD로 유지한다.
다음은 COUNT_TIME의 HTTP 재시도/Retry·DLQ·commit RPC 실패 경계 보강과 같은 정책의10만 건/실시간 tail 검증이다.
기존 전체 회귀의 재시도 검증을 새 모드의 모든 장애 조합 검증으로 해석하지 않는다. 이후 실제 적용 여부를 판단한다.
100만 건 최초 timeout 원인은 여전히 미확정이며 이번에100만 건/여러 서버/실제 공급자/푸시는 실행하지 않았다.

## 기본 회귀

최종 `caffeinate -i ./gradlew --no-daemon --console=plain build`는 BUILD SUCCESSFUL15분22초/종료 코드0이었다.
최종37개 클래스 JUnit XML과 콘솔을 대조해 **294개 통과/실패·오류·스킵0**을 확인했다.
기존283개와 새 정책/모드 추가11개이며 별도1천/1만 건 결과와 구분한다.
두 실행 JAR task는UP-TO-DATE였고 기본 회귀는 실제 재실행했다. 운영 Java/resources diff는0이다.
종료 뒤 소스 해시/원래100만 건 FAIL 원본이 같음을 확인했고 격리 컨테이너는 정리됐다.
기존 사용자 MySQL만healthy로 남았다. Git identity와 원격main(9cf1786)을 읽기 전용으로 확인했으며 푸시하지 않았다.

## 참고

- [Spring Kafka3.3.16 listener container](https://docs.spring.io/spring-kafka/reference/3.3/kafka/receiving-messages/message-listener-container.html):
  COUNT_TIME/RECORD, syncCommits와 poll 단위 처리 경계.
- [Spring Kafka3.3.16 오류 처리](https://docs.spring.io/spring-kafka/reference/3.3/kafka/annotation-error-handling.html):
  오류 처리 전 pending commit, isAckAfterHandle과 컨테이너 정지. 기록을 성공 처리한 척 건너뛰지 않는다.
- [실제3.3.16 컨테이너 소스](https://github.com/spring-projects/spring-kafka/blob/v3.3.16/spring-kafka/src/main/java/org/springframework/kafka/listener/KafkaMessageListenerContainer.java):
  processCommits/countAcks/timedAcks/commitSync 경로를 로컬 동일 버전 sources JAR와 대조했다.
