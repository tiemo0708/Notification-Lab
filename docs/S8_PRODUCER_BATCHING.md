# S8 producer batching·압축 적용 경계의 Kafka 단독 사전 비교

2026-11-04. 시작 커밋 790a02b. 앱 발송 경로에 설정을 적용하기 전에 현재 ACK 구조와 Kafka의 동작을 확인한다.
운영 Java/resources·토픽·Worker·quota·Outbox 저장은 변경하지 않는다.

## 먼저 확인한 구조

현재 `OutboxRelay`는 단건 DB 선점 → `KafkaTemplate.send(...).get(12초)` → ACK 확인 → PUBLISHED 저장 순서다.
선택 옵션 `app.async.relay.publish-batch-size=10`도 **ACK 후 DB 완료 저장**을 묶을 뿐 Kafka send/ACK는 순차다.
Kafka의 `batch.size`나 `linger.ms`만 늘려도 이 한 Relay가 다음 레코드를 ACK 전에 제출하게 되지는 않는다.
한 건을 기다리는 동안 같은 파티션의 다음 레코드를 제출하지 않으면 linger의 추가 대기만 생길 수 있다.

Kafka client는 **3.9.2**이고 브로커는 **4.0.2**다. 브로커 버전을 producer client 기본 설정으로 간주하지 않는다.
현재 client 3.9의 기본 linger=0, batch.size=16384, compression=none을 기준으로 삼았다.
실제 프로젝트 버전은 Gradle dependencyInsight와 실행 보고서의 clientVersion을 대조했다.

Kafka batching은 **파티션별 record batch**, 네트워크 Produce request에는 여러 파티션의 batch가 들어갈 수 있다.
`records-per-request-avg≈10`이 파티션별 batch마다 10건이라는 뜻은 아니다.
또 `max.in.flight.requests.per.connection=1`은 broker 연결의 요청 상한이고, 애플리케이션이 보관하는 record future 10개와 다른 값이다.

## 고정 조건과 측정 범위

| 조건 | 값 |
| --- | --- |
| 실행 순서 | A → B → C → D → D → C → B → A, 각 2회 |
| A | 단건 제출/ACK 대기, linger0, none |
| B | 단건 제출/ACK 대기, linger5ms, none |
| C | 최대10건 제출 뒤 모두 ACK 확인, linger5ms, none |
| D | 최대10건 제출 뒤 모두 ACK 확인, linger5ms, LZ4 |
| 매 실행 데이터 | 준비150건 + 측정1500건, 총1650건 |
| 인프라 | 같은 새 Kafka 4.0.2 단일 브로커, CPU1/1024MiB/heap256~512MiB |
| 토픽 | 실행별 새 토픽, 파티션3/복제1, explicit partition=순번%3 |
| producer | acks=all, idempotence=true, max.in.flight=1, retries3 |
| 버퍼/배치/시간 | buffer.memory1MiB, batch.size16384, max.block3초/request3초/delivery10초 |
| 제출 대기 | future 최대1 또는10개, 한 제출 창의 공통 ACK deadline12초 |
| 검증 | poll 최대100건, 고정 배열1650개, 소비20초/commit5초, close3초 |
| 전체 상한 | JVM heap512MiB, JUnit180초/task4분, 기본 build/test에서 제외 |

Amazon Java21.0.6/macOS aarch64에서 실행했다. 첫 실행 이후에도 브로커/호스트 상태가 완전히 같다고 볼 수 없다.
인접 조건 A→B는 linger만, B→C는 제출 창만, C→D는 압축만 바뀐다. A→C의 차이를 한 설정의 효과로 설명하지 않는다.
모든 실행은 같은 합성 key/value 목록과 고정 record timestamp를 사용했다.
반복 문자열과 UUID형 nonce를 가진 합성 데이터로 실제 토큰/본문/수신자 데이터가 아니며 압축에 유리한 부분이 있다.
고엔트로피/암호화된 실제 payload의 압축률이나 고정 key의 앱 파티셔너 결과를 대표하지 않는다.
fixture SHA와 직렬화된 value 총 바이트는 원본에 남기고 내용/식별자는 보고서에 내보내지 않는다.

각 producer의 metadata 조회와 준비150건의 모든 ACK를 확인한 뒤 측정1500건을 제출한다.
준비 레코드는 key/value에 별도 접두어를 붙여 본 측정 레코드와 구분하고, 동일 토픽에서 최종 검증한다.
측정 시간은 첫 본 측정 제출부터 마지막 ACK까지이며 준비·producer 생성·최종 소비 검증은 제외한다.
**MySQL/Outbox/Redis/Worker/HTTP Mock 호출은 0**이다. 아래 건/초는 알림 공급자 수락이나 앱의 완료 처리량이 아니다.

## 계측 의미와 첫 실패 보완

Kafka의 `record-send-total`에는 재전송된 레코드도 들어간다. 고유 ACK 개수와 동일하다고 가정하지 않는다.
전송/error/retry/request/outgoing-byte **누적 counter는 준비 직후와 측정 종료의 차이**를 사용한다.
이 차이에서 전송1500, retry/error0을 검증했다. 고유 레코드 수는 별도의 ACK metadata와 실제 소비 내용/offset으로 확인한다.

`batch-size-avg`, `records-per-request-avg`, `compression-rate-avg` 등 평균 metric은 producer의 표본 창 값이다.
준비150건 및 그때의 재시도도 포함하므로 준비 구간을 뺀 평균처럼 표현하지 않는다. 평균을 빼서 구간 평균을 만들지도 않는다.
`outgoing-byte-total` 차이는 producer 프로토콜 트래픽을 포함하며 payload 크기나 실제 네트워크 과금량과 구분한다.
CPU는 제출/ACK 구간의 **클라이언트 JVM 전체** 누적 CPU 시간/벽시계 시간이다(100%=한 코어). broker CPU 비용은 분해하지 않았다.
JIT·내부 스레드의 CPU도 포함하므로 압축 함수만의 비용으로 해석하지 않는다. buffer.memory1MiB는 대기 버퍼 설정이며 프로세스 RSS 상한이 아니다.

- 첫 실험은 **34초/테스트1개 실패**였다. 새 토픽의 NOT_LEADER_OR_FOLLOWER 응답으로 재시도가 발생해
  두 번째 실행의 record-send-total이1500 대신1501이었다. 준비 구간이 없는 이 결과를 정상 처리량 비교로 채택하지 않는다.
  [첫 실패 원본](experiments/s8-producer-batching-2026-11-04-fixture-failure.json)과
  [당시 소스](experiments/s8-producer-batching-2026-11-04-before-source.json)를 보존했다.
- 준비 구간 추가 뒤 **35초/테스트1개 실패**가 있었다. 준비 전송 counter를 고유150건으로 단정한 세 번째 실행에서157이 관측됐다.
  [Kafka 3.9.2 Sender 원본](https://github.com/apache/kafka/blob/3.9.2/clients/src/main/java/org/apache/kafka/clients/producer/internals/Sender.java)을 확인하고,
  준비 ACK/실제 기록 검증과 측정 counter 차이를 분리했다. [두 번째 실패](experiments/s8-producer-batching-2026-11-04-warm-fixture-failure.json)와
  [당시 소스](experiments/s8-producer-batching-2026-11-04-warm-before-source.json)도 보존했다.

두 번 모두 fixture/계측 검증 실패이며 운영 변경은 없다. 소스를 보완한 뒤 새 실험을 실행했으며 자동 재시도로 실패를 숨기지 않았다.

## 실제 결과

보완 뒤 opt-in task **55초/종료코드0/테스트1개 통과**였다. 8회 **13200건(준비1200+본 측정12000)**을 전수 대조했다.
각 실행에서 내용·key·timestamp·파티션 내 순서·ACK offset과 실제 committed=end=[550,550,550]을 확인했다.
본 측정 retry/error는 모두0이다. 준비 retry는 순서대로0/1/0/0/3/3/1/0으로 별도 기록한다.
idempotence로 준비 재시도 후에도 실제 레코드가 늘어나지 않은 이 실행을 전수 검증했으나 모든 장애의 exactly-once 보장을 뜻하지 않는다.

| 순서 | 조건 | 본 측정 초 | 본 측정 건/초 | records/request 평균(준비 포함) | compression 평균(준비 포함) |
| --- | --- | --- | --- | --- | --- |
| 1 | A | 1.066 | 1406.521 | 1.000 | 1.000 |
| 2 | B | 12.629 | 118.777 | 0.999 | 1.000 |
| 3 | C | 1.167 | 1284.912 | 10.000 | 1.000 |
| 4 | D | 1.247 | 1202.947 | 9.940 | 0.265 |
| 5 | D | 1.235 | 1214.181 | 9.898 | 0.264 |
| 6 | C | 1.208 | 1242.114 | 9.958 | 1.000 |
| 7 | B | 11.813 | 126.982 | 0.999 | 1.000 |
| 8 | A | 1.373 | 1092.356 | 1.000 | 1.000 |

| 조건 | 실행별 처리량 산술평균 건/초 | 본 측정 outgoing bytes 평균 | 클라이언트 CPU 평균 % |
| --- | --- | --- | --- |
| A | 1249.438 | 738500 | 42.57 |
| B | 122.879 | 738500 | 11.95 |
| C | 1263.513 | 548300 | 11.55 |
| D | 1208.564 | 176020 | 20.17 |

단건 ACK에서 linger5만 추가한 B는 같은 파티션의 후속 기록을 충분히 모으지 못한 채 대기 시간이 증가했다.
5ms는 추가 대기 설정이지 관측 지연의 정확한 상한이 아니며 broker/네트워크/클라이언트 스케줄링 시간도 포함된다.
C는 B보다 같은 제출 창에서 기록을 모았지만 **원래 기준선 A보다 일관되게 빠르다고 할 수 없다**. A의 반복 변동도 컸다.
평균만 보고 운영의 단건 Relay보다10배 빠르다고 표현하지 않는다.

C→D의 outgoing byte counter 차이는 같은 합성 데이터에서 **67.90% 감소**했다.
이는 압축률 metric 약0.264와 경계가 다르다. D의 평균 처리량은 C보다 낮았고 클라이언트 CPU 평균은 높았다.
CPU 범위 C8.93~14.16%/D12.25~28.09%는 겹치며, 첫 A도19.07~66.07%로 변동했다.
각2회/짧은 구간/공유 호스트로 일반적인 비용·처리량 효과를 확정하지 않는다.

## 적용 판단과 다음 단계

운영 설정/코드를 유지한다. 현재 앱의 DB 선점/완료 저장 비용과 혼합 실시간 지연은 이번 실험에 포함되지 않았다.
특히 현재 Relay의 DB 선점 사이에도 시간이 흘러, 이처럼 DB 없이 연속 제출하는 fixture보다 실제 batching이 적을 수 있다.
전송 바이트 감소가 앱 처리량 향상이나 실시간 지연 개선을 자동으로 뜻하지 않는다.

1. 기존 순차 ACK/linger0을 유지한 실제 HTTP Mock 경로에서 none/LZ4를 고정 조건으로 비교할 수 있다.
2. 제출 창을 앱에 도입하려면 별도 단계로 상한1..10, ACK 이전 PUBLISHED 금지, 부분 실패·timeout·중단·lease fencing·재시작을 먼저 검증한다.
3. 그 뒤 같은 Consumer/pool/quota에서 앱 전체 처리량·실시간 p95/p99·lag·CPU를 반복 측정한다.

이 문서는 Kafka 단독 사전 검증 완료다. 앱 경로의 producer batching/압축 비교 항목은 아직 미완료로 둔다.

```bash
./gradlew --no-daemon --console=plain producerBatchingPilot
```

기본 build/test에서는 실행하지 않는다. 보고서는 매번 `build/reports/producer-batching/run-*/result.json`에 구분해서 저장한다.
[최종 소스163개](experiments/s8-producer-batching-2026-11-04-source.json),
[성공 원본](experiments/s8-producer-batching-2026-11-04-result.json),
[요약·SHA·계산 경계](experiments/s8-producer-batching-2026-11-04-summary.json)를 보존했다.
새 플러그인 저장소 설정과 함께 전체 build를 별도로 실행했다. **19분59초/종료코드0**,
기본 test 새 실행 **46개 클래스/363개 통과, 실패·오류·스킵0**이었다. bootJar는 UP-TO-DATE다.
WorkerConcurrencyTest 11개도 모두 통과했고, 새 producer 실험은 기본 build에서 제외됐음을 확인했다.
소스163개 불변·운영 소스 diff0·JSON 민감 필드/비유한값·문서 링크와 원본/JUnit XML의 SHA를 대조했다.
[최종 로컬 검증](experiments/s8-producer-batching-2026-11-04-validation.json)을 보존한다.
이 결과는 보완 후 원격 CI 성공을 뜻하지 않는다. 이번 새 변경은 로컬 커밋까지 진행한다.

공식 참고: [Kafka 3.9 producer 설정](https://kafka.apache.org/39/configuration/producer-configs/)
(batch.size/linger.ms/compression.type/acks/idempotence/max.in.flight).
