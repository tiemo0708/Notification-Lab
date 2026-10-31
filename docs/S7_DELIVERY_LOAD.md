# S7 — 10만 건 HTTP 발송 실험

## 목적과 범위

1천/1만 건 [사전 실험](S7_DELIVERY_PILOT.md)의 정상 경로를 10만 캠페인으로 확대한다.
실제 Batch로 사전 저장한 후 Relay → Kafka → Worker → loopback HTTP Mock → DB 결과/offset을 대조한다.
캠페인 발송 중 실시간 요청도 계속 접수하고 단독/혼합 구간의 완료 지연을 별도로 측정한다.
공급자 수락을 검증하며 단말 도착, exactly-once, 운영 성능, S7 전체 완료를 의미하지 않는다.

## 조건과 상한

운영 코드·스키마·기본 발송 예산은 바꾸지 않는다. 격리 Testcontainers와 실험 전용 Redis 키만 사용한다.
기존 사전 실험과 같은 Java 21 / JVM heap 512 MiB, MySQL 8.4.8(2 CPU/2 GiB, buffer pool 512 MiB),
Kafka 4.0.2(1 CPU/1 GiB), Redis 7.4.11(1 CPU/256 MiB, maxmemory 128 MiB/noeviction)을 사용한다.
Batch chunk 100/Writer 1, 앱별 Worker3/HTTP3/Redis3/Hikari4, Worker 대기열 0이다.
총 1000/s·burst100 중 캠페인 200/s·burst20, 실시간 800/s·burst80이며 서로 차용하지 않는다.
HTTP Mock은 SUCCESS/지연1ms/동시8/10000요청·1초 창이다.

- 캠페인 대상: 정확히 100000명만 허용. 100만 건은 별도 설계·검증 단계다.
- 실시간: 단독50개 포함 최대20000개. HTTP 응답 후200ms 대기하는 단일 순차 생성기다.
- 발송 구간: 60분. 공유 테스트 메서드는 80분, Gradle task는 90분으로 제한한다.
- HTTP 감사: 캠페인 BitSet 약12.21 KiB, 실시간 BitSet 약2.45 KiB 및 최대20001칸 ID 배열.
  범위를 먼저 검사한 뒤 기록한다. 전체 캠페인 ID나 본문을 메모리에 보관하지 않는다.
- 완료 지연: notification 기본 키 keyset 조회로 페이지당 최대1000행.
  최대100000개의 primitive double 배열(약781.25 KiB)을 정렬해 nearest-rank p50/p95/p99/max를 계산한다.
  결과 개수·완료 시각·음수/비유한 값도 검사한다. 메모리는 무제한 스트림/전체 행 Map 목록으로 증가하지 않는다.
- 캠페인 전수 대조: 먼저1000개 campaign_target 페이지를 materialize한 뒤 PK JOIN으로 읽는다.
  notification/task/outbox/attempt의 EXPLAIN이 모두 eq_ref/PRIMARY인지 확인한다.
  결정적 ID, 대상 번호·참조, CAMPAIGN, 수락 완료, PUBLISHED, 성공 시도1회를 모두 대조한다.
- 샘플은 약2초 간격이며 발송 시간 상한과 생성기 대기로 수가 제한된다. JSON에는 개인 식별·본문을 저장하지 않는다.

## 실행

```bash
# 수정한 공통 경로의 작은 사전 검증
./gradlew --no-daemon --console=plain campaignDeliveryPilot -PdeliveryTargets=1000
# 10만 건, 기본 회귀와 분리된 명시적 실행
./gradlew --no-daemon --console=plain campaignDeliveryLoad
# 기본 회귀 및 실행 JAR 확인
./gradlew --no-daemon --console=plain build
```

macOS에서는 필요하면 caffeinate -i를 앞에 붙인다. 같은 checkout에서 실험을 겹쳐 실행하지 않는다.
출력은 build/reports/campaign-delivery-load/load-100000.json과 해당 Gradle 테스트 보고서다.
실행별 결과는 docs/experiments에 별도로 보존한다. 같은 출력 파일 재실행은 이전 값을 덮어쓴다.

## 측정과 통과 기준

1. 캠페인 사전 저장 시간과 발송 구간 시간을 분리한다. Batch 완료 후 발송 시도0을 확인한다.
2. 실시간 단독50개 완료·offset을 확인한 뒤 캠페인을 시작한다.
3. 발송 구간은 캠페인 앱 시작부터 완료 카운터100000 관측까지이며 시작/샘플링 비용을 포함한다.
4. 혼합 실시간 요청은 최소10개, DB 완료 후 모든 요청/Retry/DLQ 파티션의 end=committed를 직접 확인한다.
5. 캠페인100000개 HTTP 고유 수락, DB 결과, 전체 참조, 성공 시도1회, 역할별 quota ALLOWED가 일치해야 한다.
6. 실시간 HTTP ID는 API 반환 ID와 대조한다. 중복/잘못된 매핑/Mock429·5xx·busy·영구실패는0이어야 한다.
7. 지연은 DB created_at→completed_at이며 API 응답 지연과 구분한다. 캠페인 지연에는 사전 저장·단독 구간 대기가 포함된다.
8. 실패·Worker 중단·상한 소진은 FAIL이다. 실패 결과도 보존하며 부분 완료를 통과로 기록하지 않는다.

## 실제 결과

2026-10-31(KST), macOS15.7.7 arm64 / Amazon Corretto21.0.6 / Gradle8.14.5,
호스트10 logical CPU/16 GiB, Docker Desktop10 CPU/약7.65 GiB에서 각1회 실행했다.
기존 IDE/로컬 MySQL과 호스트를 공유한다. 전용 측정 서버나 반복 비교 실험은 아니다.

- 공통 경로1000건 smoke: **1개 통과**, BUILD SUCCESSFUL(52초), 종료 코드0.
  [원본](experiments/s7-delivery-pilot-2026-10-31-1000-v2.json), 사전 저장2.057초/발송11.390초,
  실시간101개, HTTP1101건, 참조1000개/PK JOIN 계획을 대조했다.
- `caffeinate -i ./gradlew --no-daemon --console=plain campaignDeliveryLoad`:
  **1개 통과**, BUILD SUCCESSFUL(25분30초), 종료 코드0.
  [10만 원본 JSON](experiments/s7-delivery-load-2026-10-31-100k.json)을 보존했다.

| 항목 | 실제 결과 |
| --- | ---: |
| Batch 사전 저장 | 151.762초 |
| 캠페인 발송 구간 | 1346.993초(약22분27초) |
| 캠페인 처리량 | 74.239건/초 |
| 최종 지연·참조·건수 대조 | 2.305초 |
| 실시간 단독 / 혼합 | 50 / 5292건 |
| HTTP 수락 / 고유 수신 | 105342 / 105342건 |
| 캠페인 전수 참조 | 100000개, n/t/o/a 모두eq_ref/PRIMARY |
| HTTP 중복 / 잘못된 매핑 | 0 / 0 |
| Mock429 / 5xx / busy / 영구실패 | 모두0 |
| HTTP 최대 동시 처리 | 4 |
| 캠페인 / 실시간 quota ALLOWED | 100000 / 5342 |

| 지연(밀리초) | p50 | p95 | p99 | 최대 |
| --- | ---: | ---: | ---: | ---: |
| 단독 DB 완료(50개) | 146.128 | 553.657 | 616.772 | 616.772 |
| 혼합 DB 완료(5292개) | 195.261 | 918.136 | 1213.208 | 1905.362 |
| 단독 API 응답 | 14.667 | 28.583 | 78.601 | 78.601 |
| 혼합 API 응답 | 7.375 | 334.452 | 810.932 | 1609.802 |

혼합 구간에서 p95/p99 완료 지연과 API 응답 p95가 증가했다. 처리 경로/예산을 나눠도 공유 DB·호스트의
간섭이 사라졌다고 주장할 수 없다. 단독50개는 작고 측정 순서/warmup/생성률 차이가 있어 증가 원인은
이번1회로 확정하지 않는다. 고정 도착률의 실시간 SLO를 입증한 결과도 아니다.
캠페인 created→completed p95는1294653.334ms이며, 사전 저장과 단독 구간 대기가 포함된 별도 경계다.

606개 샘플의 null/비유한 값은0이었다. 관측 최대 JVM heap116.32 MiB, MySQL cgroup1307.98 MiB,
스레드97개였다. 샘플 구간 GC3680ms, JVM 평균CPU약0.222코어 상당이다. 앱 두 역할/Mock/감사/생성기를
함께 실행한 값이며 순간 최대/단일 Worker 자원으로 해석하지 않는다.
캠페인 lag 최대215/미발행Outbox100000, 실시간2/2였고, 최종 역할별 모든 파티션의committed=end를 확인했다.
따라서 lag만으로 전체 적체를 판단하지 않는다. 실시간 총5342개는 기존pilot 상한5000을 넘었지만
새 상한20000 안에서 전수 대조했다. 동일한 정상 조건1회 성공이며 재시도/장애 부하를 포함하지 않는다.

## 한계와 후속 작업

두 역할 앱·Mock·감사·생성기는 같은 JVM에서 실행된다. 별도 OS 프로세스/서버의 격리 검증은 아니다.
일정 도착률의 open-loop 부하가 아니며, 실시간 응답이 느려지면 제출률도 떨어진다.
정상 SUCCESS 한 번 실행이므로 장애·429·지연 증가·반복 측정과 운영 SLO는 별도 검증이다.
저장과 발송은 동시에 실행하지 않는다. HTTP Mock 100만 건은 이 결과를 확인한 다음 진행한다.
