# S7: HTTP 발송 경로 사전 실험

2026-10-20 (KST). 10만→100만 건 발송 부하 전에 측정 도구와 실험 설정을 검증하는 **1천/1만 건 pilot**이다.
실제 Batch → MySQL Outbox → Kafka → Worker → HTTP Mock → DB 완료 → offset 커밋 경로를 사용한다.
공급자 수락을 확인하며 단말 도착은 측정하지 않는다. 10만/100만 건 발송 완료로 해석하지 않는다.

## 문제와 선택

DB 인계가 성공해도 발송에는 Relay, Kafka 소비, Redis 허가, DB/HTTP 연결 풀의 제약이 추가된다.
대량 실행 전에 종료 조건, 수신자별 누락·중복 대조, 실시간 지연 측정이 제대로 동작하는지 확인해야 한다.

| 선택 | 이유 | 비용·한계 |
| --- | --- | --- |
| 사전 저장 후 발송 측정 | 이전 DB 인계 실험과 발송 적체 해소 시간을 구분 | Batch와 발송을 동시에 수행하는 상황은 제외 |
| 실제 HTTP Mock 앱 + 테스트 전용 감사 필터 | 정상 어댑터/응답을 유지하며 수신자별 실제 도착을 독립 대조 | 감사 처리 비용도 측정에 포함 |
| 실시간 단독 → 캠페인 혼합 순차 구간 | 동일한 설정에서 실시간 완료 지연을 관측 | 순서·warmup·호스트 상태가 달라 인과적 개선/회귀를 단정할 수 없음 |
| 격리 Redis에 명시적 실험 예산 | 기본 캠페인 2건/초로는 100만 건에 약 5.8일이 걸림 | 이 실험의 높은 예산은 실제 공급자 한도나 운영 권장값이 아님 |

두 역할과 HTTP Mock은 같은 테스트 JVM의 독립 Spring context다. HTTP는 실제 loopback TCP를 사용하고,
역할별 풀·Worker·토픽·group은 분리된다. JVM heap/CPU/GC는 공유하므로 OS 프로세스 장애 격리나 역할별 JVM 자원을 측정한 것은 아니다.
사용자 로컬 DB/.env/기존 Redis 키를 쓰지 않는다. 새로운 Testcontainers 인프라와 빈 quota 키를 사용하고 종료 후 정리한다.

## 고정 조건과 상한

| 항목 | 설정 |
| --- | --- |
| 데이터 | SYNTHETIC_V1 캠페인 1,000 또는 10,000건, 가상 실시간 요청 |
| Batch | chunk 100, 단일 Writer, 인계 중 Relay/Worker 비활성 |
| MySQL | 8.4.8, 2 CPU/2 GiB, buffer pool 512 MiB |
| Kafka | 4.0.2, 1 CPU/1 GiB, heap 256~512 MiB, 각 토픽 3파티션 |
| Redis | 7.4.11, 1 CPU/256 MiB, maxmemory 128 MiB/noeviction, 실험용 비영속 컨테이너 |
| JVM | Java 21, 최대 heap 512 MiB |
| 역할별 Worker / HTTP / Redis / Hikari | 3 / 3 / 3 / 4개, Worker 대기열 0 |
| 공급자 전체 rate/burst | 1,000/s / 100 |
| 캠페인 rate/burst | 200/s / 20 |
| 실시간 rate/burst | 800/s / 80, 다른 몫 차용 없음 |
| HTTP Mock | SUCCESS, 응답 지연 1ms, 10,000요청/1초 창, 동시 8개 |
| 실시간 요청 생성 | HTTP 202 응답 뒤 200ms 대기, 단일 순차 생성기 |
| 표본/실행 상한 | 단독 50개, 전체 실시간 최대 5,000개, 발송 15분, 테스트 20분/task 25분 |

HTTP timeout·재시도 정책·Relay 주기 등은 기존 앱 설정을 유지한다. Circuit Breaker는 기본 비활성이다.
동시 소비자 3개에 Redis 연결 기본값 2개를 그대로 쓰면 fail-closed 중지가 가능하다.
최종 실험은 소비자 3개/연결 3개로 맞춘다. 운영 기본값이나 제한기 실패 정책을 바꾸지 않았다.

## 실행과 측정 경계

```bash
# 사전 smoke: 별도 새 JVM/인프라, 본 실행의 warmup이 아님
./gradlew --no-daemon --console=plain campaignDeliveryPilot -PdeliveryTargets=1000
# 기본은 10,000건. 허용 범위 1,000..10,000
./gradlew --no-daemon --console=plain campaignDeliveryPilot
# 기본 회귀에는 pilot을 포함하지 않음
./gradlew --no-daemon --console=plain build
```

macOS에서는 필요하면 `caffeinate -i`를 명령 앞에 붙인다. 동일 checkout에서 실험을 중복 실행하지 않는다.
`build/reports/campaign-delivery-pilot/pilot-N.json`과 `build/reports/tests/campaignDeliveryPilot/`에 결과를 남긴다.
같은 대상 수의 JSON은 재실행 시 덮어쓰므로 비교 결과는 `docs/experiments/`에 실행별로 보존한다.

1. 실제 Batch로 캠페인 N건을 인계하고 발송 시도 0을 확인한다. preloadSeconds는 Batch 실행 시간이다.
2. HTTP Mock과 REALTIME 앱을 시작한다. 실시간 50건을 순차 접수하고 DB 완료/offset까지 기다려 단독 구간을 만든다.
3. CAMPAIGN 앱을 시작하고 캠페인이 완료될 때까지 실시간 요청을 계속 접수한다. 혼합 구간 요청이 최소 10개 있어야 한다.
4. campaignDrainSeconds는 CAMPAIGN 앱 시작 직전부터 완료 카운터 N 관측까지다. 앱 시작/샘플링/최대 약 200ms 관측 간격을 포함한다.
   preload, 단독 구간, 최종 전수 대조/offset 대기 시간은 제외한다. N/이 시간은 이 조건의 적체 해소 처리량이다.
5. 실시간 지연은 DB created_at → completed_at이다. HTTP API 응답 지연은 별도 기록한다.
   campaignCreatedToCompletedMs는 캠페인 사전 저장과 단독 구간 대기까지 포함하므로 발송 구간 시간과 다르다.
6. p50/p95/p99는 완료된 해당 구간 전체 표본의 nearest-rank 값이다. 단독 50개에서는 p99가 최대값과 같다.
   생성기는 응답 후 대기하므로 느려지면 제출률도 떨어진다. 일정 도착률의 open-loop 부하나 과부하 SLO 검증으로 사용하지 않는다.

약 2초 간격으로 역할별 lag/미발행 수/가장 오래된 미발행 시간/지표 나이/Worker 처리·대기,
JVM heap·CPU·GC·스레드 및 MySQL cgroup 메모리를 수집한다. 수집 비용은 발송 구간에 포함된다.
앱 지표는 5초마다 갱신하므로 샘플과 실제 시점이 다르다. NaN/null은 0이 아니며 지표 나이를 함께 본다.
MySQL cgroup 메모리는 페이지 캐시 등을 포함하며 프로세스 RSS와 다르다.
주기 관측 최댓값은 순간 최대가 아니다. 최종 lag는 캐시된 지표 대신 Kafka Admin으로 모든 파티션을 직접 대조한다.

## 정합성과 실패 기준

- 네 테이블 notification/delivery_task/outbox_event/delivery_attempt 각각 캠페인+실시간 건수,
  두 역할의 PROVIDER_ACCEPTED 건수, attempt_count=1/성공 이력 1개, REQUESTED/PUBLISHED 행 수를 확인한다.
- HTTP Mock 감사 필터가 최대 8KiB의 요청을 관측한다. 캠페인 대상 번호와 결정적 notificationId를 대조하고,
  실시간 notificationId는 API가 반환한 값과 비교한다. 범위를 넘긴 값·예상 밖 HTTP 상태·중복을 실패 처리한다.
- 캠페인 최대 10,000개 BitSet, 실시간 최대 5,000개 BitSet/ID 배열로 메모리를 제한한다.
  실제 본문·수신자·ID는 결과 JSON에 저장하지 않는다.
- Mock 요청 수=수락 수=고유 캠페인+고유 실시간을 확인한다. 429/500/busy/영구 실패는 0이어야 한다.
- 역할별 Kafka 요청/Retry/DLQ의 end 합계가 예상 건수와 같고, **각 파티션** committed=end인지 확인한다.
- quota ALLOWED도 역할별 기대 건수와 일치해야 한다. Worker 중지, 상한 초과, 정합성 실패는 최종 FAIL이다.
- 이번은 정상 SUCCESS 시나리오의 한 번 수락 대조다. 장애 시 외부 중복 가능성이 없다는 보장은 아니다.

## 실제 결과

환경은 macOS 15.7.7 arm64, Amazon Corretto 21.0.6, Gradle 8.14.5, 호스트 10 logical CPU/16 GiB,
Docker Desktop 10 CPU/약 7.65 GiB다. IDE/기존 로컬 MySQL과 호스트를 공유한다. 날짜는 KST, 원본 JSON 시각은 UTC다.

- 최초 1천 건은 Worker 중지로 실패했다(38초). 중지 지표를 추가한 재현도 실패했다(35초).
  [실패 원본](experiments/s7-delivery-pilot-2026-10-20-1000-pool-failure.json)에는 quotaUnavailable=1,
  quotaRejectedUnavailable=1, admissionFull=0을 기록했다. 이 두 실행의 Redis 연결 상한은 기본 2였다.
- 소비자 3개에 맞춰 **실험 설정의 Redis 연결만 3개**로 바꿨다. 수정 과정에서 수집기의 지표 이름도 실제 이름에 맞췄다.
  원래 Worker의 fail-closed 정책을 유지했다. 실패 원본은 FAIL로 보존한다.
- 최종 1천/1만 건은 각각 1회 실행해 통과했다. 이 소수 실행으로 통계적 성능 차이나 운영 SLO를 주장하지 않는다.

| 최종 실행 | 1천 건 | 1만 건 |
| --- | ---: | ---: |
| 원본 JSON | [1천 결과](experiments/s7-delivery-pilot-2026-10-20-1000.json) | [1만 결과](experiments/s7-delivery-pilot-2026-10-20-10000.json) |
| Batch 사전 저장 | 2.483초 | 16.184초 |
| 발송 구간(앱 시작 포함) | 12.467초 | 115.394초 |
| 캠페인 처리량 | 80.211건/초 | 86.659건/초 |
| 실시간 단독 / 혼합 표본 수 | 50 / 55 | 50 / 514 |
| HTTP 전체 수락 수 | 1105 | 10564 |
| 중복 / 잘못된 매핑 | 0 / 0 | 0 / 0 |
| Gradle 결과 | SUCCESSFUL, 46초 | SUCCESSFUL, 2분 43초 |

1만 건 실행의 실시간 DB 완료 지연과 API 응답 지연은 다음과 같다.

| 측정 | p50 | p95 | p99 | 최대 |
| --- | ---: | ---: | ---: | ---: |
| 단독 DB 완료(50개) | 259.530ms | 479.459ms | 631.212ms | 631.212ms |
| 혼합 DB 완료(514개) | 184.111ms | 485.969ms | 571.243ms | 642.492ms |
| 단독 API 응답(50개) | 12.345ms | 25.188ms | 172.560ms | 172.560ms |
| 혼합 API 응답(514개) | 8.028ms | 25.244ms | 89.306ms | 249.114ms |

단독 표본이 작고 순차 측정/warmup 효과가 있으므로 p50/p99 감소를 캠페인의 개선 효과로 해석하지 않는다.
혼합 부하에서 실시간 요청이 완료되고 그 지연을 실제로 측정했다는 결과다. 고정 지연 SLO나 모든 부하에서의 격리를 보장하지 않는다.

1만 실행에서 53개 샘플을 수집했다. JVM heap 관측 최대 102.37 MiB, MySQL cgroup 메모리 479.14 MiB,
스레드 98개, 첫/마지막 샘플 사이 GC 651ms, JVM 평균 CPU 약 0.440코어 상당이었다.
이 수치는 두 앱·HTTP Mock·감사 도구·생성기를 함께 실행한 JVM 기준이다. 해당 샘플에서 null/NaN은 없었다.

캠페인 lag 관측 최대는 179, 미발행 Outbox는 10000이었다. 실시간은 각각 1/1이었다.
**Kafka lag만 낮아도 DB Outbox에 아직 발행하지 않은 작업이 많이 남을 수 있다.** 두 적체를 함께 관측해야 한다.
최종에는 각 역할의 요청/Retry/DLQ 모든 파티션에서 committed=end였다.
Mock의 429/500/busy/영구 실패는 0, 최대 동시 HTTP 처리는 4였다.
캠페인 10000명과 실시간 564명의 고유 수신을 대조했고, 각 작업의 성공 시도는 1회였다.

운영 코드/기본 정책 변경 없이 사전 도구와 조건을 검증했다. 전체 회귀와 커밋 결과는 [PROGRESS](PROGRESS.md)를 따른다.

## 다음 단계

이 pilot이 통과해도 PLAN의 10만→100만 HTTP Mock 발송 부하는 미완료다.
2026-10-31 [10만 건 발송](S7_DELIVERY_LOAD.md)에서 조회·메모리·시간 상한을 확정하고 전수 대조를 통과했다.
그 결과를 확인한 다음 100만으로 확대한다. 저장/발송 동시 실행, 별도 JVM, 일정 도착률 실시간 부하,
고정 실패율/429/지연 증가 시나리오와 반복 측정은 별도 실험이다.

## 2026-10-31 — 공통 검증 도구 보완

10만 건 [발송 실험](S7_DELIVERY_LOAD.md)을 위해 같은 테스트 메서드에 명시적 load 모드를 추가했다.
pilot 대상 범위1000..10000, 실시간 최대5000, 발송15분/task25분은 유지한다.
당시 공유 메서드의 JUnit 상한은80분으로 늘렸으며 pilot 전체 상한은 더 짧은 Gradle task가 제한했다.
완료 지연 조회는 최대1000행 keyset 페이지와 고정 크기 double 배열을 사용한다.
캠페인의 결정적 ID·대상·업무 참조·성공 이력을1000행 페이지로 전수 대조하고 PK JOIN 계획을 검사한다.
동일한 발송 정책으로1000건 smoke를 실행해1개 테스트 통과, BUILD SUCCESSFUL(52초), 종료 코드0을 확인했다.
사전 저장2.057초/발송11.390초/초당87.797건, 실시간 단독50+혼합51개를 대조했다.
전수 참조1000개, n/t/o/a 모두eq_ref/PRIMARY, 최종 대조0.179초였다.
[원본 JSON](experiments/s7-delivery-pilot-2026-10-31-1000-v2.json)을 보존한다.
기존1만 실행과 다른 시점의 소규모 실행이며 처리량 개선의 증거로 사용하지 않는다.

이후 [100만 건 도구 준비](S7_DELIVERY_MILLION.md)에서 JUnit 상한을 각 Gradle task의 설정으로 분리했다.
현재 pilot은 JUnit20분/task25분, 10만 건은80분/90분, 100만 건은11시간/12시간이다.
100만 건 task는 기본 build에 포함하지 않는다. 2026-11-02 첫 실제 실행은 실험용 접수 probe의 응답 timeout으로 실패했고, 100만 건 발송 검증은 미완료다.
