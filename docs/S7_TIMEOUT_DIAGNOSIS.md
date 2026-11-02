# S7 — 접수 응답 timeout의 작은 진단

## 목적과 판정

2026-11-02 첫100만 건 실행은 실시간 접수 클라이언트가5초 안에 응답을 관측하지 못해 실패했다.
[첫 실패 기록](S7_DELIVERY_MILLION.md)을 보존한다. 서버 접수 여부와 원인은 아직 미확정이다.
이번 단계는 같은 장애가 작은 규모에서도 나타나는지, Outbox SQL이 어떤 인덱스와 비용을 갖는지 확인한다.
운영 코드·인덱스·API 대기 제한·발송 예산을 변경하지 않는다.100만 건 장시간 실행은 반복하지 않는다.

## 두 실험의 경계

### 기존1만 건 HTTP 경로

```bash
caffeinate -i ./gradlew --no-daemon --console=plain campaignDeliveryPilot -PdeliveryTargets=10000
```

기존 Batch 사전 저장, 캠페인/실시간 앱, Kafka, Redis, HTTP Mock을 사용한다.
앞선 커밋05a94c8의 Hikari 및 cgroup 진단을 포함한다.
단독50개와 닫힌 루프 실시간 probe, 동일 JVM/개발 호스트라는 한계는 유지한다.
성공하더라도 실패했던100만 건의 원인이 해결됐거나 장시간 안정성이 검증됐다는 뜻은 아니다.

### 격리 MySQL 쿼리 진단

```bash
caffeinate -i ./gradlew --no-daemon --console=plain outboxQueryProbe
```

- 별도 opt-in task다. 기본 test/check/build에는 포함하지 않는다. JUnit8분/task10분 상한을 둔다.
  결과 JSON은 `build/reports/outbox-query-probe/result.json`, JUnit은 `build/test-results/outboxQueryProbe/`에 쓴다.
- MySQL8.4.8/2CPU/2GiB/buffer pool512MiB, JVM512MiB, 역할별 Hikari4/연결 대기3초를 사용한다.
- 가상 notification/delivery_task/outbox_event를 SQL로1000행씩 커밋해1만→10만 행을 만든다.
  각 행의 참조와 이벤트 구조를 구성하지만 campaign_target/Batch 실행 비용을 재현하지 않는다.
- 실제 OutboxStore.claim/published를 호출한다. Spring 프록시 대신 TransactionTemplate으로
  선점과 상태 기록의 두 트랜잭션을 분리한다. Kafka를 사용하지 않으므로 PUBLISHED는 DB 진단용 상태 변경일 뿐이다.
  이 task의 성공을 발송·Kafka ACK·공급자 수락의 증거로 사용하지 않는다.
- 각 크기에서 캠페인/실시간 순으로 집계20회, 선점 및 상태 기록20회를 측정한다.
  선점 코드가 실제 실행한 SQL을 캡처하고 타이밍 측정 뒤 EXPLAIN한다.
  집계는 PipelineMetrics.refresh의 SQL을 별도 복사한 것이므로 운영 쿼리가 바뀌면 함께 확인해야 한다.
- 이후3개 스레드에서 캠페인 선점100회, 집계10회, 실제 SubmissionService 접수40회를 겹친다.
  접수는 한 번씩 완료 후50ms 대기, 집계는200ms 대기다. 집계 주기는 운영5초보다 촘촘하다.
- JDBC 쿼리 상한30초/각 Future 대기60초를 둔다. API/HTTP5초 제한을 재현하는 실험은 아니다.
- SQL UUID()와 동시각 중심 사전 저장, 작은 표본, 고정 실행 순서/따뜻한 캐시, Worker DB 처리 부재가 있다.
  실제 캠페인의 결정적 ID·43분 저장 분포·큰 데이터의 버퍼 풀 교체·장시간 flush를 동일하게 만들지 않는다.
- 원본에는 식별자·수신자·본문·인증정보 대신 설정/실행 계획/집계/지연만 저장한다.

## 실제 결과

### 1만 건 HTTP 경로: 성공, 기존 timeout 미재현

- BUILD SUCCESSFUL2분53초, 종료 코드0, 별도 JUnit1개 통과/실패·오류·스킵0.
- 사전 저장24.895초, 캠페인 발송109.671초,91.182건/초.1만 건+실시간544건=HTTP10544건을 전수 대조했다.
- HTTP 감사 중복·잘못된 매핑0, 최종 DB 참조/시도 이력/offset/quota 일치.
- 실시간 **API 응답** p95: 단독22.901ms/혼합24.756ms, 혼합 최대112.488ms(494개).
- 실시간 **DB 완료** p95: 단독498.399ms/혼합491.728ms. 다른 측정 경계이며 성능 개선으로 해석하지 않는다.
- 자원51개 샘플에서 Hikari 대기 최대는 두 역할 모두0, active 최대 캠페인4/실시간2였다.
  순간 관측이므로 사이에 있었을 대기를 배제하지 않는다.
- MySQL cgroup 메모리 관측 최대477.40MiB, 관측 시작~끝 oom/oom_kill/max와 CPU throttle 카운터는0이었다.
  이1만 건 실행의 자료이며 앞선100만 건 실행에 소급하지 않는다.
- [HTTP 원본](experiments/s7-timeout-diagnosis-2026-11-02-http-10000.json)을 그대로 보존했다.
  단일 개발 호스트 실험이며 DB 진단 도구는 이 실행 종료 후 시작했다.
  HTTP 실행 중 SQL 도구의 compileTestJava를 병행했고10초 후 컴파일 오류로 종료됐다.
  추가 CPU 부하가 있는 관측이므로 다른 회차와의 성능 개선 비교에 사용하지 않는다.

### DB 쿼리 진단

BUILD SUCCESSFUL32초, 종료 코드0, 별도 JUnit1개 통과/실패·오류·스킵0.
아래는 각 작업20회 중 p95다. 선점+상태 기록은 두 트랜잭션의 합이고 Kafka 대기는 포함하지 않는다.

| 캠페인 누적 행 | 캠페인 집계 | 실시간 집계 | 캠페인 선점+기록 | 실시간 선점+기록 | 겹친 접수 DB 최대(40개) |
| --- | ---: | ---: | ---: | ---: | ---: |
| 10000 | 8.833ms | 17.291ms | 7.913ms | 24.959ms | 16.983ms |
| 100000 | 96.295ms | 79.304ms | 9.163ms | 18.893ms | 11.832ms |

- 초기1만 행은 모두 PENDING,10만 행 시작은 PENDING99880/PUBLISHED120이었다.
  이전 단계의 선점·상태 변경을 유지한다. 실시간 접수는40→80개로 누적했다.
- 집계와 실제 선점의 PENDING/PROCESSING 쿼리 모두 **idx_outbox_status_available**을 선택했다.
  역할별 **idx_outbox_traffic_status_available**이 존재하지만 이번 계획에서 선택되지 않았다.
  Extra는 Using index condition; Using where였고 이 계획에는 filesort가 없었다.
- EXPLAIN의 rows 추정은 집계2/선점1이었다. 특히 캠페인 집계의2는 실제 약10만 행 적체와 크게 달랐다.
  이는 실제 읽은 행 수가 아니라 옵티마이저 추정이며, 선점 LIMIT1도 읽는 행 수1을 보장하는 자료가 아니다.
  빠른 SQL 사전 저장 직후 persistent 통계의 상태가 영향을 줬을 가능성이 있다.
  ANALYZE TABLE이나 FORCE INDEX를 이번 실행에 적용하지 않았다.
- 캠페인 행이 늘 때 실시간 집계 비용도 늘어난 것은 후속 확인할 단서다.
  계획과 지연만으로 정확한 스캔 행 수, lock wait,원래5초 timeout의 원인을 확정하지 않는다.
- 겹친 실시간 접수 DB 트랜잭션에서도 초 단위 지연은 재현되지 않았다.
  HTTP 대기 시간과 측정 경계가 다르고 Worker의 DB 부하/장시간 실행이 없다는 한계가 있다.
- [DB 진단 원본](experiments/s7-timeout-diagnosis-2026-11-02-outbox-query.json)을 보존했다.
  초기 컴파일은 AssertJ/TransactionTemplate 제네릭 추론 충돌로10초 만에 실패했다.
  Boolean 지역 변수로 타입을 명시한 뒤 위 task의 컴파일·실행은 성공했다.

## 판단과 다음 단계

이번에는1만 건 HTTP와1만/10만 행 DB에서 이전 timeout을 재현하지 못했다.
작은 실험의 정상 동작과100만 건 실험의 실패를 함께 보존한다. S7의100만 건 항목은 계속 미완료다.

다음 작은 비교는 같은 적체에서 **ANALYZE TABLE 전후** 실행 계획·실제 읽은 행 수·지연을 확인한다.
통계 갱신만으로 달라지는지 먼저 보고, 필요하면 진단용 인덱스 힌트를 별도 조건으로 비교한다.
운영 인덱스 강제나 timeout 확대를 먼저 적용하지 않는다. 실제1m 실행의 당시 계획·통계는 없어
후속 재현 결과만으로 과거 원인을 소급해 확정할 수 없다.

기본 `build`는33개 클래스/268개 통과, 실패·오류·스킵0으로 성공했다(12분21초, 종료 코드0).
별도 HTTP 실험1개와 SQL 진단1개는268개에 포함되지 않는다.
새 SQL 진단과100만 건 실험은 기본 build에서 실행되지 않았다. 운영 실행 JAR 생성은 UP-TO-DATE였다.
