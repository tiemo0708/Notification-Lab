# S7: 캠페인 Batch의 10만 → 100만 건 인계 실험

2026-10-20. 이 실험은 캠페인 대상별 알림·작업·Outbox·매핑을 실제 MySQL에 저장하는 부하다.
**발송 처리량이나 단말 도착 성능을 측정하지 않는다.** CAMPAIGN 발송 예산 2건/초를 유지하면
100만 건의 공급자 호출에만 약 5.8일이 필요하다. DB 인계와 발송 경로를 구분해 먼저 인계 기준선을 만든다.

## 질문과 실험 조건

대상 수가 10배가 되면 처리 시간, 메모리, DB 사용량은 어떻게 변하는가?
대상 전체를 한꺼번에 메모리에 적재하지 않고도 정확히 N건을 저장하며, 완료 재요청이 중복 인계를 만들지 않는가?

운영 코드를 바꾸지 않고 대상 수만 100,000 → 1,000,000으로 바꾼다.
각 실행은 새 테스트 JVM/새 MySQL/Kafka 컨테이너에서 수행한다. 1,000건 실행은 도구 사전 검증이며
대량 실행 JVM의 warmup이 아니다. 실행 횟수와 실패는 아래 결과에 구분한다. 소수의 개발 환경 측정으로 통계적 성능 우위/운영 SLO를 주장하지 않는다.

| 조건 | 값 |
| --- | --- |
| 대상 | SYNTHETIC_V1, 고정 가상 텍스트, 대상별 결정적 ID |
| 실행 | 실제 CampaignService 생성/시작 + CampaignBatchService |
| chunk / Writer | 100 / 단일 스레드, 기존 세션 소유권 방식 |
| JVM | Java 21, 최대 heap 512 MiB |
| MySQL | 8.4.8, CPU 상한 2코어, 메모리 상한 2 GiB, buffer pool 512 MiB |
| 내구성 설정 | innodb_flush_log_at_trx_commit=1, sync_binlog=1 (실행 시 실제 값 기록) |
| Hikari | 최대 4 + Batch 전용 연결 1개 |
| Kafka | 4.0.2, CPU 상한 1코어, 메모리 상한 1 GiB, heap 256~512 MiB |
| Relay / Worker | 모두 비활성, Kafka 레코드/공급자 시도 0 검증 |
| Redis | 미기동. Worker를 실행하지 않아 공유 제한기를 호출하지 않음 |
| 관측 | 기존 PipelineMetrics 5초 수집 유지 + 실험 자원 샘플 약 5초 간격 |
| 종료 상한 | 테스트 110분, Gradle task 2시간 |

기존 로컬 DB·Redis 예산·.env는 사용하거나 수정하지 않는다. 컨테이너 포트는 루프백에만 바인딩한다.
Testcontainers가 실험 DB를 새로 생성하고 종료 후 제거한다. 남기는 것은 집계/자원 샘플 JSON이다.
테스트 프로세스 외에 IDE·Docker Desktop·기존 로컬 MySQL이 있는 개발용 호스트이므로 독점 벤치마크 환경이 아니다.

## 재현 명령

```bash
# 기본 회귀: 대량 실험은 포함하지 않음
./gradlew --no-daemon --console=plain test

# 별도 실행. 같은 입력으로 다시 실행해도 UP-TO-DATE 처리하지 않음
./gradlew --no-daemon --console=plain campaignLoadTest -PcampaignTargets=1000
./gradlew --no-daemon --console=plain campaignLoadTest -PcampaignTargets=100000
./gradlew --no-daemon --console=plain campaignLoadTest -PcampaignTargets=1000000
```

campaignTargets의 기본은 100000, 허용 범위는 1~1000000이다. 결과는
`build/reports/campaign-load/handoff-대상수.json`, JUnit 결과는 `build/test-results/campaignLoadTest/`에 남는다.
같은 크기의 재실행은 해당 build JSON을 덮어쓰므로 비교할 결과는 실행별 이름으로 따로 보존한다.
같은 저장소에서 이 task를 중복 실행하지 않는다. JUnit 출력 경로와 호스트 자원을 공유한다.
RSS 수집에는 macOS/Linux의 ps를, 컨테이너 자원 수집에는 cgroup v2를 사용한다.
macOS에서는 필요하면 명령 앞에 `caffeinate -i`를 붙여 실험 중 유휴 절전을 방지한다.

## 측정과 정합성 확인

- elapsedSeconds는 첫 자원 샘플부터 Batch execute 반환까지의 시간이다. 컨테이너/앱 부팅, 캠페인 생성/시작,
  이후 전체 데이터 대조와 완료 재요청 시간은 제외한다. 샘플 수집 비용과 기존 지표 수집 부하는 포함한다.
- targetsPerSecond는 N / elapsedSeconds다. 공급자 호출/수락 처리량과 다르다.
- heap 사용량, 프로세스 RSS, JVM 누적 CPU·GC 횟수/시간·스레드 수, MySQL cgroup 메모리/CPU와 마지막 커밋 순번을 기록한다.
  주기 샘플의 최댓값은 관측 최댓값이며 순간 최대를 보장하지 않는다. GC를 인위적으로 강제하지 않는다.
- MySQL memory.current는 프로세스 RSS와 달리 페이지 캐시 등을 포함하는 cgroup 사용량이다.
  [Linux cgroup v2 정의](https://docs.kernel.org/admin-guide/cgroup-v2.html)를 따른다. unavailable/sampleError는 0으로 간주하지 않는다.
- 실험 전후 MySQL 연결 수와 InnoDB 삽입/데이터 쓰기/redo 쓰기 누적값도 기록한다. 스키마 초기화/검증 비용과 구분한다.
- 네 업무 테이블 각각 N행, notification QUEUED=N, Outbox PENDING=N, delivery_attempt=0을 검증한다.
- 모든 매핑을 keyset으로 최대 1,000행씩 읽고 순번 1..N 연속성, 결정적 ID 3개, 수신자, 작업/이벤트의 알림 참조,
  이벤트 payload의 taskId를 대조한다. 표본만 검사하지 않는다. FK와 유니크 제약도 그대로 유지한다.
- 검증 SQL은 먼저 최대 1,000행을 materialize하고 기본 키로 관련 행을 조회한다.
  실행 계획의 세 JOIN이 모두 eq_ref/PRIMARY인지도 확인한다. 이 제한은 검증 도구에만 적용한다.
- Batch read/write/checkpoint=N을 확인한다. 완료 재요청 후 같은 executionId와 N행, JobExecution 1개를 다시 확인한다.
- Kafka 레코드 0을 확인한다. final result=PASS는 위 검증까지 모두 통과했을 때만 기록한다.

## 학습: 어떤 문제에 어떤 선택을 했나

| 문제 | 선택과 이유 | 비용과 남은 한계 |
| --- | --- | --- |
| 100만 대상 전체를 메모리에 올리면 실행 크기에 따라 heap 요구량이 커짐 | 대상 번호를 keyset으로 읽고 chunk 100씩 커밋 | 요청/SQL 왕복이 많고 chunk 크기에 따른 성능은 별도 실험 필요 |
| DB 저장과 외부 발송을 한 번에 측정하면 병목 구분이 어려움 | 먼저 실제 Outbox 인계를 측정하고 발송은 분리 | 이 결과로 공급자 수락/단말 도착 처리량을 설명할 수 없음 |
| 행 수만 같아도 다른 대상 또는 잘못된 참조가 들어갈 수 있음 | 1..N 순번, 결정적 ID와 참조를 전수 대조 | 검증에도 DB I/O가 들므로 저장 시간과 별도 기록 |
| 검증 JOIN이 큰 테이블을 읽으며 시간 초과 | 최대 1,000개 매핑을 먼저 고정하고 기본 키 lookup, 실행 계획 검사 | MySQL에 의존하는 검증 SQL이며 버전 변경 시 계획을 다시 확인해야 함 |
| 한 번 빠르게 끝난 측정을 개선 효과로 오해할 수 있음 | 실패/재실행을 보존하고 측정 경계와 환경을 함께 기록 | 통계적 결론에는 같은 환경에서 추가 반복과 원인별 실험 필요 |

이번 단계의 설명 포인트는 “Spring Batch를 썼다”보다 “어디까지 커밋됐는지 확인하면서 큰 집합을 유한 메모리로 인계하고,
전체 결과를 별도 검증했다”다. Batch 완료와 발송 완료는 서로 다른 상태다.

## 실제 결과

환경은 macOS 15.7.7 arm64, Amazon Corretto 21.0.6, Gradle 8.14.5, 호스트 10 logical CPU/16 GiB,
Docker Desktop 10 CPU/약 7.65 GiB다. 아래 시각/날짜는 KST 기준이며 JSON 타임스탬프는 UTC다.

| 실행 | DB 인계 시간 | 대상/초 | 전체 정합성 검사 | 최종 결과 |
| --- | ---: | ---: | ---: | --- |
| [10만 최초](experiments/s7-campaign-handoff-2026-10-20-100k.json) | 198.029초 | 504.976 | 34.994초 | PASS |
| [100만 최초](experiments/s7-campaign-handoff-2026-10-20-1m-verification-timeout.json) | 2779.705초 | 359.750 | 시간 초과 | **FAIL** |
| [10만 최종 검증 SQL](experiments/s7-campaign-handoff-2026-10-20-100k-v3.json) | 180.395초 | 554.339 | 1.654초 | PASS |
| [100만 최종 검증 SQL](experiments/s7-campaign-handoff-2026-10-20-1m-v3.json) | 2545.391초 | 392.867 | 67.739초 | PASS |

각 크기는 2회 실행했다. 최종 검증 SQL은 `bounded-primary-key-page-v3`이며, 변경 범위는 저장 시간 측정 이후의 검증 쿼리다.
최초 100만 건은 Batch COMPLETED였으나 후속 매핑 JOIN이 socket timeout으로 실패했다.
저장 완료만으로 전체 실험 통과를 인정하지 않고 실패 원본을 보존했다. 해당 실패 실행의 EXPLAIN은 수집하지 못했다.
소규모 재현에서 관련 테이블 행 수를 1로 추정한 hash join/full scan을 확인한 뒤, 제한된 페이지와 PK lookup을 사용했다.
최종 10만/100만에서는 n/t/o 모두 eq_ref/PRIMARY, campaign_target은 PRIMARY range로 조회되는 것을 검증했다.
검증 쿼리 수정 전후의 저장 시간 차이는 운영 코드 개선 효과로 해석하지 않는다.

최종 동일 검증 SQL 실행을 비교하면 대상 수 10배에 인계 시간이 약 14.11배,
평균 처리량은 약 29.1% 낮아졌다. 개발 호스트의 단일 측정 비교이며 원인을 하나로 확정하지 않는다.

| 최종 실행의 관측 지표 | 10만 | 100만 |
| --- | ---: | ---: |
| JVM heap 관측 최대 | 63.97 MiB | 64.03 MiB |
| JVM RSS 관측 최대 | 271.47 MiB | 269.34 MiB |
| MySQL cgroup 메모리 관측 최대 | 711.19 MiB | 2047.95 MiB |
| JVM 평균 CPU 사용량(코어 상당) | 0.219 | 0.195 |
| MySQL 평균 CPU 사용량(코어 상당) | 0.434 | 0.413 |
| JVM GC 누적 시간 차이 | 788 ms | 9618 ms |
| 자원 샘플 / 오류 / unavailable | 37 / 0 / 0 | 502 / 0 / 0 |
| Batch commit / rollback | 1001 / 0 | 10001 / 0 |
| 완료 재요청 시간 | 0.057초 | 0.076초 |

평균 CPU는 첫/마지막 샘플의 누적 CPU 시간 차이를 해당 경과 시간으로 나눈 값이다. 1.0은 평균 한 코어에 해당한다.
MySQL cgroup 사용량은 100만 실행에서 2 GiB 상한에 근접했다. JVM heap/RSS가 대상 수에 비례해 증가하는 현상은
이 샘플에서 관측되지 않았지만, 이 사실만으로 모든 실행 규모의 메모리 상한이나 병목 원인을 보장하지 않는다.
InnoDB 누적 data_written 차이는 각각 약 7.50 GiB/146.37 GiB이며 DB 파일의 최종 크기와 다르다.

최종 두 실행 모두 네 업무 테이블 각각 N행, read/write/checkpoint=N, 순번 1..N과 모든 매핑 참조를 확인했다.
완료 재요청은 같은 executionId를 반환하고 JobExecution 1개/업무 N행을 유지했다. Kafka 레코드와 공급자 시도는 0건이다.
Gradle 결과는 10만 BUILD SUCCESSFUL(3분 20초), 100만 BUILD SUCCESSFUL(44분 1초), 각각 테스트 1개 통과다.
사전/중간 smoke와 전체 회귀 결과는 [PROGRESS](PROGRESS.md)에 별도 기록한다.

## 해석과 다음 단계

전체 인계가 완료돼도 Outbox에는 N건이 남아 있으며 발송된 것은 아니다.
가상 대상 계산은 실제 회원 테이블 조회 비용을 포함하지 않는다. 처리 시간에는 대상별 조회/INSERT와 인덱스/FK/redo 비용이 포함된다.
이번에는 chunk 크기, JDBC batching, 파티션 수, Worker 수, 발송 예산을 동시에 바꾸지 않는다.

후속 발송 부하는 별도 HTTP Mock과 명시적인 실험 예산으로 설계한다. 그 단계에서 공급자 수락·lag·완료 지연과
REALTIME 간섭을 측정해야 한다. 현재 결과만으로 100만 건 발송, exactly-once, 운영 성능을 주장하지 않는다.
