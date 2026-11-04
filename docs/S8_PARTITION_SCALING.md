# S8 1만 건 HTTP Mock의 캠페인 파티션 3/6 비교

2026-11-04 KST. 기준 커밋 9445fbd. [파티션 변경 계약](S8_PARTITION_EXPANSION.md)과
[12개 복구 이력 오류 경계](S8_PARTITION_RECOVERY.md) 검증 뒤, 실제 발송 경로에서 처리량과 실시간 접수 지연을 비교한다.
기본 애플리케이션 Java/resources와 토픽 선언은 변경하지 않는다. 테스트 전용 task와 준비된 새 토픽을 사용한다.

## 비교 프로토콜

| 조건 | 값 |
| --- | --- |
| 변경 요인 | CAMPAIGN 요청·Retry 토픽별 파티션 3 또는 6 |
| 반복 | 3 → 6 → 6 → 3, 조건별 2회 |
| 데이터 | 실행마다 캠페인 10000건, 새 MySQL/Kafka/Redis |
| REALTIME 토픽 / 양쪽 DLQ | 각각 3으로 고정 |
| Consumer / Worker 활성 상한 | 양쪽 모두 3/3 |
| 복구 | ON, 양쪽 이력 용량 12로 고정 |
| 역할별 DB / Redis / HTTP 연결 상한 | 4 / 3 / 3 |
| Redis timeout / HTTP 응답 제한 | 250ms / 5초 |
| Relay / 완료 저장 / offset | 1 / 1건 / 동기 RECORD |
| 전체 / 캠페인 quota | 1000/s·burst100 / 200/s·burst20 |
| Mock / 실시간 probe | SUCCESS·1ms / 순차 200ms 간격, 기준선 50건 |
| JVM / 인프라 | heap 512MiB, MySQL CPU2·2048MiB·buffer pool512MiB, Kafka CPU1·1024MiB, Redis256MiB |

Amazon Corretto 21.0.6, macOS 15.7.7 aarch64(보고된 processor 10), MySQL 8.4.8/Kafka 4.0.2/Redis 7.4.11이다.
컨테이너 자원 제한과 공유 호스트의 가용 CPU·I/O는 서로 구분한다.

데이터의 제목·본문 길이·대상 번호·개수는 같지만 캠페인/알림의 합성 UUID는 실행별로 다르다.
같은 key를 고정해 전체 실행을 재현한 것이 아니며 파티션별 레코드 분포도 함께 본다.

## 토픽과 실제 설정 확인

새 브로커에 테스트용 `partition-pilot.*` 요청·Retry·DLQ 토픽을 두 역할로 분리해 **어떤 앱 context보다 먼저** 만든다.
요청/Retry는 캠페인만 선택한 N이고, 나머지는 3이다. 모든 초기 end offset=0을 검증한다.
그 뒤 사전 Batch 저장, 실시간 앱과 기준선, 캠페인 앱을 차례로 실행한다.
따라서 발송 중 3→6 변경이나 이전 offset 초기화가 성능 실험에 섞이지 않는다.

Spring Kafka 3.3.16의 [KafkaAdmin 원본](https://github.com/spring-projects/spring-kafka/blob/v3.3.16/spring-kafka/src/main/java/org/springframework/kafka/core/KafkaAdmin.java)은
이미 존재하는 토픽이 선언보다 많은 파티션을 가지면 그 수를 줄이지 않는다.
앱 선언은 3을 유지하며, 각 역할의 실제 broker metadata와 Consumer 할당을 다시 확인한다.

- REALTIME: 요청 3/Retry 3/DLQ 3, Consumer별 할당 [2,2,2]
- CAMPAIGN N=3: 요청 3/Retry 3/DLQ 3, Consumer별 할당 [2,2,2]
- CAMPAIGN N=6: 요청 6/Retry 6/DLQ 3, Consumer별 할당 [4,4,4]

양쪽 실제 Consumer/child 수와 Worker 상한 3, RECORD·동기 commit, 복구 용량 12를 읽어 검증한다.
파티션 6이 Consumer 6이나 동시 호출 6을 뜻하지 않는다.

## 판정과 측정 범위

모든 실행은 HTTP 요청·수락/DB 완료·시도/10000개 대상 매핑/모든 파티션의 committed=end 대조와
중복·잘못된 참조 0을 통과해야 한다. 전체 합계만으로 전수 정합성을 대신하지 않는다.
요청과 Retry 파티션은 늘어나지만 SUCCESS 조건이므로 실제 Retry/DLQ 부하는 없다.
해당 오류 경계는 앞선 별도 검증이며, 이번 정상 경로 처리량으로 모든 장애 조합을 보장하지 않는다.

처리량은 캠페인 앱 기동부터 마지막 DB 완료 관찰까지의 캠페인 10000건/경과 시간이다.
사전 저장·실시간 기준선·최종 전수 검증은 분모에서 제외한다.
API p95/p99/최대는 혼합 구간의 접수 지연이며 공급자 수락까지의 지연과 구분한다.
실시간 수락 합계에는 실행별 기준선 50건이 포함된다.

순차 probe는 요청 완료 후 200ms를 기다리므로 정확한 5req/s나 open-loop 부하가 아니다.
긴 실행일수록 실시간 요청이 많아진다. 두 역할과 Mock은 같은 JVM이고 인프라/호스트도 공유한다.
CPU는 표본 구간의 프로세스 또는 컨테이너 전체 누적 CPU 시간/경과 시간이다(100%=한 코어).
2초 표본의 lag/heap/pool 최댓값은 관측 최대이며 순간 최고치가 아니다. lag gauge의 나이도 확인한다.

조건별 2회/공유 호스트/합성 UUID 분포이므로 통계적 유의성이나 운영 최적값을 확정하지 않는다.
[이전 Consumer 비교](S8_CONSUMER_SCALING.md)와도 프로토콜/시점이 다르므로 직접 개선율로 비교하지 않는다.
Mock 수락은 실제 단말 도착이 아니다.

## 명령과 경계

```bash
./gradlew --no-daemon --console=plain partitionScalingPilot -PcampaignPartitions=3
./gradlew --no-daemon --console=plain partitionScalingPilot -PcampaignPartitions=6
```

명령을 3·6·6·3 순서로 실행한다. task는 명시적으로만 실행하고 기본 test/check/build에서는 제외한다.
출력은 `build/reports/partition-scaling/partitions-N-*/result.json`으로 매번 구분한다.
대상 10000건, 파티션 3/6만 허용하고 다른 Consumer/Relay/offset/API 계측/실패 주입과의 혼용을 거절한다.
Gradle task 10분/JUnit 8분(기존 drain900초보다 짧은 제한 우선), probe5000/샘플452/검증 페이지1000행 상한이다.

경계/진단 집중 테스트 16개는 10초에 통과했다. `campaignPartitions=12 --dry-run`은 실행 전에 거절됐다.
[고정 소스·반복 순서·설정](experiments/s8-partition-scaling-2026-11-04-group-1-source.json)을 보존한다.

## 실제 결과

소스 162개를 고정한 채 4회 모두 통과했다. 캠페인 **40000건 + 실시간 2938건 = HTTP 수락 42938건**이며,
각 실행의 HTTP/DB 완료·시도/대상 매핑/전체 파티션 offset을 대조했다. 관측 중복·잘못된 참조는 0이다.
각 요청은 한 번 시도했고 Retry/DLQ 발행은 0이다. 6개 조건은 실제 요청·Retry 6+6, Consumer별 [4,4,4] 할당이었다.

| 순서 | 파티션 N | 사전 저장 초 | 발송 구간 초 | 캠페인 건/초 | 실시간 수락(기준선 포함) | 혼합 API p95 / p99 / 최대 ms |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 3 | 21.938 | 152.650 | 65.509 | 689 | 37.564 / 153.201 / 2480.592 |
| 2 | 6 | 21.379 | 143.447 | 69.712 | 659 | 28.868 / 277.996 / 1034.432 |
| 3 | 6 | 21.030 | 149.095 | 67.071 | 681 | 31.747 / 490.614 / 1002.142 |
| 4 | 3 | 27.632 | 217.442 | 45.989 | 909 | 84.348 / 339.049 / 1752.210 |

| 조건 | 처리량 산술평균 건/초 | 관측 범위 건/초 | 혼합 API p95 범위 ms |
| --- | --- | --- | --- |
| N=3, 2회 | 55.749 | 45.989~65.509 | 37.564~84.348 |
| N=6, 2회 | 68.392 | 67.071~69.712 | 28.868~31.747 |

평균은 각 실행의 `10000 / 발송 구간 초`를 산술평균한 값이다. 6개/3개 평균 차이는 **+22.68%**지만,
동일한 3개 조건의 첫/마지막 처리량도 65.509→45.989로 크게 달랐다. 마지막 실행은 측정 전 사전 저장도
27.632초로 길었고, 호스트 부하·I/O·스케줄링을 분리해 통제하지 않았다. 느린 실행을 제외하거나 다시 골라서 평균을 만들지 않았다.
따라서 이 차이를 파티션 증가만의 인과 효과나 재현 가능한 개선율로 확정하지 않는다.
6개 조건에서 p95는 낮았지만 p99는 첫 3개 실행보다 높았다. 모든 지연이 개선됐다고도 할 수 없다.

### 자원과 분포

CPU는 한 코어를 100%로 환산한다. MySQL은 2코어 제한이므로 77.7%만으로 CPU 포화를 주장하지 않는다.

| 순서 | JVM CPU % | MySQL CPU % | 캠페인 lag 관측 최대 | 캠페인 DB active / 대기 관측 최대 | GC 증가 ms |
| --- | --- | --- | --- | --- | --- |
| 1 | 41.3 | 58.5 | 96 | 4 / 0 | 816 |
| 2 | 43.1 | 63.3 | 54 | 4 / 0 | 762 |
| 3 | 42.3 | 60.8 | 59 | 4 / 0 | 884 |
| 4 | 38.6 | 77.7 | 83 | 4 / 1 | 1990 |

실시간 lag 관측 최대는 모두 2, DB active 최대는 모두 3/상한 4, 대기는 모두 0이었다.
lag gauge의 나이 관측 최대는 4.931~5.536초다. 마지막 실행에서 MySQL CPU·GC 누적 시간과 DB 대기가 더 관측됐지만,
2초 표본과 전체 CPU만으로 특정 SQL/lock/외부 I/O를 원인으로 확정할 수 없다.
Redis quota 오류 6종은 두 역할·모든 실행에서 0으로 복구 동작의 효과를 비교한 실험도 아니다.
heap 관측 최대는 104.3~105.0MiB였으며 이 표본만으로 순간 메모리 최고치를 보장하지 않는다.

기존 구간 타이머에서도 마지막 실행의 증가가 여러 단계에 걸쳐 관측됐다(각 count=10000, relay.claim만 빈 조회 포함).

| 순서 | Relay 선점 평균 ms | ACK 평균 ms | PUBLISHED 저장 평균 ms | Worker handler 평균 ms | Worker 공급자 평균 ms |
| --- | --- | --- | --- | --- | --- |
| 1 | 7.171 | 1.230 | 4.584 | 27.792 | 2.032 |
| 2 | 6.895 | 1.267 | 3.909 | 25.603 | 2.013 |
| 3 | 7.247 | 1.158 | 4.325 | 26.555 | 1.933 |
| 4 | 11.126 | 2.033 | 6.424 | 40.452 | 2.157 |

Worker 최초 선점/허가 후 선점/완료 저장 평균도 1회 5.133/11.960/7.802ms에서 4회 8.004/16.998/12.110ms로 늘었다.
handler는 내부 단계와 겹치며 Worker 3개의 시간도 서로 겹치므로 이 합계를 벽시계 시간에 더하지 않는다.
이 타이머에는 DB·네트워크·스케줄링 등이 포함돼 있으므로 SQL 실행 시간만의 증가로 해석하지 않는다.
Mock 공급자 단계의 작은 차이만으로 마지막 실행의 전체 저하를 설명하기도 어렵다. 추가 원인 진단이 필요하다면 별도 고정 조건으로 수행한다.

캠페인 요청 파티션별 기록 수는 순서대로 아래와 같다. 모두 합계 10000이며 심한 key 쏠림은 관측하지 않았다.

- 1회 N=3: [3335, 3305, 3360]
- 2회 N=6: [1624, 1666, 1712, 1675, 1649, 1674]
- 3회 N=6: [1603, 1668, 1706, 1679, 1629, 1715]
- 4회 N=3: [3366, 3336, 3298]

파티션을 늘려도 Consumer/Worker 활성 상한이 3이므로 동시 발송 자원은 늘지 않는다.
이번에는 해당 설정에서 6개 토픽 구성이 정상 처리되는 점을 검증했고, 운영 기본 파티션은 3으로 유지한다.
다음 독립 항목은 producer batching/압축의 적용 경계와 비교 설계다. 이 결과만으로 producer나 운영 설정을 바꾸지 않는다.

### 보존한 근거와 회귀

- [1회 N=3 원본](experiments/s8-partition-scaling-2026-11-04-group-1-run-1-partitions-3.json)
- [2회 N=6 원본](experiments/s8-partition-scaling-2026-11-04-group-1-run-2-partitions-6.json)
- [3회 N=6 원본](experiments/s8-partition-scaling-2026-11-04-group-1-run-3-partitions-6.json)
- [4회 N=3 원본](experiments/s8-partition-scaling-2026-11-04-group-1-run-4-partitions-3.json)
- [원본 SHA·전수 대조·실행별 지표·요약](experiments/s8-partition-scaling-2026-11-04-group-1-summary.json)

측정 그룹 종료 뒤 전체 `caffeinate -i ./gradlew --no-daemon --console=plain build`를 별도로 실행했다.
**20분 6초/종료코드 0**, 기본 test 새 실행 **46개 클래스/363개 통과, 실패·오류·스킵 0**이었다.
이전 361개에 경계 검증 2개가 추가됐다. 콘솔과 새 JUnit XML의 총수를 대조했고 WorkerConcurrencyTest 11개도 모두 통과했다.
`bootJar`는 UP-TO-DATE로, 이번에 새 JAR를 생성했다고 표현하지 않는다.
새 opt-in 비교는 위 4회이며, 기존 Consumer/Relay/100만 건 opt-in 실험은 이번 회귀에서 재실행하지 않았다.
고정 소스 162개 불변, 운영 소스 diff 0, 원본 SHA/민감 필드·비유한값/문서 링크를 확인했다.
[최종 로컬 검증과 콘솔·JUnit SHA](experiments/s8-partition-scaling-2026-11-04-group-1-validation.json)를 남겼다.

원격 main은 확인 시점 c6cccc1이며 해당 기존 CI는 실패 상태다. 이전 실패의 fixture 수정은 로컬 회귀에서 통과했지만,
이번 변경을 푸시하거나 수정 후 원격 CI가 성공했다고 주장하지 않는다. 사용자 검토 후 별도 푸시가 필요하다.
