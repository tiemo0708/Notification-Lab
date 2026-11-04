# S8 캠페인 동시처리수 1·2·3 비교

2026-11-04 KST. 시작 커밋 f8bf1c0/main. [100만 건 성공 기준선](S7_DELIVERY_MILLION_SUCCESS.md)을 보존한 뒤
1만 건으로 캠페인 처리 동시성을 비교했다. **6회 모두 PASS**이며 운영 Java/resources와 기본 설정은 그대로다.

## 비교 대상과 고정 조건

처음에는 Consumer만 1로 낮추고 Worker maxActive=3을 유지하려 했으나, 기존 WorkerProperties의
`maxActive <= consumers` 검증에 걸려 27초에 시작 실패했다. 그룹 1의 유효한 성능 표본은 0개다.
[초기 소스](experiments/s8-consumer-scaling-2026-11-04-group-1-source.json),
[실패 기록](experiments/s8-consumer-scaling-2026-11-04-group-1-initial-failure.json),
[설정 검증](experiments/s8-consumer-scaling-2026-11-04-group-1-initial-verification.json)을 보존했다.

운영 검증을 완화하는 대신 캠페인 동시처리수 N에 Consumer=N/Worker maxActive=N을 함께 맞췄다.
두 설정 각각의 효과는 분리하지 않는다. 유효한 6개 표본은 수정한 동일 소스의 그룹 2에서 새로 수집했다.

| 조건 | 값 |
| --- | --- |
| 캠페인 Consumer / Worker 활성 상한 | 1/1, 2/2, 3/3 |
| 실시간 Consumer / Worker 활성 상한 | 모든 조건 3/3 |
| 데이터 | 실행마다 캠페인 10000건, 새 MySQL/Kafka/Redis |
| 반복 순서 | 1 → 2 → 3 → 3 → 2 → 1 |
| 파티션 | 토픽마다 3, 역할마다 request/retry 합계 6개 할당 |
| Relay / 완료 저장 | 앱 스케줄 Relay 1 / 1건 |
| offset | 동기 RECORD |
| 역할별 DB / Redis / HTTP 연결 상한 | 4 / 3 / 3 |
| 역할별 Worker 대기열 | 0 |
| 전체 / 캠페인 quota | 1000/s·burst 100 / 200/s·burst 20 |
| Redis / HTTP 응답 timeout | 250ms / 5초 |
| 실시간 probe 간격 / 기준선 | 순차 200ms / 50건 |
| 일시 오류 복구 | 모든 조건 ON, 오류 주입 없음 |

환경은 Amazon Corretto 21.0.6, macOS 15.7.7/aarch64, Mac16,1(호스트 16GiB/논리 CPU 10개)이다.
두 역할과 HTTP Mock은 같은 512MiB JVM에 있고 Testcontainers 인프라를 공유한다.
MySQL 8.4.8은 CPU 2/메모리 2048MiB/buffer pool 512MiB, Kafka 4.0.2는 CPU 1/메모리 1024MiB,
Redis 7.4.11은 메모리 256MiB다. 여러 서버 배포 결과가 아니다.

캠페인 정의/대상 번호/본문 길이/데이터 수는 같지만 실행별 합성 UUID는 다르므로 파티션별 record 수는 달라질 수 있다.
각 실행에서 실제 Consumer/child 수/Worker maxActive/6개 파티션 할당/RECORD·동기 커밋을 읽고 검증했다.
실험 6회 동안 [160개 소스의 SHA](experiments/s8-consumer-scaling-2026-11-04-group-2-source.json)는 바뀌지 않았다.

## 실제 결과

처리량 분모는 캠페인 앱 기동부터 마지막 DB 완료 관찰까지다. 사전 저장/실시간 기준선/최종 전수 검증은 제외한다.
각 행의 p95는 혼합 발송 구간의 실시간 API 접수 지연이며 공급자 수락까지의 지연이 아니다.
실시간 수락 수는 실행별 기준선 50건을 포함한다(6회 합계 4558건 중 기준선 300건).

| 순서 | 동시처리수 | 발송 구간(초) | 캠페인 건/초 | 실시간 수락 | 접수 p95(ms) | Gradle 전체 |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 1 | 212.047 | 47.159 | 1027 | 13.773 | 4분 26초 |
| 2 | 2 | 151.270 | 66.107 | 738 | 14.637 | 3분 21초 |
| 3 | 3 | 107.576 | 92.957 | 536 | 17.116 | 2분 39초 |
| 4 | 3 | 104.875 | 95.351 | 531 | 15.451 | 2분 41초 |
| 5 | 2 | 144.875 | 69.025 | 717 | 13.058 | 3분 15초 |
| 6 | 1 | 210.578 | 47.488 | 1009 | 13.069 | 4분 21초 |

| 동시처리수 | 평균 건/초 | 캠페인 lag 관측 최대 범위 | 평균 JVM CPU(%) | 평균 MySQL CPU(%) | 캠페인 DB active 관측 최대 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 47.324 | 5669~5698 | 28.35 | 31.93 | 2 |
| 2 | 67.566 | 2226~2315 | 35.68 | 42.58 | 3 |
| 3 | 94.154 | 114~118 | 45.24 | 59.35 | 4 |

CPU는 첫 표본부터 마지막 표본까지 누적 CPU 시간 차이/경과 시간이며, **100%가 한 CPU 코어**를 뜻한다.
JVM CPU는 실시간·캠페인·Mock 전체, MySQL CPU는 컨테이너 전체다. 사전 저장을 포함한 총 비용이나 캠페인만의 비용이 아니다.
2초 간격 표본의 최댓값은 관측 최대이며 실제 순간 최고치가 아니다.
lag gauge의 관측 나이는 최대 약 5.01초였다. 모든 실행에서 실시간 lag 관측 최대는 2,
양쪽 DB pool 대기자 관측 최대는 0이었다. 순간 대기나 DB 포화가 없었다고 단정하지 않는다.

캠페인 60000+실시간 4558=**64558건**의 HTTP 요청·수락/DB 완료·시도 이력/대상 매핑/최종 offset 대조를 통과했다.
중복/잘못된 참조 0, 양쪽 역할의 Redis 조회 오류도 0회다. 이 실험에서 일시 오류 복구가 발동한 것은 아니다.
[6개 원본의 SHA·세부 지표와 집계](experiments/s8-consumer-scaling-2026-11-04-group-2-summary.json)를 함께 보존한다.

## 판단과 한계

이 조건에서 3의 평균 처리량은 1의 약 1.99배였고, 캠페인 적체는 줄었다. 같은 시간의 CPU 사용량과 DB 활성 연결 수도 증가했다.
실시간 접수 p95는 1에서 13.069~13.773ms, 3에서 15.451~17.116ms였다.
처리량만으로 비용과 실시간 응답의 상충을 생략하지 않는다. 운영 기본값은 이미 3이며 이번에는 바꾸지 않았다.

이 결과는 Consumer/Worker 활성 상한을 함께 맞춘 효과다. RECORD 동기 커밋을 포함한 처리 경로 전체를 측정했으므로
HTTP, DB, Kafka 중 하나만이 병목이었다고 확정하지 않는다. 현재 200/s quota에도 도달하지 않았으므로 quota 증가 효과도 검증한 것이 아니다.
순서를 뒤집어 반복했지만 공유 호스트·조건별 2회로 일반적인 최적값이나 통계적 유의성을 확정할 수 없다.

순차 probe는 요청 뒤 200ms sleep에 요청·표본 수집 시간이 더해진다. 정확한 5req/s나 open-loop 부하가 아니며,
긴 실행은 더 많은 실시간 요청을 포함한다. Mock 수락은 실제 단말 도착이 아니다.
100만 건 결과와 데이터 크기가 다른 이번 처리량을 직접 개선율로 비교하지 않는다.
원래 100만 건 첫 실패의 API timeout 원인도 여전히 미확정이다.

다음 미완료 항목은 파티션 수 변경 시 key mapping/순서/offset의 영향 검증이다.
기존 토픽의 파티션을 바로 늘리기 전에 새 실험 토픽에서 의미와 복구 경계를 검증한다.
이번 단계에서는 파티션 변경이나 producer 압축 비교를 실행하지 않았다.

## 재실행과 검증

```bash
./gradlew --no-daemon --console=plain consumerScalingPilot -PcampaignConsumers=1
./gradlew --no-daemon --console=plain consumerScalingPilot -PcampaignConsumers=2
./gradlew --no-daemon --console=plain consumerScalingPilot -PcampaignConsumers=3
```

비교 순서는 위 명령을 1·2·3·3·2·1로 실행한다. 대상은 10000건 고정이고 load/Relay 병렬/offset 비교/다른 API 계측/
실패 주입과의 혼용을 거절한다. 기본 test/check/build는 기존 campaign-delivery-pilot tag를 제외하므로 별도 task로만 실행한다.
출력은 `build/reports/consumer-scaling/consumers-N-*/result.json`으로 매번 구분해 덮어쓰지 않는다.
JVM 512MiB/Gradle task 10분/JUnit 8분/기존 drain 900초(더 짧은 제한 우선), probe 5000/샘플 452/페이지 1000행 상한이다.

경계값 집중 테스트 14개는 수정 후 6초에 통과했다. 위 성능 실험 6회는 기본 회귀와 별도이며 각 JUnit 1개가 통과했다.
`caffeinate -i ./gradlew --no-daemon --console=plain build`는 **18분 18초/종료코드 0**이었다.
기본 test를 새로 실행했고 콘솔/JUnit XML 44개 클래스에서 **349개 통과, 실패·오류·스킵 0**을 대조했다.
bootJar는 UP-TO-DATE로 기존 산출물을 재사용했다. 원격 CI 결과는 아니다.
[최종 검증과 원본 SHA](experiments/s8-consumer-scaling-2026-11-04-validation.json)에 결과를 보존했다.
