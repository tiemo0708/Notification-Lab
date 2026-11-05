# S8 순차 ACK의 실제 HTTP Mock 경로 none/LZ4 압축 비교

2026-11-05 KST. 시작 커밋 d0685b6. 시작 시 원격 main과 같고,
[해당 원격 CI](https://github.com/tiemo0708/Notification-Lab/actions/runs/41791853619)의 기본 회귀363개 통과를 확인했다.
[Kafka 단독 사전 비교](S8_PRODUCER_BATCHING.md) 뒤 실제 앱 경로에서 압축 한 요인만 비교한다.
운영 Java/resources와 기본 producer 설정은 변경하지 않는다.

## 프로토콜과 고정 조건

| 항목 | 값 |
| --- | --- |
| 유일한 변경 요인 | CAMPAIGN producer compression.type=none 또는 lz4 |
| 반복 | none → lz4 → lz4 → none, 각2회 |
| 데이터 | 매 실행 새 MySQL/Kafka/Redis, 사전 저장 캠페인10000건 |
| 실시간 producer | 모든 실행 none |
| 전송 방식 | 기존 Relay1, 단건 send 후 ACK 대기, 완료 저장1건 |
| producer 고정 | linger0, batch.size16384, acks=all, idempotence=true, max.in.flight1, retries3 |
| producer 버퍼/시간 | buffer.memory1MiB, max.block3초/request3초/delivery10초 |
| 토픽 | 양쪽 요청/Retry/DLQ 각각3, 실제 토픽 compression.type=producer 확인 |
| Consumer/Worker 활성/queue | 양쪽3/3/0, RECORD·동기 commit |
| 복구 | ON, 양쪽 이력 용량12 |
| 역할별 연결 | DB4 / Redis3·250ms / HTTP3·응답5초 |
| quota | 전체1000/s·burst100 / 캠페인200/s·burst20 |
| HTTP Mock | SUCCESS·1ms, in-flight8, 10000req/s |
| probe | 기준선50개 후 혼합 구간 순차 요청, 매 응답 뒤200ms 대기 |
| JVM/컨테이너 | heap512MiB, MySQL8.4.8 CPU2·2048MiB·buffer512MiB, Kafka4.0.2 CPU1·1024MiB, Redis7.4.11 CPU1·256MiB |
| client | Kafka3.9.2, 브로커 버전과 구분 |

같은 JAR의 역할별 context와 Mock이 한 JVM/공유 호스트에서 실행된다. 다중 서버 결과가 아니다.
대상 번호/제목/본문/개수는 같지만 실행별 캠페인·알림의 합성 UUID와 생성 시각은 다르다.
압축에 유리한 큰 반복 문자열을 새로 넣지 않고 기존 캠페인 발송 fixture를 그대로 사용했다.
그래도 실제 고객 데이터나 암호화된 고엔트로피 payload를 대표하는 것은 아니다.

새 `compressionDeliveryPilot`만 명시적으로 실행한다. 1만 건/none·lz4만 허용하며 다른 Consumer/파티션/Relay/offset/API/실패 주입 모드와 혼용하지 않는다.
Gradle10분/JUnit8분, 기존 probe5000·샘플452·검증 페이지1000행 상한을 유지한다.
정상 발송 경로를 비교하며 이번 단계에서 앱의 동시 제출이나 새 재시도 경로를 도입하지 않는다.

```bash
./gradlew --no-daemon --console=plain compressionDeliveryPilot -PcampaignCompression=none
./gradlew --no-daemon --console=plain compressionDeliveryPilot -PcampaignCompression=lz4
# 위 두 조건을 none, lz4, lz4, none 순서로 실행
```

출력은 매번 `build/reports/compression-delivery/codec-N-*/result.json`으로 구분한다.
[고정 소스165개·순서·설정](experiments/s8-compression-delivery-2026-11-05-group-1-source.json)을 실행 전에 보존했다.

## 실제 설정·정합성·계측

각 앱의 ProducerFactory에서 압축과 고정된11개 설정을 읽어 검증한다. 설정 전체/주소/client-id를 보고서에 내보내지 않는다.
실제 broker metadata·Consumer 할당[2,2,2]·토픽별 압축 정책·복구 이력 용량12도 확인한다.
전송 코드가 순차 ACK를 유지하는지는 운영 소스 diff0과 Relay/완료 저장1건의 고정 조건으로 확인한다.

실행마다 HTTP 요청/수락·DB 완료/시도·10000개 대상 매핑과 **모든 파티션의 committed=end**를 전수 대조한다.
lag0이나 합계만으로 누락·중복 검증을 대신하지 않는다. Mock 수락은 단말 도착이 아니다.

- 발송 구간: 캠페인 앱 시작~마지막 DB 완료 관찰이다. 기동과 설정 확인도 포함하며 사전 저장/실시간 기준선/최종 검증은 제외한다.
- API p95/p99/최대: 혼합 구간의 접수 응답 지연이다. 공급자 수락 완료 지연과 구분한다.
- `producerLifetimeMetrics`: 해당 **live 앱 producer의 시작부터 최종 대조 뒤 스냅샷까지**의 관측이다.
  캠페인은10000건 전체를 포함하고 실시간은 기준선50건과 혼합 요청을 포함한다. 앱 전용 준비 레코드를 추가하지 않았다.
- 전송/error/retry/request/outgoing-byte의 total은 누적 counter다. record-send-total은 재전송도 포함하며 고유 처리량으로 사용하지 않는다.
  outgoing-byte-total은 metadata/초기화/프로토콜/재시도를 포함할 수 있어 payload 크기·실제 네트워크 과금량과 구분한다.
  producer 송신 counter이며 Consumer 수신/HTTP/DB를 포함한 시스템 전체 네트워크 트래픽이 아니다.
- compression-rate-avg/records-per-request-avg/batch-size-avg 등은 Kafka의 **시간 창 평균**이다. 전체10000건의 평균이 아니다.
  유효한 창이 없으면 null이며0으로 바꾸지 않는다. 누적 counter가 비유한값/음수이거나 필요한 값이 누락되면 실패한다.
- JVM CPU는 기존 자원 표본의 첫/마지막 누적 CPU 시간/경과 시간이며 두 앱/Mock/JIT/내부 스레드를 모두 포함한다.
  MySQL CPU도 컨테이너 전체다(100%=한 코어). 압축 함수/브로커 CPU를 분리해 측정하지 않는다.
- lag/heap/pool은 약2초 표본의 관측 최대이고 순간 최고치가 아니다. lag gauge의 나이도 함께 기록한다.

순차 probe는 정확한5req/s open-loop가 아니고 긴 실행일수록 요청 수가 많다. 실시간 누적 바이트의 총수를 고정 부하처럼 비교하지 않는다.
공유 호스트/각2회/실행별 UUID·시각/초기화·재시도 변동의 한계가 있다. 이전 Consumer/파티션/100만 실험과의 직접 개선율도 계산하지 않는다.
정합성 통과와 성능 개선은 별도 판정이며 기본 설정은 결과를 검토한 뒤에도 별도 선택한다.

## 사전 검증과 실제 결과

경계·진단·producer 관측 집중 테스트21개가6초/종료코드0에 통과했다.
지원하지 않는 `campaignCompression=gzip --dry-run`은 실행 전 거절됐다.

4회 모두 PASS다. **캠페인40000 + 실시간2352 = HTTP 수락42352건**의 DB/시도/대상 매핑/전체 offset을 대조했다.
관측 중복·잘못된 참조0, Retry/DLQ 발행0이었다. 양쪽 producer 전송 오류/재시도 및 Redis quota 오류6종도 모두0이었다.
캠페인 record-send-total은 각10000이며 이번 실행에서는 재전송 때문에 counter가 증가하지 않았다.
이 정상 경로 결과가 앞선 오류 경계를 대체하거나 단말 도착/exactly-once를 보장하지는 않는다.

| 순서 | 캠페인 codec | 사전 저장 초 | 발송 구간 초 | 캠페인 건/초 | 실시간 수락(기준선 포함) | 혼합 API p95 / p99 / 최대 ms |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | none | 23.076 | 142.727 | 70.064 | 661 | 28.477 / 222.517 / 753.134 |
| 2 | lz4 | 19.246 | 139.124 | 71.878 | 632 | 31.133 / 642.890 / 1250.192 |
| 3 | lz4 | 17.915 | 128.104 | 78.062 | 592 | 55.880 / 560.200 / 1140.339 |
| 4 | none | 15.256 | 90.931 | 109.973 | 467 | 11.144 / 22.654 / 359.210 |

| 조건(각2회) | 실행별 처리량 산술평균 건/초 | 처리량 범위 건/초 | 혼합 API p95 범위 ms | 캠페인 outgoing byte counter 평균 |
| --- | --- | --- | --- | --- |
| none | 90.019 | 70.064~109.973 | 11.144~28.477 | 3800313 |
| lz4 | 74.970 | 71.878~78.062 | 31.133~55.880 | 3618446 |

처리량은 각 실행의 `10000 / 발송 구간 초`를 산술평균했다. 평균 차이 **-16.72%**는 관측 비교일 뿐,
LZ4가 앱 처리량을 그만큼 감소시킨다는 인과적 결론이 아니다. 첫 none은 두 LZ4보다 느렸고 마지막 none은 둘보다 빨랐다.
동일 none의70.064→109.973 변동이 크며 발송 전 사전 저장도23.076→15.256초로 줄었다.
호스트 부하/캐시/I/O/스케줄링을 분리해 통제하지 않았고 빠른·느린 실행을 골라서 제외하지 않았다.
LZ4의 p95/p99도 이번 두 관측에서는 높았으나 codec만의 원인으로 확정하지 않는다.

### 바이트와 자원의 해석

캠페인 producer outgoing byte counter는 none 3,800,336/3,800,290, LZ4 3,618,423/3,618,469였다.
평균 차이는 **-4.79%**, 고유 캠페인1건당 평균은 약380.031→361.845바이트다.
이는 producer 프로토콜 트래픽까지 포함한 수치이며 전체 서비스 네트워크의 절감률이 아니다.
모든 실행에서 records-per-request-avg 관측은1.0이었다. 시간 창 compression-rate-avg는 none1.0, lz4약0.9188이었다.
압축률 평균과 전체 outgoing counter 차이는 측정 경계가 달라 같은 비율이 아니다.

앞선 Kafka 단독의67.90% 감소를 이 앱 결과의 기대치로 재사용하지 않는다.
그 실험은 제출창10/linger5와 별도 반복 문자열 fixture였고, 이번에는 기존 순차ACK/linger0/실제 앱의 짧은 합성 이벤트다.
묶음/데이터/경로가 함께 다르므로 두 절감률의 차이를 한 요인으로 설명하지 않는다.

| 순서 | JVM CPU % | MySQL CPU % | 캠페인 lag 관측 최대 | 캠페인 DB active / 대기 관측 최대 | heap 관측 최대 MiB | GC 증가 ms |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 38.69 | 55.10 | 84 | 4 / 0 | 110.89 | 610 |
| 2 | 42.32 | 48.34 | 96 | 4 / 0 | 103.20 | 699 |
| 3 | 41.92 | 48.59 | 112 | 4 / 0 | 102.68 | 618 |
| 4 | 48.85 | 48.71 | 159 | 4 / 0 | 101.35 | 363 |

실시간 lag 관측 최대는 모두2였고 DB 대기 관측 최대는 모두0이었다. lag gauge 나이의 관측 최대는4.721~5.016초다.
none/LZ4의 JVM CPU 평균은43.77/42.12%, MySQL은51.91/48.47%였다. 이 비율만으로 압축 CPU 비용 유무를 판단하지 않는다.
구간 길이와 실시간 요청 수가 달랐고 두 앱/Mock을 함께 측정했기 때문이다. MySQL은2코어 제한이며 위 비율100%는 한 코어다.
DB active가 상한4로 관측됐지만2초 표본에서 대기0인 사실만으로 순간 대기/잠금이 없었다고 보장하지 않는다.

기존 단계 타이머에서도 마지막 none의 여러 구간이 함께 빨라졌다. 단위는 호출당 평균ms다.

| 순서 | Relay 선점 | Kafka send~ACK | PUBLISHED 저장 | Worker handler | Worker 공급자 |
| --- | --- | --- | --- | --- | --- |
| 1 | 6.768 | 1.049 | 4.194 | 24.783 | 1.898 |
| 2 | 6.573 | 1.005 | 4.147 | 25.023 | 1.842 |
| 3 | 5.888 | 0.977 | 3.750 | 22.583 | 1.852 |
| 4 | 3.967 | 0.694 | 2.160 | 15.444 | 1.843 |

이 타이머는 DB 연결/트랜잭션/SQL/commit/네트워크/스케줄링을 포함할 수 있다. 특정 SQL/lock/압축 함수의 비용을 분해한 결과가 아니다.
handler와 내부 단계는 겹치고 Worker3개의 누적 시간도 겹치므로 합계를 벽시계 시간의 비율로 더하지 않는다.

## 적용 판단과 보존한 근거

이 조건에서는 producer 바이트 감소를 관측했지만 앱 처리량·실시간 지연 개선의 근거는 부족하다.
**운영 기본 compression=none과 순차ACK/linger0을 유지한다.** 앱의 동시 제출/batching을 도입했다고 표현하지 않는다.
다음 별도 단계는 제한된 제출 창의 ACK 이전 PUBLISHED 금지·부분 실패·timeout·중단·lease fencing·복구 경계 검증이다.
그 뒤 효과를 확인할 수 있을 때만 앱의 batching 처리량을 비교한다.

- [1회 none 원본](experiments/s8-compression-delivery-2026-11-05-group-1-run-1-none.json)
- [2회 lz4 원본](experiments/s8-compression-delivery-2026-11-05-group-1-run-2-lz4.json)
- [3회 lz4 원본](experiments/s8-compression-delivery-2026-11-05-group-1-run-3-lz4.json)
- [4회 none 원본](experiments/s8-compression-delivery-2026-11-05-group-1-run-4-none.json)
- [원본SHA·지표·계산 경계·요약](experiments/s8-compression-delivery-2026-11-05-group-1-summary.json)

측정 그룹 종료 뒤 `caffeinate -i ./gradlew --no-daemon --console=plain build`를 별도로 실행했다.
**17분26초/종료코드0**, 기본 test 새 실행 **47개 클래스/368개 통과, 실패·오류·스킵0**이었다.
기존363개에 경계2개/producer 관측3개가 추가됐고 WorkerConcurrencyTest11개도 모두 통과했다.
콘솔과 새 JUnit XML의 총수·해시를 대조했다. bootJar는 UP-TO-DATE로 이번에 새 실행 JAR를 만들었다고 표현하지 않는다.
opt-in 비교는 기본 build에서 제외되며 이번 비교4회와 기본 회귀를 구분한다.
기존 Consumer/파티션/100만 건 opt-in 실험은 이번에 재실행하지 않았다.
고정 소스165개 불변·운영 소스 diff0·JSON 민감 필드/비유한값·문서 링크와 원본SHA를 확인하고
[최종 로컬 검증](experiments/s8-compression-delivery-2026-11-05-group-1-validation.json)을 보존했다.
시작 기준 d0685b6 원격 CI 성공과 이번 새 코드의 CI는 구분한다. 이번 변경은 로컬 커밋까지 진행하고 새 푸시/원격CI는 미실행이다.

공식 참고: [Kafka3.9 producer 설정](https://kafka.apache.org/39/configuration/producer-configs/)의 compression.type/batch.size/linger.ms/acks/idempotence/max.in.flight.
