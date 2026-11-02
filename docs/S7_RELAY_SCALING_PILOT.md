# S7 로컬 Relay 병렬 처리 사전 실험

2026-11-02. 시작 ada8b67/main. [10만 건 계측](S7_RELAY_100K_COMPARISON.md)의 수정판에서
선점 caller 누적753.411초/완료 저장156.363초/ACK52.936초가 관측됐다.
Worker 증설 전에 Relay가 작업을 공급하는 속도를 확인한다. 중첩된 caller 시간을 wall time 비율로 합산하지 않는다.

## 문제와 선택

기존 Relay 한 개는 DB 선점→트랜잭션 종료→Kafka ACK→완료 저장을 순차 수행한다.
이미 Worker는 역할당3개이며 캠페인 공유 한도는200/s다. Worker나 서버 수만 늘려도 빨라진다고 가정하지 않는다.

기존 `OutboxRelay`를 수정하지 않고 테스트 전용 실행기에서1개와2개의 독립 루프를 비교한다.
같은 역할의 짧은 선점 트랜잭션은 기존 역할 인덱스/READ COMMITTED/`SKIP LOCKED`를 사용한다.
각 루프의 Kafka ACK/lease token/유한 시도/부분 완료 저장은 기존 구현을 따른다.
외부 호출을 포함하는 DB 트랜잭션이나 선점 묶음, 새로운 비동기 offset 처리는 추가하지 않는다.

여러 서버/프로세스 대신 **같은 JVM, 같은 캠페인 앱, 같은 DB 연결 풀과 Kafka producer**를 공유한다.
이 실험의 효과는 로컬 Relay 병렬화에 한정한다. 서버 자원 증가·네트워크 장애·서버 장애 격리를 검증하지 않는다.
1개 조건도 테스트 전용 스케줄러를 사용한다. 예전 자동 스케줄러의1만/10만 결과를 비교 기준선으로 재사용하지 않는다.

## 완료 조건과 고정 조건

- 조건별 가상 캠페인10000건. 실행 순서는1→2→2→1로 각2회, 매번 새 JVM/격리 인프라다.
- 완료 저장 최대10건은 양쪽 모두 고정하며 실시간 Relay는 원래 앱의1개/완료 저장1건을 유지한다.
- 역할별 Worker3/HTTP3/Redis3/Hikari4/queue0, 요청·Retry 토픽 각3파티션을 유지한다.
- 총1000/s·burst100 중 캠페인200/s·burst20, HTTP Mock SUCCESS/1ms/max-in-flight8을 유지한다.
- Java21/Boot3.5.16/Gradle8.14.5/test JVM512MiB, MySQL8.4.8(2CPU·2GiB·buffer pool512MiB),
  Kafka broker4.0.2(1CPU·1GiB)/client3.9.2, Redis7.4.11(1CPU·256MiB)을 유지한다.
- 실시간 단독50건 뒤 응답+200ms의 순차 요청을 보낸다. 접수 timeout5초/발송900초/task25분 상한을 유지한다.
- 캠페인/실시간의 대상 매핑·HTTP 전수 감사·DB 성공 시도1회·PUBLISHED·offset=end·quota 허가 수를 대조한다.
- 자원 샘플은2초마다 읽고 기존 표본/배열 상한을 유지한다. 처리량과 API p95/최대, DB 완료 지연을 함께 기록한다.
- 실행기는1..3개만 허용하고 고정 주기 작업만 등록한다. 건별 future/제출 대기열을 만들지 않는다.
  같은 루프는 겹쳐 실행하지 않는다. 주기100ms는 작업 **완료 후** 지연이다.
- 자동 캠페인 Relay bean이 있으면 시작을 거부해 이중 실행을 방지한다. 예상 밖 주기 작업 종료는 실패로 감지한다.
  신규 선점을 멈추고 실행 중 ACK/부분 묶음을 기다린 뒤 DB/producer 앱을 종료한다.
  정상 대기30초 후 interrupt15초도 종료되지 않으면 실패로 남긴다. 기본 앱의 종료 정책 변경은 아니다.
- 정합성 통과와 실제 단말 도착/exactly-once를 구분한다.100만 건 재실행과 푸시는 포함하지 않는다.

```bash
caffeinate -i ./gradlew --no-daemon --console=plain relayScalingPilot -PcampaignRelays=1
# 원본을 별도 이름으로 보존한 뒤2,2,1을 각각 실행
caffeinate -i ./gradlew --no-daemon --console=plain relayScalingPilot -PcampaignRelays=2
```

`-PdeliveryTargets=1000`으로 작은 실행도 가능하다. 이 task는1000..10000건만 허용하고
`test`/`build`나 기존10만/100만 task에서 자동 실행하지 않는다.
출력은 `build/reports/relay-scaling-pilot/relays-N-targets-10000.json`이다.
같은 조건을 다시 실행하면 build 원본이 교체되므로 매 실행 뒤 `docs/experiments/`에 별도 보존한다.

발송 구간은 앱 시작/Worker 준비를 포함하고 사전 저장/단독50개/최종 대조는 제외한다.
동시 Relay 타이머는 중첩된다. `relay.mark.published` count는 행 수가 아닌 완료 저장 호출 수다.
`*ApiLatencyMs`와 DB 완료 기준 `*LatencyMs`는 다른 경계다. 공유 호스트/소수 반복/closed-loop probe의 한계가 있다.

## 실제 결과

네 실행은 같은 수정 후 테스트 코드이며 [파일별 SHA-256](experiments/s7-relay-scaling-2026-11-02-source.json)을 고정했다.
기준 커밋ada8b67 대비 운영 Java/resources는 변경하지 않았다. 아직 커밋하지 않은 실험 도구를 포함하므로
ada8b67 자체에서 실행한 결과라고 표현하지 않는다.

| 순서 / 조건 | 사전 저장초 | 발송초 / 처리량 | 혼합 API p95 / 최대ms | DB 완료 p95 / 최대ms | 실시간 수 | task 전체 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 / Relay1 a | 15.139 | 82.887 / 120.646/s | 13.578 / 29.703 | 500.499 / 588.435 | 435 | 2분7초 |
| 2 / Relay2 a | 14.697 | 72.809 / 137.346/s | 11.583 / 22.956 | 498.305 / 588.367 | 390 | 1분56초 |
| 3 / Relay2 b | 17.055 | 101.600 / 98.426/s | 40.438 / 1490.550 | 544.918 / 1604.166 | 479 | 2분30초 |
| 4 / Relay1 b | 14.615 | 76.114 / 131.381/s | 10.804 / 78.501 | 478.241 / 620.033 | 407 | 2분1초 |

각 task BUILD SUCCESSFUL/종료 코드0/테스트1개 통과/실패·오류·스킵0이었다.
캠페인40000+실시간1711=HTTP41711건을 대상 매핑/성공 시도1회/Outbox PUBLISHED/HTTP/전체 offset/quota로 대조했다.
중복·매핑·Mock 오류0이며 네 독립1만 건의 합계다. 단일 연속4만 건 결과로 표현하지 않는다.
혼합 요청만의 표본 수는385/340/429/357이고 실시간 수에는 각50개 단독 요청도 포함한다.
단독 APIp95는22.446/22.093/21.783/21.318ms였다.

### 관측과 판단

1차 쌍에서 처리량은13.84% 증가했지만, 2개 반복은98.426/s로 내려가 두1개 실행보다 느렸다.
Relay1 범위120.646~131.381/s, Relay2 범위98.426~137.346/s다.
평균 수치 하나로 병렬화 효과를 확정하지 않는다. 반복2개/공유 호스트에서 일관된 개선을 입증하지 못했다.
특히 Relay2 b의 API 최대1490.550ms 증가 원인은 이 실험만으로 확정할 수 없다.
5초 접수 timeout은 네 실행에서 발생하지 않았지만 원래100만 건 timeout의 해소 증거가 아니다.

| 조건 | 선점 호출 / 누적초 | ACK 누적초 | 완료 저장 호출 / 누적초 | Relay run 호출 / 누적초 |
| --- | ---: | ---: | ---: | ---: |
| Relay1 a | 10038 / 44.837 | 6.915 | 1011 / 6.893 | 238 / 58.690 |
| Relay2 a | 10554 / 53.745 | 7.622 | 1009 / 7.738 | 753 / 69.154 |
| Relay2 b | 10715 / 87.916 | 8.522 | 1070 / 13.477 | 914 / 109.972 |
| Relay1 b | 10048 / 39.040 | 5.880 | 1002 / 5.992 | 248 / 50.950 |

Relay2는 빈 폴링을 포함한 선점 호출과 누적 DB caller 시간이 늘었다. 병렬 timer 합계는 wall time을 초과할 수 있다.
이 수치를 DB 내부 실행 시간이나 잠금 대기로 해석하지 않는다. 별도 잠금 관찰기는 이번 작은 비교에서 실행하지 않았다.

| 조건 | 자원샘플 | heap / MySQL cgroup 최대MiB | 스레드최대 | Hikari waiting 최대 C / RT |
| --- | ---: | ---: | ---: | ---: |
| Relay1 a | 40 | 102.262 / 474.809 | 99 | 0 / 0 |
| Relay2 a | 35 | 102.530 / 478.020 | 100 | 2 / 0 |
| Relay2 b | 47 | 104.248 / 479.656 | 99 | 1 / 0 |
| Relay1 b | 37 | 114.779 / 479.176 | 99 | 0 / 0 |

159개 표본의 Hikari 상태 결측은0이며 MySQL cgroup max/oom/oom_kill은 모두0이었다.
표본 waiting0은 표본 사이의 대기 부재를 증명하지 않는다. 동일한 Hikari4 안의 경쟁 증가와 성능 편차가 함께 관측됐으며
연결 취득/DB 명령/commit/offset 저장 비용을 더 나눠 확인하는 것이 다음 작은 진단이다.
2개로 늘린 것이 지연 증가의 유일한 원인이라고 결론 내리지 않는다.

각 실행의 최대 동시 루프1/2/2/1과 모든 루프의 호출을 확인했다.
정상 종료 후 active0/대기 주기 작업0/terminated=true였다. 동일 작업의 루프별 분담 건수는 측정하지 않는다.
이 성공 경로 비교는 다중 프로세스 강제 종료/동시 장애 테스트가 아니다.
여러 루프가 ACK 후 DB 기록 전 구간을 동시에 가지므로 재발행 가능 범위가 커질 수 있다.
기존 at-least-once 한계를 유지하며 운영 기본값이나 여러 서버의 배포 설정으로 자동 채택하지 않는다.

### 원본과 실패 보존

- [Relay1 a](experiments/s7-relay-scaling-2026-11-02-1a.json): `9c73ca6faf7666c967fa1cee5e15fffe205f8cf3534d000a0a9511ee0377b048`
- [Relay2 a](experiments/s7-relay-scaling-2026-11-02-2a.json): `85ec3d41a85166986520b39669b28dfd9a108eb8535ab892ea71c308a547d3c1`
- [Relay2 b](experiments/s7-relay-scaling-2026-11-02-2b.json): `0f3a0c763ade46ee90bd0da1f02febaa1376b48e29b93d8265f0ae2c04f50273`
- [Relay1 b](experiments/s7-relay-scaling-2026-11-02-1b.json): `ef413056f194596707de5127211101246299892d014394d87b0445285e79ef09`
- [처음 실행의 FAIL](experiments/s7-relay-scaling-2026-11-02-initial-bean-failure.json):44초/테스트1개 실패.
  Boot가 Object/Object 형태로 노출한 KafkaTemplate을 String/String 제네릭 타입으로 조회한 실험 도구 오류였다.
  사전 저장과 단독50건 뒤 캠페인 Relay 시작 전에 실패했으며 캠페인 발송 정합성 검증을 수행한 실행이 아니다.
  기존 전송 bean을 조회해 String serializer 설정에 맞춰 사용하도록 수정했고 이후 네 실행에 같은 소스를 사용했다.

원본은 수정 없이 복사했으며 UUID/비유한 값 미포함을 확인했다. 실제 수신자·토큰·본문·인증정보를 포함하지 않는다.
100만 건 첫 실패 원본의 SHA-256도 그대로임을 확인했다.100만 건 재실행과 푸시는 하지 않았다.

## 기본 회귀

`caffeinate -i ./gradlew --no-daemon --console=plain build`는 BUILD SUCCESSFUL12분8초/종료 코드0이었다.
최종35개 클래스의 JUnit XML과 콘솔을 대조해278개 통과/실패·오류·스킵0을 확인했다.
기존275개와 신규 실행기3개가 포함되며 위 별도 발송 실험4회와 구분한다.
운영 코드가 같아 bootJar/mockServerJar는 UP-TO-DATE였고 기본 회귀는 실제로 재실행했다.
신규 실행기 테스트는 개수 상한/동시 실행·진행 중 종료·종료 멱등성/주기 작업 실패 감지를 검증한다.
빌드 뒤에도 실험 소스의 고정 SHA-256을 대조했다. 격리 컨테이너는 정리됐고 기존 사용자 MySQL만 healthy로 남았다.

## 참고

- [MySQL8.4 locking reads](https://dev.mysql.com/doc/refman/8.4/en/innodb-locking-reads.html):
  `SKIP LOCKED`의 큐형 테이블 활용과 일반적인 일관된 읽기에 사용할 수 없는 한계.
- [Java21 ScheduledThreadPoolExecutor](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/concurrent/ScheduledThreadPoolExecutor.html):
  같은 주기 작업의 비중첩, 취소 작업 제거와 종료 정책. 기본 큐 자체는 무한이므로 전용 실행기가 등록 수를 제한한다.
- [Spring Kafka3.3 전송](https://docs.spring.io/spring-kafka/reference/3.3/kafka/sending-messages.html):
  send future 완료 확인. 전송 API의 비동기성과 DB 원자성을 혼동하지 않는다.
- [Kafka client3.9.2 KafkaProducer](https://kafka.apache.org/39/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html):
  스레드 간 공유가 가능한 producer와 애플리케이션 재전송까지 제거하지 못하는 멱등성 범위.
