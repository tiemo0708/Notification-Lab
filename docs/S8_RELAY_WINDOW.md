# S8 Relay 유한 제출 창의 ACK·실패·복구 경계

2026-11-05. 시작 커밋 b352571. producer-only pilot과 실제 HTTP 압축 비교 다음 단계다.
성능 비교 전에 Relay가 여러 record를 제출했을 때의 완료·실패·소유권 경계를 검증한다.
기본 설정은 계속 단건 순차 ACK이며 이번 단계의 처리량 개선 수치는 없다.

## 문제와 선택

기존 Relay는 선점 → send → ACK → 다음 선점을 순서대로 수행했다.
`publish-batch-size=10`은 ACK 후 DB 완료 저장만 묶으므로, 여러 record를 ACK 전에 모으는 설정과 다르다.
앞선 broker-only pilot의 제출 창을 앱에 연결하려면 일부만 ACK되거나 DB 저장이 실패할 때의 정책이 먼저 필요했다.

선택 옵션 `app.async.relay.submission-window-size`를 추가했다. 기본1, 허용1~10이다.
1은 기존 순차 처리 경로를 유지한다. 2~10은 최대 N건을 제출한 뒤 해당 창을 drain하는 경로다.
슬라이딩 창·추가 executor·콜백 DB 쓰기·무한 queue는 도입하지 않는다.
앱이 보관하는 future 수와 Kafka `max.in.flight.requests.per.connection`의 요청 수는 다른 경계다.

`RelayProperties`의 기존 한 인자 생성자는 호환용으로 유지한다.
생성자가 두 개가 되므로 canonical 생성자에 명시적 `@ConstructorBinding`을 붙이고 실제 Spring 바인딩을 테스트했다.
설정 미지정1/1, 완료 저장1·제출10, 잘못된 제출0/11의 기동 거절을 확인했다.
YAML·producer·topic·Worker·quota 기본값과 DB migration은 바꾸지 않았다.

## 한 창의 실행 순서

1. 한 Relay 객체의 실행권을 원자적으로 확보한다. 예약 호출과 수동 호출이 겹치면 후속 호출은 대기열 없이 반환한다.
   이전 주기에 timeout/interrupt/DB 예외로 남은 미완료 future가 있으면 새 선점도 하지 않는다.
   최대 N개의 future만 보관하고 모두 완료됐을 때 참조를 해제한다. 늦은 완료 관찰은 DB 갱신으로 사용하지 않는다.
2. 하나의 공통 ACK deadline을 시작하고, 짧은 DB 트랜잭션으로 한 건씩 선점한다.
3. DB 트랜잭션 밖에서 send를 호출하고 future와 claim/token을 최대 N개 보관한다.
4. 이미 관측한 실패, deadline 초과, interrupt, 종료 신호, idle에서는 추가 제출을 중단한다.
5. 제출된 future를 확인한다. 먼저 보낸 건이 실패해도 이후 건의 확인 가능한 ACK를 처리한다.
6. 확인한 ACK만 기존 token 조건부 PUBLISHED 저장에 전달한다. 완료 저장 묶음의 상한도 별도로 지킨다.
7. 창 끝에는 작은 미완성 완료 묶음도 저장한다. 하나라도 실패/미확인이면 해당 relay 호출에서 새 창을 만들지 않는다.
8. 정상 창만 다음으로 진행하며 한 relay 호출에서 기존 최대50건 선점 상한을 유지한다.

창의 기본 ACK 대기 예산은 **전체12초**다. 건별12초를 N번 기다리지 않는다.
다음 예약 주기는 다시 실행될 수 있다. 완료 예외는 기존 backoff/최대 시도 정책을 따르며,
아직 완료되지 않은 future가 있을 때만 그 다음 주기의 새 선점까지 차단한다. 영구적인 운영자 해제 latch를 추가한 것이 아니다.
예산은 첫 claim 전부터 제출·drain에 공통 적용한다. 다만 이미 실행 중인 동기 DB/send 호출을 강제로 중단하지는 않는다.
send의 metadata/buffer 대기는 producer max.block, DB 대기는 해당 DB/client 정책을 따른다.
따라서 이 값은 relay 전체 실행·프로세스 종료의 엄격한12초 상한이 아니다.
DB 선점 뒤 deadline/종료를 관측해 미제출한 claim도 token/lease를 남기고 복구한다. 그 선점의 시도 예산은 이미 소비됐다.

## 실패별 상태

| 상황 | 처리 | 남는 복구/비용 |
| --- | --- | --- |
| send가 동기 예외를 던짐 | 해당 claim은 기존 failed/backoff 정책, 이전 future는 drain | 브로커 기록 부재를 단정하지 않음 |
| future 예외·취소 | 해당 claim은 token 조건부 failed, 다른 ACK는 저장 | 재발행 중복 가능, 완료되지 않은 건을 PUBLISHED로 만들지 않음 |
| ACK 대기 timeout | 미확인 건을 PROCESSING/lease 상태로 보존 | 실제 전송이 뒤늦게 성공해도 DB 콜백 쓰기 없음; lease 뒤 재발행 가능 |
| thread interrupt | interrupt flag 복원, 나머지 claim의 lease 보존 | 이미 확인한 ACK만 저장, 나머지 완료 여부 추정 금지 |
| 다음 claim 실패 | 앞서 제출한 future는 finally에서 drain | 모호한 DB claim commit은 lease 복구 |
| PUBLISHED DB 저장 실패 | 같은 완료 묶음을 finally에서 재시도하지 않음 | commit 불확실성과 중복 가능성을 durable lease로 처리 |
| 선점 소유권 변경 | ACK·실패 갱신 모두 기존 token 조건 사용 | 오래된 실행자가 새 소유자 상태를 덮지 못함 |
| 종료 신호 | 새 제출 중단, 제출된 창만 공통 예산으로 drain | 프로세스 강제 종료까지 보장하는 유예 시간은 아님 |

timeout 후 future.cancel로 브로커 전송이 취소됐다고 간주하지 않는다.
자동 ACK 콜백으로 뒤늦게 DB 상태를 바꾸지도 않는다. lease 만료 후 다른 실행자가 새 token으로 처리할 수 있기 때문이다.
공급자 수락 뒤 DB 기록 전 종료 같은 기존 외부 중복 발송 한계는 이 변경으로 없어지지 않는다.

## 검증 방법과 실제 집중 결과

실제 MySQL8.4.8/Kafka4.0.2 Testcontainers, Java21, 기존 Gradle Wrapper를 사용했다.
기존 사용자 DB/토픽에는 접속·쓰기하지 않았다. 합성 future를 사용하는 테스트도 선점·완료·실패는 실제 MySQL 트랜잭션이다.
future 오류/ACK 순서를 통제한9건과 실제 Kafka 복구1건을 구분한다. 합성 ACK를 실제 브로커 수락으로 세지 않는다.

| 시나리오 | 확인한 결과 |
| --- | --- |
| 창10·11개 작업 | ACK 전10개 PROCESSING·1개 미선점, 겹친 relay 호출도 추가 제출0, 이후11개 완료 |
| 창3·역순 ACK·첫 future 실패 | 확인한2개만 PUBLISHED, 실패1개 PENDING, 네 번째 작업 미선점 |
| 세 번째 send 동기 예외 | 앞선2개 ACK 저장, 실패1개 backoff, 나머지 미선점 |
| 창3·두 ACK 누락 | 테스트 전용 공통5초 안에 drain, 성공1/미확인2/미선점1; 다음 주기도 추가 제출0; 늦은 ACK 자체의 상태 변경0, 해소 뒤 미선점1개만 새 제출 가능 |
| 종료 | 제출3개 완료, 미선점2개 유지, 종료 후 재호출도 새 제출0 |
| interrupt | flag 복원,3개 lease·미선점1개 보존, 다음 주기도 추가 제출0, 뒤늦은 합성 ACK가 완료를 기록하지 않음 |
| 두 claim의 lease 재선점 | 오래된 ACK와 실패가 새 token을 덮지 못함, 유효1개만 PUBLISHED |
| 다음 claim 예외 | 앞선2개 ACK 저장, 세 번째 미선점 |
| 창3·51개 작업 | 한 번에50개 완료, 마지막 부분 창 flush,1개 미선점 |
| 실제 Kafka ACK 뒤 DB 완료 저장 오류 | 처음3개 기록/DB PROCESSING3개 → lease 만료 제어/새 Relay 재발행 → Kafka6개/DB PUBLISHED3개·attempt2 |

마지막 실제 Kafka 복구에서는 각 payload가 정확히2개 기록됐고 Worker를 시작한 뒤
공급자 수락3개·delivery_attempt3개·전체 committed offset 합6을 대조했다.
이 결과는 ACK 후 DB 완료 저장을 실패시킨 제어군이며 실제 SIGKILL 실험은 아니다.
lease는 DB 시각을 앞당기는 제어로 만료시켰으므로 실제30초 벽시계 대기를 검증했다고 쓰지 않는다.
Worker 공급자는 기존 in-process Mock이며 별도 HTTP Mock 부하·실제 단말 도착은 검증하지 않았다.

첫13개 집중 실행은 **31초/종료코드0/13개 통과, 실패·오류·스킵0**이었다.
기존 설정2개+신규 바인딩1개+Relay10개다. 수정 직후의 중간11개 실행도32초에 통과했다.
후속 검토에서 주기 사이의 미완료 future 보존을 추가하고 timeout/interrupt 테스트에 재진입 차단·늦은 완료 뒤 재개를 보강했다.
보강 전 통과 기록도 별도로 보존했다. 미완료 future 보강 뒤 집중 실행도 **31초/종료코드0/13개 통과, 실패·오류·스킵0**이었다.
최종 고정 소스의 전체 결과는 별도 build로 확인한다.
[최종 집중 결과·소스165개 SHA·테스트별 시간](experiments/s8-relay-window-2026-11-05-focused.json)을 보존했다.
아래 전체 회귀는 최종 설정 fixture 분리 후의 고정 소스로 별도 실행한 결과다.

```bash
./gradlew --no-daemon --console=plain test \
  --tests dev.jinhyeon.notificationlab.config.AsyncPropertiesTest \
  --tests 'dev.jinhyeon.notificationlab.outbox.AsyncNotificationTest.submissionWindow*' \
  --tests 'dev.jinhyeon.notificationlab.outbox.AsyncNotificationTest.stoppingSubmissionWindow*' \
  --tests 'dev.jinhyeon.notificationlab.outbox.AsyncNotificationTest.interruptedSubmissionWindow*'
```

## 첫 실패도 보존

첫 집중 실행은 **27초/실행11개 중9개 실패**였다. 호환 생성자를 추가하면서 암묵적 record 생성자 바인딩이 적용되지 않아
No default constructor found로 새 앱이 시작하지 못했다. 설정 테스트1개와 Relay 테스트8개가 이 기동 오류에서 실패했다.
업무 시나리오를 실행한 결과로 쓰지 않는다. 명시적 canonical `@ConstructorBinding`을 추가한 뒤 재실행했다.
[실패 목록·당시 소스/로그/XML SHA](experiments/s8-relay-window-2026-11-05-binding-failure.json)를 보존한다.

첫13개 통과 뒤 전체 회귀를 시작했지만, 코드 검토에서 timeout 이후 주기 사이에 미완료 future를 잊으면 새 제출이 추가될 수 있음을 확인했다.
208개 PASS/실패0을 관측한 시점에 해당 실행을 의도적으로 중단했다. 전체 성공이나 테스트 실패로 기록하지 않는다.
[중단 경위](experiments/s8-relay-window-2026-11-05-interrupted-build.json)와
[보강 전 집중 소스](experiments/s8-relay-window-2026-11-05-focused-before-unresolved-guard.json)를 보존했다.
최종 구현은 미완료 future를 최대 한 창만큼 다음 주기에도 보존해 해소 전 추가 제출을 막는다.

후속 회귀는331개PASS/실패0을 관측한 시점에 설정 테스트 helper의 탐색 범위를 분리하려고 중단했다.
helper의 `@Configuration` stereotype을 제거하고 ApplicationContextRunner가 `@EnableConfigurationProperties`를 명시적으로 불러오게 했다.
앱 통합 테스트에서는 해당 helper가 자동 탐색으로 등록되지 않았음을 확인한다. 운영 코드는 바꾸지 않았다.
[중단 경위](experiments/s8-relay-window-2026-11-05-fixture-isolation-interrupted-build.json)와
[분리 전 집중 소스](experiments/s8-relay-window-2026-11-05-focused-before-fixture-isolation.json)를 보존한다.
분리 후 최종 집중은 **32초/종료코드0/13개 통과, 실패·오류·스킵0**이었다.

## 최종 전체 회귀

최종 `./gradlew --no-daemon --console=plain build`는 **16분30초/종료코드0**이었다.
기본 test를 새로 실행했으며 콘솔과 JUnit XML47개 클래스에서 **379개 통과, 실패·오류·스킵0**을 대조했다.
기존368개+신규 설정1개/Relay10개이고 WorkerConcurrencyTest11개도 다시 통과했다.
집중 검증의 소스165개 SHA는 전체 회귀 후에도 같았고 별도 opt-in 부하 실험은 실행하지 않았다.
알림/HTTP Mock 실행 JAR task는 UP-TO-DATE였으며 해당0.3.0 산출물 SHA를 기록했다.
기존0.2.0 JAR는 삭제하지 않고 이번 빌드의 검증 산출물과 구분했다.
[최종 전체 검증·JUnit/소스/JAR SHA](experiments/s8-relay-window-2026-11-05-validation.json)를 보존한다.
새 원격 CI·푸시 결과는 아니다.

## 지표와 다음 단계

`outbox.publish.unconfirmed`는 새 창 경로에서 get timeout으로 관측한 future 수다.
interrupt·DB 예외 등으로 남은 모든 lease의 개수나 실제 브로커 유실 수가 아니다.
`outbox.published`는 token 조건을 통과해 DB에 기록한 행 수다.
새 창의 relay.ack timer는 제출 시작부터 해당 future를 drain한 시점까지의 관찰 시간이다.
다른 claim과 앞선 ACK 대기가 포함되므로 producer 자체 ACK latency나 기존 순차 ACK timer의 직접 대조값으로 쓰지 않는다.
창 끝마다 완료 저장을 flush하므로 publish-batch-size보다 창이 작으면 실제 묶음도 작아진다.

다음은 **같은 producer/Consumer/pool/quota·payload에서 제출창1 vs10의 실제 HTTP 경로 비교**다.
먼저 창만 바꾸고 linger/압축·완료 DB 묶음은 고정한다. batching이 실제로 생겼는지 records/request 등으로 확인한다.
처리량·API p95/p99·DB/CPU·적체·정합성을 함께 보고 기본값 변경 여부를 판단한다.
이번에는 대형 부하, 새 원격 CI, 서버/LB 배포, 실제 강제 프로세스 종료를 실행하지 않았다.

공식 참고:
- [Spring Kafka3.3 KafkaTemplate와 CompletableFuture](https://docs.spring.io/spring-kafka/reference/3.3/kafka/sending-messages.html)
- [Kafka3.9.2 producer send·buffer 대기·응용 재발행 한계](https://kafka.apache.org/39/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html)
