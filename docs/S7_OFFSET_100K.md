# S7 offset 오류 경계와 10만 건 비교

2026-11-02. 시작651f8d7/main. [1만 건 비교](S7_OFFSET_COMMIT_PILOT.md)의 후속이다.
완료 offset의 COUNT_TIME 묶음 저장을 테스트에서만 선택한다. 운영 Java/resources와 기본 RECORD는 유지한다.

## 문제와 판단 기준

1만 건에서는 동기 offset 저장 호출이 약78~80% 줄었지만 재시도/오류 경계와 더 긴 실시간 혼합 부하의 확인이 남았다.
호출 수 감소만으로 기본 정책을 바꾸지 않는다. 완료 전 offset 보존, 실패 뒤 복구, HTTP/DB/파티션별 offset 전수 대조와
실시간 tail을 함께 확인한다. 100만 건 최초 timeout의 원인은 여전히 미확정이다.

## HTTP 재시도와 DLQ

실제 격리 MySQL/Kafka와 HTTP Mock에서 RECORD/COUNT_TIME 각각 다음5개를 실행한다.

| HTTP 조건 | DB 최종 상태 | 시도 / Retry 레코드 / DLQ | Mock 수락 |
| --- | --- | --- | ---: |
| 성공 | PROVIDER_ACCEPTED | 1 / 0 / 0 | 1 |
| 500 | RETRYABLE_FAILURE | 3 / 2 / 1 | 0 |
| 429 | RETRYABLE_FAILURE | 3 / 2 / 1 | 0 |
| 영구 실패400 | PERMANENT_FAILURE | 1 / 0 / 1 | 0 |
| 응답700ms / 클라이언트200ms timeout | RETRYABLE_FAILURE / HTTP_UNKNOWN_TIMEOUT | 3 / 2 / 1 | 3 |

각각 DB 시도 이력/Outbox PUBLISHED/요청·Retry의 각 partition committed=end와 DLQ 레코드 수를 확인한다.
timeout의 Mock 수락3회는 클라이언트가 성공을 모르는 외부 호출의 중복 가능성이다. COUNT_TIME이 이를 제거하지 않는다.
DLQ는 별도 소비자가 없으므로 레코드 수를 대조하고 DLQ 소비 offset 완료를 주장하지 않는다.

## 실제 RPC 앞뒤의 오류 주입

warmup20건 뒤12건을 partition0에 ACK 확인 발행한다. API의 Relay는 끄고 consumer1개로 경계를 분리한다.
이미 처리한 여러 레코드의 commitSync 직전에 DB 트랜잭션 밖임을 확인하고 다음을 주입한다.

- HOLD: native RPC 전에 대기시키고 JVM을 강제 종료한다.
- BEFORE_RPC: native RPC를 호출하지 않고 Kafka TimeoutException을 던진다.
- AFTER_RPC: 실제 native commitSync 성공 후 호출자에게 동일한 TimeoutException을 던진다.

오류 주입 후 추가 commitSync는 계속 거절해 종료/retry가 관측 경계를 덮어쓰지 않게 한다.
두 오류 case는 consumer group 멤버가 모두 나갈 때까지 기다린다. DB 완료/브로커 offset을 독립 조회하고 프로세스를 종료한다.
같은 group/topic의 새 JVM이 정상 재할당으로 복구한다. seek/reset/재발행을 사용하지 않는다.
이미 완료된 notification/task/시도 행의 fingerprint가 같고 전체32건의 공급자 호출/성공 이력이 각각32건인지 확인한다.

실제 브로커 RPC를 사용하지만 **클라이언트 경계에 오류를 넣는 합성 실험**이다. 물리 네트워크 단절이나 모든 응답 유실을 재현하지 않는다.
AFTER_RPC의 오류만 보고 커밋 실패를 단정할 수 없다. 재시작 위치는 브로커가 보관한 offset으로 결정된다.

| 사전 선택 실행 | 이전 성공 offset | DB 완료 / 시도한 offset | 브로커 관측 | 완료 재소비 / 신규 호출 |
| --- | ---: | ---: | ---: | ---: |
| BEFORE_RPC | 22 | 28 | 22 | 6 / 4 |
| AFTER_RPC | 22 | 29 | 29 | 0 / 3 |

각32건의 호출/성공 시도32개, 완료 행 불변/최종offset32를 확인했다.
[이전 RPC 오류](experiments/s7-offset-100k-2026-11-02-before_rpc-selected.json),
[응답 유실](experiments/s7-offset-100k-2026-11-02-after_rpc-selected.json),
[강제 종료](experiments/s7-offset-100k-2026-11-02-safety-selected.json) 원본을 수정 없이 보존한다.
HTTP10개/프로세스4개/상한7개 **21개 통과, 실패·오류·스킵0, 2분47초/종료 코드0**였다.

## 전체 기본 회귀

`caffeinate -i ./gradlew --no-daemon --console=plain build`는 **BUILD SUCCESSFUL16분2초/종료 코드0**이었다.
37개 클래스의 새 JUnit XML과 콘솔에서 **302개 통과/실패·오류·스킵0**을 대조했다.
기존294개에 HTTP5개/커밋 오류2개/실험 상한1개를 추가했다. 기본 회귀는 실제 재실행했고 두 실행 JAR task는UP-TO-DATE였다.
운영 Java/resources diff0과 실행 뒤 고정 소스 해시 일치를 확인했다.

| 전체 회귀 | 이전 성공 offset | DB 완료 / 시도 offset | 브로커 관측 | 완료 재소비 / 신규 호출 |
| --- | ---: | ---: | ---: | ---: |
| BEFORE_RPC | 21 | 28 | 21 | 7 / 4 |
| AFTER_RPC | 22 | 27 | 27 | 0 / 5 |
| HOLD | 22 | 27 | 22 | 5 / 5 |

[RPC 전 오류](experiments/s7-offset-100k-2026-11-02-before_rpc-build.json),
[RPC 후 응답 유실](experiments/s7-offset-100k-2026-11-02-after_rpc-build.json),
[강제 종료](experiments/s7-offset-100k-2026-11-02-safety-build.json) 원본이다.
세 경우 모두 전체32건의 공급자 호출/성공 시도32개, 완료 행 불변/최종offset32를 재확인했다.
재소비 건수의 시점 편차를 그대로 기록한다.

## 10만 건 실행 조건

명시적 `offsetCommitLoad`만 정확히100000건을 허용한다. 기본 test/check/build에서 부하 task를 실행하지 않는다.
새 JVM·격리 인프라에서 RECORD→COUNT_TIME 각1회이며 두 조건의 소스를 고정한다.
[소스 SHA-256](experiments/s7-offset-100k-2026-11-02-source.json)을 실행 전후 대조한다.

- 캠페인Relay2/완료 저장 최대10건, 실시간Relay1/완료 저장1건/RECORD.
- 역할별Worker3/HTTP3/Redis3/Hikari4/queue0, 요청·Retry 각3파티션.
- 총1000/s·burst100, 캠페인200/s·burst20, 실시간800/s·burst80.
- HTTP Mock SUCCESS/1ms/max-in-flight8. 단독50건 뒤 응답+200ms 순차 실시간 probe, 접수 제한5초.
- COUNT_TIME10개/100ms, syncCommits/asyncAcks=false/autoCommit=false/max.poll.records1/poll500ms.
- 발송3600초/실시간 probe20000개/자원 표본1802개/조회 페이지1000행의 기존10만 건 상한.
- Java21/Boot3.5.16/Gradle8.14.5/JVM512MiB, MySQL8.4.8(2CPU/2GiB/buffer512MiB),
  Kafka broker4.0.2(1CPU/1GiB)/client3.9.2, Redis7.4.11(1CPU/256MiB).
- 두 조건 모두 기존 세부 JDBC/consumer 계측. 캠페인 ack 정책만 바꾼다.

```bash
caffeinate -i ./gradlew --no-daemon --console=plain offsetCommitLoad -PoffsetMode=RECORD
# 첫 원본을 보존한 뒤 별도 실행
caffeinate -i ./gradlew --no-daemon --console=plain offsetCommitLoad -PoffsetMode=COUNT_TIME
```

출력은 `build/reports/offset-commit-load/MODE-targets-100000.json`이다.
각 실행 뒤 새 이름으로 원본을 보존한다. 100만 건을 이 task로 실행하지 않는다.
전체 HTTP 대상 매핑/DB 성공 시도1회/Outbox PUBLISHED/각 partition committed=end/quota를 대조한다.
각 역할의 pool 종료와 consumer3개 wrapping/activeCalls0, 비동기 커밋0도 확인한다.

100ms는 처리/poll 사이 확인 기준이며 엄격한 마감이 아니다. 마지막 offset 반영에는 처리/idle poll/스케줄링/RPC가 더해진다.
consumer3개와 반복 장애에서 총 재소비를10건으로 보장하지 않는다.
공유 호스트/조건별1회/closed-loop 관측으로 운영 처리량이나 통계적 유의성을 확정하지 않는다.
Mock 수락은 단말 도착이 아니며 공급자 수락 뒤 DB 실패의 exactly-once도 보장하지 않는다.

## 참고

- [Spring Kafka3.3 listener container](https://docs.spring.io/spring-kafka/reference/3.3/kafka/receiving-messages/message-listener-container.html): RECORD/COUNT_TIME과 동기 offset 저장 경계.
- [Spring Kafka3.3 오류 처리](https://docs.spring.io/spring-kafka/reference/3.3/kafka/annotation-error-handling.html): 오류 처리와 컨테이너 중지.
- [실제3.3.16 소스](https://github.com/spring-projects/spring-kafka/blob/v3.3.16/spring-kafka/src/main/java/org/springframework/kafka/listener/KafkaMessageListenerContainer.java): processCommits/doCommitSync와 종료 경계.
- [Kafka3.9 Consumer API](https://kafka.apache.org/39/javadoc/org/apache/kafka/clients/consumer/Consumer.html): 실제 의존성3.9.2의 commitSync 동작과 예외.
