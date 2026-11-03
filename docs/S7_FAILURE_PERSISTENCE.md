# S7 실제 HTTP/MySQL 실패 동결과 종료 전 파일 보존

2026-11-03 한국 시간. 시작99ce59d/main.
[고정 용량 기록기와 중단 계약](S7_LONG_RUN_OBSERVATION.md)을 작은 실제 접수 경로에 연결했다.
운영 코드는 바꾸지 않았다. 실제 NotificationController → IdempotentSubmissionService → SubmissionService와
JDBC/MySQL 트랜잭션을 사용하되, Kafka/Relay/Worker/공급자 발송은 없는 **접수 전용 제어 실험**이다.

## 이번에 확인할 질문

1. HTTP timeout을 받았을 때 앱과 pool을 닫기 전에 첫 실패 파일을 저장할 수 있는가?
2. 같은 timeout이라도 commit 전과 commit 후의 DB 가시성을 구분할 수 있는가?
3. 서버가 나중에 끝나거나 저장이 다시 호출돼도 첫 파일이 바뀌지 않는가?
4. 파일 저장이 실패해도 원래 HTTP 예외가 주된 예외로 유지되는가?

이 실험은 실제 지연의 원인을 재현하려는 부하 실험이 아니다. JDBC 호출 주위에 제어용 latch를 넣어 보존 순서와 의미를 검증한다.
100만 건 task와 기존 캠페인 혼합 pilot에는 아직 연결하지 않았으며 이번에 대형 부하는 실행하지 않았다.

## 저장 순서

FailureSnapshotStore는 하나의 기록기와 하나의 출력 파일에 대응한다.

1. 첫 중단 snapshot을 메모리에서 동결한다. 서버가 끝나기를 기다리거나 요청을 재전송하지 않는다.
2. 고정된32+1개 슬롯의 JSON을 만들고, 파일 크기256KiB를 넘으면 쓰기 전에 실패시킨다.
3. 최종 파일과 같은 디렉터리의 임시 파일에 전체 내용을 쓰고 FileChannel.force(true) 후 닫는다.
4. 완성된 임시 파일에 대한 새 hard link를 최종 이름으로 만든다. 기존 이름이 있으면 실패하며 덮지 않는다.
5. 임시 이름을 정리한다. 재호출은 첫 저장 결과를 반환하며 저장 실패도 자동 재시도하지 않는다.

임시 파일을 최종 파일로 옮기는 방식의 기존 대상 교체 문제를 피하기 위해 새 hard link를 사용한다.
두 저장자가 동시에 시도하는 단위 검증에서도 하나만 성공하고 최종 파일은 완전한 JSON이었다.
지원하지 않는 파일시스템에서 안전하지 않은 copy/move 방식으로 우회하지 않는다.

[Java21 Files.createLink](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/nio/file/Files.html#createLink(java.nio.file.Path,java.nio.file.Path))는 선택적 파일시스템 연산이다.
[FileChannel.force](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/nio/channels/FileChannel.html#force(boolean))의 보장 범위도 저장 장치·파일시스템에 의존한다.
이번 검증은 로컬 파일시스템과 앱·컨테이너 정리 후 보존이다. 부모 디렉터리 fsync는 하지 않으므로 전원 장애 보존을 보장하지 않는다.
저장은 동기 I/O이며 비용0이나 고정된 완료 시간을 주장하지 않는다. 임시 이름 정리 실패 시 원본 주된 결과를 바꾸지 않으며 임시 파일이 남을 수 있다.
파일 크기는 직렬화 후 검사한다. 직렬화 입력 자체는 기록기의 고정 구조로 제한하며, 전체 프로세스 메모리 상한을 새로 보장한 것은 아니다.

preserve는 저장 성공 여부·파일명·byte 수 또는 오류 종류만 report에 기록하고 원래 예외 객체를 반환한다.
호출자는 `throw store.preserve(original, reason, report)` 형태로 원래 실패를 유지한다.
저장 실패는 내용이나 경로가 없는 suppressed 예외와 오류 종류로 남기며, raw 경로·예외 메시지·요청/응답 본문을 결과에 넣지 않는다.

## 실제 경로와 자원 상한

```bash
caffeinate -i ./gradlew --no-daemon --console=plain apiFailurePersistenceProbe
```

기본 test/check/build는 이 tag를 제외한다. 명령은 최대4분, 테스트 메서드는180초다.
MySQL8.4.8 2CPU/2GiB/buffer512MiB, pool4/minIdle1, 외부 관찰 연결1개, JVM512MiB를 사용한다.
Tomcat의 실제 maxThreads4/maxConnections16을 읽어 검증하며 acceptCount4다.
검증한 Java는 Amazon.com Inc.21.0.6, 실제 최대 heap536870912bytes였다.

두 조건 각각 새 앱에서 정상 HTTP202 요청40개를 먼저 처리해 최근32건 보관을 확인한다.
이후41번째 요청을 제어한다. 정상 probe의5초 제한은 유지하고, fault fixture만1초 timeout을 사용한다.
제어 gate는 최대10초다. 각각 한 번만 요청하며 provider는 호출되면 즉시 검증 실패다.
기록기의 BASELINE/MIXED 창 라벨을 재사용하지만 여기서 MIXED는 캠페인 혼합 부하를 뜻하지 않는다.

| 조건 | 제어 위치 | 동결된 activePart | commitReturned | 별도 연결에서 보이는 행 |
| --- | --- | --- | --- | --- |
| BEFORE_COMMIT | 실제 JDBC commit 진입 전 | COMMIT | false | warmup까지, 증가 없음 |
| AFTER_RESTORE | 실제 setAutoCommit(true) 반환 후, 계측 반환 전 | AUTOCOMMIT_STATE | true | 해당 요청까지 이미 증가 |

제어 위치는 계측 proxy 아래 Hikari JDBC 메서드 경계이며, 실제 DB 가시성을 별도 연결로 확인한다.
서버는 두 경우 모두 gate에서 대기 중이고 HTTP 클라이언트는 timeout이다. 서버 결과는 첫 파일에서UNKNOWN을 유지한다.
DB 확인은 동결 후의 독립 관찰이며 첫 snapshot 내용을 뒤늦게 고치지 않는다.

## 실제 검증 결과

선택 단위24개는 초기7초/최종4초, 모두 종료 코드0/실패·오류·스킵0이었다.
기존 증거 파일 미덮어쓰기, 실패 후 자동 재시도 없음, 두 저장자의 경합, 원래 timeout 보존과 정리 결과를 확인했다.

실제 HTTP/MySQL 제어 실험은 초기16초/최종15초, 각 테스트1개 통과/종료 코드0/실패·오류·스킵0이었다.
첫 실행 뒤 Java·heap·실제 connector 상한·파일 관찰 시각을 추가했고 저장/트랜잭션 로직은 유지했다.
초기·최종 원본과 소스 해시를 각각 보존했다.

| 최종 결과 | BEFORE_COMMIT | AFTER_RESTORE |
| --- | ---: | ---: |
| 정상 HTTP202 / timeout | 40 / 1 | 40 / 1 |
| 이전 조건의 DB 행 | 0 | 41 |
| 서버 대기 중 DB 행 | 40 | 82 |
| gate 해제 후 DB 행 | 41 | 82 |
| snapshot bytes | 68289 | 68282 |
| 최근 완료 / 진행 중 | 32 / 1 | 32 / 1 |
| 완료 수 / evicted | 40 / 8 | 40 / 8 |
| 앱·pool 종료 전 저장 | 확인 | 확인 |
| 늦은 처리·재저장 후 파일 SHA 불변 | 확인 | 확인 |
| 앱 종료 후 파일 존재 | 확인 | 확인 |

두 조건 합계 HTTP202 관찰80개와 timeout2개, DB에 남은 접수82개다.
각각 notification/delivery_task/outbox_event의 행 수를 대조했고 provider 호출 및 delivery_attempt는0이었다.
Relay를 실행하지 않아82개 Outbox는 PENDING으로 남는 것이 이 fixture의 기대값이다. 발송 성공 검증으로 표현하지 않는다.
두 timeout 모두 클라이언트 관찰은UNKNOWN이지만 DB 가시성은 서로 달랐다. timeout을 미접수로 보고 자동 재전송하면 안 되는 이유를 제어했다.
실제 장시간 timeout이나127ms 지연의 원인을 재현한 것은 아니다.

result=PASS는 예상한 제어 timeout을 검증했다는 뜻이다. 실제 부하 도구에서 timeout을PASS로 바꾸는 변경은 하지 않았다.
아직 기존 Kafka 캠페인 pilot이나100만 건 task의 실패 경로에는 연결하지 않았다. 다음은 작은 혼합 pilot의 opt-in 연결과 보존 실패 처리 검증이다.

[최종 소스](experiments/s7-api-failure-persistence-2026-11-03-source.json),
[초기·최종 원본과 파일명 대응/실행 메타데이터](experiments/s7-api-failure-persistence-2026-11-03-execution.json)를 보존했다.
원본 report의 snapshotFile은 당시 출력 디렉터리 기준이며 영구 보존 사본의 이름은 메타데이터 artifactMapping에서 대응한다.

## 전체 회귀와 보존

최종 `caffeinate -i ./gradlew --no-daemon --console=plain build`는 **15분3초/종료 코드0**이었다.
새41개 클래스의 JUnit XML과 콘솔에서 **329개 통과/실패·오류·스킵0**을 대조했다. 기존325개와 새 파일 저장 단위4개다.
기본 회귀는 실제 재실행했고 bootJar는UP-TO-DATE였다. 별도 opt-in 제어 테스트1개와 구분하며 원격CI 결과가 아니다.
고정 소스8개, 운영Java/resources diff0, 기존100만 건FAIL 원본SHA 불변을 확인했다.
기존 Git identity와 원격main9cf1786을 읽기 전용으로 확인했다. 최종 Docker에는 기존notification-lab-mysql-1만healthy로 남았고 푸시는 하지 않았다.
