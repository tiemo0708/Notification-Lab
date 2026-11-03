# S7 접수 연결 상태 변경의 메서드·경계별 진단

2026-11-03 한국 시간. 시작6edb1b1/main.
[긴 호출 우선 관찰](S7_API_SLOW_OBSERVATION.md)에서 최대 접수244.988ms 중
AUTOCOMMIT_STATE에127.318ms가 기록됐다. 세 호출의 합계여서 어느 메서드가 느렸는지는 알 수 없었다.
이번에는 테스트 계측을 분리하고 실제 트랜잭션 제어군과 작은 자연 실험에서 대응을 확인한다.
운영 Java/resources, 기본 RECORD, 풀, 격리 수준, 발송 예산은 유지한다.

## 무엇을 구분하는가

| 기록 | 정상 접수에서 기대하는 경계 | 의미 |
| --- | --- | --- |
| GET_AUTO_COMMIT | BEFORE_FIRST_SQL | 현재 auto-commit 상태 조회 |
| SET_AUTO_COMMIT_FALSE | BEFORE_FIRST_SQL | 수동 트랜잭션 모드로 전환 |
| SET_AUTO_COMMIT_TRUE | AFTER_COMMIT | 커밋 후 연결 상태 복원 |

Spring6.2.19의 doBegin은 기존 상태가true이면 복원을 표시하고 setAutoCommit(false)를 호출한다.
doCleanupAfterCompletion은 복원 표시가 있으면 setAutoCommit(true)를 호출한 뒤 연결을 반환한다.
[Spring 버전 고정 소스](https://github.com/spring-projects/spring-framework/blob/v6.2.19/spring-jdbc/src/main/java/org/springframework/jdbc/datasource/DataSourceTransactionManager.java)와 로컬 sources.jar를 대조했다.

경계는 테스트 JDBC proxy에서 관찰한 순서다. 첫 execute가 시작되면 BEFORE_COMPLETION,
인자 없는 commit/rollback이 정상 반환되면 AFTER_COMMIT/AFTER_ROLLBACK으로 바뀐다.
SQL 원문이나 트랜잭션 내부 객체를 읽지 않으며, savepoint rollback을 전체 rollback 완료로 기록하지 않는다.
서버의 정확한 트랜잭션 시각이나 물리 패킷을 추적한 trace는 아니다.

Connector/J9.7.0의 getAutoCommit은 connection lock을 잡고 이미 보관한 server session 상태를 읽는다.
setAutoCommit은 설정과 상태에 따라 내부적으로 SET autocommit 명령을 보낼 수 있다.
[MySQL 드라이버 버전 고정 소스](https://github.com/mysql/mysql-connector-j/blob/9.7.0/src/main/user-impl/java/com/mysql/cj/jdbc/ConnectionImpl.java)와 로컬 sources.jar를 대조했다.
따라서 기존 EXECUTE4회는 애플리케이션의 DML4회이며, 드라이버 내부 제어 명령을 포함한 서버 전체 SQL 횟수가 아니다.
메서드 wall time을 순수 서버 SQL 시간이나 네트워크 시간으로 해석하지 않는다.

## 계측 범위와 상한

기존 AUTOCOMMIT_STATE 합계와 횟수를 유지하고, 추가로 메서드·경계·순번·밀리초·실패 여부만 기록한다.
호출 인자 전체, 연결ID, SQL, 드라이버 오류 메시지, 수신자, 본문은 보관하지 않는다. true/false는 고정 메서드 분류에만 사용한다.
기존 순차 API probe1개/요청 최대5000개를 유지하고, 요청당 상태 호출은 최대8개다. 상한이나 예상 경계를 위반하면 실험을 실패시킨다.
성공 접수에서 세 메서드의 순서·경계와 기존 합계 횟수를 대조하고, 상세 시간 합계가 바깥 AUTOCOMMIT_STATE 시간보다 크지 않음을 확인한다.

상세 계측은 기존 타이머 안에서 실제 메서드를 한 번만 호출한다. 추가 조회나 재시도를 하지 않는다.
외부 관찰 SQL이나 샘플러 스레드를 추가하지 않는다. 자연 OFF도 기존 요청별 합계 계측을 계속 사용한다.

```bash
caffeinate -i ./gradlew --no-daemon --console=plain apiConnectionProbe
caffeinate -i ./gradlew --no-daemon --console=plain apiSessionProbe -PdbMinCallMillis=20
caffeinate -i ./gradlew --no-daemon --console=plain apiConnectionPilot -PconnectionCalls=OFF
caffeinate -i ./gradlew --no-daemon --console=plain apiConnectionPilot -PconnectionCalls=ON
```

자연 task는 정확히 캠페인1000건/COUNT_TIME, 실시간RECORD, 단독50개+응답 후200ms 순차 혼합 probe다.
MySQL8.4.8 2CPU/2GiB/buffer512MiB, Kafka4.0.2 1CPU/1GiB, Redis7.4.11 1CPU/256MiB, JVM512MiB를 유지한다.
역할별 Worker3/pool4/HTTP3/Redis3/queue0, 캠페인 Relay2/완료 저장10/200/s·burst20,
실시간 Relay1/완료 저장1/800/s·burst80, Mock SUCCESS1ms를 유지한다.
새 격리 인프라에서 같은 소스로 OFF→ON 각1회씩 순차 실행한다.

## 실제 검증 결과

선택 단위20개는6초/종료 코드0/실패·오류·스킵0으로 통과했다.
정상 접수의 세 메서드와 commit 후 복원, SQL 오류 후 rollback과 AFTER_ROLLBACK 복원,
진입 상태 변경 실패의 원래 예외 보존과 본체 미실행, 상세 누락·잘못된 완료 경계·8개 상한을 검증했다.

### 실제 MySQL 제어군

apiConnectionProbe는11초/테스트1개 통과/종료 코드0/실패·오류·스킵0이었다.
실제 SubmissionService/JdbcTransactionManager를 사용하고 정상/진입 전 보류/복원 후 보류 세 조건을 실행했다.
warmup3+측정3=접수6건이며 공급자 호출0이다. 지연은200ms latch 제어로 주입했다.

| 조건 | 전체 호출 ms | GET_AUTO_COMMIT ms | SET_FALSE ms | SET_TRUE ms | 보류 중 committed 행 |
| --- | ---: | ---: | ---: | ---: | --- |
| 정상 | 7.764 | 0.011 | 0.326 | 0.260 | 해당 없음 |
| 실제 SET_FALSE 진입 전 보류 | 217.671 | 0.013 | 206.024 | 0.677 | 증가 없음 |
| 실제 SET_TRUE 정상 반환 후 보류 | 216.230 | 0.013 | 0.455 | 209.287 | 세 테이블 모두1개 증가 |

진입 보류는 BEFORE_FIRST_SQL의 SET_FALSE에서, 복원 보류는 AFTER_COMMIT의 SET_TRUE에서 긴 시간을 확인했다.
두 조건 모두 호출자는 아직 미완료였고, 관찰 순간 DB 대기 관계는0이었다. 복원 보류에서는 이미 커밋된 행을 별도 연결에서 확인했다.
합성 제어군은 분류의 정확성을 검증한다. 과거 자연 지연의 원인이나 실제 OS 스케줄·네트워크 장애를 재현한 것은 아니다.
원본의 native는 계측 proxy 아래 원래 JDBC(Hikari) 메서드를 뜻한다. C네이티브 코드나 정확한 서버 시각을 측정한 것은 아니다.

기존 세션 관찰 apiSessionProbe(min20ms)도11초/테스트1개 통과/종료 코드0/실패·오류·스킵0이었다.
잠금 표본8개/조회17회/오류0, 종료 후 열린 요청0/대기 작업0/샘플러 스레드 종료를 확인했다.

### 자연 혼합 OFF/ON

| 항목 | 상세 OFF | 상세 ON |
| --- | ---: | ---: |
| task 시간 | 41초 | 39초 |
| 캠페인 완료초 | 8.594 | 8.257 |
| 전체/혼합 접수 표본 | 88 / 38 | 86 / 36 |
| 혼합 API p95 / max ms | 11.962 / 12.730 | 13.744 / 14.392 |
| AUTOCOMMIT_STATE 최대 합계 ms | 3.506 | 3.265 |
| 상세 호출 수 | 0 | 258 |

각 task는 테스트1개 통과/종료 코드0/실패·오류·스킵0이었다.
캠페인2000+실시간174=HTTP2174건의 대상 매핑·시도 이력1회·Outbox PUBLISHED·HTTP 감사·partition별 offset=end·quota를 전수 대조했다.
중복·잘못된 매핑·Mock 제한 및 오류는0이었다. SQL4회/획득·commit·반환1회/rollback0/상태 호출3회를 유지했다.
역할별 pool 종료, consumer3개 wrapping, activeCalls0을 확인했다.

ON의86개 접수 모두 세 메서드와 경계가 일치했고, 상세 시간 합계가 바깥 합계보다 크지 않았다.
각 메서드 최대는 GET_AUTO_COMMIT0.239ms/SET_FALSE2.765ms/SET_TRUE1.585ms였다.
앞선127.318ms 상태 합계는 재현하지 못했다. 서로 다른 실행의 최대값으로 과거의 느린 메서드를 추정하지 않는다.
ON 전체 최대 접수86.389ms는 BASELINE에서 서버41.025ms/store7.907ms/상태 합계1.090ms였다.
나머지 시간의 원인은 이번 상세 계측만으로 분해하지 않았다. 상태 변경이 항상 접수 전체 지연을 설명하는 것은 아니다.
각1회·공유 호스트·순차 probe·적은 혼합 표본으로 OFF·ON p95 차이를 계측 비용의 인과 효과나 성능 개선으로 판단하지 않는다.

## 판단과 다음 단계

상태 합계를 세 메서드와 관찰 경계로 분리하는 도구, 오류·복원 정확성을 검증했다.
과거10만 건p95/100만 건timeout/127ms 상태 합계의 원인은 여전히 미확정이다.
다음은 현재 계측을 고정한 제한된1만 건 반복에서 긴 상태 호출의 표본을 확보할 수 있는지 판단하는 단계다.
기존100만 건FAIL 원본을 유지했다. 10만·100만 건 재실행, 실제 공급자, 유료 자원, 푸시는 실행하지 않았다.

[고정 소스](experiments/s7-api-connection-2026-11-03-source.json)와
[원본·실행 메타데이터](experiments/s7-api-connection-2026-11-03-execution.json)를 보존했다.

## 전체 회귀와 보존 확인

최종 `caffeinate -i ./gradlew --no-daemon --console=plain build`는 **14분14초/종료 코드0**이었다.
새39개 클래스의 JUnit XML과 콘솔에서 **317개 통과/실패·오류·스킵0**을 대조했다.
기존314개와 새 단위3개다. 별도 제어군·기존 세션 회귀·자연 OFF/ON의 각1개 테스트와 구분한다.
기본 회귀는 재실행했고 bootJar는 UP-TO-DATE였다. 원격 CI 결과가 아니다.
고정 소스9개 해시와 운영 Java/resources diff0, 기존100만 건FAIL 원본 SHA 불변을 확인했다.
Git identity를 유지했고 원격 main9cf1786은 읽기 전용으로 확인했다.
최종 Docker에는 기존 notification-lab-mysql-1만 healthy로 남았고 임시 검증 컨테이너는 정리됐다.

후속 [고정 계측의1만 건 반복](S7_API_CONNECTION_10K.md)부터 `-PdeliveryTargets=10000`을 허용한다. 생략 시1000을 유지한다. 두 실행 모두127ms 상태 합계는 미재현이다.
