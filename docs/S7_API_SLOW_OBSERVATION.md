# S7 긴 접수 호출 우선 DB 관찰

2026-11-03 한국 시간. 시작03ac275/main.
[같은 연결·호출 구간의 관찰](S7_API_DB_OBSERVATION.md)의 후속이다.
운영 Java/resources, 기본 RECORD, 발송 예산, 풀, 격리 수준은 그대로다.

## 문제와 선택

앞선 자연 ON 실험은87개 요청 중4개만 관측했고22개 조회 중18개가 호출 종료 뒤 돌아와 버려졌다.
이번에는 호출 시작 후 경과시간을 보고20ms 이상인 execute/commit만 별도 연결로 조회한다.
이 기준은 원인 판정이나 서비스 SLA가 아니라 관찰 예산을 배분하는 실험 조건이다.

- `-PdbMinCallMillis=0`: 기존 즉시 조회. 생략 시에도0으로 유지한다.
- `-PdbMinCallMillis=20`: 호출이 아직 활성 상태이고20ms 이상 경과했을 때만 조회한다.
- 두 값만 허용한다. ON/OFF와 별도 조건이며 이번 자연 비교는 모두ON이다.
- 각 호출 시작 시 단조 시계로 새 기준을 잡는다. 다음 SQL이나 같은 연결을 재사용한 다음 요청에 나이를 넘기지 않는다.
- 조회가 돌아왔을 때 같은 호출 객체가 활성 상태인지 다시 확인하는 기존 폐기 규칙을 유지한다.
- 주기20ms fixed delay/전체512회/요청 보관8개/연결1개/오류3회 중단/timeout과 종료 상한은 유지한다.
  저장 한도에 도달한 뒤에도 기존처럼 조회할 수 있어 긴 단일 요청의 예산 소비는 남는다.

`youngPollsSkipped`는 활성 호출이 기준보다 짧아 SQL을 실행하지 않은 주기 수다.
`eligibleCalls`는 종료 시 최소 나이 조건을 충족한 호출 수이며 설정별 분모가 다르다.
비교 가능한 별도 분모 `longCalls`는 두 방식 모두20ms 이상 호출 수다.
`observedLongCalls`는 그중 같은 호출이 활성 상태인 동안 대상 세션을 찾은 표본을 하나 이상 보관한 호출 수다.
`callAgeAtQueryStartMillis`는 해당 조회를 시작하기 직전 호출 나이다. 서버 실행시간이나 DB 대기시간이 아니다.
조회 횟수·시간은 실행 중의 주기 관찰만 집계한다. 두 방식 공통으로 수행하는 시작 시 활성 설정 검증 SELECT 2회와 연결 생성 비용은 별도다.

긴 호출도 주기 사이에 끝나거나 관찰 쿼리보다 빨리 끝나면 놓친다.
특히20ms 문턱과20ms 주기를 함께 쓰면 최초 조회가 대략20~40ms 이후에야 가능하며 스케줄 지연은 더해질 수 있다.
이론상100% 커버리지를 보장하지 않는다. 세션 부재, 조회 실패, 늦은 표본, 보관 한도, 예산 소진은 그대로 구분한다.

## 검증 방법

선택 단위17개(기존15+새2)는6초/종료 코드0/실패·오류·스킵0으로 통과했다.
가짜 단조 시계로600개의20ms 미만 호출이 조회 예산을 쓰지 않는지, 정확히20ms 경계의 관찰,
주기 없이 끝난 긴 호출의 미관찰 분모, 다음 호출의 나이 초기화, 같은 연결 재사용 중 늦은 결과 폐기를 검증했다.

```bash
caffeinate -i ./gradlew --no-daemon --console=plain apiSessionProbe -PdbMinCallMillis=0
caffeinate -i ./gradlew --no-daemon --console=plain apiSessionProbe -PdbMinCallMillis=20
caffeinate -i ./gradlew --no-daemon --console=plain apiDbObservationPilot -PdbObservation=ON -PdbMinCallMillis=0
caffeinate -i ./gradlew --no-daemon --console=plain apiDbObservationPilot -PdbObservation=ON -PdbMinCallMillis=20
```

각 실행은 새 격리 Testcontainers에서 순서대로1회씩 비교한다. 동시에 실행하지 않는다.
제어군은 정상/Outbox gap 잠금/네이티브 commit 완료 후 호출자 보류3조건, 접수6건/공급자0이다.
자연 실험은 캠페인1000건/COUNT_TIME, 실시간RECORD/기본50건+혼합 순차 probe다.
MySQL8.4.8 2CPU/2GiB/buffer512MiB, Kafka4.0.2 1CPU/1GiB, Redis7.4.11 1CPU/256MiB, JVM512MiB를 유지한다.
Worker3/역할별pool4/HTTP3/Redis3/queue0, 캠페인Relay2/완료저장10/200/s·burst20,
실시간Relay1/완료저장1/800/s·burst80, Mock SUCCESS1ms를 유지한다.
짧은 실험은 진단용이며10만 건 tail 재현이나100만 건 성공 검증을 대신하지 않는다.

## 실제 결과

### 실제 MySQL 제어군

두 task는각11초/종료 코드0/테스트1개 통과/실패·오류·스킵0이었다.

| 항목 | 즉시0ms | 긴 호출20ms |
| --- | ---: | ---: |
| 관찰 조회 | 20 | 18 |
| 짧아서 건너뛴 주기 | 0 | 3 |
| 20ms 이상 호출/관측 호출 | 2 / 2 | 2 / 2 |
| 늦어 버린 표본 | 0 | 1 |
| 보관한 잠금 표본 | 8 | 8 |
| 관찰 호출 합계/최대 ms | 45.004 / 3.775 | 41.272 / 3.662 |

두 방식 모두 정상은NOT_OBSERVED, 잠금과commit후호출자보류는OBSERVED였다.
실제 잠금에서 미커밋3개 테이블의비가시성과대상연결잠금표본을 함께 확인했다.
commit후보류는네이티브commit완료/행가시성/대기관계0/호출자미완료를 확인했다.
두 실행 모두observer오류0, 예산 미도달, 종료 후열린요청0/대기작업0/스레드종료를 확인했다.
제어군의긴호출2/2는진단경계의정확성 검증이며 자연환경의100% 커버리지 주장에 쓰지 않는다.


### 자연 혼합 실험

| 항목 | 즉시0ms | 긴 호출20ms |
| --- | ---: | ---: |
| task 시간 | 42초 | 45초 |
| 캠페인 완료초 | 10.920 | 8.247 |
| 전체 접수/혼합 접수 표본 | 95 / 45 | 86 / 36 |
| 혼합 API p95 / max ms | 62.485 / 244.988 | 13.639 / 17.195 |
| observer 조회 / 늦은 표본 폐기 | 27 / 21 | 0 / 0 |
| 짧아서 건너뛴 주기 | 0 | 20 |
| 관측한 접수/전체 접수 | 6 / 95 | 0 / 86 |
| 20ms 이상 호출/관측 호출 | 0 / 0 | 0 / 0 |
| observer 합계/최대 ms | 117.799 / 22.719 | 0 / 0 |

각 task는 테스트 1개 통과, 종료 코드 0, 실패·오류·스킵 0이었다.
두 실행 합계 캠페인 2,000건과 실시간 181건, HTTP 2,181건의 대상·발송 이력 1회·Outbox PUBLISHED·HTTP 감사·partition별 offset=end·quota를 전수 대조했다.
중복·잘못된 매핑·Mock 제한 및 오류는 0이었다. API와 관찰 행의 요청 수 및 BASELINE/MIXED 순서 181개가 일치했다.
각 접수는 SQL 4회, 연결 획득·commit·반환 1회, rollback 0회를 유지했다.
observer 오류·열린 요청·대기 작업은 0이고 스레드 종료를 확인했다. 역할별 pool 종료와 activeCalls 0도 확인했다.

이번 자연 실험에서 20ms 이상 execute/commit은 두 방식 모두 0개였다. 따라서 긴 호출 커버리지 분모가 0이며,
0/0을 100%나 대기 없음으로 표현하지 않는다. 새 방식은 짧은 활성 호출에 대한 20개 주기를 건너뛰어 주기 관찰 조회가 0회였다.
즉시 방식의 27회 중 21회는 늦게 돌아와 버렸고, 보관한 6개는 모두 20ms 미만 호출이었다.
조건에 맞지 않는 조회에 예산을 쓰지 않았다는 결과이며, 자연 긴 호출의 커버리지 개선은 미검증이다.
각 1회·공유 호스트·순차 probe·적은 표본·관측 선택 편향이 있어 p95 차이를 성능 개선이나 계측 비용의 인과 효과로 주장하지 않는다.

즉시 방식의 최대 접수 244.988ms는 서버 234.376ms, store 228.247ms였다. SQL은 각각 11.572/2.539/0.833/0.865ms,
commit은 18.109ms였다. AUTOCOMMIT_STATE 합계 127.318ms(3호출), PREPARE 합계 32.544ms(4호출), ACQUIRE 0.009ms였다.
이는 해당 요청의 계측 구간 기록이다. 어떤 setAutoCommit/getAutoCommit 호출이 느렸는지,
서버·네트워크·호스트 스케줄 중 어디에서 대기했는지는 분리하지 않았다. 합계를 단일 호출이나 순수 DB 시간으로 해석하지 않는다.
느린 요청 전체를 execute/commit 지연으로 간주하면 이 구간을 놓친다.

## 판단과 다음 단계

긴 호출 선별 기능, 예산 보존, 제어군 관측 경계는 검증했다. 자연 긴 호출의 커버리지 개선과 기존 10만 건 tail 원인은 미확정이다.
다음 작업은 AUTOCOMMIT_STATE를 메서드별로 분리해 실제 트랜잭션 진입·복원 경계와 대응하는 작은 검증이다.
그 뒤 필요할 때만 관찰 대상을 확장한다. 풀·인덱스·격리 수준·기본 정책은 근거 없이 바꾸지 않는다.
100만 건 FAIL 원본을 유지했다. 10만·100만 건 재실행, 실제 공급자, 유료 자원, 푸시는 실행하지 않았다.

[고정 소스 해시](experiments/s7-api-db-slow-2026-11-03-source.json),
[원본·실행 명령·검증 결과](experiments/s7-api-db-slow-2026-11-03-execution.json)를보존했다.
관찰테이블과연결대응의공식참고는[앞선설계문서](S7_API_DB_OBSERVATION.md)를따른다.

## 전체 회귀와 보존 확인

최종 `caffeinate -i ./gradlew --no-daemon --console=plain build`는 **14분 18초, 종료 코드 0**이었다.
새 39개 클래스의 JUnit XML과 콘솔에서 **314개 통과, 실패·오류·스킵 0**을 대조했다.
기존 312개와 새 단위 2개이며, 별도 제어군·자연 실험 각 2회와 구분한다.
테스트는 재실행했고 bootJar는 UP-TO-DATE였다. 원격 CI 결과로 표현하지 않는다.
고정 소스 7개 해시, 운영 Java/resources diff 0, 100만 건 FAIL 원본 SHA 불변을 확인했다.
Git identity를 유지했고 원격 main 9cf1786은 읽기 전용으로 확인했다.
최종 Docker에는 기존 notification-lab-mysql-1만 healthy로 남았고 임시 검증 컨테이너는 정리됐다.
