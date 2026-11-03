# S7 접수 요청과 같은 DB 연결의 관찰 대응

2026-11-03 한국 시간. 시작cfdab26/main.
[요청별 접수 시간](S7_API_ADMISSION_TIMING.md)과 [원인별 대기 제어군](S7_API_WAIT_CALIBRATION.md)의 후속이다.
이번에는 실제 혼합 요청이 사용 중인 물리 연결을 별도 관찰 연결에서 조회한다.
운영 Java/resources/기본 RECORD/풀·격리 수준/발송 예산은 변경하지 않는다.

## 연결 번호만 같으면 같은 요청인가

아니다. Hikari 연결이 반환되면 Relay/Worker나 다음 API 요청이 같은 물리 연결을 재사용할 수 있다.
관찰 쿼리를 시작할 때의 연결 번호만 보고 결과를 나중에 붙이면 다른 작업의 대기를 잘못 해석할 수 있다.

테스트 계측은 다음 순서를 사용한다.

1. 접수 트랜잭션이 연결을 획득하면 Hikari의원래 연결을 `JdbcConnection`으로 unwrap하고 `getId()`로 이미 캐시된 서버 연결 번호를 읽는다.
   실제 Connector/J9.7.0 소스에서 `session.getThreadId()`를 반환함을 확인했다. API 연결에 `SELECT CONNECTION_ID()`를 추가하지 않는다.
2. 각 JDBC execute/commit 호출 직전에 새 호출 구간 객체를 만든다. SQL1~4와commit을 서로 다른 객체로 구분한다.
3. 별도 observer 연결은 그 물리 연결 번호와 일치하는 `performance_schema.threads.PROCESSLIST_ID`만 조회한다.
4. 관찰 쿼리가 돌아왔을 때 같은 호출 구간 객체가 아직 활성 상태인지 다시 확인한다. 종료/교체됐다면 결과를 버린다.
5. JDBC 호출이 끝나면 즉시 구간을 닫는다. 연결이 풀에 반환되거나 다음 SQL로 넘어가면 이전 구간으로 다시 관찰하지 않는다.

구간 확인과 결과 저장은 짧은 메모리 임계 구역에서 수행하며 observer의DB 조회는 그 구역 밖에서 실행한다.
API 스레드가 관찰 쿼리의 응답을 기다리도록 만들지 않는다. 다만 메모리 기록/동기화 자체의 비용까지0이라고 가정하지 않는다.
물리 연결 번호/DB 스레드·이벤트 ID는 내부 대응에만 사용하고 결과나 metric label에 넣지 않는다.

## 실제로 관찰하는 것과 관찰하지 않는 것

- 대상 연결의현재 statement 종류를 INSERT/UPDATE/COMMIT/OTHER/NONE으로만 분류한다. SQL 원문·digest·파라미터는 읽어 저장하지 않는다.
- `END_EVENT_ID`로 statement가 현재 실행 중인지 본다. 마지막 완료 statement가 남아 있을 수 있으므로 단순히 행이 있다는 이유로 실행 중이라고 보지 않는다.
- `data_lock_waits.REQUESTING_THREAD_ID`가 대상 연결의DB 스레드와 같은 대기 관계 수를 센다. 전체 DB의대기 수를 해당 API의대기라고 붙이지 않는다.
- 현재 statement 정보는 **같은 연결의호출 시간 구간과 겹친 관찰 맥락**이다. 개별 서버 이벤트를 완벽히 추적한 trace나 정확한SQL별 서버 소요시간으로 표현하지 않는다.
- I/O/fsync/mutex 시간이나 DB 서버CPU를 분해하지 않는다. 대기 관계0은 순간 표본이며 전체 기간의대기 부재를 뜻하지 않는다.
- 연결 획득/prepare/연결 상태 변경이나 서버 진입 전후 대기는 샘플러의대상이 아니다. 요청이느려도execute/commit 밖에서만 지연됐다면NOT_OBSERVED일 수 있다. 먼저기존요청별시간으로구간을구분해야한다.

기존 statement consumer와INSERT/UPDATE/COMMIT instrument의활성 상태를 시작 시 읽어 검증한다.
서버 계측 설정을 UPDATE로 바꾸지 않는다. 별도 observer는 격리 Testcontainers MySQL의root 권한으로 읽기만 수행하며 운영 권한 설계의권장안이 아니다.

## 상한과 상태

| 제한 | 값 |
| --- | ---: |
| 진행 중인 순차 API probe | 1개 |
| API 요청별 관찰 행 | 최대5000개 요청 |
| 샘플러 | 별도 연결1개/스레드1개/20ms fixed delay |
| 전체 observer 조회 | 최대512회 |
| 요청별 보관 표본 | 최대8개 |
| 관찰 쿼리 timeout / network timeout | 1초 / 2초 |
| observer 오류 | 3회 이후 조회 중단 |
| 종료 대기 | 3초, 필요 시interrupt·연결 종료 후2초 |

fixed delay 방식으로 늦어진 관찰을 나중에 몰아서 실행하지 않는다.
예산 초과/보관하지 못한 표본/오류/늦게 돌아와 버린 표본을 각각 기록한다.
`queryTotalMillis`와`queryMaxMillis`는 별도 관찰 호출의클라이언트 조회·결과 확인 비용이며 DB 서버CPU가 아니다.
관찰이 없는 구간을0ms 대기로 대체하지 않는다.

| 요청 관찰 상태 | 의미 |
| --- | --- |
| DISABLED | OFF 대조군이어서 실행 중 관찰 조회를 하지 않음 |
| NOT_OBSERVED | 관찰 가능한 표본을 보관하지 못함. 짧은 호출/샘플 시점/버려진 결과/예산 등 여러 이유가 가능 |
| SESSION_UNAVAILABLE | 표본에서 대상 세션을 찾지 못함. 대기0을 증명한 것이 아님 |
| OBSERVED | 같은 연결·호출 구간에서 관찰 결과를 보관함. 서버 statement가실행 중이었는지/대기 관계가있었는지는 개별 표본을 따로 확인 |

## 정확성 검증

단위 검증은 같은 연결 번호를 재사용하는 다음 요청이 시작된 뒤 이전 조회가 돌아오는 경우,
전체512회/요청별8개 상한, observer 오류3회 중단과메시지 미보관, OFF에서조회0/기존타이머 전달을 확인한다.
기존 요청 대응/트랜잭션 테스트를 포함한 선택15개는6초/실패·오류·스킵0으로 통과했다.

```bash
caffeinate -i ./gradlew --no-daemon --console=plain apiSessionProbe
```

기존SQL 전용3조건 제어군에 실제세션 observer를 붙였다. 처음21초 연결 검증 뒤 조회 비용·종료 상태를 추가했고,
최종17초/종료 코드0/테스트1개 통과/실패·오류·스킵0이었다.
Outbox 잠금 조건은 기존 독립 DB 대기/미커밋 가시성 검증과 동시에 대상 접수 연결의잠금 표본을 확인한다.
최종결과는 조회19회/오류0/늦은표본2개 버림/잠금표본8개였다.
정상 접수는NOT_OBSERVED, 잠금과커밋후보류 조건은OBSERVED였다.
전체관찰호출44.888ms/최대8.200ms, 종료 후queuedTasks0/samplerTerminated=true/openRequests0을 확인했다.
초기3조건은 자연 발생 원인이 아니라 정확성 확인을 위한 제어군이다.

## 짧은 자연 혼합 OFF/ON 비교

```bash
caffeinate -i ./gradlew --no-daemon --console=plain apiDbObservationPilot -PdbObservation=OFF
caffeinate -i ./gradlew --no-daemon --console=plain apiDbObservationPilot -PdbObservation=ON
```

이 task는 정확히 캠페인1000건/COUNT_TIME만 사용한다. 실시간은RECORD/단독50건 뒤 응답+200ms 순차 요청이다.
역할별Worker3/Hikari4/HTTP3/Redis3/queue0, 캠페인Relay2/완료 저장10/200/s·burst20,
실시간Relay1/완료 저장1, 기존MySQL/Kafka/Redis CPU·메모리/JVM512MiB를 유지한다.
OFF/ON 모두 observer 연결과시작 시상태 검증은 같고 주기적관찰 조회만 끄고 켠다.
따라서 OFF는 모든 계측이 없는 기준선이 아니다. 설정 확인/세션 대응 자체와새 요청 계측의비용은 남아 있다.

당시 출력은 `build/reports/api-db-observation-pilot/OFF-targets-1000.json`와ON 파일이었다.
[긴 호출 우선 관찰 후속](S7_API_SLOW_OBSERVATION.md)부터 파일명에 `-min-0-` 또는 `-min-20-`을 넣어 조건을 구분한다.
요청별 API 시간의배열과DB관찰 배열의순서·window·총수가 일치하는지 확인한다. 전체DB/HTTP/대상 매핑/partition offset 대조도 유지한다.
조건별1회/짧은실험/공유호스트/적은혼합표본으로 통계적유의성이나 운영 오버헤드를 확정하지 않는다.
기존10만 건p95/100만 건timeout의원인을 해결한 결과가 아니다.

## 실제 자연 혼합 결과와 관찰 범위

[고정 소스](experiments/s7-api-db-2026-11-03-source.json)에서새JVM/격리인프라로 OFF→ON 각1회 실행했다.
두 task 모두 BUILD SUCCESSFUL/종료 코드0/각테스트1개 통과/실패·오류·스킵0이었다. 전체47초/39초였다.

| 조건 | 캠페인 발송초 | 실시간 전체 / 혼합 수 | 혼합 접수 p95 / 최대ms | 주기적 관찰 조회(설정 확인 제외) |
| --- | ---: | ---: | ---: | ---: |
| OFF | 8.743 | 88 / 38 | 16.774 / 22.338 | 0 |
| ON | 8.450 | 87 / 37 | 16.524 / 17.130 | 22 |

캠페인2000+실시간175=**HTTP2175건**의전체대상/시도이력1회/Outbox PUBLISHED/HTTP 감사/각partition offset=end/quota를 대조했다.
중복·매핑·Mock 제한/오류0이었다. API와DB관찰 배열의요청 수·window 순서도175건 모두 대응됐다.
각역할consumer3개/pool1개 종료/activeCalls0을 확인했다. CPU·메모리·발송 예산을 변경하지 않았다.

OFF는88개요청 모두DISABLED였고 ON은87개중OBSERVED4개/NOT_OBSERVED83개였다.
ON의혼합37개만 보면 OBSERVED2개/NOT_OBSERVED35개다.
늦게 돌아와 버린 조회18개, 오류0, 쿼리예산 초과0, 보관 표본4개였다.
보관된4개 모두같은연결·commit호출구간에서COMMIT 실행 중으로 관측됐고, 해당순간rowWaitEdges는0이었다.
관찰호출전체44.055ms/최대5.274ms였다. 이 시간은 observer클라이언트 비용이며 API지연에 더할 수 있는 DB서버CPU 측정값이 아니다.

**이번 짧은 자연 실험은 대부분의요청에서 DB상태를 관찰하지 못했다.**
짧은호출·샘플주기·조회완료시점의영향과일치하며, 조회18개는실제구간이종료·교체돼버렸다.
대기0을본4개시점으로87개요청전체나과거10만 건의대기부재를 주장하지 않는다.
ON의p95가0.250ms낮았다는 결과로 관찰오버헤드0이나 성능개선을 주장하지 않는다.
시작설정·드라이버연결대응·새계측이두조건에공통으로남고, 각1회·서로다른짧은표본이다.

### 현재 판단

같은물리연결·유효한호출구간에만관찰을붙이고늦은결과를버리는정확성과상한은확인했다.
잠금제어군에서는8개잠금표본을잡았고 자연혼합에서는4개commit표본을보관했다.
그러나 원인분석에충분한긴요청표본은확보하지못했다. 다음은긴호출을우선관찰해예산을배분하는조건과커버리지를작게검증하는단계다.
현재증거로풀/인덱스/격리수준을바꾸거나10만·100만 건부하를재실행하지않는다. 과거지연원인은계속미확정이다.

### 원본과 실행 기록

- [초기 세션 제어](experiments/s7-api-db-2026-11-03-control-initial.json), [최종 세션 제어](experiments/s7-api-db-2026-11-03-control-final.json)
- [OFF 원본](experiments/s7-api-db-2026-11-03-off.json), [ON 원본](experiments/s7-api-db-2026-11-03-on.json)
- [명령·실행 시각·원본 해시](experiments/s7-api-db-2026-11-03-execution.json)

원본은수정없이복사했다. 내부시각은UTC이며파일명의날짜는한국작업일이다.
최종세션제어후자연실험의출력경로만분리했으며그차이는소스기록에명시했다. 원래100만 건FAIL원본은유지한다.

## 학습 질문

1. 같은 연결 번호만 확인하면 연결 풀 재사용 때 어떤 오인이 생길 수 있는가?
2. NOT_OBSERVED와관찰한대기0은 어떻게 다른가? 후자는전체기간 대기0을 의미하는가?
3. `events_statements_current`에 행이 남아 있는 것과 지금 SQL이 실행 중인 것은 왜 다른가?
4. ON/OFF를각1회 실행해p95가달라졌다면 어디까지 주장할 수 있는가?

## 참고

- [MySQL8.4 events_statements_current](https://dev.mysql.com/doc/refman/8.4/en/performance-schema-events-statements-current-table.html)
- [MySQL8.4 data_lock_waits](https://dev.mysql.com/doc/refman/8.4/en/performance-schema-data-lock-waits-table.html)
- [실제Connector/J9.7.0 ConnectionImpl.getId](https://github.com/mysql/mysql-connector-j/blob/9.7.0/src/main/user-impl/java/com/mysql/cj/jdbc/ConnectionImpl.java)


## 전체 기본 회귀와 보존

최종 `caffeinate -i ./gradlew --no-daemon --console=plain build`는 **BUILD SUCCESSFUL15분6초/종료 코드0**이었다.
새39개 클래스 JUnit XML과 콘솔에서 **312개 통과/실패·오류·스킵0**을 대조했다.
기존308개+세션대응·예산·오류·OFF 단위4개다. 기본회귀는실제재실행했고두실행JAR task는UP-TO-DATE였다.
별도세션제어1개/자연OFF·ON각1개테스트와구분한다. 고정소스7개해시와운영Java/resources diff0을확인했다.
드라이버조회경로는getId→CoreSession→NativeCapabilities.threadId메모리값반환까지동일버전sources에서대조했다.
원래100만 건FAIL원본과사용자DB/.env/IntelliJ/volume,기본정책을유지했다.
새원본의UUID·비유한값·민감필드미포함과문서링크를확인했다.10만·100만건/실제공급자/유료자원/푸시는실행하지않았다.
최종Docker에는기존notification-lab-mysql-1만healthy로남았고임시검증컨테이너는정리됐다.
