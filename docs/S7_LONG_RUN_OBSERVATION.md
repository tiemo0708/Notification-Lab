# S7 장시간 실행의 실패 직전 관찰과 중단 기준

2026-11-03 한국 시간. 시작392b130/main.
[1만 건 두 번의 반복](S7_API_CONNECTION_10K.md)에서 기존127ms 상태 합계와5초 timeout을 재현하지 못했다.
같은 작은 실험을 계속 확대하는 대신, 다음 장시간 실행에서 무엇을 남기고 언제 멈출지 먼저 정의한다.
이번 변경은 **별도 기록기 프로토타입과 검증**이다. 실제100만 건 도구에 연결하거나 장시간 실행을 시작하지 않았다.
운영 Java/resources/기본 RECORD/풀/격리 수준/발송 예산은 유지한다.

## 기존 도구의 빈틈

현재 ApiRequestTiming은 소규모 실험용이다. 최대5000개 접수 행을 모두 보관하며 성공 응답과 JDBC 합계를 대응한다.
장시간용으로 상한만 늘리면 진단 메모리가 요청 수에 따라 증가한다.
또 HTTP timeout 때 완료 전인 JDBC 호출을 분리해 동결하는 기능은 없다.

기존 CampaignDeliveryDiagnostics는 timeout을 서버 결과UNKNOWN으로 기록하고 예외를 다시 던진다.
캠페인 발송 중 실패 시 live pool/lag/감사 정보를 수집하지만 모든 호출 구간의 진행 상태를 보관하지는 않는다.
기록기만으로 DB·네트워크·CPU·스케줄 중 원인을 확정할 수 없으므로, 최초 증거와 나중 확인을 구분해야 한다.

## 결정: 최근32건과 진행 중1건

ApiFailureWindow 프로토타입은 순차 HTTP probe1개만 허용한다.
완료된 최근32건을 고정 용량 deque에 보관하고, 진행 중인 요청1개를 별도 슬롯에 둔다.
다음 완료 행을 넣을 때 가장 오래된 행을 제거하며 전체 opened/completed/evicted 수를 따로 센다.
요청 수가 늘어도 기록기 내부의 보관 슬롯은 늘리지 않는다.

| 항목 | 상한 또는 의미 |
| --- | --- |
| 성공 접수 이력 | 최근32건 |
| 진행 중 접수 | 1건 |
| 호출 중첩 | STORE_CALL+하위 JDBC 호출, 최대2단계 |
| JDBC 합계 | 고정14개 Part의 횟수·시간 배열 |
| 애플리케이션 SQL 실행 | 최대4개 시간 |
| 상태 메서드 상세 | 최대8개/요청 |
| 전체 probe | 설정값1..150000, 상한을 넘기기 전에 중단 |
| 첫 중단 결과 | 한 번만 생성한 불변 snapshot |
| 서버 스레드 참조 | 진행 중1개 내부 대응에만 사용, 결과에 ID·이름 미보관 |

이는 진단 기록기 내부의 상한이다. 기존 장시간 harness의 probes/감사/자원 표본 배열까지 전체 JVM 메모리가 고정됐다는 뜻은 아니다.
32건은 요청 개수이며 고정된 초 단위 구간이 아니다. 응답이 느려지면 같은32건이 포함하는 시간도 길어진다.
freeze는 메모리 snapshot이며 디스크 저장이나 JVM 강제 종료 후 보존을 아직 보장하지 않는다.

## 진행 중인 호출과 완료 시간을 섞지 않는다

각 요청은 완료된 Part의 횟수·합계와 별도로 activePart와 해당 호출의 경과시간을 기록한다.
예를 들어 COMMIT이 진행 중이면 완료 COMMIT 횟수는0이어도 activePart=COMMIT/경과5초일 수 있다.
완료 합계0을 대기0으로 해석하지 않는다.

STORE_NOT_ENTERED는 기록 대상 STORE_CALL에 아직 진입하지 않았다는 뜻이다.
서버가 요청을 받지 않았다는 증거는 아니다. 연결 수립·서블릿 대기·컨트롤러 처리 등을 이 프로토타입은 분리하지 않는다.
STORE_RETURNED는 계측한 store 호출이 끝났다는 뜻이며 클라이언트가 응답을 받았다는 뜻은 아니다.

commitReturned/rollbackReturned는 원래 JDBC 메서드의 정상 반환을 관찰한 표시다.
HTTP 응답이 관찰되지 않은 진행 중 요청은 이런 표시가 있어도 activeServerOutcome=UNKNOWN을 유지한다.
HTTP503이나 계측이 불완전한HTTP202는 responseObserved=true와 실제 status를 보관하되 서버 처리 결과는별도로UNKNOWN을유지한다.
진행 중 AUTOCOMMIT_STATE의 정확한 메서드명은 아직 표시하지 않는다. connectionCalls에는 이미 완료한 상태 메서드만 들어간다.
서버 SQL/물리 세션/스택/SQL 원문/본문/수신자/토큰/인자 전체/예외 메시지는 수집하지 않는다.

## 최초 중단을 동결한다

freeze는 짧은 메모리 임계 구역에서 현재 요청과 최근 이력을 불변 Map/List로 만든다. JDBC·HTTP I/O를 기다리거나 실행하지 않는다.
같은 시점에 서버 callback이 와도 임계 구역 순서에 따라 완료 전 또는 완료 후의 한 상태를 보관한다.
두 번째 중단 이유와 늦은 callback/HTTP202는 첫 snapshot을 바꾸지 못한다.
동결 후 새 probe는 열지 않는다. 늦은 서버 callback은 기록만 생략하고 서버의 실행을 취소하거나 결과를 바꾸지 않는다.

동결이 실제 스케줄 시각과 완전히 같은 순간이라는 보장은 없다. lock 획득과 복사 비용도0이라고 가정하지 않는다.
후속 실제 DB 확인을 수행한다면 별도 시각의 별도 결과로 저장해야 하며 첫 실패 snapshot을 덮어쓰지 않는다.

## 중단 판단과 구현 상태

| 조건 | 기록/처리 | 이번 구현 상태 |
| --- | --- | --- |
| HTTP5초 timeout | HTTP_TIMEOUT, 응답 미관찰/서버 결과UNKNOWN, 자동 재전송 금지 | 기존 helper는 예외 전달; 단위 검증의 호출자가 freeze를 연결함 |
| timeout 외 HTTP I/O 예외 | HTTP_IO_FAILURE, 원래 예외 유지, 서버 결과를 거부로 추정하지 않음 | 프로토타입 분류만 제공, 장시간 연결 미구현 |
| HTTP202 외 응답 | HTTP_STATUS로 동결하고 다음 probe 금지 | complete에서 구현 |
| SQL/서버 실패 | SERVER_FAILURE로 동결, 원래 예외 유지 | 호출자 연결을 합성 JDBC 검증에서 확인 |
| 호출 수·경계 불일치 | PROTOCOL_FAILURE, 성공으로 기록하지 않음 | complete에서 구현; callback 오류도 호출자가 이 분류로 중단해야 함 |
| probe 상한 | PROBE_LIMIT, 추가 요청을 열기 전에 중단 | open에서 구현 |
| 실행 시간 상한 | RUN_LIMIT, 부분 완료를PASS로 처리하지 않음 | 분류·계약 정의, 실제 장시간 연결 미구현 |
| 인프라 실패 | INFRA_FAILURE, Worker/Relay 중단·확인된OOM 증가 등 증거 별도 보존 | 분류·계약 정의, 실제 장시간 연결 미구현 |

현재100만 건 task의 HTTP5초/최대probe150000/발송 drain8시간/JUnit11시간/Gradle12시간 상한은 변경하지 않는다.
이 표는 task를 실행하라는 지시가 아니다. 총 실행 시간과 발송 drain은 서로 다른 구간이다.
누락된 메트릭·pool 정보·cgroup 값을0으로 채워 정상으로 판정하지 않는다.

다음 실제 연결은 아래 순서를 지켜야 한다.

1. HTTP 예외를 받은 즉시 앱/DB를 닫기 전에 첫 실패 구간을 동결한다. 서버 완료를 기다리거나 요청 제한 시간을 늘리지 않는다.
2. 새 probe를 중단하고 원래 예외와 FAIL을 유지한다. timeout을 미접수로 간주하거나 자동 재전송하지 않는다.
3. 이미 사용 가능한 live pool/lag/자원 카운터를 별도 항목으로 수집한다. 진단 실패는 누락으로 표시하고 첫 실패를 덮지 않는다.
4. 별도 실패 파일에 우선 저장한 뒤 앱·컨테이너를 정리한다. 파일 저장 실패·강제 종료의 보존 한계도 따로 기록한다.
5. 성공 종료 때는 기존 전수 정합성 검증을 계속 수행한다. 최근32건으로 전체 발송 이력을 대신하지 않는다.

지연 경고용1초 같은 새 임계값이나 무진행 자동중단 시간은 이번에 추가하지 않는다.
추가 중단 기준이 필요하면 정상적인 재시도·quota 대기와 구분해 작은 검증부터 해야 한다.

## 실제 검증

처음 선택31개는5초/종료 코드0/실패·오류·스킵0이었다.
이후 원래 SQL 예외와 rollback 증거 보존 검증1개를 추가했고, 최종 선택32개도5초/종료 코드0/실패·오류·스킵0이었다.

- 합성 접수5000건 뒤 최근32건/evicted4968/최신 ordinal5000을 확인했다.
- 가짜 단조 시계의 진행 중 COMMIT5초에서 완료 횟수0과 activePartAgeMillis5000을 구분했다.
- commit 반환 후 복원 구간에서도 HTTP 미관찰 결과는UNKNOWN으로 남겼다.
- 첫 중단 후 늦은 callback·HTTP202·다른 중단 이유가 같은 snapshot을 바꾸지 못함을 확인했다.
- probe 상한/중첩2단계/다른 서버 스레드/겹친 요청/불완전한202를 거부했다.
- 실제 Spring TransactionTemplate+합성 JDBC로 DML4회·정상 commit 및 원래 SQL 오류·rollback을 확인했다. 추가 드라이버 호출이나 SQL 수집은 없었다.
- 실제 loopback HttpServer/HttpClient에서 warmup 뒤500ms timeout을 만들었다. 서버 호출 마커는 합성이며 DB는 없는 fixture다.
  warmup1개+실패 probe1개만 처리했고, 늦은 응답이 첫 동결 결과를 덮지 않았다. URI·요청/응답 본문은 결과에 없었다.

이 검증은 실제 MySQL과 HTTP를 결합한 장시간 경로 검증이 아니다. 해당 연결은 다음 작은 pilot에서 확인해야 한다.
기록기 자체는 추가 SQL/연결/스레드를 생성하지 않지만 실제 부하의 계측 비용이나 원래 timeout 원인은 아직 미확정이다.

[최종 고정 소스](experiments/s7-api-failure-window-2026-11-03-source-final.json)와
[실행 메타데이터](experiments/s7-api-failure-window-2026-11-03-execution.json)를 보존한다.

## 전체 회귀

응답 관찰 여부 보완 전의 첫 `caffeinate -i ./gradlew --no-daemon --console=plain build`는14분10초/종료 코드0이었다.
새40개 클래스의 JUnit XML과 콘솔에서 **325개 통과/실패·오류·스킵0**을 대조했다. 기존317개와 새8개다.
기본 회귀는 실제 재실행했고 bootJar는UP-TO-DATE였다. 원격CI 결과가 아니다.
고정 소스8개와 기존100만 건FAIL 원본SHA 불변, 운영Java/resources diff0을 확인했다.

### 최종 검토에서 보완한 관찰 의미

처음에는 HTTP202 외 응답을 받은 경우에도 freeze 결과가 responseObserved=false였다.
서버 결과UNKNOWN과 응답 미관찰을 혼동하지 않도록 실제503 및 불완전한202의 status/관찰 여부를 보존하게 수정했다.
수정 후 선택32개는6초/종료 코드0/실패·오류·스킵0으로 통과했다. timeout과 늦은 응답 불변 검증도 함께 통과했다.
수정 전 소스·전체 회귀 결과는 별도 보존하며 최종 소스는source-final.json이다.

### 수정본 최종 회귀

응답 관찰 보완 후 최종 build는 **14분23초/종료 코드0**이었다.
새40개 클래스의 JUnit XML과 콘솔에서 **325개 통과/실패·오류·스킵0**을 다시 대조했다.
기본 회귀는 실제 재실행했고 bootJar는UP-TO-DATE였다. 수정 전 결과와 구분하며 원격CI 결과가 아니다.
최종 Docker에는 기존notification-lab-mysql-1만healthy로 남았다. Git identity와 원격main9cf1786을 읽기 전용으로 확인했고 푸시는 하지 않았다.

후속 [실제 HTTP/MySQL 접수와 첫 실패 파일 보존](S7_FAILURE_PERSISTENCE.md)에서 commit 전/복원 후 timeout을 제어하고 앱 종료 전 저장을 검증했다. 기존 캠페인/장시간 task 연결은 아직 하지 않았다.
