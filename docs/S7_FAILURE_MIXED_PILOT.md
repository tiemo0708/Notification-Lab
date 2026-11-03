# S7 작은 Kafka 혼합 실험의 첫 실패 보존

후속 단계에서는 [장시간 도구의 선택적 보존 연결과1천 건 리허설](S7_LONG_RUN_WIRING.md)을 검증했다.
아래는 이 문서 작성 시점의 구현·실행 기록이며 실제100만 건 재실행은 여전히 미완료다.

2026-11-03, 시작 커밋 cdf21d9. 접수 전용 제어에서 확인한 고정 기록기와 파일 보존을
기존 CampaignDeliveryPilotTest의 실제 Kafka 캠페인 혼합 경로에 선택적으로 연결했다.
운영 Java/resources, 기본 RECORD 정책, pool/격리 수준/역할별 예산은 변경하지 않았다.

## 실행과 범위

```bash
caffeinate -i ./gradlew --no-daemon --console=plain apiFailurePilot -PfailureMode=NONE
caffeinate -i ./gradlew --no-daemon --console=plain apiFailurePilot -PfailureMode=TIMEOUT
caffeinate -i ./gradlew --no-daemon --console=plain apiFailurePilot -PfailureMode=STORAGE_FAILURE
```

각 명령은 새 Testcontainers MySQL/Kafka/Redis와 HTTP Mock을 사용한다. 대상은 캠페인1000건으로 고정한다.
기존 fineTiming/COUNT_TIME 실험 구성에서 Relay2/Worker3/publish 최대10건을 사용하며,
API 상세 계측·별도 DB 관찰·connection pilot·load 옵션과의 동시 사용을 거절한다.
기본 test/build는 이 opt-in tag를 제외한다. 기존 도구의 기본 실행 경로도 유지한다.

HTTP 요청은 순차적이며 timeout5초, 연결2초를 유지한다. task5분/테스트240초/JVM512MiB다.
기존 데이터 구조의 probe5000개·자원 표본452개·페이지1000행 상한을 유지한다.
새 기록기만 최근 완료32건과 진행1건으로 제한한다. 전체 프로세스가 상수 메모리라는 뜻은 아니다.
파일은256KiB 상한이며 저장의 파일시스템 의존성과 전원 장애 한계는 [기존 보존 계약](S7_FAILURE_PERSISTENCE.md)을 따른다.

실행 환경은 Java Amazon.com Inc.21.0.6, macOS15.7.7/aarch64, JVM 최대536870912bytes다.
MySQL8.4.8은2CPU/2GiB/buffer512MiB, Kafka4.0.2는1CPU/1GiB, Redis7.4.11은256MiB다.
캠페인 quota200/s·burst20, 총1000/s·burst100, 역할별 HTTP 연결3개, Mock 지연1ms를 사용한다.
실시간 기준선50건 후 캠페인과 겹치는 요청을200ms 간격으로 보낸다. 공유 호스트의 합성 실험이다.

## 실패 처리 순서

정상 요청은 기록기를 열고 실제 트랜잭션의 연결 획득·SQL·상태 변경·commit·반환을 관찰한 뒤 HTTP 응답을 기록한다.
HTTP 비202 응답도 먼저 기록하므로 timeout과 구별된다. 첫 예외는 메모리 기록을 동결하고,
앱·pool 정리와 추가 진단에 앞서 완성 파일을 공개한다. 중첩된 실패 처리기는 다시 저장하지 않는다.
저장 실패는 원래 예외의 suppressed 오류와 정제된 오류 종류로 남긴다. 원래 실패는 계속 전파한다.
Throwable 지원을 추가하여 AssertionError도 같은 방식으로 유지한다.

TIMEOUT은 첫 MIXED 요청의 실제 commit과 auto-commit 복원이 반환된 직후, 호출자 반환만 최대8초 지연한다.
제어 필터는 POST /notifications 요청 스레드에만 적용한다. Worker/Relay 연결에 지연을 주입하지 않는다.
클라이언트는5초에 timeout을 관찰하고, 저장 후 gate를 해제한다. 클라이언트 결과는 UNKNOWN이다.
commitReturned=true는 관찰한 commit 반환을 뜻하며 단말 도착이나 전체 작업 완료를 뜻하지 않는다.

STORAGE_FAILURE는 같은 timeout 제어에 더해 목적 파일에 합성 기존 증거를 미리 생성한다.
새 hard link 생성이 FileAlreadyExistsException으로 실패해야 하고 기존 파일은 그대로 남아야 한다.
이 경우 메모리 snapshot은 result.json에 남지만 첫 실패 전용 파일의 새 저장은 실패다.

별도 제어 테스트는 원래 harness의 결과를 검증한다. NONE의 harness 결과는 PASS,
두 주입 조건은 HttpTimeoutException과 FAIL이어야 한다. 검증 성공은 verification.json의 PASS로만 기록한다.
실제 부하 실패를 성공으로 바꾸거나 누락된 발송을 성공 처리하지 않는다.

## 실제 결과

각 조건1회 실행했다. 정상 NONE은40초, TIMEOUT은35초, STORAGE_FAILURE는38초에 Gradle이 종료 코드0으로 끝났다.
제어 검증의 통과와 아래 원래 harness 결과를 구분한다.

| 조건 | harness 결과 | 완료 기록 / evicted | 전용 파일 | 제어 검증 |
| --- | --- | --- | --- | --- |
| NONE | PASS | 87 / 55 | 생성 안 함 | PASS |
| TIMEOUT | FAIL / HttpTimeoutException | 50 / 18 | 68,422bytes | PASS |
| STORAGE_FAILURE | FAIL / HttpTimeoutException | 50 / 18 | 새 저장 실패·기존35bytes 보존 | PASS |

정상은 캠페인1000+실시간87(기준선50+혼합37)=1087건의 HTTP·DB·대상 매핑·offset을 대조했다.
Mock 요청/수락1087건, 중복·잘못된 참조0, 최종 committed=end였다. 단말 도착 검증은 아니다.
기록기는 완료87개 중 최근32개만 보관했고 진행 요청이 없었다.

주입 조건은 모두51번째(첫 MIXED) 요청에서5초 timeout이었다. 완료50개 중 최근32개와 진행1개가 남았다.
두 조건 모두 activePart=AUTOCOMMIT_STATE, commitReturned=true, responseObserved=false, 서버 결과UNKNOWN이었다.
첫 보존 시점에 앱 active/pool open/gate 대기를 읽어 확인했다. 메모리 동결 이후 gate를 해제했다.
TIMEOUT 파일은 정리 후에도 frozen snapshot과 byte 단위로 일치했고 원래 예외의 suppressed는0개였다.
STORAGE_FAILURE는 FileAlreadyExistsException을 기록하고 기존 합성 파일을 그대로 보존했다.
원래 HttpTimeoutException을 유지하고 suppressed 오류는 중첩 처리에도1개였다.

[고정 소스](experiments/s7-api-failure-mixed-2026-11-03-source.json)와
[원본 목록·SHA·실행 기록](experiments/s7-api-failure-mixed-2026-11-03-execution.json)을 함께 보존한다.
저장 실패의 earlier-evidence.txt는 JSON snapshot이 아니라 의도적으로 선점한 합성 기존 파일이다.
초기 컴파일의 타입/생성자/변수명 오류3개를 수정했고, 관련 단위27개는7초에 실패·오류·스킵0으로 통과했다.

## 한계와 다음 범위

주입한 오류의 순서와 보존 계약을 검증하는 실험이며 처리량 개선 비교가 아니다.
중도 실패한 두 실행은 최종 HTTP/DB/offset 전수 정합성을 완료한 것으로 표현하지 않는다.
종료 중 새 오류나 기록기 초기화 이전 실패까지 모두 종료 전에 보존한다고 보장하지 않는다.
고정 기록기는 메서드가 진행 중일 때 상태 그룹만 알며 정확한 내부 JDBC 메서드를 추정하지 않는다.
100만 건 task에는 연결하지 않았고 대형 부하도 실행하지 않았다. 기존100만 건 timeout의 원인은 여전히 미확정이다.
다음은 이 보존 계약을 장시간 도구에 연결할지 검토하는 단계이며 실제 장시간 실행은 별도다.

## 최종 회귀와 보존

최종 `caffeinate -i ./gradlew --no-daemon --console=plain build`는14분45초/종료 코드0이었다.
새42개 클래스의 JUnit XML과 콘솔에서332개 통과/실패·오류·스킵0을 대조했다.
직전329개에 새 단위3개를 추가했으며, opt-in 제어3회와 기존 pilot1회는 이332개와 별도다.
bootJar는UP-TO-DATE였고 원격 CI 결과가 아니다.
고정 소스10개와 보존 원본10개의 SHA를 대조했고 운영 소스·기존100만 건 실패 원본은 변경하지 않았다.
