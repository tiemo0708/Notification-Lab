# S7 장시간 도구의 선택적 첫 실패 보존 연결

2026-11-03. 시작67269e3/main. 기존1천 건 혼합 실험의 보존 계약을 장시간 도구에서 선택할 수 있도록 연결했다.
이번 작업에서는100만 건·10만 건 발송을 실행하지 않았다. 운영 Java/resources와 기본 발송 정책도 변경하지 않았다.

## 실행을 분리한다

`campaignDeliveryMillion`의 새 `-PfailureObservation=ON` 옵션이 기록기를 켠다. 기본은OFF다.
실행 여부와 설정 확인은 다르다. 이번에는 실제 task 실행 없이 설정을 확인하고1천 건 리허설만 실행했다.
실제100만 건 실행은 여전히 별도의 장시간 작업이다. 아래 dry-run은 발송하지 않는다.

```bash
./gradlew --no-daemon --console=plain campaignDeliveryMillion -PfailureObservation=ON --dry-run
./gradlew --no-daemon --console=plain apiFailureLongRunRehearsal -PfailureMode=NONE
./gradlew --no-daemon --console=plain apiFailureLongRunRehearsal -PfailureMode=TIMEOUT
./gradlew --no-daemon --console=plain apiFailureLongRunRehearsal -PfailureMode=STORAGE_FAILURE
```

장시간 관찰 LONG_RUN 프로필은 load=true/대상100만 건/주입NONE만 허용한다.
별도1천 건 REHEARSAL 프로필에서만 오류 주입을 허용하며 대상 확대나 load 옵션을 거절한다.
기존 PILOT은 Relay2/COUNT_TIME/세부 계측 구성을 계속 사용하며 새 리허설과 구분한다.
오류를 주입하려는 장시간 프로필과 다른 계측·Relay 병렬·offset 비교 옵션의 혼용은 인프라 시작 전에 거절한다.

## 유지하는 조건과 관찰 범위

장시간 기본의 RECORD·앱 스케줄 Relay1개·publishBatchSize1을 리허설에서도 사용한다.
실제 장시간 task의 기존 publishBatchSize 옵션은 유지하며 이번 검증은1이다.
Worker3/역할별 Hikari4/HTTP 연결3/캠페인200/s·burst20/총1000/s·burst100을 바꾸지 않는다.
장시간 모드에서는 캠페인 경로에 DeliveryTiming을 연결하지 않는다.
실시간 앱의 JDBC/서비스 계측은 기록기를 위해 켜며 해당 앱의 Outbox/Worker 고정 집계도 포함한다.
전체 구간 계측·별도 DB 관찰·물리 세션 조회는 켜지 않는다. 관찰 비용이0이라고 주장하지 않는다.

| 상한 | 장시간 task | 리허설 |
| --- | --- | --- |
| 캠페인 대상 | 1000000 | 1000 |
| 실시간 probe | 150000 | 5000 |
| 발송 drain | 8시간 | 기존900초(더 짧은 task 제한 적용) |
| JUnit / Gradle | 11시간 / 12시간 | 240초 / 5분 |
| JVM 최대 heap | 512MiB | 512MiB |
| HTTP 응답 / 연결 | 5초 / 2초 | 동일 |
| 기록기 | 완료최근32건+진행1건 | 동일 |
| 첫 파일 | 256KiB | 동일 |

기존 장시간 전체 자료구조도 페이지1000행/자원표본14402개/audit125008bytes/latency 배열최대8000000bytes로 제한한다.
다른 probe·샘플 자료구조가 남아 있으므로 전체 프로세스의 상수 메모리를 주장하지 않는다.
새 가상 요청 검증은5000회와150000회 모두 완료 후32건만 남고, 상한 다음 open이 전송 전에 거절되는지 확인한다.
동결된 JSON이256KiB 이내인지도 검사한다. 실제150000건 HTTP 요청을 보낸 검증은 아니다.

## 첫 실패의 의미

HTTP timeout은 원래 HttpTimeoutException과 FAIL을 유지한다. 서버 결과를 미접수로 바꾸거나 자동 재전송하지 않는다.
실시간 앱 본문에서 관찰한 실패는 앱·pool 정리와 추가 진단 전에 파일 저장을 한 번 시도한다. 늦은 서버 완료나 중첩된 catch는 첫 snapshot을 바꾸지 않는다.
저장 실패는 원래 오류에 정제된 suppressed 오류1개로 남고, 기존 파일을 덮지 않는다.
출력은 공유 `build/reports/api-failure-pilot/run-*`의 고유 디렉터리다. report의 failureObservationProfile이 실행 종류를 구분한다.
원래 harness 결과 result.json과 별도 제어 검증 verification.json을 구분한다.

제어 timeout은 첫 MIXED 요청의 실제 commit과 auto-commit 복원 후 반환을 최대8초 지연한다.
이 제어는 실제 장시간 원인의 재현이 아니다. 저장 방식·파일시스템·전원 장애 한계는 [기존 계약](S7_FAILURE_PERSISTENCE.md)을 따른다.
preload·앱 초기화·종료 중 오류, JVM 강제 종료, 외부 kill, Gradle/JUnit의 최종 강제 중단까지 종료 전 파일을 보장하지 않는다.
새 파일 실패 시 메모리 snapshot은 마지막 report 쓰기가 가능할 때만 남는다. 기존 최종 report 쓰기 자체가 실패하면 새 I/O 오류가 바깥 예외를 가릴 수 있으며 이 경로의 원래 예외 유지까지 보장하지 않는다.

## 실제 검증 결과

관련 단위35개는6초에 실패·오류·스킵0으로 통과했다.5000회/150000회는 메모리 기록기의 합성 callback이다.
실제 리허설은 조건별1회씩 실행했다.

| 조건 | Gradle | 원래 harness 결과 | 제어 검증 | 보존 결과 |
| --- | --- | --- | --- | --- |
| NONE | 45초 | PASS | PASS | 실패 파일 없음 |
| TIMEOUT | 36초 | FAIL / HttpTimeoutException | PASS | 첫 파일68395bytes |
| STORAGE_FAILURE | 36초 | FAIL / HttpTimeoutException | PASS | FileAlreadyExistsException·기존35bytes 유지 |

정상은 캠페인1000+실시간104(기준선50+혼합54)=1104건 HTTP·DB·전체 참조·offset을 대조했다.
중복·잘못된 참조0, Mock 요청/수락1104, 최종 offset committed=end였다. 최근32/완료104/evicted72/진행없음을 확인했다.
두 제어는 첫MIXED 요청(51번째)에서5초 timeout이었다. 완료50/evicted18/최근32+진행1건을 동결했다.
active AUTOCOMMIT_STATE/commitReturned=true/responseObserved=false/클라이언트결과UNKNOWN이었다.
앱active/pool open/gate 대기 중 첫 저장을 확인했고, TIMEOUT 파일은 정리 후 frozen snapshot과 byte 일치했다.
저장 실패의 suppressed는 중첩 처리 후에도1개, 정상 저장의 suppressed는0개였다.
두 실패 실행의 최종 발송 정합성까지 완료했다고 집계하지 않는다.

장시간 task 자체는 OFF/ON 각각 dry-run3초/2초·종료코드0으로 설정만 확인했고 task는SKIPPED였다.
대상100만 건/RECORD/주입NONE/계측·병렬Relay·offset비교OFF/heap512m/fork1/JUnit11h/task12h를 읽어 대조했다.
이것은 실제100만 건 실행 또는12시간 보존 능력의 검증이 아니다.
[설정 확인 원본](experiments/s7-long-observation-2026-11-03-plans.json),
[실제 사용한 설정 검사 소스](experiments/s7-long-observation-2026-11-03-plan-inspector.gradle),
[고정 소스](experiments/s7-long-observation-2026-11-03-source.json),
[실행·원본 SHA](experiments/s7-long-observation-2026-11-03-execution.json)를 보존한다.
Java Amazon.com Inc.21.0.6/macOS15.7.7 aarch64, MySQL8.4.8/Kafka4.0.2/Redis7.4.11의 격리된 Testcontainers를 사용했다.
JVM최대536870912bytes/MySQL2CPU·2GiB·buffer512MiB/Kafka1CPU·1GiB/Redis256MiB였고 환경 상세는 원본 report에 있다.
이는 오류 보존 제어이며 처리량 비교가 아니다.

기존 PILOT 프로필도39초에 통과했다. 캠페인1000+실시간86=1086건 전수 대조/중복0이었다.
정상 리허설과 합계2190건이며 오류 주입 실행은 최종 성공 집계에 포함하지 않는다.

## 남은 작업

기존100만 건 실패의 원인은 여전히 미확정이며 이번 연결과 리허설이 과거 실패를 해소하지 않는다.
장시간을 다시 실행한다면 자원·실행 조건과 실패시 산출물 경로를 검토하고 별도로 시작해야 한다.
Mock 수락은 단말 도착이나 exactly-once 보장이 아니다.

## 최종 회귀와 보존

최종 `caffeinate -i ./gradlew --no-daemon --console=plain build`는14분23초/종료코드0이었다.
새43개 클래스의 XML과 콘솔에서338개 통과/실패·오류·스킵0을 대조했다.
직전332개에 프로필 검증5개와150000회 가상 기록 조건1개를 추가했다.
별도 opt-in 리허설3회·기존PILOT1회·발송 없는 설정확인2회와 이338개를 구분한다.
bootJar는UP-TO-DATE였고 원격CI 결과가 아니다. 고정소스12개와 보존원본13개의SHA가 일치했다.
운영Java/resources와 기존100만 건FAIL 원본은 변경하지 않았고 푸시는 하지 않았다.
