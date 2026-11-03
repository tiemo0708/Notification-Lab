# S7 최종 보고서 실패가 원래 오류를 가리지 않도록 보완

2026-11-03, 시작0b47a1c/main. [직전 장시간 도구 연결](S7_LONG_RUN_WIRING.md)에서 남긴
최종 report 쓰기 실패의 예외 우선순위 문제를 보완했다. 실제100만 건 실행이나 과거 timeout 원인 확정은 아니다.
운영 Java/resources/기본RECORD/pool/격리 수준/발송 예산은 변경하지 않았다.

## 문제와 변경

기존 도구는 첫 실패 snapshot을 먼저 보존하고 원래 오류를 전파했지만, 마지막 finally에서 report 쓰기가 실패하면
새 I/O 오류가 원래 HttpTimeoutException을 가릴 수 있었다. 첫 파일과 마지막 report는 서로 다른 산출물이다.

바깥 catch에서 원래 실패 객체를 보관하고, 마지막 진단·report 처리를 PilotFinalization으로 감쌌다.

| 앞선 실행 | 마지막 처리 | 결과 |
| --- | --- | --- |
| 실패 | 성공 | 원래 실패 그대로 전파 |
| 실패 | Exception 또는 AssertionError | 원래 객체 유지·정제된 suppressed IOException1개 추가 |
| 성공 | Exception 또는 AssertionError | 마지막 오류 자체 전파·메모리 결과FAIL로 변경 |
| 성공 | 성공 | 기존 정상 결과 유지 |

추가 오류의 종류와 원래 실패 유지 여부만 메모리 report의 finalizationFailure에 기록한다.
예외 메시지·파일 경로·cause를 산출물에 복사하지 않는다. 기존 suppressed 오류도 보존한다.
마지막 처리를 재시도하거나 원래 실패를PASS로 바꾸지 않는다.
JVM 치명적 Error, 강제 종료, 전원 장애까지 포착하거나 파일을 보장하는 변경은 아니다.
마지막 처리가 중간에 실패하면 뒤의 report 항목이나 파일이 생성되지 않을 수 있다.

## 실제 제어 경로

```bash
./gradlew --no-daemon --console=plain apiFailureLongRunRehearsal -PfailureMode=REPORT_FAILURE
./gradlew --no-daemon --console=plain apiFailureLongRunRehearsal -PfailureMode=NONE
```

REPORT_FAILURE는1천 건 REHEARSAL에서만 제공하는 제어다. 첫MIXED 요청을 commit/auto-commit 복원 후 지연해
기존5초 HTTP timeout을 만든다. 마지막 result.json 경로에는 시작 전에 빈 디렉터리를 만들어 실제 쓰기를 실패시킨다.
LONG_RUN은 mode=NONE만 허용하고 새 오류 주입도 인프라 시작 전에 거절한다. 기존 PILOT task의 허용 모드는 그대로다.

관련 단위20개는7초에 실패·오류·스킵0으로 통과했다. 새 단위3개는 실제 파일 쓰기 실패와 timeout 객체 동일성,
앞선 실패 없는 보고서 오류의 전파/FAIL 변경, AssertionError와 기존 suppressed 오류 보존을 검증한다.

REPORT_FAILURE는37초에 제어 검증을 통과했다. 원래 harness 결과는FAIL/HttpTimeoutException이다.
완료50/evicted18/최근32+진행1건, active AUTOCOMMIT_STATE/commitReturned=true/응답미관찰/클라이언트UNKNOWN이었다.
앱active/pool open/gate 대기 중68414bytes의 첫 파일을 저장했고, 정리 후 frozen snapshot과 byte 일치를 확인했다.
그 뒤 실제 마지막 파일 쓰기는FileNotFoundException으로 실패했다. 원래 timeout과 suppressed1개를 유지했다.
최종 result.json은 파일로 저장되지 않았으며 여전히 제어 디렉터리였다.

검증기가 메모리 결과를 별도 control-observed-report.json에 복사해 finalizationFailure를 확인했다.
이는 실제 도구가 자동 복구하거나 report 쓰기를 재시도한 결과가 아니다.
verification.json의PASS와 원래 harness의FAIL을 구분한다. 실패 실행의 최종 발송 정합성을 완료로 집계하지 않는다.

NONE은45초에 통과했다. 캠페인1000+실시간105=1105건 HTTP·DB·전체 참조·offset을 대조했다.
Mock 요청/수락1105, 중복·잘못된참조0, 최종offset committed=end였다. 첫 실패 파일은 생성하지 않았다.
두 조건 각1회이며 처리량 비교가 아니다. Worker3/Relay1/RECORD/완료 저장1건과 기존 자원 상한을 유지했다.
Java Amazon.com Inc.21.0.6/macOS15.7.7 aarch64, 격리된 MySQL8.4.8/Kafka4.0.2/Redis7.4.11을 사용했다.
실제 JVM 최대536870912bytes와 인프라·설정 상세는 정상 원본 report에 보존했다.

[고정 소스](experiments/s7-final-report-2026-11-03-source.json),
[파일 경로 형태 관찰](experiments/s7-final-report-2026-11-03-target-observation.json),
[실행·원본 SHA](experiments/s7-final-report-2026-11-03-execution.json)를 보존한다.
실제100만 건 재실행과 원래5초 timeout 원인 진단은 계속 미완료다. Mock 수락은 단말 도착이 아니다.

## 최종 회귀와 보존

최종 `caffeinate -i ./gradlew --no-daemon --console=plain build`는14분19초/종료코드0이었다.
새44개 클래스 XML과 콘솔에서341개 통과/실패·오류·스킵0을 대조했다. 기존338개와 새 단위3개다.
기본 회귀를 재실행했고bootJar는UP-TO-DATE였다. 별도1천 건 제어2회와 구분하며 원격CI 결과가 아니다.
고정소스10개와 보존원본7개의SHA를 대조했다. 운영Java/resources와 기존100만 건FAIL 원본은 변경하지 않았다.
이번 작업 범위는 최종 보고서 저장 실패 보완까지이며, 실제 장시간 실험과 푸시는 하지 않았다.
