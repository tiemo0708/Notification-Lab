# Worker 동시성 테스트의 순간 관찰 창 보완

2026-11-04. S8 파티션 계약 검증 중 기존 c6cccc1의 원격 CI 실패를 확인해 별도 변경으로 처리한다.
운영 코드·HTTP timeout·Worker 상한은 변경하지 않는다.

## 확인한 실패와 해석

[GitHub 실행](https://github.com/tiemo0708/Notification-Lab/actions/runs/41757502204)은
build 20분 6초/349개 중 1개 실패로 끝났다. 실패는 `WorkerConcurrencyTest`의
`twoActiveAndOneWaitingKeepOffsetsAndClaimsBehindCompletedWork(RECORD)`였다.
로그에는 line 78의 `ConditionTimeoutException`이 있고, 해당 줄은 HTTP active=2 관찰이다.
COUNT_TIME 조건과 앞선 로컬 전체는 통과했다.

기존 fixture는 첫 HTTP를 시작하고 active=1을 본 뒤 두 번째를 발행했다.
Mock 응답 지연은 1500ms여서 첫 응답이 끝나기 전에 두 번째가 진입해야 active=2를 볼 수 있었다.
호스트가 느려 이 구간이 겹치지 않으면 Awaitility의 5초 대기를 늘려도 지나간 동시성을 되살릴 수 없다.
다만 원격의 상세 스케줄링/실제 active 시계열은 보존되지 않았으므로 **정확한 원격 원인 확정은 아니다**.
이번 변경은 확인 가능한 fixture의 시간 창 의존성을 제거한다.

## 변경한 검증 계약

[WorkerConcurrencyTest](../src/test/java/dev/jinhyeon/notificationlab/worker/WorkerConcurrencyTest.java)의
문제 조건에서만 테스트 전용 `@Primary PushProvider`를 명시적으로 등록한다.
두 작업은 실제 Worker admission과 DB 선점 이후, 실제 HttpPushProvider를 호출하기 직전에 기다린다.
제어 신호를 열면 같은 실제 HTTP 어댑터와 Mock 서버로 위임한다.
이 대기는 실제 HTTP 요청 전에 있으므로 HTTP 응답 제한 시간을 늘리지 않는다.

1. 첫 작업을 1700ms 동안 유지해 이전 1500ms 응답 창보다 오래 기다려도 상태가 유지됨을 검증한다.
2. 두 작업이 provider 진입 중이고 admission active=2인 상태에서 한 작업은 queue에 둔다.
3. 대기 작업과 같은 파티션의 후속 작업이 DB 선점/시도를 시작하지 않았고 offset이 0임을 확인한다.
4. finally에서 신호를 열어, 실패 시에도 테스트 작업을 해제한다.
5. 네 작업이 실제 HTTP/DB 완료 후 offset [2,1,1]로 끝나고 queue가 비었음을 확인한다.

provider 동시성 관측 최대는 정확히 2, 실제 HTTP 관측 최대는 1~2로 검증한다.
**실제 HTTP 두 요청이 동시에 활성이라는 순간 자체를 새 fixture에서 보장했다고 표현하지 않는다.**
admission/DB/offset 검증과 실제 네 HTTP 호출의 성공은 유지한다.
추가 스레드나 무제한 대기열은 없고 latch 대기는 최대 20초다. 자동 재시도나 테스트 재실행으로 실패를 숨기지 않는다.

## 실제 검증

```bash
./gradlew --no-daemon --console=plain test --tests '*WorkerConcurrencyTest'
```

RECORD/COUNT_TIME를 포함한 기존 11개가 **1분 1초/종료코드 0**에 통과했다.
XML의 두 제어 기록에서 `heldBeyondFormerWindow=true`를 확인했다.
[원격 실패·집중 검증·고정 소스](experiments/ci-worker-overlap-2026-11-04-source.json)에 결과와 161개 소스 SHA를 보존한다.

이 변경은 원격에 아직 반영하지 않았으므로 원격 CI 성공을 주장하지 않는다.
전체 회귀는 이 변경 후 별도로 실행했다.

최종 `caffeinate -i ./gradlew --no-daemon --console=plain build`는 **17분 45초/종료코드 0**이었다.
기본 test를 새로 실행하고 콘솔/JUnit XML **45개 클래스/354개 통과, 실패·오류·스킵 0**을 대조했다.
Worker 11개와 두 모드의 gate 기록도 다시 확인했다. bootJar는 UP-TO-DATE다.
고정 소스 161개는 실행 중 바뀌지 않았고, 운영 소스 diff는 0이다.
[최종 로컬 검증](experiments/ci-worker-overlap-2026-11-04-validation.json)을 보존한다.
이 결과는 수정 후 원격 CI 실행을 대체하지 않는다.
