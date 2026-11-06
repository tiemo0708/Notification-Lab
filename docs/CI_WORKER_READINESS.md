# 원격 CI의 Worker 동시성 실패와 시작 조건 보완

## 확인한 실패

`main`의 `70fe60f0ca872166b07b1d8c1b7d23c62a889992`를 푸시한
[CI 실행 42152219576](https://github.com/tiemo0708/Notification-Lab/actions/runs/42152219576)은 실패했다.
397개 중 394개 통과, 3개 실패, 스킵 0개였고 Gradle 실행은 24분 48초였다.

실패는 모두 `WorkerConcurrencyTest`의 독립 파티션 진행을 기다리는 부분이었다.

- `twoActiveAndOneWaitingKeepOffsetsAndClaimsBehindCompletedWork(RECORD)`: 활성 공급자 호출 2개 대기.
- `parallelCompletionPreservesFailedPartitionAndRecoversInANewContext(RECORD)`와 `COUNT_TIME`: 다른 파티션의 첫 작업 완료 대기.

같은 원격 실행에서 S9 `LocalTopologyIntegrationTest` 1개와 `CommitWakeupRetryTest` 7개는 통과했다.
따라서 전체 CI 실패와 해당 기능별 통과를 구분한다. CI가 JUnit/컨슈머 그룹 관측 파일을 업로드하지 않아
당시 소비자별 할당은 확인할 수 없다.

## 준비 조건의 빈틈과 변경

기존 준비 조건은 부모 컨테이너의 할당 합계가 6개인지 확인했다. 요청 토픽 3개와 Retry 토픽 3개가
한 소비자에게 모두 할당된 상태도 이 조건을 만족한다. 같은 소비자는 파티션 간에도 동기적으로
리스너를 호출하므로, 첫 작업을 테스트용 gate로 붙잡으면 그 소비자가 가진 다른 파티션도 진행하지 못한다.
독립 파티션 소유자 3개를 전제한 테스트를 시작하기에는 약한 조건이었다.

테스트의 `awaitAssigned()`만 다음을 함께 기다리도록 보완했다.

1. 설정한 수만큼 자식 컨테이너가 실행 중이고 각각 요청 파티션을 소유한다.
2. 요청/Retry의 전체 6개 파티션이 중복이나 누락 없이 할당되어 있다.
3. Kafka Admin이 보는 그룹 상태가 `STABLE`이며 멤버 수도 설정과 같다.
4. 로컬 컨테이너와 브로커가 보는 멤버별 파티션 집합이 일치한다.

3개 소비자에서는 요청 파티션도 3개이므로 각각 하나씩 소유하게 된다. 기존 1개 소비자 테스트도
설정에 맞춰 검증한다. 시작 대기 상한 20초, 처리/HTTP/ACK 제한, 기존 업무 결과 assertion은 바꾸지 않았다.
운영 코드, 앱 기본값, 의존성, CI 워크플로도 변경하지 않았다.

## 실제 제어 실험

새 `aggregateAssignmentDoesNotProveThreeIndependentRequestOwners`는 실제 MySQL/Kafka/HTTP Mock으로 확인한다.

| 단계 | 관측/검증 |
| --- | --- |
| 소비자 1개 | 그룹 `STABLE`, 멤버 1개가 파티션 6개 소유. 기존 합계 조건은 참이지만 3개 소유자 준비 조건은 거부 |
| 파티션 0 첫 작업 유지 | 1.2초 동안 파티션 1은 `QUEUED`, 시도 0, HTTP 호출 0, 두 파티션 커밋 0 |
| 유지 해제 | 두 작업 완료 및 커밋 합계 2, HTTP 수락 2 |
| 같은 그룹에 소비자 3개로 새 앱 시작 | `STABLE`, 멤버 3개/각 2개 파티션, 로컬/브로커 할당 일치, 추가 HTTP 수락 없음 |

이 제어 실험은 약한 조건의 반례와 소유자가 하나일 때의 대기 현상을 보여준다.
3개 소비자의 실제 시작 중간 상태를 강제한 실험은 아니며, 과거 원격 실패와 같은 원인을 재현했다고 단정하지 않는다.
할당 직후 새로운 리밸런스나 다른 CI 자원 지연까지 없앤다는 보장도 아니다.

## 검증과 남은 확인

수정 전 원격 실패 대상 두 메서드를 RECORD/COUNT_TIME으로 실행한 4개는 로컬에서 모두 통과했다(44초).
원격 실패 자체는 로컬에서 재현되지 않았다. 개발 중 새 제어 테스트의 1개 소비자/활성 상한 2 설정은
기존 유효성 검사에 거부됐다(16개 중 기존 15개 통과/새 1개 실패, 1분 24초).
운영 검사를 완화하지 않고 제어 설정을 1/1로 바로잡았다.

최종 클래스 집중 실행은 **16개 통과, 실패/오류/스킵 0, 1분 26초**였다.

```bash
./gradlew --no-daemon --console=plain test \
  --tests '*WorkerConcurrencyTest.twoActiveAndOneWaitingKeepOffsetsAndClaimsBehindCompletedWork' \
  --tests '*WorkerConcurrencyTest.parallelCompletionPreservesFailedPartitionAndRecoversInANewContext'
./gradlew --no-daemon --console=plain test --tests '*WorkerConcurrencyTest'
./gradlew --no-daemon --console=plain build
```

전체 build는 **22분 10초/종료 코드 0/51개 클래스·398개 통과, 실패·오류·스킵 0**이었다.
기존397개에 제어 테스트1개를 더한 기본 회귀이며, 집중 실행16개와 합산하지 않는다.
test task를 실제 실행했고 콘솔398개 PASS와 JUnit 집계를 대조했다.
운영 소스가 같아 `bootJar`/`mockServerJar`는 `UP-TO-DATE`로 기존 산출물을 재사용했다.
원격 실패 대상3개, 새 제어 테스트, S9 토폴로지, 커밋 Wakeup 재시도7개, Batch CLI 재실행도 포함해 통과했다.

소스172개 중 변경은 `WorkerConcurrencyTest.java` 1개이며 나머지171개는 기준 `70fe60f`와 같다.
전체 실행 전후 소스172개/구성23개의 SHA 불변을 확인했고, 이전 S9 구성15개와도 동일하다.
집중/전체 실행의 제어 관측2개, JUnit/로그/JAR SHA, 실패 단계와 제한은
[검증 원본](experiments/worker-readiness-2026-11-06-validation.json)에 보존했다.
원본 로그와 JUnit/HTML은 무시되는 `build/reports/worker-readiness` 아래에서 단계별로 보존한다.

환경은 macOS arm64 / Amazon Corretto 21.0.6 / Gradle Wrapper 8.14.5,
격리된 Testcontainers MySQL 8.4.8 / Kafka 4.0.2다. 원격 Ubuntu 24.04 / Temurin 21과 구분한다.
기존 사용자 DB와 모니터링 서비스를 초기화하지 않았다.

아직 이 보완은 푸시하지 않았으며 새 커밋의 원격 CI 통과는 미검증이다.
로컬 통과만으로 원격 실패 해결을 완료했다고 기록하지 않는다. 다음은 사용자 검토와 푸시 승인 후
동일 커밋 SHA의 원격 CI를 확인하는 것이다. AWS/FCM/별도 호스트 및 장시간 대량 실험은 실행하지 않았다.
