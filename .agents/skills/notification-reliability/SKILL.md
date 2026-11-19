---
name: notification-reliability
description: Verify Notification Lab changes to Outbox publishing, worker offsets, retries, idempotency, delivery concurrency, or provider protection. Use for 알림 유실·중복·재시도·정합성 changes; choose failure-boundary tests and distinguish provider acceptance from device delivery.
---

# 알림 처리 정합성

저장소 루트의 `AGENTS.md`, `docs/CURRENT_STATE.md`와 변경에 해당하는 계획/실험 절을 읽는다.
이 스킬의 경로와 명령은 저장소 루트 기준이다. 코드와 설정에서 현재 동작을 재확인한다.

## 변경 경로와 실패 경계

- 접수·멱등성: `notification/NotificationService`, `outbox/IdempotentSubmissionService`에서 요청/Outbox의 원자적 저장과 유니크 제약을 추적한다. 같은 키·같은 본문 재응답, 다른 본문 충돌, 동시 요청을 구분한다. `exists` 조회만으로 경합이 해결됐다고 판단하지 않는다.
- 발행: `outbox/OutboxRelay`, `OutboxStore`에서 선점/lease, 트랜잭션 밖 publish, future ACK 후 PUBLISHED 저장을 추적한다. ACK 전 종료와 ACK 후 DB 저장 전 종료의 재발행 가능성을 각각 확인한다.
- 소비: `worker/NotificationWorker`, `WorkerStore`, `DeliveryTaskStore`와 offset commit 경로를 확인한다. 비동기 완료가 앞선 미완료 offset을 건너뛰거나 오류가 성공 commit으로 바뀌지 않는지 검증한다.
- 재시도·DLQ: `worker/RetryPolicy`와 DB due 시각, 시도 예산, backoff/jitter, 최종 실패/DLQ Outbox의 저장 경계를 확인한다. 재전달과 실제 공급자 재호출은 다른 사건이다.
- 공급자 보호: 동시 실행/대기열/재시도 상한, timeout, lease, quota와 여러 JVM 간 공유 범위를 확인한다. 단일 JVM 관측으로 다중 인스턴스 보호를 주장하지 않는다.

위 클래스는 `src/main/java/dev/jinhyeon/notificationlab/` 아래에 있다. 경로가 바뀌면 `rg`로 실제 호출부를 찾는다.

## 검증 선택

변경한 실패 경계에 맞는 기존 테스트를 먼저 선택한다. 예를 들어 Worker 동시성은
`WorkerConcurrencyTest`, 프로세스 종료는 `OutboxProcessCrashTest` 또는
`WorkerProcessCrashTest`, 재할당은 `WorkerRebalanceTest`, commit wakeup은
`CommitWakeupRetryTest`를 확인한다. 이름만 맞추지 말고 실제 검증 조건을 읽는다.

```sh
./gradlew --no-daemon --console=plain test --tests '*RetryPolicyTest'
./gradlew --no-daemon --console=plain test
```

첫 명령은 재시도 정책을 위한 예시이며 전체 전달 보장의 검증이 아니다.
Java 21을 사용하고 실제 DB/Kafka 검증에는 Docker가 필요하다. Docker 부족으로 통합 검증을
스킵한 뒤 완료로 표시하지 않는다. 기본 test는 reconciliation·부하·pilot/probe 태그를
제외하므로 `build.gradle`과 관련 실험 문서에서 필요한 별도 task를 선택한다.
장시간 부하·원격 자원·실제 공급자 실행을 일반 회귀 테스트에 자동으로 추가하지 않는다.

## 결과 해석

명령, 대상 코드, 환경/동시성, 실제 실행 수·실패·skip, 관측한 DB 상태·offset·공급자 호출을
함께 기록한다. 캐시된 테스트와 새 실행을 구분하고 관련 실험의 전후 조건을 맞춘다.
접수, 고유 작업 완료, 재전달, 재시도, 중복 공급자 수락을 분리해 집계한다.
Mock 수락은 단말 도착이나 exactly-once의 증거가 아니다.
민감한 본문·토큰·수신자 정보 없이 `docs/PROGRESS.md`의 관련 기록과 현재 요약을 갱신한다.
