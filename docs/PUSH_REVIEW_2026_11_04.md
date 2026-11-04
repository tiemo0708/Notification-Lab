# 2026-11-04 푸시 검토 자료

현재 main의 로컬 검증/커밋을 검토하기 위한 기록이다. 이번 단계까지 누적 30개 로컬 커밋을 검토한다. 푸시나 원격 CI 완료 기록이 아니다.
원격 main은 읽기 전용 조회에서 `9cf178636edc4b03a5aa1d560f9cfe3c71f36568`이었다.
Git identity는 기존 JinHyeon, GitHub HTTPS 인증 계정은 tiemo0708을 유지했다.

## 누적 변경의 이유와 범위

원격 이후 로컬 커밋에는 10만~100만 건 HTTP Mock 실험, 실패 원인 조사와 작은 제어 실험,
Outbox 잠금 수정/선택적 완료 저장 묶음, Redis 일시 오류의 유한 복구, S8 동시처리수 비교가 포함된다.
실험 원본과 실패 기록이 다수를 차지한다. 가장 큰 개별 원본은 약 20.44MB인 100만 건 성공 결과다.

| 확인할 변경 | 확인할 파일 | 판단 기준 |
| --- | --- | --- |
| 교차 역할 선점 잠금 | [OutboxStore](../src/main/java/dev/jinhyeon/notificationlab/outbox/OutboxStore.java), [실험](S7_OUTBOX_CLAIM_LOCKS.md) | 역할별 인덱스와 claim 전용 READ COMMITTED, 트랜잭션 경계 |
| ACK 뒤 완료 저장 묶음 | [OutboxRelay](../src/main/java/dev/jinhyeon/notificationlab/outbox/OutboxRelay.java), [비교](S7_RELAY_100K_COMPARISON.md) | Kafka ACK 성공 뒤 PUBLISHED, 기본 1 유지, 선택 상한 10 |
| Redis 진단과 일시 오류 복구 | [계약](S7_QUOTA_RECOVERY.md), [RedisRateLimiter](../src/main/java/dev/jinhyeon/notificationlab/ratelimit/RedisRateLimiter.java), [WorkerPauseErrorHandler](../src/main/java/dev/jinhyeon/notificationlab/worker/WorkerPauseErrorHandler.java) | 기본 OFF, 알려진 일시 오류만 최대 3번 pause, 미완료 offset 유지 |
| 기존 실패와 성공 근거 | [두 번째 실패 분석](S7_OBSERVED_MILLION_FAILURE.md), [성공 결과](S7_DELIVERY_MILLION_SUCCESS.md) | FAIL 원본 보존, 성공 범위와 미확정 원인 구분 |
| 이번 S8 비교 | [결과](S8_CONSUMER_SCALING.md), [소스](experiments/s8-consumer-scaling-2026-11-04-group-2-source.json), [집계](experiments/s8-consumer-scaling-2026-11-04-group-2-summary.json) | 1만 건 × 6회, Consumer/maxActive 함께 변경, 운영 기본값 유지 |
| CI 실행 상한 | [workflow](../.github/workflows/test.yml) | 기존 로컬 회귀 시간을 반영한 15→30분, 원격 실행은 미확인 |
| 다음 계획과 실제 이력 | [PLAN](../PLAN.md), [PROGRESS](PROGRESS.md) | 완료 표시와 측정/미검증 범위 일치 |

## 실제 검증과 남은 한계

- e5e5b01의 장시간 JUnit 1개: 4시간 19분 16초, 캠페인 1000000+실시간 54124건 전수 대조 PASS, 중복 0.
- S8 별도 JUnit 6회: 캠페인 60000+실시간 4558건 전수 대조 PASS, 중복/잘못된 참조 0.
  동시처리수 1/2/3의 평균은 47.324/67.566/94.154건/초였다.
- 성능 실험 종료 후 전체 `build`: **18분 18초/종료코드 0**, 기본 test 새 실행.
  콘솔과 JUnit XML 44개 클래스에서 **349개 통과, 실패·오류·스킵 0**을 대조했다.
  bootJar는 UP-TO-DATE로 기존 산출물을 재사용했다. [검증 원본](experiments/s8-consumer-scaling-2026-11-04-validation.json)을 참고한다.
- 첫 100만 건 실행의 5초 API timeout 원인은 미확정이다. 후속 성공 실행과 S8의 Redis 오류는 0회여서 복구 발동 효과를 입증하지 않는다.
- RECORD 기본 정책, Consumer 기본 3, 완료 저장 기본 1, 복구 기본 OFF를 유지한다.
  Consumer/maxActive의 개별 효과, 여러 서버 확장, 실제 FCM/단말 도착, 운영 최적값은 검증 범위 밖이다.
- 원격 CI 결과는 아직 없다. 기존 게시 이력을 재작성하거나 자동 병합하지 않는다.

승인 후 실행할 작업은 검토된 main 커밋을 origin/main으로 일반 푸시하는 것이다.
푸시 직전 원격 main을 다시 확인하고, 원격이 변경됐다면 충돌 여부부터 검토한다.
