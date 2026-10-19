# S7: Spring Batch checkpoint와 Outbox 인계

2026-10-19. 특정 START_REQUESTED 캠페인 한 개를 명시적으로 실행해 가상 대상별 알림·발송 작업·Outbox를 만든다.
Spring Batch COMPLETED는 **모든 대상을 DB Outbox에 인계했다는 뜻**이다. Kafka 발행 완료, 공급자 수락,
단말 도착을 뜻하지 않는다. 기존 캠페인 상태는 시작 의도인 START_REQUESTED를 유지한다.

## 문제와 선택

대상 7명을 chunk 3개씩 처리한다고 가정한다. 1~3번 저장 후 4~6번 처리 중 실패하면,
재시작은 4번부터 시작해야 한다. checkpoint만 먼저 저장하면 작업을 잃고, 작업 저장과 checkpoint가
각각 커밋되면 같은 범위를 중복 저장할 수 있다.

| 대안 | 이점 | 실패 경계와 비용 |
| --- | --- | --- |
| Kafka 직접 발행 후 checkpoint | DB Outbox 적재가 필요 없음 | ACK 후 DB checkpoint 전 실패하면 재발행. 실패한 publish의 수락 여부도 불확실 |
| 대상마다 별도 트랜잭션 접수 API 호출 | 기존 단건 API 재사용 가능 | chunk와 별도 커밋되어 일부만 저장됨. 보상/중복 처리와 추적 필요 |
| chunk 내 알림·작업·Outbox·checkpoint 동시 저장 | MySQL 한 트랜잭션으로 인계와 재개 위치를 확정 | Outbox 저장량과 Relay 지연이 생김. Kafka 전송 자체는 별도 경계 |

세 번째 방식을 선택했다. 기존 IdempotentSubmissionService는 caller 트랜잭션을 중단하므로
Batch Writer에서 호출하지 않는다. Writer의 모든 DB 쓰기와 JobRepository는 같은 DataSource와
PlatformTransactionManager를 사용한다. 실제 checkpoint 저장 실패를 주입해 함께 롤백되는지 검증했다.

Spring Batch 5.2의 [chunk 설정](https://docs.spring.io/spring-batch/reference/5.2/step/chunk-oriented-processing/configuring.html)과
[ItemStream 계약](https://docs.spring.io/spring-batch/reference/5.2/readers-and-writers/item-stream.html)에 따라
Reader의 update에서 마지막 순번을 ExecutionContext에 넣으며, 커밋 전 저장한다.
[JobRepository](https://docs.spring.io/spring-batch/reference/5.2/job/configuring-repository.html)는 실행 이력과 재시작 상태를 영속화한다.

## 처리 흐름

1. 생성/시작 API로 정의와 시작 의도를 저장한다. 이 단계만으로 Batch를 실행하지 않는다.
2. 명령으로 캠페인 UUID 하나를 지정한다. DRAFT/없는 캠페인은 거절한다.
3. Reader가 마지막 커밋 순번 다음부터 가상 대상을 읽는다. 기본 chunk는 100, 허용 범위 1~1,000이다.
4. Writer가 notification, delivery_task, outbox_event, campaign_target을 저장한다.
5. 같은 트랜잭션에 `campaign.lastOrdinal` checkpoint를 저장하고 커밋한다. skip/자동 retry는 사용하지 않는다.
6. 별도 CAMPAIGN Relay가 Outbox를 읽어 Kafka에 전송한다. publish future 성공 뒤에만 PUBLISHED를 기록한다.
7. 기존 CAMPAIGN Worker가 발송 예산·회로·시도 제한·DB 완료/offset 정책으로 처리한다.

Batch 명령 자체는 Relay/Worker를 끈다. 명령 종료 시 그 프로세스의 미전송 메모리 작업에 의존하지 않는다.
Relay/Worker 앱을 따로 켜 두면 커밋된 앞 chunk부터 처리될 수 있으므로, **나중 chunk 실패가 이미 인계된 발송을 취소하지 않는다.**

## 실행과 조회

기존 [S6 역할 실행](S6_TRAFFIC_ISOLATION.md)과 [캠페인 API](S7_CAMPAIGN_API.md)를 따른다.
MySQL/Kafka 연결 환경을 설정하고, 생성 후 `/campaigns/{id}/start`를 호출한다.
로컬 .env의 DB 비밀번호는 기존과 같이 현재 셸의 환경변수로 로딩한다.

```bash
# 시작 요청을 저장한 캠페인 한 개만 실행
./gradlew campaignBatch --args='생성한_UUID'

# 동일 명령: FAILED/STOPPED는 마지막 커밋에서 재시작, COMPLETED는 기존 결과 반환
./gradlew campaignBatch --args='같은_UUID'
```

성공 시 `CAMPAIGN_BATCH executionId=... status=COMPLETED`와 종료 코드 0을 반환한다.
실패/진행 중 충돌/잘못된 ID는 종료 코드 1이다. 명령의 최종 실패 요약은 예외 타입만 출력한다.
Spring Batch는 처리 실패를 로그/메타데이터에 남긴다. Writer의 SQL 예외는 본문 노출을 막도록 안전한 타입 메시지로 바꾼다.

`bootRun`/일반 실행 JAR는 여전히 HTTP 앱을 실행한다. `app.campaign.batch.enabled` 기본 false이며
설정만 켜도 예약 실행이나 전체 캠페인 순회는 발생하지 않는다. 명령이 CAMPAIGN/split/Batch를 명시적으로 켠다.
명령은 메타데이터의 LocalDateTime이 UTC로 저장되도록 전용 JVM 시간대를 UTC로 설정한다.
직접 Batch 인프라를 켜는 앱도 CAMPAIGN 역할과 UTC JVM이어야 한다. 일반 앱의 시간대는 변경하지 않는다.

Spring Batch 버전은 새 버전을 임의 고정하지 않고 기존 Boot 3.5.16 BOM의 **5.2.6**을 사용한다.
Flyway V9는 framework 메타데이터 테이블과 campaign_target만 추가한다. 기존 행과 V1~V8은 변경하지 않는다.
BATCH_* 테이블/컬럼 이름은 프레임워크가 요구하는 공식 MySQL 스키마를 유지한다.
업무 테이블 campaign_target은 기존 snake_case 규칙과 PK/unique/FK를 따른다.

실행 결과를 DB에서도 확인할 수 있다. 같은 JobInstance에 실행 이력이 누적되며 매번 새 인스턴스를 만들지 않는다.

```sql
SELECT i.JOB_INSTANCE_ID, e.JOB_EXECUTION_ID, e.STATUS, e.EXIT_CODE
FROM BATCH_JOB_INSTANCE i JOIN BATCH_JOB_EXECUTION e USING (JOB_INSTANCE_ID)
WHERE i.JOB_NAME = 'campaign-expand-v1'
ORDER BY e.JOB_EXECUTION_ID;

SELECT STEP_EXECUTION_ID, JOB_EXECUTION_ID, STATUS, READ_COUNT, WRITE_COUNT, COMMIT_COUNT, ROLLBACK_COUNT
FROM BATCH_STEP_EXECUTION ORDER BY STEP_EXECUTION_ID;

SELECT campaign_id, COUNT(*) AS durable_targets, MAX(target_ordinal) AS highest_target
FROM campaign_target GROUP BY campaign_id;
```

이 SQL의 대상 행 집계는 발송 완료 수가 아니다. 실제 결과는 연결된 notification/delivery_attempt과
Outbox 상태, Kafka lag를 구분해 조회한다. ExecutionContext는 Spring Batch가 직렬화하므로 수동 편집하지 않는다.

## 동일 캠페인과 대상의 식별

Job 이름은 `campaign-expand-v1`, 식별 JobParameter는 campaignId 하나다. 시간/run.id를 붙여 새 인스턴스로 우회하지 않는다.
재시작 때 대상 정의/ID 규칙과 발송 정책 설정을 유지한다. 직접 DB 정의 수정이나 실행 중 코드 교체는 지원 범위가 아니다.
프로세스당 실행 슬롯은 1개이며 추가 호출을 메모리 대기열에 쌓지 않는다. 서로 다른 프로세스의 같은 캠페인은
JobRepository의 실행 상태/트랜잭션/유니크 제약으로 조율한다. 실행 중인 동일 인스턴스는 거절한다.

대상별 고정 ID의 입력은 UTF-8 `notification-lab:campaign:v1:<campaignId>:<ordinal>:<kind>`이며,
Java UUID.nameUUIDFromBytes를 사용한다. kind는 notification/task/event로 나눠 이름 공간을 분리한다.
이는 보안 토큰이나 외부 공급자 멱등 키가 아니다. 기존 캠페인의 매핑 규칙을 바꾸지 않는다.

campaign_target의 `(campaign_id, target_ordinal)` PK와 notification/task/event unique/FK가 중복을 제한한다.
기존 매핑을 읽는 것은 재생 최적화이며 최종 경쟁은 DB 제약이 결정한다. 경쟁 INSERT는 조용히 무시하지 않고
chunk를 실패/롤백시킨다. 기존 매핑·정의가 맞으면 그대로 재생하고 발송 완료·시도 횟수·PUBLISHED를 초기화하지 않는다.

## 실패와 재시작의 의미

| 상황 | 결과/대응 |
| --- | --- |
| chunk Writer 실패 | 해당 chunk 전부 롤백, 이전 커밋 유지. 원인 제거 후 동일 UUID로 명시적 재시작 |
| checkpoint 저장 실패 | 업무 행과 checkpoint 함께 롤백. 실제 DB 트리거로 검증 |
| 완료 후 동일 명령 | 기존 COMPLETED 실행 반환. 대상/작업/Outbox 추가 없음 |
| Kafka ACK 확인 전 | Outbox는 PUBLISHED가 아님. Batch checkpoint는 DB 인계 기준이라 이미 앞설 수 있음 |
| Kafka 수락 후 ACK 결과 유실 | Relay가 재발행할 수 있음. 같은 알림/작업 ID를 유지해 Worker의 기존 중복 처리 적용 |
| JVM 강제 종료/DB 결과 불확실성 | STARTED/UNKNOWN이 남을 수 있음. 자동으로 FAILED로 바꾸거나 새 JobInstance로 우회하지 않음 |

FAILED/STOPPED로 확인된 실행의 재시작을 이번에 연결했다. **강제 종료로 남은 STARTED/UNKNOWN의
안전한 복구 명령은 아직 제공하지 않는다.** 이전 실행이 정말 종료됐는지, 다른 호스트에서 쓰고 있지 않은지
확인하는 소유권/차단 절차 없이 상태를 바꾸면 중복 실행될 수 있다. 이 경우 메타데이터를 임의 수정하지 말고,
다음 단계에서 프로세스 종료·소유권 확인·안전한 재시작을 별도로 검증한다.

## 검증 범위

실제 MySQL 8.4.8/Kafka 4.0.2/Redis 7.4.11에서 다음을 확인한다.

- 대상 7명/chunk 3: 두 번째 chunk의 5번 저장 실패 후 3건/checkpoint 3만 남고, 새 앱에서 4건을 읽어 총 7건으로 완료.
- checkpoint 테이블 갱신 실패에서도 앞 chunk 3건만 보존.
- 두 앱이 같은 캠페인을 실행하면 진행 중 두 번째 실행 거절.
- 2개 대상의 Kafka 수락 뒤 ACK 불확실성을 주입하면 레코드는 3개가 되지만 PUBLISHED/공급자 시도는 각각 2건.
  완료된 대상 Writer 재생 후에도 작업/이력/Outbox 상태를 유지.
- 별도 JVM에서 명령 실행/완료 재요청 및 DRAFT 거절. 일반 앱은 자동 실행하지 않음.
- 고정 ID 기준값, 저장 checkpoint와 미커밋 읽기의 구분, 잘못된 checkpoint, 트랜잭션 없는 Writer 거절.

```bash
./gradlew --no-daemon --console=plain test --tests '*CampaignBatchTest' --tests '*CampaignCheckpointTest'
./gradlew --no-daemon --console=plain build
```

실제 통과 수와 명령 결과는 [PROGRESS](PROGRESS.md)에 기록한다. 전체 대상의 영속 인계 정합성을 검증한 작은 실험이며,
10만/100만 건 DB·발송 부하, 외부 FCM/APNs, 전역 처리량/정확히 한 번 단말 도착을 입증하지 않는다.
