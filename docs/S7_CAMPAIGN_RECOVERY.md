# S7: 강제 종료된 Batch의 소유권과 재시작

2026-10-20. 같은 캠페인 UUID로 명령을 다시 실행하면, 이 버전이 남긴 STARTING/STARTED/STOPPING/UNKNOWN 실행을
소유권 확인 뒤 FAILED로 정리하고 마지막 **커밋된** checkpoint부터 이어 처리한다.
Batch COMPLETED는 Outbox까지의 DB 인계 완료다. 공급자 수락이나 단말 도착을 의미하지 않는다.

## 문제 상황과 판단 기준

프로세스가 1~3번을 커밋하고 4~6번을 처리하다 강제 종료되면 Spring Batch에 STARTED가 남는다.
상태만 FAILED로 바꾸면, 사실은 GC·네트워크 문제로 잠시 멈췄던 이전 프로세스가 돌아와 쓸 수 있다.
업무 Writer만 차단해도 부족하다. 이전 JobRepository가 checkpoint나 실행 상태를 덮어쓰지 못해야 한다.

이번 단계는 **쓰기 소유권을 잃은 이전 실행이 업무 데이터와 Batch 메타데이터 모두에 다시 접근할 수 없는가**를
기준으로 선택했다. 만료 시간만 보고 프로세스가 죽었다고 판단하지 않는다.

| 선택지 | 장점 | 비용/한계 |
| --- | --- | --- |
| 상태/경과 시간만 보고 FAILED 변경 | 구현이 작음 | 살아 있는 이전 실행의 쓰기를 막지 못함 |
| lease + fencing token | 연결을 여러 개 쓰는 구조와 확장 가능 | 모든 업무·메타데이터 트랜잭션의 토큰 검사와 갱신이 필요함 |
| MySQL 세션 잠금 + 같은 물리 연결로 모든 실행 쓰기 | 단일 스레드·동기 Batch 구조에서 DB가 이전 연결의 쓰기를 직접 차단 | 실행 동안 연결 1개 점유, 다중 DB/failover/병렬 Step에 그대로 확장할 수 없음 |

세 번째 방식을 적용했다. 현재는 한 프로세스에서 캠페인 하나를 동기 실행하며, 업무 DB와 Batch 저장소가 같은 MySQL이다.
다중 스레드 Step이나 partition 처리를 추가하려면 이 설계를 그대로 재사용하지 않고 별도의 fencing 설계를 먼저 해야 한다.

## 소유권과 트랜잭션

1. 캠페인 정의/START_REQUESTED를 확인하고, 풀에서 빌리지 않은 전용 MySQL 물리 연결을 연다.
2. DB 이름과 캠페인 UUID에서 만든 이름으로 `GET_LOCK(name, 0)`을 실행한다. 대기열이나 자동 재시도는 없다.
3. 잠금을 얻지 못하면 기존 소유자가 있는 것으로 보고 실행을 거절한다. STARTED 기록이 오래됐어도 바꾸지 않는다.
4. 그 연결만 반환하는 DataSource와 TransactionManager로 실행 전용 JobRepository/JobExplorer/동기 Launcher를 구성한다.
5. 미완료 기록이 있으면 `ownershipProtocol=mysql-session-v1`을 확인한다. 이 값은 비식별 JobParameter이며
   실행 생성 트랜잭션에 함께 저장된다. JobInstance 식별자는 기존대로 campaignId뿐이다.
6. 이전 실행의 불확실한 Step/Job 상태를 한 트랜잭션에서 FAILED로 정리한다. checkpoint, 대상 매핑, 완료 Step은 보존한다.
   상태 정리 중 실패하면 전부 롤백한다. 새 실행은 같은 JobInstance에서 마지막 커밋 위치를 읽는다.
7. Writer·checkpoint·실행 상태의 읽기/쓰기는 전용 연결을 사용한다. 마지막에 연결을 닫아 잠금을 반환한다.

[MySQL 8.4의 GET_LOCK](https://dev.mysql.com/doc/refman/8.4/en/locking-functions.html)은 트랜잭션 커밋으로 풀리지 않고,
세션 종료 시 해제된다. 따라서 chunk를 여러 번 커밋하는 동안 소유권이 유지된다.
연결이 끊긴 이전 JVM이 재개하더라도 그 연결로 쓸 수 없다. 새 실행은 새로운 연결로만 시작한다.

Connector/J의 `autoReconnect`, `autoReconnectForPools`가 켜진 연결과 다중 호스트 프록시는 거절한다.
프레임워크가 `getConnection()`을 다시 호출해도 같은 물리 연결을 반환하며 풀에서 새 연결을 빌리지 않는다.
연결은 기본 `spring.datasource.url/username/password` 설정을 사용한다. Hikari 전용 옵션은 이 연결에 적용되지 않는다.

**시퀀스 번호 할당은 예외다.** Spring의 MySQL incrementer는 내부에서 자체 커밋하므로 기존 풀의 별도 연결을 사용한다.
이를 실행 연결과 공유하면 메타데이터 저장 도중 의도하지 않은 커밋이 생긴다.
별도 연결은 BATCH_*_SEQ 번호만 할당하고 업무 행·실행 상태·checkpoint를 쓰지 않는다. 미사용 번호의 빈 간격은 허용한다.
[Spring JDBC 구현](https://github.com/spring-projects/spring-framework/blob/v6.2.19/spring-jdbc/src/main/java/org/springframework/jdbc/support/incrementer/MySQLMaxValueIncrementer.java)을 참고한다.

## 실행과 거절 시 대응

기존 명령을 그대로 사용한다. 생성/시작 API만으로 자동 실행되지는 않는다.

```bash
./gradlew campaignBatch --args='캠페인_UUID'
```

| 상태 | 같은 명령의 결과 |
| --- | --- |
| 다른 MySQL 세션이 소유 중 | 즉시 거절. 실행 상태/데이터 변경 없음 |
| 소유 세션 종료 + 새 프로토콜의 STARTED/UNKNOWN | 이전 기록을 FAILED로 정리한 뒤 같은 인스턴스에서 재시작 |
| FAILED/STOPPED | 기존 checkpoint에서 재시작 |
| COMPLETED | 기존 실행 결과 반환, 추가 인계 없음 |
| 구버전이 남긴 STARTING/STARTED/STOPPING/UNKNOWN | 소유권 증명 불가로 거절, 상태 유지 |
| 복구 상태 저장 실패 | Step/Job 상태 정리 전체 롤백, 원인을 제거한 뒤 같은 명령 재실행 |

명령은 성공 시 종료 코드 0, 실패/소유권 충돌 시 1이다. 성공 출력은 executionId/status만 포함한다.
프로세스를 일시 정지한 것만으로 소유권을 가져올 수 없다. 네트워크 단절 시 DB가 이전 세션을 살아 있다고 보면 계속 거절한다.
이 경우 가용성보다 중복 실행 방지를 우선한다. 명령이 다른 세션을 임의로 KILL하거나 시간 기준으로 강제 탈취하지 않는다.

이 프로토콜을 사용하는 실행기는 같은 MySQL 서버/DB를 사용해야 한다. 잠금은 단일 MySQL 서버 범위이므로,
프록시를 통한 독립 primary 전환·split brain·자동 DB failover는 검증/지원 범위가 아니다.
구버전 실행기와 새 버전의 동시 운용, 메타데이터/프로토콜 표시의 수동 변경도 지원하지 않는다.
구버전이 남긴 불확실한 실행에는 자동 복구를 제공하지 않는다. FAILED로 임의 수정하거나 run.id를 추가해 우회하지 않는다.

## 실패 실험

CampaignBatchTest에서 격리된 MySQL 8.4.8/Kafka 4.0.2/Redis 7.4.11을 사용한다.

- **실제 JVM 강제 종료:** 7명/chunk 3, 앞 3건 커밋 후 다음 chunk에서 JVM을 강제 종료한다.
  STARTED와 checkpoint 3을 확인하고 새 JVM 명령으로 4건을 이어 저장한다. 인스턴스 1개, 실행 2개, 최종 대상 7건이다.
- **살아 있는 이전 JVM:** 앞 chunk 이후 JVM을 SIGSTOP한다. DB 세션이 살아 있으므로 재시작이 거절된다.
  테스트 관리자 연결에서 해당 MySQL 세션을 종료하고 새 실행을 완료한다. 이전 JVM을 SIGCONT하면 종료 코드 1이며,
  업무 행뿐 아니라 Job/Step/StepExecutionContext의 전체 조회 결과도 바뀌지 않는다.
- **UNKNOWN과 복구 실패:** 불확실 상태를 주입하고 Job 상태 갱신 트리거를 실패시킨다.
  먼저 시도한 Step 상태 변경도 롤백되는지 확인한 뒤, 실패를 제거하고 checkpoint 3부터 총 7건으로 완료한다.
- **구버전/자동 재접속:** 소유권 표시가 없는 STARTING/UNKNOWN은 유지하고, 자동 재접속 연결은 실행 생성 전 거절한다.
- **실행 생성 원자성:** 파라미터 저장을 실패시켜 인스턴스/실행/파라미터가 부분 커밋되지 않는지 확인한다.
  시퀀스 번호 할당은 독립적이어도 실행 생성의 원자성은 유지해야 한다.
- **서로 다른 캠페인:** 두 앱에서 각각 실행해 대상별 ID와 소유권이 섞이지 않는지 확인한다.
- 기존 chunk/checkpoint 원자성, 두 앱 동시 실행 거절, ACK 유실/재발행, 완료 재요청 검증도 유지한다.

실제 JVM 정지/재개 실험은 macOS/Linux의 POSIX `kill -STOP/-CONT`를 사용한다. 로컬 macOS와 Ubuntu CI 환경을 대상으로 하며,
Windows에서 같은 기본 테스트를 실행하려면 이 신호 실험을 지원하는 환경이 필요하다. 지원하지 않는 OS에서 조용히 스킵하지 않는다.

이는 작은 실패 실험이다. 10만/100만 건 DB·발송 부하는 다음 단계다.
실행 명령과 실제 테스트 수/결과는 [PROGRESS](PROGRESS.md)에 기록한다.
