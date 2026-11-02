# S7 DB 대기와 커밋 후 호출자 대기 구분

2026-11-03 한국 시간. 시작3f4362b/main.
[요청별 접수 진단](S7_API_ADMISSION_TIMING.md)에서 긴 SQL/commit 호출을 관측했지만 그 시간이 전부 DB 내부 처리인지는 알 수 없었다.
이번에는 원인을 미리 정한 작은 대조군으로 어떤 증거를 볼 수 있는지 검증한다.
**이 실험은 기존10만 건의p95 악화나100만 건의5초 timeout 원인을 재현·해결한 결과가 아니다.**

## 실행 범위

실제 운영 `SubmissionService`, `JdbcNotificationStore`, `DeliveryTaskStore`와 Spring 트랜잭션 프록시를 작은 별도 context로 실행한다.
HTTP 컨트롤러/Kafka/Relay/Worker/공급자는 시작하지 않는다. SQL 경계만 다루며 HTTP202 응답 검증이나 발송 성능 실험으로 표현하지 않는다.
운영 Java/resources/기본 RECORD/풀·격리 수준 설정은 변경하지 않는다.

- MySQL8.4.8/2CPU/2GiB/buffer512MiB, Java21/JVM512MiB.
- 앱Hikari 최대4개, 별도 blocker1개/observer1개 연결. 관찰자는 이 격리 컨테이너의 root 권한으로 읽기 전용 조회한다.
- 워밍업 접수3개, 정상·Outbox 범위 잠금·native commit 이후 호출자 대기 각1개. 전체 접수는6개다.
- 단일 platform worker/대기 큐1개, 강제 보류200ms, DB 잠금5초/쿼리5초/관찰 쿼리2초/관찰2초/gate3초/future8초 상한.
- SQL 본문/본문 인수/수신자/토큰/알림·세션·스레드·잠금 ID는 결과에 기록하지 않는다. 시간·호출 수·대기 관계 수·불변 조건만 보관한다.

```bash
caffeinate -i ./gradlew --no-daemon --console=plain apiWaitProbe
```

별도 opt-in task이며 `test/check/build`는 이 제어 실험을 실행하지 않는다.
출력은 `build/reports/api-wait-probe/result.json`이다.10만/100만 건을 발송하지 않는다.

## 세 조건의 차이

| 조건 | 의도적으로 만든 상황 | 독립 연결로 확인하는 증거 |
| --- | --- | --- |
| BASELINE | 아무 지연도 주입하지 않은 신규 접수 | 메서드 정상 반환 후 알림/작업/Outbox 행이 모두 증가한다. |
| OUTBOX_GAP_LOCK | blocker의REPEATABLE READ 트랜잭션에서 Outbox 기본키 전 범위를 잠금 조회해 새 INSERT를 막는다. | `performance_schema.data_lock_waits`와`data_locks`를 조인한 Outbox 대기 관계가 존재한다. API 트랜잭션의 미커밋 행은 아직 보이지 않는다. |
| POST_COMMIT_CALLER_PAUSE | 실제 JDBC commit이 정상 반환한 뒤 연결 래퍼에서 호출자 반환만 latch로 보류한다. | 다른 연결에서 접수3개 테이블의 새 행이 이미 보인다. 호출자 future는 끝나지 않았고 그 시점의 DB 잠금 대기 관계는0이다. |

Outbox 범위 잠금은 **재현을 위해 테스트가 일부러 만든 blocker**다. 현재 운영 Relay 선점 쿼리나 격리 수준을 이 형태로 되돌리지 않는다.
Outbox 테이블의 대기 관계 수를 읽는 관찰이며 개별 잠금 모드·키를 결과에 수집해 분석한 것은 아니다.
마지막 조건의 latch는 **합성 호출자 지연**이다. 실제 OS 선점이나 물리 네트워크 응답 유실을 재현했다고 표현하지 않는다.

두 지연 조건 모두 증거를 먼저 확인한 뒤 추가로200ms 보류한다. 관찰/스케줄링 비용이 있으므로 실제 측정값은200ms보다 길 수 있다.
blocker는 rollback으로 해제하고 commit 이후 gate는 finally에서도 해제한다. context/연결/실행기를 모두 종료한다.

## 최종 실제 결과

초기 연결 검증은22초/종료 코드0/테스트1개 통과/실패·오류·스킵0이었다.
그 뒤 테스트 설정을 `TestConfiguration`으로 분리하고 연결 proxy의Object identity 처리를 보강했다.
[최종 소스 해시](experiments/s7-api-wait-2026-11-03-source.json)를 고정해 새 격리 MySQL에서 재실행했다.
최종은 **BUILD SUCCESSFUL17초/종료 코드0/테스트1개 통과/실패·오류·스킵0**이었다.

| 조건 | 전체 트랜잭션 wall ms | 호출자 thread CPU ms | 네 번째 SQL ms | JDBC commit 호출 ms |
| --- | ---: | ---: | ---: | ---: |
| BASELINE | 12.210 | 7.388 | 0.517 | 1.105 |
| OUTBOX_GAP_LOCK | 250.688 | 3.050 | 240.601 | 3.796 |
| POST_COMMIT_CALLER_PAUSE | 216.580 | 2.742 | 1.163 | 208.463 |

- 잠금 대기 조건에서 Outbox 대기 관계1개와 미커밋 행의 비가시성을 확인했다. 해제 후 정상 완료했다.
- 호출자 지연 조건에서 native commit 반환과 접수 행의 가시성을 확인했다. 그때 future는 미완료/대기 관계0이었고 gate 해제 후 정상 완료했다.
- 각 측정 접수의 연결 획득1회/SQL4회/commit1회/연결 반환1회/rollback0, 전체 알림·작업·Outbox 각6개를 확인했다.
- 발송 시도 이력은0개, 실제 공급자 호출은0회다. 접수 SQL 정합성 결과이며 단말 도착·발송 처리량을 측정하지 않았다.

### CPU 시간으로 알 수 있는 것

`ThreadMXBean`의 현재 platform thread CPU 시간을 전체 접수 트랜잭션 전후로 읽는다.
DB 서버 프로세스의 CPU나 요청의 각SQL CPU 시간을 읽는 것이 아니다. 지원/활성 상태를 확인하며 조용히0으로 대체하지 않는다.
필요할 때 이 테스트 JVM에서만 활성화하고 원래 설정으로 복원한다.

wall−CPU는정상4.822ms/잠금247.638ms/호출자213.838ms였다.
이는 **그 호출자 스레드가 CPU에서 실행하지 않은 시간을 포함한 차이**다.
DB 서버 처리·네트워크 대기·latch·GC·스케줄링 등 여러 구간이 섞이므로 순수DB 대기나 순수OS 스케줄링 시간으로 부르지 않는다.
정밀도가 nanosecond라는 것과 실제 정확도가 nanosecond라는 것도 다르다.

이번에는 원인을 알고 있는 대조군과 독립 DB 관찰을 함께 사용했기 때문에 두 지연을 구분할 수 있었다.
일반 부하에서 CPU가 적고 wall이 길다는 사실만으로 같은 원인을 확정할 수는 없다.

## 배운 점과 남은 문제

1. SQL이 길 때 실제 DB 잠금 대기 관계를 관측하면 DB 대기라는 추가 증거가 된다. 순간 관계0은 전체 기간의 대기 부재를 보장하지 않는다.
2. commit 호출이208ms여도 DB commit 자체는 이미 완료됐을 수 있다. JDBC caller 타이머를 DB fsync 시간과 동일시하면 안 된다.
3. 기존239.539ms 요청의 commit139.999ms가 이번 latch와 같은 원인이었다고 추론하면 안 된다.
4. 기존 혼합 부하의 긴 요청에서는 세션과 시간 구간이 대응되는 DB 관찰이 더 필요하다. 현재 결과만으로 풀·인덱스·격리 수준을 바꾸지 않는다.

다음은 실제 혼합 요청에 별도 관찰 연결을 대응시키는 방법과 관찰 오버헤드/권한/메모리 상한을 정하는 작은 단계다.
10만 건p95 악화/100만 건timeout의 자연 발생 원인은 여전히 미확정이다. 기본 정책을 유지하고 장시간 부하를 재실행하지 않는다.

## 원본

- [초기 연결 검증](experiments/s7-api-wait-2026-11-03-initial.json): 최종 설정 분리 전의 검증이며 최종 수치 표에는 사용하지 않는다.
- [최종 제어 실험](experiments/s7-api-wait-2026-11-03-final.json): `60797f70976f37ede384e4dcf16c358c5f6f70dfdff72ea13e8532f0736b304c`
- 원본 내부 시각은UTC, 파일명의 날짜는 한국 작업일이다. 새 원본은 수정 없이 복사한다.

## 스스로 설명해 볼 질문

1. 두 지연 조건 모두wall이200ms 이상인데 어떤 독립 증거로 원인이 다름을 확인했는가?
2. 호출자CPU가3ms인데 SQL 호출이240ms일 수 있는 이유는 무엇인가?
3. DB 행은 보이는데 future가 끝나지 않은 상황에서 DB commit과 메서드 반환을 어떻게 구분하는가?
4. 이 결과를 보고 지난139.999ms commit 호출을 fsync 문제라고 결론 내릴 수 없는 이유는 무엇인가?

## 참고

- [MySQL8.4 data_lock_waits](https://dev.mysql.com/doc/refman/8.4/en/performance-schema-data-lock-waits-table.html)
- [MySQL8.4 InnoDB 잠금](https://dev.mysql.com/doc/refman/8.4/en/innodb-locking.html)
- [Java21 ThreadMXBean](https://docs.oracle.com/en/java/javase/21/docs/api/java.management/java/lang/management/ThreadMXBean.html)


## 기본 회귀와 보존

최종 `caffeinate -i ./gradlew --no-daemon --console=plain build`는 **BUILD SUCCESSFUL16분10초/종료 코드0**이었다.
38개 클래스의 새JUnit XML과 콘솔에서 **308개 통과/실패·오류·스킵0**을 대조했다.
기본 회귀는 실제 재실행했고 두 실행 JAR task는UP-TO-DATE였다. 별도제어실험1개/3조건 결과와 구분한다.
고정 소스6개 해시와 운영 Java/resources diff0을 확인했다. 원래100만 건 FAIL 원본도 유지했다.
[실행 명령·시각·원본 해시·회귀 결과](experiments/s7-api-wait-2026-11-03-execution.json)를 보존한다.
비유한 값/UUID/민감 필드 미포함과 문서 링크를 확인했다. 사용자DB/.env/IntelliJ/volume을 변경하지 않았다.
10만/100만 건 재실행·실제 공급자·유료 자원·푸시는 하지 않았다.
최종 Docker에는 기존 notification-lab-mysql-1만healthy로 남았고 격리 컨테이너는 정리됐다.
