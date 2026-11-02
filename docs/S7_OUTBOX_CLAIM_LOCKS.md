# S7 — Outbox 선점의 교차 역할 잠금 재현과 수정

## 이번에 확인한 문제

역할별 토픽·연결 풀을 나눠도 공유 DB의 선점 쿼리가 다른 역할의 작업을 잠글 수 있었다.
기본 자동 통계 조건에서 적재 직후 상태 인덱스를 선택했고, 실시간1건을 찾는 SELECT가 캠페인 행도 잠갔다.
통계 갱신 후에는 역할별 인덱스로 바뀌어 이 현상이 사라졌지만 통계 상태에 의존했다.

역할별 인덱스를 명시하는 것만으로는 충분하지 않았다. REPEATABLE READ의 갭 잠금 때문에
캠페인 작업을 PENDING→PROCESSING으로 바꾸는 인덱스 키 삽입이 기다리는 경로도 관측했다.
수정은 기존 V7 역할별 인덱스 경로를 고정하고 **OutboxStore.claim의 짧은 트랜잭션만 READ COMMITTED**로 실행하는 것이다.
이 결과를 이전100만 건 실험의 API5초 응답 timeout 원인 확정으로 사용하지 않는다.

## 실험 조건과 관측 경계

```bash
# 기본 build에서 자동 실행되지 않는 별도 진단
./gradlew --no-daemon --console=plain outboxLockProbe
```

- MySQL8.4.8/2CPU/2GiB/buffer pool512MiB/JVM512MiB, 캠페인100000+실시간100 PENDING을 SQL로 저장한다.
  자동 통계 ON/persistent ON/서버 기본 REPEATABLE READ를 확인한다. 이 task는 수동 ANALYZE나 STATS_AUTO_RECALC 변경을 하지 않는다.
- 앱 풀4/별도 캠페인 갱신 풀1/격리 DB 관측용 root 풀1을 사용한다. root는 잠금·연결 메타데이터만 조회한다.
  쿼리 상한10초/관측 쿼리5초/갱신 실험의 lock_wait_timeout3초, Future8초/JUnit8분/task10분을 둔다.
- 실시간 PENDING SELECT를 실제 실행해 반환1건과 RECORD 잠금 존재를 먼저 확인한다.
  트랜잭션을 의도적으로 약1초 더 열어 두고 잠금과 다른 작업의 대기를 관측한다.
  실제 서비스에서 이만큼 트랜잭션을 지연하도록 구현한 것은 아니다.
- 별도 root 연결로 data_locks를 인덱스·모드별 집계하고 data_lock_waits에서 해당 선점자가 막는 관계를 확인한다.
  LOCK_DATA/행ID/연결·스레드ID는 결과에 저장하지 않는다. 잠금 레코드 수는 고유 업무 행 수가 아니다.
- 캠페인 기존 행의 no-op 갱신, 새 실시간 접수 트랜잭션, 캠페인 선점 전체를 구분한다.
  최초 v2에는 앞의2개만, v3 이후에는3개를 관측한다. 모두 rollback해 같은 데이터로 다음 조건을 비교한다.
- 적재 직후와20초 관측 창 뒤를 확인하고, 상태 인덱스/역할별 인덱스 강제 대조군을 별도로 실행한다.
  대조군 힌트를 실제 기본 옵티마이저 선택으로 오해하지 않는다. 수정 후 현재 선점 SQL 자체에는 역할별 힌트가 있다.
- 실제 SELECT의 Handler_read* 차이를 별도 연산 카운터로 기록한다. 잠금은 실행 계획의 행 수에서 추정하지 않는다.
  잠금·대기는 순간 관측이며, 미관측을 모든 부하에서 대기가 없다는 보장으로 사용하지 않는다.

[MySQL 잠금 설명](https://dev.mysql.com/doc/refman/8.4/en/innodb-locks-set.html),
[data_locks](https://dev.mysql.com/doc/refman/8.4/en/performance-schema-data-locks-table.html),
[data_lock_waits](https://dev.mysql.com/doc/refman/8.4/en/performance-schema-data-lock-waits-table.html)를 참고했다.

## 수정 전의 증거

[실제 SELECT 원본](experiments/s7-outbox-locks-2026-11-02-before.json)은 기본 자동 통계 조건에서 다음을 보였다.

| 조건 | 선택된 보조 인덱스 | PRIMARY의 잠금 레코드 | 캠페인 기존 행 갱신 대기 |
| --- | --- | ---: | --- |
| 적재 직후 | status_available | 100001 | 관측 |
| 20초 창 뒤 | traffic_status_available | 1 | 미관측 |
| 상태 인덱스 강제 대조군 | status_available | 100001 | 관측 |
| 역할별 인덱스 강제 대조군 | traffic_status_available | 1 | 미관측 |

보조 인덱스 잠금 수도 따로 저장했다. PRIMARY와 더해 고유 행 수로 세지 않는다.
새 실시간 접수는 이 조건에서 완료됐고 해당 선점자로 인한 대기는 관측되지 않았다.
따라서 **다른 역할의 기존 작업 잠금**과 **원래 실시간 API 접수 지연**을 같은 문제라고 단정하지 않는다.

[인덱스만 명시한 중간 원본](experiments/s7-outbox-locks-2026-11-02-index-only.json)은
PRIMARY 잠금을1건으로 줄여도 캠페인 선점에서 `X,GAP,INSERT_INTENTION` 대기가 남는 것을 보였다.
기본 회귀에서도 캠페인 선점이 테스트의3초 상한 안에 끝나지 않았다.
이 task는 상태 인덱스 음성 대조군에서 빈 선점 결과를 잘못 실패로 간주해42초/종료 코드1로 끝났다.
앞의 두 현재 경로 관측은 보존했고, 대조군은 빈 결과도 기록하도록 수정했다. 이 실행을 통과로 세지 않는다.

### 초기 계측의 한계도 보존

첫 [EXPLAIN-only 원본](experiments/s7-outbox-locks-2026-11-02-explain-only-preliminary.json)은42초/종료 코드0이었지만
모든 보유 잠금 집계가 비어 있었다. 이것으로 경합이 없다고 결론 내리지 않았다.
실제 SELECT 실행과 RECORD 잠금 존재 검증으로 계측을 바꿨다.
이전 [통계 비교](S7_TIMEOUT_DIAGNOSIS.md)의 EXPLAIN ANALYZE 실행 행 수는 SQL 실행 관측이지만 잠금 보유 증거로 사용하지 않는다.

## 변경과 안전성 검증

- `OutboxStore.candidate`는 `idx_outbox_traffic_status_available`을 명시해 오래된 통계에서도 역할 접두사로 읽는다.
  이 이름은 기존 V7 migration에 의존한다. 인덱스 추가/삭제나 전역 통계 설정 변경은 없다.
- `claim`만 READ COMMITTED다. 실제 Relay는 바깥 DB 트랜잭션 없이 호출해 이 경계를 연다.
  selected row의 FOR UPDATE 잠금, lease token/시도 상한/조건부 상태 기록은 유지한다.
  이미 외부 트랜잭션에서 호출하면 REQUIRED로 합류한다는 Spring 계약은 그대로다.
- 일반 검색의 갭 잠금을 줄이되 FK/중복 검사 등의 예외까지 없어졌다고 표현하지 않는다.
  [MySQL 격리 수준 설명](https://dev.mysql.com/doc/refman/8.4/en/innodb-transaction-isolation-levels.html)을 참고한다.
  두 후보 사이 새 작업이 생길 수 있으므로 엄격한 전역 선착순이나 고정 대기열 스냅샷을 보장하지 않는다.
- ACK 확인 전 PUBLISHED 금지, 외부 Kafka 호출 중 DB 트랜잭션 미유지, API/Batch/Worker의 다른 트랜잭션 경계는 유지한다.
- 새 기본 회귀는 실제 Spring transaction interceptor와 생산 코드의 선언을 적용한다.
  이 회귀는 기존 AsyncNotificationTest fixture/풀을 쓰며, 위 opt-in 진단의 개별 자원 상한과는 다르다.
  실시간 SELECT 직후 latch로 열어 둔 동안 다른 역할의 선점이 끝나는지 확인하고,
  두 작업 각각의 lease 보유/시도1, 공급자 호출0, claim 내부 READ-COMMITTED와 이후 연결의 REPEATABLE-READ를 확인한다.
  초기 외부 트랜잭션 버전과 interceptor 버전의 실패를 [요약](experiments/s7-outbox-locks-2026-11-02-regression.json)에 보존했다.
수정 후 선택 회귀와 세 진단을 함께 실행한 명령은 다음과 같다.

```bash
caffeinate -i ./gradlew --no-daemon --console=plain test --tests '*AsyncNotificationTest.realtimeClaimLeavesCampaignCandidatesClaimableWithStaleStatistics' outboxLockProbe outboxStatisticsProbe outboxQueryProbe
```

- 새 회귀1개+잠금/통계/쿼리 진단3개는 수정 후1분35초/종료 코드0으로 통과했다.
  [통계 도구 원본](experiments/s7-outbox-locks-2026-11-02-statistics-after.json)과
  [쿼리 도구 원본](experiments/s7-outbox-locks-2026-11-02-query-after.json)도 보존했다.
  이 도구들은 현재 생산 SQL을 캡처하므로 예전의 힌트 없는 결과를 그대로 재현하는 명령이 아니다.

최종 v4 [수정 후 원본](experiments/s7-outbox-locks-2026-11-02-after.json)은 네 조건 모두
PRIMARY1/보조 인덱스1의 X,REC_NOT_GAP과 READ-COMMITTED를 확인했다.
캠페인 갱신·캠페인 선점·새 실시간 접수의 holder 대기는 미관측이었고 각 probe는 holder 종료 전에 완료됐다.
수정 전과 같이100100행이 모두 PENDING/시도0/lease NULL로 남았고 초기·최종 집계가 같았다.

[수정 후1천 건 HTTP 원본](experiments/s7-outbox-locks-2026-11-02-http-1000.json)도 PASS다.
사전 저장2.251초/발송12.915초, 캠페인1000+실시간107=HTTP1107건을 DB/참조/offset/quota와 대조했다.
중복·잘못된 매핑0, 혼합 API 응답p95 12.279ms/최대18.107ms(57개)였다.
작은 단일 실행이므로100만 건 성능이나 기존 실행 대비 성능 개선으로 해석하지 않는다.

최종 명령은 다음과 같다.

```bash
caffeinate -i ./gradlew --no-daemon --console=plain outboxLockProbe campaignDeliveryPilot -PdeliveryTargets=1000 build
```

**BUILD SUCCESSFUL14분8초, 종료 코드0**. 기본 회귀33개 클래스/269개와 별도 잠금1개/HTTP1개,
합계271개가 통과했고 실패·오류·스킵0이었다.269개에는 새 교차 역할 회귀1개가 포함된다.
JAR를 재생성했고100만 건 task는 실행하지 않았다. 이14분8초는 두 별도 실험을 포함한 전체 명령 시간이다.

## 남은 범위

잠금을 일부러 유지한 SQL 실험과 작은 회귀이며 장시간 HTTP 부하/SLO/원래5초 timeout의 재현이 아니다.
갭 잠금 예외·다양한 역할/상태 분포·실제 자원 압박까지 모두 격리됐다고 보장하지 않는다.
100만 건 재실행은 하지 않았다. 다음은 수정된 경로의10만 건 HTTP 재검증과 접수 지연 관측을 거쳐
100만 건 재실행 조건을 정하는 단계다. S7의100만 건 검증과 원래 timeout 원인 진단은 미완료다.
