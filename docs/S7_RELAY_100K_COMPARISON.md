# S7 완료 저장1/10건의10만 건 HTTP 비교

2026-11-02. 시작 커밋37d4d3c/main. [1만 건 결과](S7_RELAY_PUBLISH_BATCH.md)에 이어 같은 최종 Java 코드와
같은 자원/예산에서 캠페인 완료 저장 크기만1→10으로 바꿔 각각1회 실행한다.
실시간 앱은 두 조건 모두1이다. 100만 건 재실행/기본값 변경/푸시는 이번 실행에 포함하지 않는다.

## 완료 조건과 해석

- 각 조건 캠페인100000개와 동시 실시간 요청을 HTTP 참조/DB 상태/성공 시도1회/대상 매핑/Outbox PUBLISHED/전체 consumer offset/quota로 전수 대조한다.
- 5초 접수 probe 제한, 메모리/표본/실행 시간 상한을 유지한다. 응답 미관측 시 서버 결과는 UNKNOWN이며 자동 재시도하지 않는다.
- 발송 처리량, 실시간 API p95/최대와 DB 완료 지연, 구간별 caller 시간, Hikari/heap/MySQL cgroup/CPU/GC를 기록한다.
- 집계된 행 잠금 대기와 순간 wait edge를 외부에서 읽는다. 결측과 관찰 오류를 숨기지 않으며, 샘플0을 잠금 부재의 보장으로 쓰지 않는다.
- SQL 실행 계획과 held transaction의 역할 간 간섭은 별도 격리 SQL 진단으로 확인한다. 의도적으로 유지한 트랜잭션은 HTTP 처리량 실험과 섞지 않는다.

각 조건1회/고정순서/공유 Mac·Docker 호스트/같은 JVM 안의 별도 역할 앱이다. 통계적 유의성이나 여러 서버의 처리량을 보장하지 않는다.
과거40f00e7의10만 건과는 계측이 다르므로 이번 두 실행을 우선 비교한다. 100만 건으로 선형 외삽해 완료 시간을 약속하지 않는다.

## 고정 조건과 명령

Java21/Boot3.5.16/Gradle8.14.5/MySQL8.4.8(2CPU·2GiB/buffer pool512MiB),
Kafka broker4.0.2(1CPU·1GiB), Redis7.4.11(256MiB), test JVM512MiB.
역할별 Worker3/HTTP3/Redis3/Hikari4/queue0, 총1000/s 중 캠페인200/s·burst20,
Mock SUCCESS/1ms/max-in-flight8, 실시간 단독50개 후 응답 뒤200ms의 순차 probe다.

```bash
caffeinate -i ./gradlew --no-daemon --console=plain campaignDeliveryLoad -PpublishBatchSize=1
# 첫 원본과 관찰을 별도 이름으로 보존한 뒤, 새 JVM/새 격리 인프라에서 실행
caffeinate -i ./gradlew --no-daemon --console=plain campaignDeliveryLoad -PpublishBatchSize=10
```

발송 구간은 캠페인 앱 시작을 포함하고 사전 저장/실시간 단독50개/최종 대조는 제외한다.
`relay.mark.published` count는 행 수가 아닌 저장 호출 수다. 중첩/병렬 타이머를 wall time 비율로 합산하지 않는다.
`*ApiLatencyMs`와 DB 완료 기준의 `*LatencyMs`도 다른 경계다.

## 외부 잠금 관찰

`scripts/observe-delivery-locks.py`는 해당 실행 로그의 MySQL ID를 메모리에서만 선택하고,
image=mysql:8.4.8/Testcontainers label을 검증한 뒤60초마다 다음을 읽는다. 기본 상한은6000초/128회/명령당8초다.

- `Innodb_row_lock_current_waits`: 현재 행 잠금을 기다리는 수
- `Innodb_row_lock_waits`: 행 잠금 대기가 필요했던 연산의 누적 횟수
- `Innodb_row_lock_time`: 행 잠금 획득 대기의 누적 ms
- `Innodb_row_lock_time_max`: 관측 중 서버가 유지한 최대 획득 대기 ms
- `COUNT(*) FROM performance_schema.data_lock_waits`: 조회 순간의 차단 관계 수

fresh 실험 DB의 전역 값으로 Batch/업무/메타데이터 전체를 포함한다. 특정 Outbox/역할/SQL의 원인으로 단정하지 않는다.
마지막 관찰 뒤 컨테이너 정리까지의 구간과60초 사이의 순간 wait edge는 빠질 수 있다.
SHOW/COUNT는 별도 읽기 전용 연결이고 쿼리 시간에는 docker exec/클라이언트 시작 비용도 포함한다.
기존 부하의2초 자원 샘플은 유지한다. 관찰 자체의 자원 비용이 있으며 두 조건에 같은 도구/주기를 사용한다.

비밀번호는 컨테이너 내부 환경에서 mysql 클라이언트에만 전달하고 출력하지 않는다.
행/트랜잭션/연결/컨테이너 ID, SQL 본문, payload, process list, raw 오류 메시지는 JSON에 저장하지 않는다.
관찰 오류3회 연속/상한 도달은 관찰만 종료하며 본 실험의 성공으로 바꾸지 않는다. 기존 출력 파일은 덮어쓰지 않는다.

## 참고

- [MySQL8.4 상태 변수](https://dev.mysql.com/doc/refman/8.4/en/server-status-variables.html): 행 잠금 카운터/시간의 단위와 의미.
- [MySQL8.4 data_lock_waits](https://dev.mysql.com/doc/refman/8.4/en/performance-schema-data-lock-waits-table.html): 현재 차단 관계이며 이력이나 모든 잠금 목록이 아니다.
- [이전 설계 판단과 버전별 참고 문서](S7_RELAY_PUBLISH_BATCH.md).

## 수정 전37d4d3c의 동일 코드 비교 결과

두 조건 모두 PASS/각1개 테스트 통과/종료 코드0이었다. 전체 task 시간은 단건28분47초, 구 OR10건21분59초다.
캠페인200000+실시간10611=HTTP210611건을 전수 대조했고 중복·매핑 오류·Mock 오류는0이었다.

| 조건 | 사전 저장초 | 발송초 / 처리량 | 혼합 API p95 / 최대 ms | DB 완료 p95 / 최대 ms | 완료 저장 호출 / 누적초 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 단건 | 177.923 | 1512.978 / 66.095/s | 297.106 / 1231.243 | 853.201 / 1720.174 | 100000 / 520.225 |
| 구 OR10건 | 152.264 | 1132.741 / 88.281/s | 334.034 / 1731.628 | 913.679 / 1946.350 | 10462 / 121.232 |

구 OR은 발송 시간25.13% 감소/처리량33.57% 증가였지만 API p95 12.43%/최대40.64% 증가였다.
단독 API p95도16.143/24.658ms로 달랐으므로 이후 혼합 지연 차이를 코드 하나의 확정 효과로 보지 않는다.
실시간 probe는 응답 뒤200ms로 스스로 속도가 낮아지는 closed-loop이므로 고정 도착률의 실시간 SLO 검증도 아니다.
운영/Java 부하 소스는 두 실행 직전/직후37d4d3c와 같음을 diff로 확인했다. 아래 수정판의 성능 수치로 이 두 결과를 쓰지 않는다.

| 조건 | 자원샘플 | heap / MySQL cgroup 최대MiB | 스레드최대 | Hikari waiting 최대 C / RT |
| --- | ---: | ---: | ---: | ---: |
| 단건 | 686 | 101.407 / 1324.871 | 97 | 1 / 0 |
| 구 OR10건 | 515 | 104.356 / 1268.355 | 97 | 1 / 0 |

두 실행에서 cgroup max/oom/oom_kill은0이고 Hikari 상태 결측은0이었다. 순간 관측으로 모든 대기 부재를 보장하지 않는다.
외부 관찰은 단건28회/OR21회, 오류0, 마지막 전역 카운터는 각각 대기3회·18ms·최대11ms /2회·1ms·최대0ms였다.
총 관찰 명령 시간은14.400/10.714초이며 본 처리와 겹친다. 이를 wall time에서 빼거나 throughput 영향으로 그대로 환산하지 않는다.
마지막 관찰은 단건98391건/1488.894초, OR95165건/1076.884초였으므로 마지막 구간은 이 카운터에 포함되지 않는다.
순간 wait edge/현재 대기 수는 모든49개 샘플에서0이었다. 이 조건에서의 낮은 누적 대기가 다른 통계/트랜잭션 길이에서도 안전함을 뜻하지 않는다.

- [단건 HTTP 원본](experiments/s7-relay-100k-2026-11-02-batch-1.json), [잠금 관찰](experiments/s7-relay-100k-2026-11-02-batch-1-locks.json)
- [구 OR10건 HTTP 원본](experiments/s7-relay-100k-2026-11-02-batch-10.json), [잠금 관찰](experiments/s7-relay-100k-2026-11-02-batch-10-locks.json)

## 별도 SQL 진단에서 발견한 결함

`OutboxPublishProbeTest`는 새 격리 DB에 SQL UUID()로 캠페인100000+실시간100행을 넣는다.
캠페인10행/실시간1행을 선점한 뒤 실제 완료 SQL의 트랜잭션을 잠시 유지한다.
그동안 별도 연결에서 실시간 완료 저장/선점/새 접수를 시도한다. 모두 rollback하고 초기/최종 집계를 대조한다.
이는 SQL fixture이며 Kafka ACK나 실제 HTTP 지연 재현을 주장하지 않는다. UUID/데이터 배치도 HTTP 캠페인의 고정 ID와 다르다.

1. 첫 진단은19초에 FAIL이었다. 구 OR은 `key=PRIMARY`여도 `type=index` 전체 스캔, 추정rows1이었다.
   실제104103개 PRIMARY 인덱스 레코드 잠금을 유지했고 실시간 완료/새 접수는 약1초 대기했다.
   실시간 SKIP LOCKED 선점은 즉시 반환했지만 작업이 있어도 빈 결과였다.
   [첫 실패 원본](experiments/s7-relay-100k-2026-11-02-publish-locks-before.json)을 보존했다.
2. 수정 경로의 첫 재검증은17초에 FAIL이었다. 실시간 세 경로는 진행됐지만 물리 잠금11개를 상한10개에 비교한 가드가 실패했다.
   당시 LOCK_DATA의 의사 레코드 여부는 수집하지 않아 추가1개의 정확한 의미는 확정하지 않는다.
   [가드 실패 원본](experiments/s7-relay-100k-2026-11-02-publish-locks-initial-guard-failure.json)도 보존했다.
3. 최종 진단은 EXPLAIN을 별도 autocommit 연결로 옮겨 holder의 잠금 관찰과 분리했다.
   LOCK_DATA는 키 값을 출력하지 않고 supremum/unknown의 개수만 집계한다. NULL 분류가 남으면 수정 경로의 검증을 실패시킨다.
   같은 프로토콜에서 현재 단건/현재10건/구 OR 대조군을 비교해19초에 PASS했다.

| 최종 같은 fixture의 경로 | PRIMARY 계획 | 실제 기본키 레코드 / supremum | RT 완료·선점·접수의 holder 종료 전 완료 | RT 선점 반환 | 대기 관계 최대 |
| --- | --- | ---: | --- | ---: | ---: |
| 현재 단건 | range/rows1 | 1 /0 | 모두완료 | 1 | 0 |
| 현재10건 | 각 point UPDATE가 range/rows1 | 10 /0 | 모두완료 | 1 | 0 |
| 구 OR10건 대조군 | index/추정rows1 | 100100 /4003 | 완료저장·새접수는 대기 | 0 | 2 |

[최종 원본](experiments/s7-relay-100k-2026-11-02-publish-locks-after.json)의 구 OR 대조군도 실제 UPDATE만으로 같은 전체 스캔과 차단을 재현했다.
따라서 첫 진단에서 EXPLAIN이 같은 트랜잭션에 있었다는 한계만으로 결함을 무시하지 않는다.
구 OR은 이전100만 건 실패 뒤 추가한 별도 최적화이므로 원래 장애 원인으로 소급하지 않는다.
MySQL의 낮은 행 추정 상태에서 관측한 결과이며 통계가 유일 원인이라는 통제 실험이나 과거100만 건 timeout 원인 확정은 아니다.
`key=PRIMARY`라는 이름이나 추정rows만 보고 안전하다고 판단할 수 없었다.

## 수정과 재검증 범위

현재 `OutboxStore.published(List)`는 한 트랜잭션 안에서 기존 기본키 단건 `published(Claim)`을 최대10번 호출한다.
내부 호출은 새 트랜잭션을 만들지 않고 외부 묶음 트랜잭션에 참여한다. 각 UPDATE의 상태/token 조건, ACK 이전 완료 금지, 행별 fencing은 유지한다.
SQL UPDATE 수는 줄이지 않고 commit/연결·트랜잭션 관리 횟수를 줄이는 선택이다. JDBC batchUpdate를 사용하지 않는다.
한 SQL의 넓은 스캔 가능성 대신 최대10번의 SQL 왕복을 받아들인다. 단건 경로/Worker/예산/자원/기본값1은 바꾸지 않는다.

최종 SQL 회귀는 기본 테스트에 포함한다. 구 OR은 진단 대조군으로만 남기고, 수정 경로는 실제 기본키 레코드 수 상한/실시간 선점 존재/초기·최종 Outbox 상태별 집계 보존을 검증한다.
`outboxLockProbe --tests '*OutboxPublishProbeTest'`로 독립 실행할 수도 있다.
Python 관찰 도구의 대상 검증·raw 오류 미노출·기존 출력 보존·종료 후 미접근·예상 밖 필드 거절5개 테스트도 통과했다.

수정판 전체 빌드는11분56초/종료 코드0으로 성공했고34개 클래스275개 테스트가 실패/오류/스킵0으로 통과했다.
최종 XML과 콘솔을 대조했다. 신규 SQL 회귀1개는275개에 포함되며, 별도 선택 실행1개와 구분한다.
[기본 회귀 안의 SQL 원본](experiments/s7-relay-100k-2026-11-02-publish-locks-build.json)도 같은 범위의 잠금/실시간 진행을 확인했다.
수정 코드는557b2cf로 고정하고 깨끗한 작업 폴더에서 같은 명령의 `-PpublishBatchSize=10`으로 추가10만 건을 실행했다.
종료 후 Java/Gradle/scripts가557b2cf와 같음을 다시 확인했다. 세 번째 실행까지 포함한 HTTP 합계는315250건이다.
단건 기본키 경로는 그대로이므로 수정판 비교에는 첫 단건 기준선을 재사용하되, 같은 최종 소스로 단건을 재실행한 결과와 구분한다.
기본값을 전체 역할에10으로 변경하지 않고 실시간1/캠페인선택옵션을 유지한다. 100만 건 재실행은 별도 단계다.


## 수정판557b2cf의 추가10만 건 결과

**BUILD SUCCESSFUL22분53초/종료 코드0/별도 테스트1개 통과/실패·오류·스킵0**이었다.
원본 UTC2026-11-02T06:12:52~06:35:41, 사전 저장170.306초/발송1170.631초·85.424/s/최종 대조2.174초다.
캠페인100000+실시간4639=HTTP104639건의 전체 대상/DB/HTTP/Outbox/offset/quota를 대조했고 중복·매핑·Mock 오류0이었다.

| 조건 / 소스 | 발송초 | 처리량/s | 혼합 API p95 / 최대ms | DB 완료 p95 / 최대ms | 완료 저장 트랜잭션 호출 / 누적초 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 단건 /37d4d3c 기준선 재사용 | 1512.978 | 66.095 | 297.106 /1231.243 | 853.201 /1720.174 | 100000 /520.225 |
| 현재 기본키10건 /557b2cf | 1170.631 | 85.424 | 364.875 /1310.980 | 936.484 /2058.530 | 10454 /156.363 |

관측된 발송 시간은22.63% 감소/처리량29.24% 증가였다. 반면 API p95는22.81%/최대6.48% 증가했다.
수정판 단독 API p95는21.338ms였다. 실시간 지연 개선이나 동일 최종 커밋의 단건/10건 A/B 결과라고 표현하지 않는다.
구 OR10건보다 시간은3.34% 늘었다. SQL 접근을 좁히는 안전성 수정과 그 비용을 함께 기록하며 각1회의 공유 호스트 결과다.
현재는 SQL UPDATE를 줄이는 방식이 아니라 같은 트랜잭션으로 최대10건을 커밋한다.
선점753.411초/ACK52.936초 등 다른 구간 비용도 남아 있어 모든 병목이 해결된 것은 아니다.

자원샘플528개에서 heap최대104.355MiB/MySQL cgroup1273.152MiB/스레드97,
Hikari waiting최대 C1·RT0, cgroup max/oom/oom_kill0/상태 결측0이었다.
잠금 관찰22회/오류0/순간 current wait·edge0, 마지막 누적 대기4회·5ms·최대2ms를 기록했다.
마지막 관찰은96243건/1125.675초여서 이후 구간을 포함하지 않는다. 관찰 명령 누적10.761초는 본 처리와 겹친다.

- [수정판 HTTP 원본](experiments/s7-relay-100k-2026-11-02-point-batch-10.json), SHA-256 `8a8c9b9f39571a35245edeac8504c409de7d9be66bcc8c62f61cd704ecc61c45`
- [수정판 잠금 관찰](experiments/s7-relay-100k-2026-11-02-point-batch-10-locks.json), SHA-256 `122956855d8a2dca40d7e167bb3b9ed041cd34f23fa2d35f293e5996e1f4037e`

세100k 실험 모두 전수 대조를 통과했지만 하나의 연속30만 건 부하가 아니다.
각각 새 데이터/새 격리 인프라에서 캠페인300000+실시간15250=HTTP315250건을 검증한 합계다.
전체 기본값은1로 유지한다. 캠페인10건은 처리량과 tail latency의 트레이드오프를 받아들이는 선택 옵션이다.
실시간 설정을10으로 올리지 않았다. 100만 건 완료와 원래 접수 timeout 원인은 여전히 미완료다.

## 추가 참고

- [MySQL8.4 EXPLAIN](https://dev.mysql.com/doc/refman/8.4/en/explain-output.html): type=index는 인덱스 트리를 스캔하는 접근이고 key 이름만으로 point lookup을 뜻하지 않는다.
- [MySQL8.4 data_locks](https://dev.mysql.com/doc/refman/8.4/en/performance-schema-data-locks-table.html): LOCK_DATA의 supremum/NULL 의미. 인덱스 레코드 수를 고유 업무 행 수로 그대로 해석하지 않는다.


- [Spring Framework6.2 트랜잭션 프록시](https://docs.spring.io/spring-framework/reference/6.2/data-access/transaction/declarative/annotations.html): 내부 호출에 별도 프록시 경계가 생기지 않으므로 묶음 메서드의 외부 트랜잭션이 전체 UPDATE와 rollback을 담당한다.
