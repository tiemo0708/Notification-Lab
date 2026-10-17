# S6: Redis 장애·예산 유실의 fail-closed 정책

## 문제와 의사결정

Redis가 응답하지 않을 때 발송을 허용하면 앱마다 공급자 한도를 따로 쓰게 된다.
기존 구현은 연결 오류를 `UNAVAILABLE`로 처리했지만, quota 키가 없으면 **B개 초기 버스트를 자동 생성**했다.
따라서 exhausted 예산을 삭제하거나 Redis 데이터를 잃으면 이미 사용한 버스트가 다시 생길 수 있었다.

| 선택 | 장점 | 비용 |
| --- | --- | --- |
| fail-open | Redis 장애 중에도 발송을 계속함 | 공유 quota를 보장할 수 없음 |
| 앱별 임시 제한기 | 완전 무제한보다는 요청을 줄임 | 앱 수·복구 경합에 따라 전체 허용량이 달라짐 |
| 주기적 자동 재연결·소비 재개 | 짧은 연결 장애에서 자동 복구 | 복원된 예산의 출처/유실/rollback을 확인하지 못한 채 발송할 수 있음 |
| fail-closed + 명시적 복구 | 예산을 확인하지 못하면 새 발송을 시작하지 않음 | 발송 지연·적체, 운영자 복구 절차가 필요함 |

학습용 서비스에서는 **발송 가용성보다 공급자 예산 보호를 우선**한다.
Redis timeout/연결 실패/풀 포화/상태 오류뿐 아니라 키 부재와 예상 밖 TTL도 허가하지 않는다.
Worker는 DB 선점 전에 소비를 중지하고, 원인 확인 뒤 명시적으로 재시작한다. 자동 로컬 fallback은 없다.
정상 토큰 부족 `LIMITED`만 기존 pause/poll/resume를 유지한다.

## 구현 계약

- `provider-token-bucket.lua`: version=2의 영속 Hash만 사용한다. 키가 없거나 TTL이 있으면 오류를 반환하며 키를 만들거나 바꾸지 않는다.
- 기존 version=1도 이제 거절한다. 다른 rate·burst/손상된 필드의 거절 정책은 유지한다.
  만료를 자동 제거하거나 상태를 추정 복구하지 않는다.
- Java는 스크립트 오류를 안전한 `UNAVAILABLE`로 변환한다. Redis 오류 원문이나 payload를 로그/메트릭 label로 내보내지 않는다.
- quota 키 부재는 최초 설치인지 예산 유실인지 구분할 수 없다. **최초 설치도 명시적인 초기화가 필요하다.**
- `initialize-provider-quota.lua`는 존재하지 않는 키에만 version=2, credit=0, 현재 Redis 시각을 저장한다.
  기존 타입/상태가 무엇이든 키가 있으면 거절한다. rate/burst 1~10000과 키 이름도 검증한다.
- 초기화는 원자적이며 동시에 호출해도 한 호출만 생성한다. 첫 토큰은 초기화 후 최소 `ceil(1000/r)`ms의 정상 보충을 거쳐 얻는다.
  이 대기는 예약이 아니며 다른 Worker가 먼저 소비할 수 있다. B는 최대 축적량이지 초기 지급량이 아니다.
- 초기화 스크립트는 앱에서 자동 호출하지 않는다. 삭제/환불/기존 예산 리필 기능도 제공하지 않는다.

별도 DB migration은 없다. Redis 상태 형식과 운영 절차의 호환성이 바뀌므로 구버전 앱과 혼합 실행하지 않는다.
기본 `client-enabled=false`와 memory/sync 비교 모드에는 영향을 주지 않는다.

## 최초 실행

README의 DB/Kafka/.env 준비 후 Redis를 시작한다. 다음은 기본 키/r=10/B=10 예시다.
모든 앱의 Redis 주소·키·rate·burst와 반드시 일치해야 한다.

```bash
docker compose --profile redis up -d --wait redis
# 최초 설치: 참여 앱을 실행하기 전에 한 번만 수행
# -e는 Redis 오류를 0이 아닌 CLI 종료 코드로 반환한다.
docker compose exec -T redis redis-cli -e --eval /dev/stdin \
  notification-lab:provider:mock:quota , 10 10 \
  < src/main/resources/redis/initialize-provider-quota.lua
# 예상 응답: QUOTA_INITIALIZED_EMPTY
./gradlew bootRun --args='--app.provider.mode=HTTP --app.rate-limit.client-enabled=true'
```

기존 키에는 `QUOTA_ALREADY_EXISTS` 오류가 난다. 이를 보고 자동으로 DEL하거나 키 이름을 바꾸지 않는다.
초기화 응답이 유실돼도 키가 이미 생성됐을 수 있다. 다시 삭제하지 말고 상태를 조회해 확인한다.
초기화 스크립트는 연결/정합성 점검의 대체 수단이 아니다. 실제 공급자 대신 HTTP Mock을 먼저 실행한다.
`redis-cli --eval`의 파일/키/인자 구분은 [Redis CLI 공식 문서](https://redis.io/docs/latest/develop/tools/cli/)를 따른다.

## 장애 확인과 복구 절차

### 1. 발송 주체를 먼저 멈추고 증거 확인

동일 공급자 quota를 쓰는 **모든 앱/Worker**를 중지하고 진행 중 HTTP와 이미 허가받은 작업이 끝났는지 확인한다.
종료 유예만으로 원격 공급자가 중단됐다고 단정하지 않는다. 응답 유실의 불확실성은 [S4 정책](S4_UNCERTAIN_OUTCOMES.md)을 따른다.
앱 프로세스를 종료하는 절차를 기본으로 사용한다. 이번에는 운영자 재시작용 HTTP API를 새로 만들지 않았다.

`notification_worker_running=0`, `notification_worker_quota_rejected_total{reason=unavailable}` 증가,
Redis 상태와 DB QUEUED/WAITING_RETRY, 시도 이력, Kafka offset/lag를 함께 확인한다.
`health UP`이나 Redis `PING`만으로 예산 보존이 증명되지는 않는다.

```bash
# 읽기 전용 확인: 실제 운용 키로 맞춘다.
docker compose exec -T redis redis-cli TYPE notification-lab:provider:mock:quota
docker compose exec -T redis redis-cli HGETALL notification-lab:provider:mock:quota
docker compose exec -T redis redis-cli PTTL notification-lab:provider:mock:quota
```

정상 Hash의 version=2, rate/burst 일치, credit 범위 0..B×1000, 유효한 last_ms, PTTL=-1을 확인한다.
필드가 정상이라는 사실과 최신 예산이 보존됐다는 사실은 다르다. 장애 유형·AOF/복제 이력·설정 변경을 함께 조사한다.

### 2. 신뢰할 수 있는 기존 예산은 유지

단순 연결 장애이고 동일 Redis의 최신 상태가 유지됐음을 확인했다면 **키를 수정하지 않고** 앱을 재시작한다.
중지 중에도 실제 시간이 흘렀으므로 다음 Lua 호출에서 정상적으로 최대 B까지 보충될 수 있다.
이는 새 초기 버스트 지급이 아니라 시간 기반 보충이다. 완료된 DB 작업은 재소비 때 다시 발송하지 않는다.
Redis가 살아나도 이미 중지된 Worker가 저절로 시작하지는 않는다.

### 3. 키 유실·형식 변경·신뢰할 수 없는 복원은 통제된 재초기화

1. 모든 발송 주체를 중지하고 이전 허가와 진행 중 HTTP가 더 이상 사용되지 않을 조건을 먼저 확보한다.
2. 기존 예산과 장애 증거를 보관한다. version=1 업그레이드, 정책 변경, snapshot rollback도 이 경로다.
3. 마지막 발송/허가 사용이 끝난 뒤 적어도 `ceil(1000 × B/r)`ms 동안 조용한 구간을 둔다.
   r/B를 바꾸면 기존 값과 새 값의 B/r 중 더 긴 시간을 사용한다. 실제 공급자의 별도 고정 윈도우가 있다면 그 회복 조건도 적용한다.
4. 키가 없으면 그대로 초기화한다. 잘못되거나 신뢰할 수 없는 기존 키가 있으면 **운영자가 보관·정지·대기 조건을 확인한 뒤 해당 키 하나만 삭제**한다.
   일괄 FLUSHDB/FLUSHALL 또는 새 키로 우회하지 않는다. 이번 도구는 이 삭제를 자동화하지 않는다.
5. 위 초기화 명령으로 credit=0을 생성하고 새 버전 앱을 실행한다. DB 작업/Outbox/offset은 초기화하지 않는다.
6. 초기에는 정상 보충을 기다릴 수 있다. 시도 이력·HTTP 수·offset과 lag가 일관되게 진행하는지 확인한다.

대기 시간은 이 프로젝트의 token-bucket 허가 계약을 위한 보수적 절차다.
기존 프로세스가 나중에 다시 허가를 쓰거나 원격 요청이 계속 진행 중이면 이것만으로 공급자 도착률을 보장할 수 없다.
이미 초과 발송한 과거 이력을 없애는 절차도 아니다.

## 영속화·failover의 경계

Compose는 단일 Redis, AOF 기본 everysec, `noeviction`이다. 모든 허가 응답의 영구 보존을 보장하지 않는다.
Redis의 [영속화 문서](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/)는 AOF 설정별 손실 범위를 설명한다.
[복제 문서](https://redis.io/docs/latest/operate/oss_and_stack/management/replication/)의 비동기 복제는 failover 중 이미 응답한 쓰기의 유실 가능성을 명시한다.
따라서 AOF 또는 WAIT만으로 전역 quota가 모든 장애에서 정확히 유지된다고 주장하지 않는다.

**형식이 정상인 오래된 Hash의 복원은 현재 제한기가 자동 감지하지 못한다.**
이미 소비한 credit을 되돌린 snapshot은 다시 허가를 줄 수 있다. 외부의 영속 세대 기준이나 합의 시스템을 추가하지 않았기 때문이다.
현재 구성은 Sentinel/Cluster/자동 primary 전환을 지원하지 않는다. failover가 필요한 환경으로 확대하기 전
모든 발송 주체를 중지하고 위 복구 절차를 적용해야 한다. 무중단 자동 failover는 후속 설계 대상이다.

키 삭제·TTL·형식 오류는 자동 차단하지만, 정상 형식의 rollback까지 자동 차단한다고 표현하지 않는다.
공급자별 Redis 키 분리나 여러 Redis로 우회하는 것도 공유 한도 보장이 아니다.

## 실제 검증 범위

`RedisRateLimiterTest`와 `WorkerQuotaIntegrationTest`에서 다음 조건을 확인한다.
최종 실행 결과와 실패 이력은 [PROGRESS](PROGRESS.md)에 남긴다.

| 시나리오 | 확인 내용 |
| --- | --- |
| 키 없음/삭제/새 클라이언트 | UNAVAILABLE, 자동 키 생성 없음 |
| 명시적 초기화 | credit=0/TTL 없음, 실제 Redis TIME 1초 후 첫 토큰(r=1) |
| 기존/손상된 키·잘못된 인자·동시 초기화 | 기존 상태 무변경, 초기 생성은 한 번만 성공 |
| 예상 밖 TTL | UNAVAILABLE, credit과 TTL 정책을 자동 수정하지 않음 |
| 실제 redis-cli 표준입력 실행 | 최초 성공, 기존 키 재호출은 0이 아닌 종료 코드 |
| 정상 형식의 오래된 snapshot 주입 | 자동 감지 불가를 재현, 통제된 빈 예산 초기화 확인 |
| 실제 Redis SIGKILL, 6초 이상 중지 관찰 | API/Relay 접수 유지, 새 발송 시도/lease/HTTP 없음, offset 보존, 자동 재확인/로컬 fallback 없음 |
| 같은 컨테이너 재시작과 AOF 복구 | 저장한 예산 필드 보존, Redis 복구만으로 소비 재개하지 않음, 명시적 Worker 재시작 뒤 수락 완료 |
| 키 삭제 후 앱 context 재생성 | 여전히 미선점 중지. 명시적 빈 초기화/재시작 후 정상 처리 |

통합 테스트 Redis는 `appendfsync always`이며 동일 컨테이너의 파일 시스템을 유지한 SIGKILL/start를 사용한다.
Compose everysec에서 모든 ACK의 보존, 호스트/디스크 손실, 실제 replica 승격을 검증한 것이 아니다.
오래된 snapshot 사례는 제어된 상태 주입이며 실제 failover 실험과 구분한다.
기존 quota/회로 테스트의 초기 예산은 명시적 테스트 fixture로 준비해 이전 시나리오 조건을 유지했다.
사용자 로컬 Redis/DB/.env를 수정하지 않는다.

## 남은 한계와 다음 작업

발송 중지 기간 동안 API 접수를 유지하면 DB/Outbox/Kafka 적체가 증가한다. 무한 저장 용량이나 알림 만료 정책은 없다.
6초 관찰은 중지 정책과 무재시도 동작의 검증이며 수시간 장애의 용량/운영 복구 시간을 측정한 결과가 아니다.
이미 발급한 허가·진행 중 HTTP는 Redis 장애만으로 취소되지 않는다. timeout 뒤 토큰 환불도 하지 않는다.
외부 중복 가능성, 공급자 수락과 단말 도착의 차이는 그대로다.
다음 PLAN 항목은 실시간/캠페인 자원 분리와 공급자 한도 내 우선순위 배분이다.
