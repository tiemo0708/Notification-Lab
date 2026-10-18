# S7: 캠페인 생성과 영속 시작 요청

2026-10-18 구현 범위. 캠페인 정의를 저장하고 시작 요청을 한 번 수락하는 단계다.
실제 대상자 확장, Spring Batch Job 실행, Kafka 발행과 공급자 호출은 아직 연결하지 않았다.

## 문제와 선택

운영자가 시작 버튼을 두 번 누르거나 응답을 받기 전에 연결이 끊어질 수 있다.
앱 인스턴스가 여러 개라면 Java의 `synchronized`나 메모리 플래그만으로 중복 시작을 막을 수 없고,
재시작하면 접수 여부도 사라진다.

| 대안 | 장점 | 비용과 한계 |
| --- | --- | --- |
| 메모리 플래그 | 구현이 간단함 | 다른 앱과 공유되지 않고 재시작 시 소실 |
| 먼저 조회하고 INSERT/UPDATE | 정상 흐름이 읽기 쉬움 | 조회와 변경 사이에 경쟁 가능 |
| 분산락 + DB 기록 | 여러 프로세스 조율 가능 | 별도 락의 lease/장애와 DB 트랜잭션을 함께 관리해야 함 |
| DB 유니크 제약 + 조건부 UPDATE | 같은 DB를 쓰는 앱들이 원자적으로 경쟁 | DB 가용성에 의존하며 같은 캠페인 요청은 행 잠금을 기다림 |

현재 필요한 것은 DB 안의 정의와 시작 의도 한 건을 확정하는 일이다.
생성 키에는 유니크 제약을, 시작에는 `WHERE status = 'DRAFT'` 조건부 갱신을 선택했다.
별도 분산락과 실행 대기열은 추가하지 않았다.

## API 계약

`!memory & !sync`의 **CAMPAIGN 역할 앱**에만 등록된다. REALTIME 앱에서는 404다.
CAMPAIGN 역할은 S6의 split/shared Redis/pause 설정을 필요로 한다.
인증이 없는 로컬 학습 API이므로 기존 loopback 실행 범위를 유지한다.

| 요청 | 최초 성공 | 동일 요청 재시도 | 실패 |
| --- | --- | --- | --- |
| `POST /campaigns` | 201, DRAFT 정의 커밋 | 같은 키·같은 정의는 200과 현재 저장 상태 | 다른 정의 409, 잘못된 입력/키 400 |
| `GET /campaigns/{id}` | 200, 현재 상태 | 동일 | 존재하지 않는 UUID 404, 잘못된 UUID 400 |
| `POST /campaigns/{id}/start` | 202, START_REQUESTED 커밋 | 200, 같은 상태와 최초 시작 요청 시각 | 없는 캠페인 404, 저장 실패 503 |

생성/시작 응답의 `Location`은 `/campaigns/{id}`다. DB 장애는 기존 공통 오류 처리로 503이며
SQL/본문/수신자 정보는 오류 응답에 넣지 않는다. HTTP 응답은 커밋 후 반환한다.
시작은 본문과 별도의 멱등 키가 필요 없으며 캠페인 ID가 재요청의 기준이다.

생성 본문:

- `name`, `title`: 공백만으로 구성할 수 없고 최대 100자.
- `body`: 공백만으로 구성할 수 없고 최대 500자.
- `targetCount`: 1~1,000,000의 JSON 정수. 소수·문자열·null은 거절한다.
- 문자열 길이는 Java Bean Validation의 UTF-16 길이 기준이다. 이모지는 두 단위를 차지할 수 있다.
- `Idempotency-Key`: 필수 헤더 1개, `[A-Za-z0-9._:-]{1,128}`. 대소문자를 구분한다.
  키는 campaign 테이블 내에서 유일하며 `/notifications`의 키 공간과 별개다. 만료/삭제 기능은 없다.
- 같은 키의 `name/title/body/targetCount`를 정확히 비교한다. 공백 정규화는 하지 않는다.
  잘못된 입력은 키를 예약하지 않는다. 시작 후 생성 재요청도 기존 캠페인의 현재 상태를 반환한다.

응답은 `campaignId/name/title/body/audience/targetCount/status/createdAt/startRequestedAt`이다.
`audience`는 서버가 `SYNTHETIC_V1`로 고정한다. 이는 후속 가상 대상자 규칙의 버전 식별자이며,
지금 대상자 데이터가 존재하거나 스냅샷을 만들었다는 뜻은 아니다.
시각은 DB의 UTC 마이크로초 값을 사용하며 DRAFT의 `startRequestedAt`은 null이다.

## 실행 예

[S6 역할별 실행 및 v2/v3 예산 전환](S6_TRAFFIC_ISOLATION.md)을 먼저 따른다.
8082의 CAMPAIGN 앱에서 실행한다. 기존 앱을 새 JAR로 시작하면 Flyway V8이 campaign 테이블을 추가한다.
V8은 기존 notification/Outbox/작업 이력을 변경하지 않는다.

```bash
curl -i http://127.0.0.1:8082/campaigns \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: campaign-demo-001' \
  -d '{"name":"가상 10만 명 실험","title":"학습 알림","body":"가상 데이터만 사용","targetCount":100000}'

# 아래 UUID를 생성 응답의 campaignId로 바꾼다.
curl -i -X POST http://127.0.0.1:8082/campaigns/응답_UUID/start
curl -i -X POST http://127.0.0.1:8082/campaigns/응답_UUID/start
curl http://127.0.0.1:8082/campaigns/응답_UUID
```

첫 시작은 202, 두 번째는 200이다. **현재 START_REQUESTED에서 자동으로 다음 상태로 진행하지 않는다.**
이 API는 알림/Outbox/발송 작업 행, Kafka 이벤트, Redis 발송 허가 또는 공급자 호출을 만들지 않는다.
`targetCount=1000000` 생성은 작은 정의 한 행을 저장할 뿐, 100만 건 발송 성능 검증이 아니다.

## 트랜잭션과 실패 경계

생성 INSERT는 독립된 저장 트랜잭션에서 수행한다. 서비스는 호출자의 트랜잭션을 중단하고,
유니크 충돌이 롤백된 다음 커밋된 정의를 조회한다. 오래된 REPEATABLE READ 스냅샷도 재사용하지 않는다.

시작은 `REQUIRES_NEW` 트랜잭션에서 DRAFT 행을 조건부 UPDATE하고 `SELECT ... FOR UPDATE`로
최신 상태를 읽는다. 두 앱이 동시에 시작하면 한 요청만 상태와 시각을 바꾸고 나머지는 그 결과를 읽는다.
호출자가 가진 이전 스냅샷이나 호출자 롤백에 의해 수락된 시작 의도가 되돌아가지 않는다.
이 API는 시작 의도를 독립적으로 수락하는 경계이므로 상위 업무 트랜잭션과 묶어 사용하는 계약은 아니다.

| 중단/실패 지점 | 저장 상태와 재시도 |
| --- | --- |
| 생성/시작 커밋 전 실패 | 변경 롤백, 같은 요청 재시도 가능 |
| 커밋 후 응답 유실 | 클라이언트는 결과를 모를 수 있음. 같은 키/ID로 재시도해 저장 결과 확인 |
| 앱 종료와 재시작 | DB에 커밋된 정의/시작 의도/최초 시각 유지 |
| 여러 앱의 동시 시작 | DB 상태 변경 1회. 이는 외부 발송 exactly-once 보장이 아님 |

DB CHECK는 상태와 시각의 조합, 가상 대상 버전, 대상 수 범위를 제한한다.
외부 발행과 Batch checkpoint는 이 트랜잭션에 포함되지 않는다. 후속 단계에서 DB에 남은 시작 의도를
발견하고 작업을 복구하는 실행 경계를 설계해야 한다.

## 검증과 다음 단계

`CampaignApiTest`는 실제 MySQL 8.4.8/Kafka 4.0.2와 독립 앱 context 3개를 사용한다.
CAMPAIGN 두 앱의 12개 동시 시작에서 202 한 번/200 열한 번과 저장 행 1개를 대조한다.
동시 동일/충돌 생성, 재시작, 기존 트랜잭션 스냅샷, 트리거를 이용한 갱신 실패·롤백,
DB CHECK, 입력 오류, REALTIME 엔드포인트 부재를 검증한다.
생성·시작 후 발송 관련 DB 행, Kafka end offset과 Redis 허가 카운터는 0인지 확인한다.

```bash
./gradlew --no-daemon --console=plain test --tests '*CampaignApiTest' --tests '*MySqlNotificationTest'
./gradlew --no-daemon --console=plain build --rerun-tasks
```

실행 결과와 최초 실패를 포함한 이력은 [PROGRESS](PROGRESS.md)에 기록한다.
별도 OS 프로세스 강제 종료, 운영 처리량, 대량 데이터, 실제 FCM/APNs는 이번 검증 범위가 아니다.
다음 작업은 **고정된 가상 대상자 집합과 keyset pagination**이다. 그 뒤 Batch checkpoint,
결정적 대상자 작업 ID, 실패 후 재처리를 연결한다.
