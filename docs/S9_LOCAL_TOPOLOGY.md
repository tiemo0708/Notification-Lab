# S9: 로컬 LB와 API 2개·Worker 2개

## 목적과 변경 이유

단일 앱에서 검증한 DB 멱등성, Relay fencing, Kafka 완료 후 커밋, Redis 예산이
서로 다른 JVM에서도 함께 동작하는지 확인한다. 이 실험은 처리량 개선율을 측정하지 않는다.
기존 단일 프로세스 검증에 실제 HAProxy와 4개 앱 프로세스를 추가하며, 운영 기본값은 바꾸지 않는다.

| 구성요소 | 수 | 역할 |
| --- | ---: | --- |
| HAProxy 3.4.7 | 1 컨테이너 | API 두 개에 round-robin, HTTP health check, 제외 확인 |
| API | 2 JVM | 요청·발송 작업·Outbox 저장. Worker/Relay 비활성 |
| Worker | 2 JVM | 같은 REALTIME group 소비, HTTP 공급자 호출. 각 JVM의 Relay 활성 |
| MySQL 8.4.8 | 1 컨테이너 | 네 JVM의 공통 영속 저장소 |
| Kafka 4.0.2 | 1 컨테이너 | requested/retry/DLQ 각각 3파티션·복제본 1 |
| Redis 7.4.11 | 1 컨테이너 | Worker 두 개의 공통 v2 공급자 quota |
| 기존 HTTP Mock | 테스트 부모 JVM | 실제 HTTP 수락과 요청별 ID·수신자·본문 매핑 감사 |

REALTIME 한 lane의 배포 정합성에 범위를 고정했다. 기존 REALTIME/CAMPAIGN 역할 분리 실험과 다르다.
API 두 개가 캠페인/실시간 역할을 하나씩 맡는 구성이 아니다.
Worker는 LB backend에 포함하지 않는다. 역할 플래그는 서버의 endpoint 접근 제어를 대신하지 않는다.

## 재현

Java 21과 Docker가 필요하다. 사용자 `.env`/기존 DB를 쓰지 않고 Testcontainers로 새 인프라를 만든다.

```bash
./gradlew --no-daemon --console=plain test --tests '*LocalTopologyIntegrationTest'
```

신규 기본 회귀 테스트이며 `./gradlew test`와 CI의 `build`에도 포함된다. Docker가 없으면 스킵하지 않는다.
실제 앱 Bean을 사용하는 별도 JVM 네 개를 실행하고, 테스트 전용 진입점은 준비 상태 메타데이터만 쓴다.
HTTP 공급자, Worker, Relay 또는 저장소를 가짜 Bean으로 교체하지 않는다.
각 실행은 `build/reports/local-topology/<시각>-<임의값>/`에 자식 로그·준비 파일·HAProxy 설정·`result.json`을 남긴다.
로그와 합성 요청 식별자를 가진 로컬 파일은 커밋하지 않는다. 보존 결과는 집계와 역할별 offset만 담는다.

각 앱의 최대 heap은 256MiB, DB pool은 3, Tomcat worker는 8, Consumer/활성 발송 상한은 1/1, 앱 대기열은 0이다.
Testcontainers의 MySQL/Kafka/Redis/LB는 각각 메모리 512/1024/64/128MiB, CPU 1개 상한이다.
HTTP Mock은 1ms 지연·in-flight 4, Worker는 Redis rate 10/s·burst 2를 공유한다.
이 수치는 테스트 조건이며 운영 권장 용량이나 측정 처리량이 아니다.
컨슈머 session/heartbeat는 테스트에서만 6초/1초로 줄여 종료 후 재배정 대기를 제한했다.
메서드 제한은 360초, HTTP/관리 API/프로세스 정리 대기에도 별도 상한이 있다.

## 확인하는 경계

1. **역할과 준비 상태**: 네 PID가 서로 다르고 API의 listener/Relay가 꺼져 있으며 Worker의 둘은 켜져 있는지 확인한다.
   Admin API로 group STABLE, 실제 멤버 2개, requested/retry 총 6파티션의 누락·중복 없는 할당을 대조한다.
2. **LB 분배와 API 간 재접수**: 두 backend가 UP인 뒤 POST를 보낸다. HAProxy가 덮어쓰는 upstream 헤더로 분배를 확인한다.
   같은 key/본문은 202 이후 다른 backend의 200과 같은 ID를 반환하고 DB 작업 수를 늘리지 않는다.
   `apiRoutes`는 202/200 응답의 헤더 수이며, 409 충돌은 별도로 검증한다.
3. **공통 예산**: 요청 파티션마다 최소 3개 작업이 있도록 최대 60개 UUID를 샘플링한다.
   실제 Redis Lua 초기화 후 테스트에서만 두 토큰을 넣고 clock refill을 30초 동안 고정한다.
   두 Worker가 모두 quota pause에 들어간 상태에서 전체 HTTP 수락·시도 이력·커밋은 정확히 2개,
   나머지 작업은 QUEUED/attempt 0이다. 별도 수동 할당 Consumer로 실제 요청 레코드를 읽고
   개별 파티션의 미완료 offset을 커밋이 넘지 않았는지도 확인한다. 이 상태를 1.2초 더 유지해 JVM별 두 토큰이 생기지 않음을 확인한다.
   테스트용 clock을 해제해도 기존 pause deadline은 존중한다. 실제 rate 처리량 측정이나 운영 초기화 절차가 아니다.
4. **API 제외**: 전체 작업의 완료·offset 반영 이후 API1을 강제 종료한다. stats CSV의 DOWN을 확인한 뒤
   같은 요청을 API2로 재접수하고 같은 ID/200, 다른 본문은 409를 확인한다. 새 작업도 API2에서 접수·발송된다.
5. **Worker 재배정**: 다시 모두 완료된 뒤 Worker2와 그 Relay를 강제 종료한다.
   group STABLE/멤버 1개/전체 6파티션 인수 이후 각 요청 파티션에 새 작업을 보낸다.
   살아 있는 Worker의 실제 공급자 시도 증가분이 신규 작업 수와 일치하고 공유 예산을 다시 초기화하지 않는다.
6. **전수 대조**: 각 완료 단계에서 고유 ID, HTTP 요청의 수신자·제목·본문, 공급자 message ID,
   notification 완료, 작업 attempt 1, 완료 이력 1, Outbox PUBLISHED와 requested의 개별 파티션 end=committed를 확인한다.
   retry/DLQ end는 모두 0이다. 없는 offset은 숫자 0으로 치환하지 않고 존재 여부와 -1로 구분한다.

각 coverage 단계는 최대 60개, 전체 고유 작업은 최대 181개로 제한한다.
매번 UUID가 달라 실제 샘플 수는 달라질 수 있으며 결과에 실제 건수를 기록한다.
이 실험의 정상 경로에서 HTTP 중복 0이라는 관측을 외부 exactly-once 보장으로 일반화하지 않는다.

## 선택적 수동 LB 설정

`compose.topology.yml`은 LB만 실행하는 Docker Desktop용 선택 구성이다.
API를 띄우거나 Worker를 생성하지 않는다. 네 JVM·공유 DB/Kafka/Redis·quota 준비는 별도다.
자동 테스트는 같은 `deploy/local/haproxy.cfg`의 API host/port만 동적 Testcontainers 주소로 바꿔 사용한다.

| 주소 | 용도 |
| --- | --- |
| `127.0.0.1:18000` | LB HTTP 입구 |
| native `127.0.0.1:18010`, `:18011` | API1/API2. 둘 다 `--app.async.worker-enabled=false --app.async.relay-enabled=false` |
| `127.0.0.1:18404/stats` | 읽기 전용 HAProxy 상태 |
| Worker용 별도 로컬 포트 | `--app.async.worker-enabled=true --app.async.relay-enabled=true`. LB pool에는 넣지 않음 |

각 앱은 같은 DB·Kafka group/topic·Redis key/policy와 HTTP Mock origin을 명시해야 한다.
Worker에는 `--app.provider.mode=HTTP --app.rate-limit.client-enabled=true`도 필요하다.
테스트와 같은 예산은 `--app.rate-limit.rate-per-second=10 --app.rate-limit.burst=2`이며,
[공식 초기화·전환 절차](S6_REDIS_RATE_LIMIT.md)에 따라 새 실험용 예산을 준비한다.
실시간/캠페인을 분리하면 각 lane의 group/topic·분할 quota 계약을 기존 문서대로 다시 설정해야 한다.
수동 실행 전에는 새 데이터/실험용 예산인지 확인하고 기존 quota를 덮어쓰지 않는다.

```bash
docker compose -f compose.topology.yml config --quiet
docker compose -f compose.topology.yml up -d
# HTTP Mock 수락 결과를 단말 도착으로 해석하지 않는다.
curl -fsS 'http://127.0.0.1:18404/stats;csv'
# 이 LB 프로젝트만 종료. 사용자 데이터 볼륨은 건드리지 않는다.
docker compose -f compose.topology.yml down
```

`retries 0`으로 LB 자체 재시도는 끄고 `http-server-close`를 사용한다.
GET health 200만 정상으로 받아들이며, 200ms 간격·실패 2회로 제외하고 성공 1회로 복귀시킨다.
클라이언트/서버 대기 5초, 연결/check 500ms, 큐 대기 1초, 전체 maxconn 64,
backend별 동시 연결/큐 각각 16 상한이다.
health 성공은 API의 HTTP/DB 상태 확인이며 시스템 전체나 실제 발송 가능성의 보증이 아니다.
인증·TLS가 없는 루프백 실험이므로 공개 배포 설정으로 재사용하지 않는다.

설정 의미는 [HAProxy 3.4 공식 구성 문서](https://docs.haproxy.org/3.4/configuration.html)의
`retries`, `balance`, `http-check`, `maxconn`, `maxqueue`, `stats`를 확인했다.
실제 이미지의 `haproxy -c`와 요청·health·stats 동작도 테스트한다.

## 검증 중 발견한 커밋 중단

초기 실행에서 마지막 Worker 인수 후 `commitSync(Map, Duration)`가 `WakeupException`을 던졌고,
기존 fail-closed handler가 listener를 멈췄다. 완료된 DB 작업은 남았지만 이후 작업이 정체돼 테스트가 실패했다.
이를 테스트 대기 시간 증가나 ACK 방식 변경으로 우회하지 않는다.

Spring Kafka 3.3.16 소스의 timed 명시적 offset 커밋 경로와
[공식 Wakeup 커밋 이슈 #4622](https://github.com/spring-projects/spring-kafka/issues/4622),
[pause 관련 이슈 #2738](https://github.com/spring-projects/spring-kafka/issues/2738)를 대조했다.
Wakeup은 다음 blocking 호출에 영향을 줄 수 있다. 관련 upstream 이슈의 적용 버전·호출 경로가
현재 현상과 완전히 같다고 단정하지 않으며, 전체 라이브러리를 임의 업그레이드하지 않는다.

프로덕션 Consumer post-processor는 Spring이 넘긴 **명시적 완료 offset과 timeout**의 동기 커밋에 한해서
Wakeup을 한 번만 재시도한다. offset snapshot을 유지하며 첫 호출이 소비한 시간을 빼서 같은 deadline 안에서 수행한다.
두 번째 실패, 일반 오류, deadline 소진은 그대로 기존 중단 handler에 전달한다.
poll·리스너·공급자 호출·implicit-position 커밋·async commit은 재시도하지 않는다.
`notification_kafka_commit_wakeup_retries_total`은 이 재시도만 세며 식별자 label이 없다.
라이브러리 업그레이드 시에는 해당 경로와 회귀 테스트를 확인해 어댑터 유지 여부를 재검토한다.

손으로 작성한 Consumer로 정상/첫 Wakeup/반복 Wakeup/시간 소진/일반 오류/poll/lifecycle/implicit commit 경계를
7개 테스트에서 확인하고, 실제 네 JVM 시나리오를 다시 수행한다.
재시도 성공 여부를 확인하지 않고 성공으로 반환하거나 새 offset을 계산하는 코드는 없다.

## 결과와 한계

환경은 macOS 15.7.7/arm64, Corretto Java 21.0.6, Gradle Wrapper 8.14.5,
Docker Desktop 가용 CPU 10개/메모리 8319438848 bytes였다. 별도 로컬 MySQL·Prometheus·Grafana는 유지했다.

개발 중 실행은 최초 import 충돌(컴파일 실패/실행 테스트 0), 감사 Bean 이름 충돌,
Mock이 notification ID와 같은 provider ID를 반환한다고 잘못 가정한 assertion 실패,
실제 Worker commit Wakeup 중단을 각각 구분한다. Mock은 임의 provider ID를 반환하므로
응답을 캡처하고 그 값을 notification ID별 DB 저장값과 대조하도록 테스트를 고쳤다.

보완 뒤 집중 실행은 `2m 4s`/종료 0, **8개 통과/실패·오류·스킵 0**이었다.
고유 작업 24개/감사 중복·잘못된 매핑 0, API 제외와 Worker 인수 뒤 개별 offset을 확인했다.
이후 topology 결과의 retry counter/이미지 ID 필드와 Consumer 어댑터의 Object 동등성 계약을 보완했다.
최종 전체 빌드에서 다시 실행한다. 어댑터의 자기 동등성·서로 다른 객체와의 구분·안정된 hash는 기존 lifecycle 경계 테스트에서 확인한다.

첫 전체 빌드는 기존 `CampaignBatchTest.explicitCommandRunsInSeparateJvmAndCompletedReplayDoesNotExpandAgain`의
두 번째 CLI 종료 코드 검증이 실패해 중단했다(전체 통과로 집계하지 않음).
기존 helper가 성공/실패 모두 자식 로그를 삭제해 최초 원인은 미확정이다.
예상과 다른 결과는 `build/reports/campaign-command-failures/`에 로그를 보존하도록 바꿨으며
기존 성공/실패 기대 코드와 assertion은 유지했다. 단독 재검증은 **39초/1개 통과**해 실패가 재현되지 않았다.
이 결과를 Batch 원인 해결 증거로 쓰지 않는다.

두 번째 전체 실행 안의 토폴로지도 고유 17개/중복·매핑 오류 0으로 통과했다.
그러나 코드 검토에서 위 Object 계약 위반을 발견해 전체 실행을 중단했다. 이 실행을 전체 통과로 세지 않는다.
해당 토폴로지의 실제 Wakeup 재시도는 0회였고, 재시도 자체는 손으로 만든 Consumer의 오류 주입으로 확인한다.

전체 회귀 안의 네 JVM 실행은 고유 작업 **21개**, HTTP 수락/완료 이력 각 **21개**, 감사 중복·잘못된 매핑 **0개**였다.
실제 timed commit Wakeup 재시도가 **1회** 사용됐고 이후에도 동일한 전수 대조와 신규 작업 처리가 통과했다.
공급자 호출 재시도 횟수와 섞지 않는다. 정리 오류는 0개였다.

| 전체 회귀 실행 단계 | 등록한 고유 작업 | HTTP 수락 누적 | Worker 멤버 |
| --- | ---: | ---: | ---: |
| refill 고정, 두 Worker pause | 13 | 2 | 2 |
| 예산 해제 후 완료 | 13 | 13 | 2 |
| API1 제외·재접수·충돌·추가 작업 완료 | 17 | 17 | 2 |
| Worker2 종료·6파티션 인수·추가 작업 완료 | 21 | 21 | 1 |

UUID partition coverage 표본 수가 달라 집중 24개/중간 17개/최종 21개를 같은 처리량으로 비교하지 않는다.
[단계별 집계 원본](experiments/s9-topology-2026-11-06-observation.json)에 실패 실행도 분리해 남긴다.
전체 `build`는 **25분 53초/종료 0**, **51개 클래스/397개 통과·실패·오류·스킵 0개**였다.
소스 172개/구성 15개 SHA가 실행 중 유지됐고 기본 test를 새로 실행했다.
알림 JAR의 commit retry class는 현재 compiled class와 같고 테스트 진입점은 포함되지 않는다.
HTTP Mock JAR는 기존 산출물을 재사용했다. [검증·소스·JUnit·JAR SHA](experiments/s9-topology-2026-11-06-validation.json)를 보존한다.

전체 회귀 후에는 테스트 표본 하한만 파티션당 2개에서 3개로 강화했다.
공통 토큰 2개를 한 Worker가 모두 쓰더라도 최소 하나의 미완료 작업이 각 Worker에 남도록 하는 변경이다.
등록량 상한 181개는 유지하며, 운영 소스·설정은 전체 회귀 시점과 같다.
같은 토폴로지 테스트의 최종 재실행은 **1분 33초/1개 통과·실패·오류·스킵 0개**였다.
고유 작업 **27개**, HTTP 수락/완료 이력 각 27개, 중복·잘못된 매핑 0개, 실제 Wakeup 재시도 0회였다.
전체 397개 결과와 합산하지 않는다. 변경은 `coverage(2)`→`coverage(3)` 한 줄이며 다른 소스·구성·JAR SHA는 같다.
원래 전체 JUnit/HTML은 `build/reports/s9-topology-validation/`에 별도로 보존한다.

동일 PC의 JVM 분리는 별도 머신 장애·네트워크 단절·멀티 AZ 검증이 아니다.
LB/MySQL/Kafka/Redis가 각각 하나라 여전히 단일 장애점이다. LB 추가만으로 처리량 증가나 고가용성을 확보했다고 말하지 않는다.
API 종료는 응답이 끝난 뒤, Worker 종료는 모든 작업/offset이 정리된 뒤다.
처리 중 강제 종료, 응답 유실 직후 재접수, 공급자 수락 후 DB 실패의 불확실성은 기존 S5/S8 경계 테스트와 구분한다.
기존 S7 HTTP 5초 timeout 원인도 이 결과로 해결됐다고 간주하지 않는다.
새 S9 토폴로지의 캠페인 동시 lane·장시간 부하·프로세스 재시작·확장, 실제 서버/LB, AWS, FCM은 실행하지 않았다.
기존 S5/S8 테스트에서의 처리 중 종료·재시작 회귀 실행과 구분한다.
