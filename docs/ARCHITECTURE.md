# Notification Lab 현재 구현 아키텍처

기준 소스: `76c4042`, 2026-11-04. [README 그림](../README.md#아키텍처)은
역할 분리·Redis quota·HTTP 공급자 옵션을 켠 로컬 실험 구조다.
논리적 처리 경로를 그린 것으로, 독립 서버나 클라우드 배포를 완료했다는 뜻은 아니다.

## 두 가지 입력 경로

1. **실시간 알림**: `POST /notifications`가 notification, delivery_task, outbox_event를 같은 MySQL 트랜잭션으로 커밋한 뒤 202를 반환한다.
   조회는 저장된 처리 상태를 보여준다. 접수와 공급자 수락은 다른 시점이다.
2. **캠페인**: CAMPAIGN 역할의 `/campaigns`와 `/{id}/start`는 정의와 시작 의도를 저장한다.
   시작 API는 Batch를 자동 실행하지 않는다. 별도 `./gradlew campaignBatch --args='캠페인_UUID'`가
   합성 대상자를 읽고 chunk마다 대상별 알림·발송 작업·Outbox·checkpoint를 함께 저장한다.
   Batch COMPLETED는 DB 인계 완료다. 발송은 CAMPAIGN Relay/Worker가 이어간다.

두 경로 모두 영속 Outbox에 도달한 뒤 같은 발송 계약을 사용한다.
일시 실패는 다음 시도 예약과 Retry Outbox를 함께 저장하고, due 이후 Relay가 Retry 토픽으로 발행한다.
최종 실패는 DLQ Outbox를 저장하며 DLQ를 자동 소비하지 않는다. 선택 재발행은 별도 API와 상한을 따른다.

## 기술과 책임

| 구성요소 | 현재 책임 | 코드 근거 |
| --- | --- | --- |
| Java 21 / Spring Boot | 같은 JAR에서 역할별 API·Relay·Worker 구성 | [앱](../src/main/java/dev/jinhyeon/notificationlab/NotificationLabApplication.java), [역할 라우팅](../src/main/java/dev/jinhyeon/notificationlab/config/TrafficRouting.java) |
| MySQL / Flyway | 알림·작업·Outbox·시도 이력·Batch 메타데이터 영속화, 제약과 스키마 변경 | [접수 트랜잭션](../src/main/java/dev/jinhyeon/notificationlab/outbox/SubmissionService.java), [migration](../src/main/resources/db/migration) |
| Spring Batch | 명시적 실행, chunk·checkpoint와 재시작 | [명령](../src/main/java/dev/jinhyeon/notificationlab/campaign/batch/CampaignBatchCommand.java), [writer](../src/main/java/dev/jinhyeon/notificationlab/campaign/batch/CampaignTargetWriter.java), [설정](../src/main/java/dev/jinhyeon/notificationlab/campaign/batch/CampaignBatchConfiguration.java) |
| Outbox Relay | 짧은 DB 선점 후 Kafka 발행, ACK 후 PUBLISHED 저장 | [Relay](../src/main/java/dev/jinhyeon/notificationlab/outbox/OutboxRelay.java), [저장소](../src/main/java/dev/jinhyeon/notificationlab/outbox/OutboxStore.java) |
| Kafka | 역할별 요청·Retry·DLQ 토픽과 consumer group | [토픽 설정](../src/main/java/dev/jinhyeon/notificationlab/config/AsyncConfiguration.java), [라우팅](../src/main/java/dev/jinhyeon/notificationlab/config/TrafficRouting.java) |
| Worker | 한도 확인, 작업 선점, 외부 호출, DB 완료 후 offset 처리 | [Worker](../src/main/java/dev/jinhyeon/notificationlab/worker/NotificationWorker.java), [저장소](../src/main/java/dev/jinhyeon/notificationlab/worker/WorkerStore.java) |
| Redis | 선택적 공유 호출 예산, 실시간·캠페인 고정 예약 몫 | [제한기](../src/main/java/dev/jinhyeon/notificationlab/ratelimit/RedisRateLimiter.java), [Worker quota](../src/main/java/dev/jinhyeon/notificationlab/worker/WorkerQuota.java) |
| HTTP Mock / Circuit Breaker | 선택적 독립 합성 공급자, 응답·timeout과 장애 전파 제어 | [어댑터](../src/main/java/dev/jinhyeon/notificationlab/provider/HttpPushProvider.java), [회로 차단기](../src/main/java/dev/jinhyeon/notificationlab/provider/ProviderCircuitBreaker.java) |
| Micrometer / Actuator | 처리·Outbox·lag·pool 지표와 Prometheus scrape endpoint | [지표](../src/main/java/dev/jinhyeon/notificationlab/outbox/PipelineMetrics.java), [README](../README.md#지표) |
| Gradle / Docker / Testcontainers | 빌드와 실제 MySQL·Kafka·Redis 기반 검증 | [빌드](../build.gradle), [검증 이력](PROGRESS.md) |

Prometheus/Grafana 서버를 상시 배포한 그림은 아니다. 현재는 계측과 노출 endpoint가 있고 Grafana 대시보드는 계획에 남아 있다.

## 실행 경계와 보장

- 역할 분리를 켜면 같은 JAR를 REALTIME/CAMPAIGN의 별도 프로세스로 실행한다.
  역할별 토픽·group·Relay 선점 경로·Worker/HTTP/DB pool은 나뉘고, MySQL/Kafka/Redis 서버와 호스트 자원은 공유한다.
  부하 테스트 일부는 같은 JVM의 독립 Spring context로 이를 비교했다. 여러 서버 배포 결과로 표현하지 않는다.
- 기본 `split-enabled=false`, Redis client OFF, provider MOCK이다. 그림은 구현된 선택 옵션을 함께 켠 구성이다.
  일시 Redis 오류의 유한 복구도 기본 OFF이며 [별도 계약](S7_QUOTA_RECOVERY.md)을 따른다.
- Kafka ACK 전 Outbox를 PUBLISHED로 만들지 않는다. DB 완료/재시도 인계 전 미완료 offset을 건너뛰지 않는다.
  외부 공급자 호출 중 DB 트랜잭션을 유지하지 않는다.
- DB와 Kafka의 두 쓰기가 하나의 로컬 트랜잭션은 아니다. 재전달을 허용하고 영속 상태·제약·lease로 중복 작업을 제어한다.
  공급자 성공 직후 DB 저장 전 종료되는 구간에는 재호출 가능성이 남는다. 외부 발송 exactly-once를 보장하지 않는다.
- HTTP Mock 수락은 실제 FCM/APNs 전송이나 단말 도착이 아니다. 실제 회원 DB 대상자 조회, 인증과 운영 배포도 별도 미완료 범위다.

[요청/Outbox 계약](../README.md#처리-흐름과-복구), [역할 분리](S6_TRAFFIC_ISOLATION.md),
[Batch 계약](S7_CAMPAIGN_BATCH.md), [현재 계획](../PLAN.md)을 함께 참고한다.

## 그림 수정과 재생성

[HTML 원본 템플릿](architecture/notification-lab.template.html)에서 노드·그룹·화살표와 설명을 수정한다.
[완성 HTML](architecture/notification-lab.html)은 로고와 두 폰트를 내장하며 브라우저에서 밝은/어두운 테마를 바꿀 수 있다.
README는 HTML 실행 대신 두 PNG를 사용한다. PNG는 같은 HTML을 Chromium으로 렌더링한 결과다.

생성 도구는 [caru-ini/stack-diagram](https://github.com/caru-ini/stack-diagram)의
`1ed53e3e9525364ae5d82e0430f2bb3f7ef4c838` 버전이다. 로컬 Codex 스킬로 설치했으며 앱 실행/Gradle 빌드의 의존성은 아니다.
Python 3.9+, PNG 렌더용 Playwright/Chromium이 필요하다. 아래 경로는 스킬 기본 설치 위치다.

```bash
DIAGRAM_SKILL="$HOME/.codex/skills/stack-diagram"
python3 "$DIAGRAM_SKILL/scripts/fetch_icons.py" /tmp/notification-diagram-icons \
  springboot:spring mysql kafka redis batch:lucide/list-checks \
  relay:lucide/arrow-right-left worker:lucide/cpu mock:lucide/server
python3 "$DIAGRAM_SKILL/scripts/build.py" \
  docs/architecture/notification-lab.template.html /tmp/notification-diagram-icons \
  docs/architecture/notification-lab.html --fonts "$DIAGRAM_SKILL/assets/fonts"
python3 "$DIAGRAM_SKILL/scripts/render_png.py" \
  docs/architecture/notification-lab.html docs/architecture/notification-lab.png --selector .canvas --scale 2
```

아이콘 재조회 결과는 upstream 변경에 따라 달라질 수 있다. 현재 출처 URL은 [manifest](architecture/icons-manifest.json)에 보존했다.
두 테마에서 한글·아이콘·선·라벨·화살표 끝과 잘림을 실제 이미지로 확인한다.
[라이선스와 출처](architecture/THIRD_PARTY_NOTICES.md)도 함께 유지한다.
