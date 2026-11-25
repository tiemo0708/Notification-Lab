# 현재 작업 진입점

기준일: 2026-11-26. 이 파일은 탐색용 요약이며 완료 근거는 연결된 원본에서 확인한다.

## 현재 구현과 다음 작업

- Java 21 / Spring Boot 3.5.16 / Gradle 기반 단일 저장소다. 기본 비동기 흐름은 요청과 Outbox 저장 후 HTTP 202, Kafka 발행, Worker의 공급자 호출과 결과 저장이다.
- S1~S8 및 S9의 로컬 다중 프로세스 검증은 [계획](../PLAN.md)에 기록돼 있다. 공급자 수락과 단말 도착을 구분한다.
- 2026-11-17 서울 EC2 한 대에서 합성 알림 20건과 후속 100건의 정상 경로를 검증했다. 후속 실행은 31.373초에 접수·DB 완료·Mock 수락 각 100건, Kafka 미처리·재시도·DLQ 0건이었다. 앱·Mock·Compose 서비스는 종료했고 EC2와 데이터 볼륨은 보존했다. [조건과 실험 결과](S9_SEPARATE_HOST_PREPARATION.md)
- 같은 날 Mac과 서울 EC2 사이의 SSH 터널 단절·재연결을 검증했다. 재연결 후 기존 접수 7건은 같은 알림 ID를 반환했고, 단절 중 도달하지 못한 3건은 한 번씩 접수됐다. 10건 모두 DB 완료·Mock 수락이 일치했고 관측한 유실·중복 수락·Kafka 미처리는 0건이었다. [재연결 실험 결과](S9_SEPARATE_HOST_PREPARATION.md#ssh-터널-단절과-재연결-검증)
- 서울 EC2 앱 JVM을 강제 종료하고 재시작하는 두 상황을 검증했다. 미발행 Outbox 5건은 유실·중복 수락 없이 복구됐다. 이미 Kafka에 발행된 3건도 모두 복구됐고, Mock 수락 직후 DB 결과 저장 전에 종료된 1건은 복구 과정에서 Mock이 한 번 더 수락했다. [강제 종료 실험 결과](S9_SEPARATE_HOST_PREPARATION.md#앱-프로세스-강제-종료와-재시작-검증)
- 같은 서울 EC2를 중지·시작한 뒤 미발행 Outbox 4건과 Kafka에 발행된 미커밋 4건을 복구했다. 재부팅 직후 앱을 켜기 전 두 그룹의 상태가 그대로였고, 최종 DB 완료·Mock 수락 각 8건, 관측한 유실·중복 수락·Kafka 미처리 0건이었다. [인스턴스 중지·시작 실험](S9_SEPARATE_HOST_PREPARATION.md#ec2-인스턴스-중지와-시작-검증)
- 서울 EC2 두 대가 같은 MySQL·Kafka를 사용한 상태에서 앱 전용 호스트 B를 중지했다. A가 Kafka 파티션 0·1을 인계받아 미처리 6건을 완료했고, B가 중지된 상태에서 신규 3건도 처리했다. 관측한 유실·HTTP Mock 중복 수락·Kafka 미처리는 0건이었다. [두 EC2의 인계 실험](S9_SEPARATE_HOST_PREPARATION.md#두-ec2-앱-호스트의-처리-인계-검증)
- 같은 서울 EC2 구성에서 공유 MySQL과 Kafka를 각각 중단·복구했다. MySQL만 복구했을 때는 두 Worker가 멈춘 상태였고, 앱 재시작 후 미처리 7건을 복구했다. Kafka를 중단한 동안 접수한 8건은 Outbox에 저장됐고 Kafka 복구만으로 처리됐다. 관측한 유실·HTTP Mock 중복 수락·최종 Kafka 미처리는 0건이었다. [공유 인프라 장애 실험](S9_SEPARATE_HOST_PREPARATION.md#공유-mysql과-kafka-중단-및-복구-검증)
- MySQL 연결 장애로 발송 작업 선점에 실패한 경우 Worker가 오프셋을 보존하고 DB 응답을 확인한 뒤 자동 재개하도록 보완했다. 로컬에서는 미처리 7건, 서울 EC2 두 대에서는 미처리 5건이 앱 재시작 없이 복구됐다. 서울 실험의 합성 요청 10건에서 DB 성공 시도는 알림당 1회였고 유실·HTTP Mock 중복 수락·Kafka 미처리는 각각 0건이었다. 임시 SSH 허용 주소는 실험 후 이전 규칙으로 되돌렸다. [MySQL 자동 재개 검증](S9_SEPARATE_HOST_PREPARATION.md#mysql-복구-후-worker-자동-재개)
- HTTP Mock 수락 뒤 DB 결과 저장 전의 MySQL 장애를 로컬과 서울 EC2에서 재현했다. 서울 EC2에서는 MySQL만 복구했을 때 Worker가 정지 상태였고, 선점 기한 만료 뒤 앱을 재시작하자 3건 모두 완료됐다. Mock 수락 4회 중 1회는 중복이었다. 해당 DB 시도 이력은 `UNKNOWN → PROVIDER_ACCEPTED`였다. 임시 SSH 허용 주소는 원래 규칙으로 복원했다. [수락 후 MySQL 장애 실험](S9_SEPARATE_HOST_PREPARATION.md#서울-ec2의-수락-후-mysql-장애)
- HTTP 공급자 어댑터가 알림 UUID를 멱등성 키로 보내고, 같은 Mock 프로세스가 최초 수락 ID를 반환하도록 보완했다. 서울 EC2에서 수락 직후 MySQL 장애를 다시 재현했다. 앱 재시작 후 알림 3건이 완료됐고, HTTP 요청 4회 중 최초 수락 3회와 기존 결과 재응답 1회를 확인했다. 유실·중복 수락·Kafka 미처리는 각각 0건이었다. SSH 규칙은 기존 주소로 복원했다. [멱등성 적용 후 서울 EC2 실험](S9_SEPARATE_HOST_PREPARATION.md#http-mock-멱등성-적용-후-서울-ec2-재검증)
- HTTP Mock의 멱등성 키와 최초 수락 ID를 로컬 H2 파일에 저장하도록 보완했다. 서울 EC2에서 MySQL이 응답하지 않는 동안 Mock을 강제 종료했고, 새 Mock 프로세스가 같은 파일에서 수락 기록 1건을 복구했다. Worker 재시작 후 합성 알림 3건이 완료됐고 HTTP 요청 4회 중 기존 결과 재응답은 1회였다. 중복 수락·유실·Kafka 미처리는 0건이었고 SSH 규칙을 원복했다. [서울 EC2의 Mock 재시작 실험](S9_SEPARATE_HOST_PREPARATION.md#서울-ec2에서-mock-재시작-검증)
- 2026-11-19에 Mock 수락 직후 MySQL 응답을 중단하고 서울 EC2를 중지·시작했다. 같은 EBS의 H2에 남은 최초 수락 ID가 Worker 재처리 후 MySQL에 저장된 공급자 ID와 일치했다. 합성 알림 3건 완료, Mock 최초 수락 3회·기존 결과 재응답 1회, 유실·중복 수락·Kafka 미처리 각각 0건이었다. 실험 서비스를 종료하고 SSH 규칙을 원복했다. [EC2 중지·시작 검증 결과](S9_SEPARATE_HOST_PREPARATION.md#ec2-중지시작-후-ebs-수락-기록-검증)
- Worker 자동 재개와 HTTP Mock 멱등성 변경을 포함한 로컬 전체 빌드에서 테스트 403개를 새로 실행해 모두 통과했다. [전체 회귀 검증 기록](PROGRESS.md#2026-11-19-정합성-변경-전체-회귀-검증)
- 전용 Firebase 프로젝트와 Android API 34 Google APIs 에뮬레이터를 연결했다. 콘솔 단건 검증에 이어 Notification-Lab Worker가 FCM HTTP v1로 실제 발송 1건을 수락받았다. 앱의 `smokeId`는 Worker의 알림 ID와 같고, 수신과 Kafka 요청 커밋은 각각 1회였다. 실험용 키와 발송 권한은 회수했다. [Worker 단건 결과](FCM_DEVICE_SMOKE.md#notification-lab-worker에서-fcm-http-v1로-보내기)
- 마감에 여유 있는 대형 캠페인에 `campaign-flexible` 프로필을 선택하면 초기 Outbox를 최대 100명씩 저장하고 캠페인 워커 2개를 사용한다. 로컬 멱등 HTTP Mock이 수락한 묶음 내 성공 결과는 최대 5건씩 커밋한다. 실시간 워커는 3개이며 실시간 알림과 개별 재시도는 단건 커밋을 사용한다. [실행과 단건 복귀 절차](S7_CAMPAIGN_BATCH.md#마감에-여유-있는-대형-캠페인)

## 작업별로 읽을 자료

| 작업 | 진입점 |
| --- | --- |
| 계획·진행·과거 실행 증거 | [PLAN](../PLAN.md), [PROGRESS](PROGRESS.md)의 최신 또는 해당 날짜 절 |
| 코드 구조·학습 | [README](../README.md), [클래스 안내](CLASS_GUIDE.md) |
| 장애·정합성·성능 변경 | `notification-reliability`와 변경 경로의 실제 코드·테스트, [설계 판단](S9_DECISIONS.md) |
| 브랜치·커밋·PR | [CONTRIBUTING](../CONTRIBUTING.md) |

## 검증

- 기본: `./gradlew --no-daemon --console=plain test`. Java 21과 Docker가 필요하며 실제 MySQL/Kafka Testcontainers가 포함된다.
- CI와 같은 빌드: `./gradlew --no-daemon --console=plain build`.
- 부하·정합성 실험은 `build.gradle`의 별도 task와 해당 실험 문서를 확인한다. 기본 `test`가 모든 실험을 포함하지 않는다.
- Codex Actions는 [.codex/environments/environment.toml](../.codex/environments/environment.toml)에 있다. 자동 setup은 비워 두고 검증은 필요한 Action으로 실행한다.
- `UP-TO-DATE`와 새 테스트 실행을 구분한다. 과거 398개 통과를 새 실행 결과로 재사용하지 않는다.

진행 상태가 바뀌면 이 요약과 해당 단계/실험 근거를 함께 갱신한다. 원시 로그와 민감한 접속정보는 이 파일에 넣지 않는다.
