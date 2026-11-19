# S9 후속: 별도 머신과 AWS 실험 준비

서울 리전에서 **Mac → SSH 터널 → EC2 한 대**로 가상 알림 20건과 100건의 정상 경로를 확인했고,
후속 10건으로 SSH 터널 단절과 재연결을 검증했다. 앱 JVM 강제 종료와 EC2 인스턴스 중지·시작은 각각 가상 알림 8건으로 검증했다.
이후 서울 EC2 두 대에서 앱 전용 호스트 중지 후 미처리 6건의 인계와 신규 3건의 처리를 확인했다.
같은 두 호스트에서 공유 MySQL과 Kafka를 각각 중단해 복구 경로의 차이도 확인했다.
배포 대상 SHA는 `03f88cc`이며 [원격 CI](https://github.com/tiemo0708/Notification-Lab/actions/runs/42173414421)의
회귀 398개는 앞선 `b8f5043`에서 실행한 결과다.

## 첫 20건 검증

2026-11-17 13:53 KST에 Ubuntu 24.04.5 LTS(x86_64), `m7i-flex.large`, gp3 20GiB에서
Java 21.0.13.0.3, Docker 29.10.0, Compose v5.7.1로 실행했다. DB와 Kafka 오프셋은 모두 0에서 시작했다.
Mac 검증 도구의 전체 소요 시간은 6.491초다.

| 항목 | 확인 결과 |
| --- | --- |
| HTTP | 202 접수 20건. 같은 키·같은 본문은 동일 ID로 200, 같은 키·다른 본문은 409 |
| MySQL | 알림 완료 20건, 서로 다른 공급자 ID 20개, 알림당 시도 1회, 발행 완료 Outbox 20건 |
| HTTP Mock | 요청 20건, 수락 20건, 오류 0건 |
| Kafka | 요청 파티션의 끝 오프셋은 3·10·7. 커밋값 일치, 미처리 0건, Retry/DLQ 끝 오프셋 0 |

결과와 로그는 로컬 `build/reports/remote-smoke/aws-20-xk7FUi`에 보존했다.
EC2는 다음 실험을 위해 실행 상태로 뒀고, 앱·Mock과 전용 Compose 서비스는 종료했다.
MySQL·Kafka 데이터 볼륨은 보존했다.

## 후속 100건 검증

2026-11-17 15:14:38 KST에 같은 EC2와 배포 SHA에서 `python3 scripts/remote_smoke.py --count 100`을 실행했다.
같은 MySQL·Kafka 볼륨을 다시 연결해 기존 DB 알림 20건과 요청 파티션 끝 오프셋 3·10·7을 확인했다.
앱과 Mock은 새 프로세스로 시작했고 신규 요청은 Mac에서 SSH 터널을 거쳐 순차 접수했다.
Mock 집계는 0에서 시작했다. 검증 도구 전체 소요 시간은 31.373초다.

| 항목 | 후속 실행에서 관측한 변화 |
| --- | --- |
| HTTP | 신규 202 접수 100건. 같은 키·같은 본문 재요청은 동일 ID로 200, 다른 본문은 409 |
| MySQL | 알림·완료·고유 공급자 ID·발송 작업·시도 이력·발행 완료 Outbox 각각 20 → 120. 신규 알림별 성공 시도 1회 |
| HTTP Mock | 요청과 수락 각각 100건. 중복 수락, 제한, 서버 오류, 영구 실패, busy 응답 0건 |
| Kafka | 요청 파티션 끝 오프셋 3·10·7 → 34·48·38. 증가량 31·38·31, 합계 100. 세 파티션 모두 커밋값이 끝에 도달했고 미처리는 0건. Retry/DLQ 끝 오프셋은 모두 0 유지 |

기존 systemd 서비스가 종료 뒤에도 같은 이름으로 남아 있어 이번 앱과 Mock에는 새 이름을 사용했다.
검증 이후 이번 서비스와 Compose 컨테이너, SSH 터널을 종료했다. EC2는 실행 상태이고 MySQL·Kafka 볼륨은 보존했다.
전후 스냅샷, 원본 보고서와 대조 결과는 로컬 `build/reports/remote-smoke/aws-100-JZL9zf`에 보관했다.
Gradle 기본 테스트 명령은 3초에 성공했고 `UP-TO-DATE`로 새 JUnit 실행은 0개였다.
이 실행은 정상 경로 1회의 결과다.

## SSH 터널 단절과 재연결 검증

2026-11-17 15:23:49 KST에 같은 EC2·배포 SHA `03f88cc`·MySQL/Kafka 볼륨으로 실행했다.
직전 정상 실행까지의 DB 120건과 Kafka 요청 오프셋 34·48·38을 기준선으로 대조했다.
Mac의 검증 스크립트가 10개의 서로 다른 합성 요청 키를 사용했다. 앱·MySQL·Kafka·HTTP Mock은 EC2에서 계속 실행했다.

| 조건 | 재연결 후 결과 |
| --- | --- |
| 단절 전 클라이언트가 HTTP 202를 받은 3건 | 같은 키와 본문으로 다시 보내 HTTP 200과 처음 받은 알림 ID를 확인 |
| 서버가 HTTP 202를 반환했으나 클라이언트가 응답을 받지 못한 4건 | Mac의 시험용 중계기가 서버 응답을 보류한 상태에서 실제 SSH 터널과 클라이언트 연결을 닫았다. 다시 연결해 같은 키와 본문으로 보내자 모두 HTTP 200을 반환했고, 중계기가 보관한 원래 알림 ID와 일치 |
| 단절 중 연결이 거절된 3건 | 같은 키와 본문으로 보내 HTTP 202와 각 요청의 새 알림 ID를 확인 |
| 응답을 잃은 요청 중 1건의 본문을 바꿔 재요청 | HTTP 409를 반환 |

터널이 끊긴 동안 별도의 SSH 관리 세션에서 DB와 Kafka를 조회했다. 단절 전에 서버가 수락한 7건은 이미 DB에 완료 상태로 저장돼 있었고, 단절 중 연결이 거절된 3건은 아직 DB 건수에 포함되지 않았다.
API·Mock의 터널 접속이 다시 정상임을 확인하기까지 5.534초, 기준선 조회부터 최종 대조까지 23.280초가 걸렸다.

- MySQL의 알림, 공급자 수락 완료, 발송 작업, 단일 성공 시도, 고유 공급자 ID와 발행 완료 Outbox가 각각 120건에서 130건으로 늘었다. 신규 알림의 유실은 관측되지 않았다.
- Mock 요청과 수락은 각각 10건이었다. 중복 수락과 Mock 오류는 0건이었다. 동일 키의 API 재전송 7건은 추가 발송으로 이어지지 않았다.
- Kafka 요청 파티션 끝 오프셋은 단절 전 34·48·38, 단절 중 36·51·40, 최종 36·53·41이었다. 최종 증가량 합은 10건이고 세 파티션의 커밋값과 끝 오프셋이 일치했다. 미처리, Retry와 DLQ는 0건이었다.

이 실험의 장애 범위는 Mac의 SSH 포워딩을 경유하는 API 접근과 클라이언트의 응답 미수신이다. EC2 호스트 중단과 내부 MySQL·Kafka 또는 외부 공급자 연결 장애는 각각 다른 실험이다.
스크립트, 전후·단절 중 스냅샷, 보고서와 서비스 로그를 로컬 `build/reports/remote-smoke/aws-network-iWwdx1`에 보관했다.
테스트 서비스와 터널은 종료했고 EC2와 MySQL·Kafka 볼륨은 보존했다.
Gradle 기본 테스트 명령은 3초에 성공했고 `UP-TO-DATE`로 새 JUnit 실행은 0개였다.

## 앱 프로세스 강제 종료와 재시작 검증

2026-11-17 15:33:19 KST에 동일한 서울 EC2, 배포 SHA `03f88cc`, MySQL·Kafka 볼륨으로 실험했다.
기준선은 완료 알림 130건과 Kafka 요청 파티션의 커밋·끝 오프셋 36·53·41이었다.
합성 알림을 EC2의 앱에 접수하고 두 번 모두 대상 앱 JVM에 `SIGKILL`을 보냈다.
systemd에서 종료 신호 9와 PID 소멸을 확인했고, 새 PID에서 같은 DB와 Kafka 소비 그룹을 사용했다.
각 강제 종료 시 MySQL과 Kafka, 해당 실험의 HTTP Mock은 계속 실행했다.

| 종료 지점 | 종료 전 확인 | 재시작 결과 |
| --- | --- | --- |
| Outbox 발행 전 | Relay와 Worker를 꺼서 HTTP 202를 받은 5건을 DB의 `QUEUED`와 Outbox `PENDING`에 남겼다. Kafka 오프셋과 Mock 호출은 증가하지 않았다. | Relay와 Worker를 켠 새 JVM에서 5건 모두 DB 완료. 성공 시도 각 1회, Mock 수락 5건, 유실·중복 수락 0건. |
| Kafka 발행 후, 공급자 수락 결과를 DB에 저장하기 전 | Outbox 3건은 모두 `PUBLISHED`. Mock 응답을 4초 지연시켜 1건은 Mock이 수락했지만 DB에는 `PROCESSING`과 `STARTED`만 남겼다. 나머지 2건은 `QUEUED`로 시도 0회였다. 강제 종료 후에도 새 3건의 Kafka 오프셋은 미커밋 상태였다. | DB의 30초 작업 소유권이 자연 만료된 것을 확인하고 새 JVM을 시작했다. 3건 모두 DB 완료. 시도하지 않은 2건은 각각 한 번 수락됐고, 결과가 미확정이던 1건은 Mock이 한 번 더 수락했다. 그 알림의 DB 시도 이력은 `UNKNOWN → PROVIDER_ACCEPTED`였다. |

두 실험에서 기존 멱등 키와 본문을 다시 보내 8건 모두 동일한 알림 ID와 HTTP 200을 확인했다.
최종 DB 완료 알림은 138건이고, 발송 시도 이력은 성공 138건과 `UNKNOWN` 1건을 합쳐 139건이다.
신규 알림 8건에서 유실은 0건, Mock 수락은 총 9건으로 중복 수락은 1건이다.
두 번째 실험의 추가 호출은 미커밋 요청 레코드를 재소비하면서 발생했다. Retry 토픽에 새 레코드를 넣는 방식과 구분된다.
최종 Kafka 커밋과 끝 오프셋은 모두 37·55·46으로 일치했고 미처리·Retry·DLQ는 0건이었다.
이번 프로세스 복구 실험의 공급자 관측 지표는 HTTP Mock 수락이다.

첫 실행은 검증 스크립트에서 HTTP 모듈 이름이 함수 이름과 겹쳐 접수 전에 중단됐다.
이름을 수정한 후 기준선 130건을 다시 확인하고 두 실험을 진행했다.
스크립트, 종료·복구 이벤트, 최종 DB·Kafka 조회와 서비스 로그를 로컬 `build/reports/remote-smoke/aws-app-crash-DgeIRD`에 보관했다.
실험 서비스를 종료했고 EC2와 MySQL·Kafka 볼륨을 보존했다.
Gradle 기본 테스트 명령은 3초에 성공했다. `UP-TO-DATE`로 새 JUnit 실행은 0개였다.

## EC2 인스턴스 중지와 시작 검증

2026-11-17 서울 리전의 동일 EC2 한 대에서 배포 SHA `03f88cc`로 이전 완료 알림 138건을 기준선으로 삼았다.
합성 알림 8건을 HTTP 202로 접수했다. 처음 4건은 Relay만 켜 Kafka에 발행했고, 다음 4건은 Relay와 Worker를 꺼서 미발행 Outbox에 남겼다.
중지 직전에 8건 모두 DB `QUEUED`, 발송 시도 0회, Mock 수락 0건임을 확인했다.

EC2 콘솔에서 기본 중지를 요청하고 KST 15:46:15에 `중지됨`을 확인했다.
같은 인스턴스를 시작해 15:47:23에 `실행 중`을 확인했다. 루트 EBS 볼륨과 파일시스템 UUID는 같았고 Linux 부팅 ID와 공인 IPv4는 바뀌었다.
새 주소에 기존 SSH 호스트 키로 접속했다. 기본 중지는 OS의 정상 종료를 시도한다. [AWS 중지·시작 동작](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/Stop_Start.html)

| 중지 전 상태 | EC2 시작 후 앱 기동 전 | 정상 설정으로 앱 기동 후 |
| --- | --- | --- |
| DB에만 저장된 Outbox `PENDING` 4건 | 기존 알림 ID로 `QUEUED`, Outbox `PENDING`, 발송 시도 0회를 대조 | 4건 신규 발행 및 DB 완료. 각각 성공 시도 1회 |
| Outbox `PUBLISHED`, Kafka 미커밋 4건 | 기존 알림 ID로 `QUEUED`, Outbox `PUBLISHED`, 발송 시도 0회를 대조. 요청 오프셋과 커밋 상태가 중지 전과 일치 | 새 Kafka 레코드 없이 기존 4건을 처리. 각각 성공 시도 1회 |

신규 알림 8건의 DB 완료와 Mock 요청·수락이 각각 8건으로 일치했다. 관측한 유실과 중복 수락은 0건이었다.
같은 키와 본문으로 다시 요청했을 때 HTTP 200을 받았고, 8건 모두 최초 알림 ID가 유지됐다.
Kafka의 중지 전 끝 오프셋은 40·56·46, 커밋은 37·55·46이었다. 앱 기동 후 끝과 커밋은 40·58·48로 일치했고 미처리·Retry·DLQ는 0건이었다.
이번 실험은 공급자 호출을 시작하기 전 남아 있던 작업을 대상으로 했다. 공급자가 수락했으나 DB에 결과가 남지 않은 경우의 별도 관측은 위 JVM 실험에 기록했다.

콘솔 상태 캡처, 단계별 DB·Kafka 관측, 스크립트와 검증 결과를 로컬 `build/reports/remote-smoke/aws-host-stop-mBD3r7`에 보관했다.
실험 서비스는 종료했고 EC2는 실행 상태로 뒀다. EBS와 MySQL·Kafka 볼륨은 보존했다.

## 두 EC2 앱 호스트의 처리 인계 검증

2026-11-17 서울 리전의 두 EC2(Ubuntu 24.04, Java 21)에 체크섬을 검증한 배포 SHA `03f88cc`의 같은 앱을 띄웠다.
A(`m7i-flex.large`)에는 API·Outbox Relay·Worker 및 공유 MySQL·Kafka·HTTP Mock을 뒀다. B(`t3.small`)에는 Relay를 끈 앱의 API·Worker를 띄웠다.
B가 Mac의 SSH 포워딩을 거쳐 A의 공유 테스트 백엔드에 접속하도록 구성했다. 앱·DB·Kafka·Mock에 인바운드 규칙을 추가하지 않았다.
두 호스트의 부팅 ID가 달랐고 Worker는 각각 한 컨슈머로 같은 `notification-worker-v1` 그룹에 참여했다. 요청 파티션은 A가 2번, B가 0·1번을 맡았다.

| 확인 구간 | MySQL·HTTP Mock | Kafka 요청 파티션 0·1·2 |
| --- | --- | --- |
| 장애 전 대조군 10건 | A 3건과 B 7건 처리 확인. DB 완료·Mock 요청·수락 각각 10건 | 끝·커밋 `42·63·51`, 미처리 0건 |
| B 중지 직전, 실험 요청 12건 | A 몫 6건 완료. B 몫 6건은 모두 `QUEUED`, Outbox `PUBLISHED`, 발송 시도 0회 | 끝 `43·68·57`, 커밋 `42·63·57`, 미처리 `1·5·0` |
| B 중지 후, 같은 12건 대조 | A가 남은 6건을 인계. 12건 모두 ID별 성공 시도 1회. 같은 키를 재요청한 12건은 기존 ID 반환, 추가 Mock 호출 없음 | A가 세 파티션을 모두 소유. 끝·커밋 `43·68·57`, 미처리 0건 |
| B가 중지된 상태에서 신규 3건 | A가 3건을 추가 처리. 이번 실험의 25건 전부 DB 완료, Mock 요청·수락 각각 25건 | 끝·커밋 `44·68·59`, 미처리·Retry·DLQ 0건 |

발송 전 미처리 상태를 확실히 만들기 위해 대조군 완료 뒤 B의 JVM을 잠시 `SIGSTOP`으로 멈췄다.
B가 여전히 그룹 내 파티션을 소유하고 미처리 6건이 확인된 뒤 16:04:47 KST에 EC2 콘솔에서 B의 기본 중지를 요청했다.
B의 SSH 연결이 끊긴 직후에도 미처리 6건이 그대로였고, 콘솔은 16:05:21에 `중지됨`을 표시했다.
16:06:27에 Kafka가 heartbeat 만료로 B를 제외했고, 16:06:30에 같은 A 소비자가 파티션 0·1을 추가로 받았다.
중지 요청부터 재할당 확인까지 약 103초였다. 미처리 상태를 준비할 시간을 확보하려고 Kafka `session.timeout.ms`를 120초로 설정한 실험값이다. 운영 복구 시간으로 일반화할 수 없다.
첫 검증은 결과를 기다리는 85초 제한에 먼저 도달했다. 서비스를 재시작하거나 실험 요청을 다시 접수하지 않고 후속 검증으로 같은 12건의 DB·Mock·Kafka 기록을 대조했다.

관측한 유실과 HTTP Mock 중복 수락은 각각 0건이다. A의 JVM·MySQL·Kafka·Mock 재시작은 없었고, 기존 실험의 `UNKNOWN` 1건도 증가하지 않았다.
공급자 호출 전에 밀린 작업의 인계를 검증했다. 공급자가 수락한 직후 DB 기록 전에 죽는 경우에는 앞선 JVM 강제 종료 실험에서 중복 수락이 1건 관측됐다.
공유 인프라가 있는 A 자체의 장애나 DB·Kafka 장애, VPC 직결 경로, 실제 FCM/단말 도착은 이 실험 범위 밖이다.

로컬 근거는 `build/reports/remote-smoke/aws-two-host-20261117/`의 `reconciliation.jsonl`, `post-stop-reconciliation.jsonl`, `broker-rebalance.txt`, `survivor-rebalance.txt`, `console-stopped.json`이다.
실험 후 A의 앱·Mock·Compose 서비스와 SSH 터널을 종료했다. A EC2는 이전과 같이 실행 상태이고 B는 중지 상태다. 두 호스트의 EBS와 기존 MySQL·Kafka 볼륨은 보존했다.
`./gradlew --no-daemon --console=plain test`는 성공했고 `UP-TO-DATE`로 새 JUnit 실행은 0개였다.
Gradle 기본 테스트는 3초에 성공했다. `UP-TO-DATE`로 새 JUnit 실행은 0개였다.

## 공유 MySQL과 Kafka 중단 및 복구 검증

2026-11-17 서울 EC2 두 대에서 같은 배포 SHA `03f88cc`와 `notification-worker-v1` 그룹을 사용했다. A에 앱·Outbox Relay·HTTP Mock·MySQL 8.4.8·Kafka 4.0.2를 두고 B에 앱과 Worker를 두었다. B는 기존 SSH 포워딩으로 A의 인프라에 접속했다. MySQL과 Kafka를 한 번에 하나씩 `docker compose stop -t 10`으로 중단하고 `docker compose start --wait`로 원래 컨테이너와 볼륨을 복구했다. 각각 다른 의존 서비스의 컨테이너 시작 시각이 유지됐음을 확인했다. [Compose 중지](https://docs.docker.com/reference/cli/docker/compose/stop/) · [Compose 시작](https://docs.docker.com/reference/cli/docker/compose/start/)

### MySQL 중단

기존 완료 알림 171건과 Kafka 요청 파티션의 커밋·끝 오프셋 `44·68·59`에서 시작했다. B의 JVM을 잠시 멈춰 합성 요청 10건 중 7건을 B 소유 파티션에 `QUEUED`, Outbox `PUBLISHED`, 발송 시도 0회로 남겼다. A는 나머지 3건을 이미 완료했다. 16:14:43 KST에 MySQL 컨테이너를 중지한 뒤 B의 JVM을 재개했다.

| 구간 | 앱과 Worker | Kafka와 Mock |
| --- | --- | --- |
| MySQL 중단 중 | 두 JVM은 살아 있었고 헬스 체크는 각각 HTTP 503. 두 Worker는 해당 메시지의 DB 처리 오류로 소비를 중지했다. | 커밋 `44·68·62`, 끝 `47·72·62`, 미처리 `3·4·0`. Mock 수락은 장애 전 3건에서 그대로였다. |
| MySQL만 복구 | 두 헬스 체크가 HTTP 200으로 돌아온 뒤 12초를 더 관찰했다. Worker 실행 지표는 두 호스트 모두 0이었다. | 같은 ID 7건이 `QUEUED`, 발송 시도 0회로 남았다. 커밋과 Mock 건수도 그대로였다. |
| 앱 JVM 두 개를 재시작 | 동일한 7건을 복구했다. 이번 요청 10건 각각에 성공 시도 1회가 있었다. 같은 키를 다시 보내면 원래 ID를 반환했다. | 커밋과 끝 `47·72·62`, Mock 요청·수락 각각 10건. 관측한 유실·중복 수락·미처리 각 0건. |

DB 처리 실패가 발생하면 `NotificationWorker`가 해당 오프셋을 커밋하지 않고 예외를 내보낸다. `WorkerPauseErrorHandler`는 DB 예외를 일시 중지 후 재개 대상에 포함하지 않아 Kafka 리스너를 중지한다. 이번 복구에서는 두 앱을 재시작한 뒤 처리가 재개됐다. HTTP 헬스 체크와 Worker 실행 지표를 각각 확인해야 하는 이유가 이 실험에서 드러났다.

### Kafka 중단

두 JVM이 같은 소비 그룹에 참여한 뒤 Kafka만 중단했다. 첫 실행의 검증 스크립트는 Outbox가 발행을 시도 중일 때 사용하는 `PROCESSING` 상태를 잘못 판정했다. `finally`에서 Kafka가 시작된 후 같은 8건이 각각 한 번 완료되고 Mock도 8건을 추가 수락한 것을 대조했다. 상태 판정을 수정해 새 요청 8건으로 중단과 복구를 다시 실행했다.

| 구간 | MySQL·HTTP Mock | 확인 결과 |
| --- | --- | --- |
| Kafka 중단 중 | A·B API 각각 4건에 HTTP 202. MySQL에 알림·Outbox 8건을 저장했다. | 알림 8건 모두 `QUEUED`, 공급자 발송 시도 0회, 신규 Mock 수락 0건. 표본 시점의 Outbox는 `PENDING` 7건(발행 시도 0회 6건, 1회 1건)과 `PROCESSING` 1건(발행 시도 1회)이었다. |
| Kafka 복구 후 | 같은 8건 각각에 DB 성공 시도 1회. Mock 요청·수락 8건씩 증가했다. 같은 키를 다시 보내면 원래 ID를 반환했다. | 두 앱 JVM의 PID가 같았고 두 Worker가 동작했다. 요청 끝·커밋은 `51·74·64`에서 `55·76·66`으로 증가했고 미처리·Retry·DLQ는 0건이었다. |

MySQL 실험과 Kafka 두 실행의 합성 요청은 총 26건이었다. DB 완료와 Mock 요청·수락이 각각 26건씩 증가했고 기존 `UNKNOWN` 1건은 그대로였다. 확인한 경계는 공급자 호출 전에 Kafka나 Outbox에 남은 작업이다. 공급자 수락 직후 DB에 결과를 쓰는 도중의 장애, 데이터 볼륨 손실과 실제 단말 도착은 별도 검증이다.

근거는 `build/reports/remote-smoke/aws-shared-dependency-shCoTW/`의 `reconciliation.jsonl`, `mysql-worker-stop-evidence.txt`, `kafka-checkpoint.jsonl`, `kafka-recheck.jsonl`이다. `./gradlew --no-daemon --console=plain test`는 성공했고 `UP-TO-DATE`로 새 JUnit 실행은 0개였다.
실험 후 양쪽 앱·Mock·Compose 서비스와 SSH 터널을 종료했다. A EC2는 실행 상태로 뒀고 B는 중지했다. MySQL과 Kafka의 데이터 볼륨은 보존했다.

## MySQL 복구 후 Worker 자동 재개

2026-11-18에 발송 선점 트랜잭션을 시작하거나 실행하는 중 확인된 JDBC 연결 오류(SQLState `08` 계열)를 자동 복구 대상으로 분류했다. 리스너는 같은 Kafka 그룹에 남아 poll을 이어가며, 실패한 레코드의 오프셋을 커밋하지 않는다. 대기 중에는 리스너 컨테이너마다 하나의 작업이 2초 간격으로 `SELECT 1`을 확인한다. 쿼리가 성공하면 리스너가 같은 레코드를 다시 처리한다. 대기와 재개는 `notification_worker_database_pauses_pending`, `notification_worker_database_resumes_total`로 관찰한다. 검증 범위는 발송 선점 단계다. 선점 커밋 결과가 불명확하거나 공급자 호출 이후 DB 결과를 저장하다 오류가 난 경우에는 기존 중지 경로로 처리한다. [MySQL Connector/J 연결 오류](https://dev.mysql.com/doc/connector-j/en/connector-j-usagenotes-troubleshooting.html) · [Spring Kafka 오류 처리](https://docs.spring.io/spring-kafka/reference/3.3/kafka/annotation-error-handling.html)

로컬에서 앱 JVM 두 개와 MySQL 8.4.8·Kafka 4.0.2·HTTP Mock을 실행했다. 이전 서울 EC2 실험처럼 Kafka는 계속 켜 둔 채 `docker compose stop -t 10 mysql`로 MySQL만 멈춘 뒤, `docker compose start --wait mysql`로 같은 컨테이너와 볼륨을 시작했다. 정상 요청 15건을 기준선으로 확보한 뒤 합성 요청 10건을 추가했다.

| 구간 | 관측 결과 |
| --- | --- |
| 중단 직전 | 10건 중 3건 완료. B가 맡은 7건은 `QUEUED`, Outbox `PUBLISHED`, 발송 시도 0회. 파티션 0·1·2의 커밋 `2·4·12`, 끝 `7·6·12` |
| MySQL 중단 중 | 두 JVM의 PID와 Kafka 소비자 ID가 유지됐다. B의 DB 대기 지표는 1이었고 미커밋 7건과 Mock 수락 건수는 그대로였다. |
| MySQL만 재시작 | 같은 두 JVM과 소비자에서 B가 한 번 자동 재개했다. 7건을 모두 복구해 이번 10건 각각에 성공 시도 1회를 확인했다. 같은 요청 키 10개가 기존 알림 ID를 반환했다. 끝·커밋 `7·6·12`, 유실·Mock 중복 수락·Kafka 미처리·Retry·DLQ 각각 0건. |

첫 로컬 실험은 EC2용 `sudo`가 포함된 Docker 상태 조회에서 중단됐다. 로컬 실행 경로에서 `sudo`를 제거해 격리된 데이터로 다시 실행했다. 별도 Testcontainers 통합 테스트에서는 MySQL 중단 상태가 Kafka의 5초 `max.poll.interval.ms`를 넘는 동안 그룹과 미커밋 오프셋을 유지하고 공급자 호출 0회를 확인했다. MySQL을 재개하자 같은 그룹 멤버가 알림을 처리했고, HTTP Mock 요청·수락과 DB 성공 시도는 각 1회였다.

`HttpWorkerTest`, `WorkerPauseErrorHandlerTest`, `WorkerRebalanceTest`, `WorkerQuotaIntegrationTest`, `WorkerCircuitBreakerIntegrationTest`의 43개 Gradle 테스트가 통과했다. 로컬 중단·재시작 검증의 원시 대조 로그는 `build/reports/remote-smoke/aws-mysql-auto-recovery-20261118/local-exact-rerun.jsonl`에 있다.

### 서울 EC2 재검증

2026-11-18 서울 EC2 두 대에 로컬에서 검증한 것과 같은 JAR를 배포하고 양쪽에서 SHA-256 `7db738e4babcf7edb02d5bf7d7f3f1e7f2194367b7d423572cf4d3c8dbfd576b`를 검증했다. A는 API·Outbox Relay·Worker와 공유 MySQL 8.4.8·Kafka 4.0.2·HTTP Mock을, B는 별도 JVM의 API와 Worker를 실행했다. 두 Worker는 같은 `notification-worker-v1` 그룹에 참여했다. 이전 실험의 정상 완료 수 197건과 Kafka 요청 파티션 끝·커밋 `55·76·66`을 기준선으로 사용했다.

| 구간 | 관측 결과 |
| --- | --- |
| MySQL 중단 직전 | 합성 요청 10건 중 A가 5건을 완료했다. B가 맡은 5건은 `QUEUED`, Outbox `PUBLISHED`, 발송 시도 0회였다. 파티션 0·1·2의 커밋 `55·76·71`, 끝 `58·78·71`, 미커밋 `3·2·0`이었다. |
| MySQL 중단 중 | A와 B에서 기존 JVM PID와 Kafka 그룹 멤버 ID를 유지했다. B의 DB 대기 지표는 1, 재개 횟수는 0이었다. 미커밋 5건과 Mock 수락 5건이 그대로였다. |
| MySQL만 복구 | 원래 컨테이너와 볼륨으로 MySQL을 시작했다. Kafka는 같은 프로세스로 유지됐다. B의 재개 횟수가 1이 됐고 기존 JVM과 그룹 멤버가 5건을 모두 처리했다. 요청 끝·커밋은 `58·78·71`로 일치했다. |

이번 합성 요청 10건 모두 기존 ID로 완료됐다. DB에서는 알림별 성공 시도가 1회였고, HTTP Mock 요청과 수락은 각각 총 10건이었다. 같은 요청 키 10개로 다시 요청했을 때 기존 ID를 받았으며 Mock 호출은 늘지 않았다. 관측한 유실·중복 수락·Kafka 미처리·Retry·DLQ는 각각 0건이었다. 이전 실험의 `UNKNOWN` 1건은 그대로였다. 이번 원격 실험의 검증 범위는 공급자 호출 전에 미커밋된 작업이다.

첫 기동은 이전 실험의 systemd 유닛 이름이 남아 중단됐다. 새 유닛 이름으로 같은 JAR를 기동한 뒤 검증을 시작했다. 실험 중에는 보안 그룹의 유일한 TCP 22 소스를 현재 핫스팟 `/32`로 잠시 교체했다. 실험 앱·Mock·Compose 서비스와 SSH 터널을 종료한 뒤 원래 `/32`로 되돌리고 임시 주소가 빠졌음을 콘솔에서 확인했다. A EC2는 실행 상태로 뒀고 B EC2는 다시 중지했다. 기존 MySQL·Kafka 데이터 볼륨은 보존했다.

원격 실행 근거는 `build/reports/remote-smoke/aws-mysql-auto-recovery-20261118/`의 `remote-reconciliation.jsonl`, `deploy-remote.txt`, `remote-services.txt`, `cleanup-remote.txt`, `ssh-rule-restored.json`에 있다.

## HTTP Mock 수락 후 MySQL 장애

2026-11-18에 보완 버전의 앱과 MySQL 8.4.8·Kafka 4.0.2·HTTP Mock으로 이 장애 시점을 로컬에서 재현했다. Mock 설정은 성공 응답을 3.5초 지연했다. 합성 요청 3건 중 1건의 Mock 수락을 확인했을 때 DB 상태는 `PROCESSING`과 시도 이력 `STARTED`였고 나머지 2건은 `QUEUED`였다. 그 상태에서 Docker `pause`로 MySQL이 응답하지 않게 한 뒤, Worker가 완료 저장을 시도하고 정지할 때까지 기다렸다. Kafka는 계속 실행했다. [Docker 컨테이너 일시 중지](https://docs.docker.com/reference/cli/docker/container/pause/)

| 구간 | 로컬 관측 결과 |
| --- | --- |
| MySQL 응답 중단 | 기존 앱 PID에서 DB 완료 처리 타이머 1회와 Worker 정지를 확인했다. Kafka 끝은 `9·11·13`이었고 커밋은 기존 `8·10·12`에 남았다. Mock 수락은 1회였다. |
| MySQL 복구만 적용 | 같은 MySQL 컨테이너가 응답하고 HTTP 헬스 체크가 복구됐다. Worker는 여전히 정지 상태였고, 첫 알림의 DB 결과는 `STARTED`로 남았다. |
| 선점 기한 만료 뒤 앱 재시작 | 3건 모두 완료됐다. 먼저 수락된 알림의 DB 시도 순서는 `UNKNOWN → PROVIDER_ACCEPTED`였고 다른 2건은 1회 시도로 완료됐다. Mock 요청·수락 각각 4건, 중복 수락 1건, 유실·Kafka 미처리·Retry·DLQ 0건이었다. |

DB에 수락 여부가 남지 않은 작업은 선점 기한 만료 뒤 동일 Kafka 레코드에서 다시 처리된다. 이번에는 Mock이 첫 요청을 이미 수락해 중복 수락이 생겼다. 로컬 원시 대조 로그는 `build/reports/remote-smoke/aws-mysql-after-accept-20261118/local-boundary.jsonl`에 있다.

### 서울 EC2의 수락 후 MySQL 장애

2026-11-18 서울 EC2 A에서 같은 자동 재개 JAR의 SHA-256을 검증하고, 기존 MySQL 8.4.8·Kafka 4.0.2 볼륨과 HTTP Mock을 사용했다. 정상 완료 207건과 Kafka 요청 파티션 끝·커밋 `58·78·71`을 기준선으로 삼았다. Mock은 성공 응답을 3.5초 지연했고, Worker HTTP 응답 제한은 5초로 설정했다. Kafka는 실험 구간 내내 같은 컨테이너에서 동작했다.

| 구간 | 서울 EC2 관측 결과 |
| --- | --- |
| Mock 수락 직후 | 합성 요청 3건이 Kafka에 발행됐다. Mock이 1건을 수락했고 해당 DB 이력에는 `PROCESSING`과 `STARTED`가 남아 있었다. 나머지 2건의 발송 시도는 0회였다. 이 시점 포착 후 53ms에 MySQL을 `pause`했다. Mock은 아직 첫 요청의 응답을 지연 중이었다. |
| MySQL 응답 중단 | DB 완료 처리 타이머에 시도 1회가 기록됐고 Worker는 정지했다. 앱 JVM PID는 유지됐다. Kafka 요청 끝은 `59·79·72`, 커밋은 `58·78·71`이었다. Mock 수락 건수는 1회였다. |
| MySQL 복구만 적용 | 같은 MySQL 컨테이너를 `unpause`했고 앱 헬스 체크가 복구됐다. Worker는 계속 정지 상태였고 첫 알림의 이력은 `STARTED`로 남았다. |
| 선점 기한 만료 뒤 앱 재시작 | 기존 알림 3건 모두 완료됐다. 먼저 수락된 알림의 DB 이력은 `UNKNOWN → PROVIDER_ACCEPTED`였고 나머지 2건은 각각 한 번에 성공했다. Mock 요청과 수락은 각각 총 4회로 중복 수락 1회를 관측했다. Kafka 끝과 커밋은 `59·79·72`로 일치했고 유실·미처리·Retry·DLQ는 0건이었다. |

같은 요청 키 3개로 재요청했을 때 모두 기존 알림 ID를 반환했고, Mock 호출은 늘지 않았다. 공급자 수락 여부가 DB에 남지 않아 Worker가 같은 Kafka 레코드를 다시 처리하면서 중복 수락이 발생했다. 여기서 수락은 HTTP Mock의 관측값이다.

실험 후 앱·Mock·Compose 서비스를 종료하고 MySQL·Kafka 데이터 볼륨을 보존했다. SSH 규칙은 기존 단일 `/32` 주소로 원복했고 임시 핫스팟 주소가 빠진 것을 콘솔에서 확인했다. 원시 로그와 복원 기록은 `build/reports/remote-smoke/aws-mysql-after-accept-20261118/remote-boundary.log`, `ssh-rule-restored.json`에 있다.

### HTTP Mock 멱등성 적용 후 서울 EC2 재검증

2026-11-18에 HTTP 공급자 어댑터가 발송과 재처리에서 알림 UUID를 동일한 `Idempotency-Key`로 보내도록 변경했다. HTTP Mock은 최초 수락과 키 등록을 한 임계 구역에서 처리하고, 같은 키와 본문으로 다시 요청하면 처음 생성한 공급자 ID를 즉시 반환한다. 키는 Mock 프로세스의 메모리에 최대 1,100,000개 보관한다. 용량이 차면 기존 키를 보존하며 새 키에 HTTP 503을 반환한다. [HTTP Mock 계약과 설정](S6_HTTP_MOCK.md)

서울 EC2 A에 새 앱과 Mock JAR를 배포하고 두 파일의 SHA-256을 확인했다. 기준선은 DB 정상 완료 210건과 Kafka 요청 파티션 끝·커밋 `59·79·72`였다. 합성 요청 3건 중 1건을 Mock이 수락하고 DB에 `STARTED`로 남은 시점을 포착했다. 첫 응답이 지연 중일 때 MySQL을 일시 중지했다. 이전 실험과 같이 완료 저장 단계에서 Worker가 멈췄고, MySQL만 복구한 직후에는 기존 JVM이 살아 있었지만 Worker는 정지 상태였다. 당시 Kafka 요청 끝은 `60·81·72`, 커밋은 `59·79·72`였다.

선점 기한이 지난 뒤 앱만 재시작했다. 앞서 수락한 알림의 Kafka 레코드가 다시 처리됐고, 같은 Mock 프로세스가 최초 수락 결과를 반환했다. 그 알림의 DB 시도 이력은 `UNKNOWN → PROVIDER_ACCEPTED`였다.

| 지표 | 멱등성 적용 전 | 멱등성 적용 후 |
| --- | ---: | ---: |
| 합성 알림의 DB 완료 | 3 | 3 |
| HTTP Mock 요청 | 4 | 4 |
| HTTP Mock 수락 | 4 | 3 |
| 기존 수락 결과 재응답 | 0 | 1 |
| 관측한 중복 수락 | 1 | 0 |
| 관측한 유실 | 0 | 0 |

최종 Kafka 끝과 커밋은 `60·81·72`로 일치했고 미처리·Retry·DLQ는 0건이었다. 같은 접수 키 3개를 다시 보내도 기존 알림 ID를 받았고 공급자 호출은 늘지 않았다. 이번 검증은 Mock 프로세스가 살아 있는 동안의 중복 수락을 다룬다. Mock을 재시작하면 메모리에 보관한 키가 사라진다.

Gradle에서 Mock 서버, HTTP 공급자, Worker, Worker 동시성 관련 테스트 54개가 새로 실행돼 모두 통과했다. 로컬 테스트는 첫 응답 지연 중 동일 키 재요청, 본문 충돌, 저장소가 찬 상태에서의 기존 결과 반환도 포함했다. 첫 JAR 전송 중 핫스팟 IP가 바뀌어 끊겼다. 현재 IP 하나로 임시 규칙을 갱신하고 이전 JAR를 기준으로 변경분을 전송한 뒤 두 파일의 체크섬을 확인했다.

실험 앱·Mock과 Compose 서비스를 종료했고 MySQL·Kafka 데이터 볼륨을 보존했다. SSH는 기존 단일 `/32` 주소로 복원했고 임시 주소가 빠진 것을 확인했다. 실행 결과와 체크섬은 `build/reports/remote-smoke/aws-mock-idempotency-20261118/remote-idempotency-rerun.log`, `verified-jars.sha256`, `ssh-rule-restored.json`에 있다.

### HTTP Mock 재시작 후 멱등성 기록 복구

2026-11-18에 HTTP Mock 전용 H2 파일 저장소를 도입했다. Mock은 알림 UUID 기본 키, 본문 SHA-256과 최초 공급자 ID를 저장하고 `CHECKPOINT SYNC`를 마친 뒤 요청을 수락한다. 같은 파일을 열면 이전 실행에서 저장한 키와 결과를 다시 사용할 수 있다. [Mock 실행 설정](S6_HTTP_MOCK.md) · [H2 동기화 명령](https://h2database.com/html/commands.html#checkpoint)

로컬에서 합성 요청 3건 중 1건이 Mock에서 수락되고 MySQL에 `STARTED`만 남았을 때 MySQL을 일시 중지했다. Worker가 완료 저장에 실패해 정지한 뒤, MySQL이 계속 응답하지 않는 동안 Mock을 `SIGKILL`로 종료하고 같은 파일을 사용해 재시작했다. 새 Mock 프로세스의 수락 카운터는 0이었고 `storedAcceptances`는 1이었다.

MySQL을 복구하고 선점 기한이 지난 뒤 앱을 재시작했다. HTTP 요청 4회에서 최초 수락 3회와 기존 수락 결과 재응답 1회를 확인했다. DB 완료 3건, 유실·중복 수락·Kafka 미처리·Retry·DLQ는 각각 0건이었다. 서버 재시작 뒤 동일한 공급자 ID를 반환하고 저장 용량을 복구하는 테스트를 포함해 관련 Gradle 테스트 56개를 통과했다. 원시 로그는 `build/reports/remote-smoke/aws-mock-persistent-20261118/local-persistent.log`에 있다.

#### 서울 EC2에서 Mock 재시작 검증

2026-11-18 서울 EC2 A에서 앱과 파일 저장소를 적용한 Mock JAR의 SHA-256을 확인했다. 기존 DB 완료 213건과 Kafka 요청 끝·커밋 `60·81·72`를 기준선으로 삼았다. Mock 성공 응답은 3.5초 지연했고, 합성 요청 3건 중 1건이 수락됐지만 DB에는 `STARTED`만 남은 순간을 포착했다.

| 구간 | 서울 EC2 관측 결과 |
| --- | --- |
| MySQL 응답 중단 | Mock 수락 1회가 기록된 상태에서 MySQL을 `pause`했다. 완료 저장 시도가 실패해 Worker가 멈췄다. Kafka 요청의 끝은 `60·83·73`, 커밋은 `60·81·72`였다. |
| MySQL 중단 중 Mock 재시작 | 기존 Mock을 `SIGKILL`로 종료했다. 새 PID에서 같은 로컬 H2 파일을 사용했다. 프로세스의 수락 카운터는 0이었고, 이전에 저장된 수락 1건을 다시 읽었다. MySQL은 이 시점에도 응답 중단 상태였다. |
| MySQL 복구와 선점 기한 만료 후 앱 재시작 | 알림 3건 모두 완료됐다. 새 Mock은 요청 3회에 최초 수락 2회와 기존 결과 재응답 1회를 기록했다. 이전 Mock까지 합친 총 요청은 4회, 최초 수락은 3회, 중복 수락은 0회였다. |

첫 알림의 DB 시도 이력은 `UNKNOWN → PROVIDER_ACCEPTED`였다. Kafka 끝과 커밋은 `60·83·73`으로 일치했고 유실·미처리·Retry·DLQ는 각각 0건이었다. 같은 접수 키 3개는 기존 ID 3개를 반환했고 Mock 호출은 늘지 않았다. 이 검증은 같은 H2 파일을 유지한 상태의 Mock 프로세스 재시작을 다룬다.

실험 앱·Mock·Compose 서비스를 종료하고 MySQL·Kafka 볼륨과 실험용 H2 파일을 보존했다. SSH 규칙을 원래의 단일 `/32`로 복원한 뒤 임시 핫스팟 주소가 빠졌는지 확인했다. 실행 증거는 `build/reports/remote-smoke/aws-mock-persistent-20261118/remote-persistent.log`, `verified-jars.sha256`, `ssh-rule-restored.json`에 있다.

#### EC2 중지·시작 후 EBS 수락 기록 검증

2026-11-19 서울 EC2 A에서 직전 Mock 재시작 실험과 같은 앱·Mock JAR의 SHA-256을 검증했다. 기존 DB 완료 216건과 Kafka 요청의 파티션 끝·커밋 `60·83·73`을 기준선으로 삼았다. Mock 성공 응답은 3.5초 지연했고, Worker HTTP 응답 제한은 5초였다.

| 구간 | 서울 EC2 관측 결과 |
| --- | --- |
| Mock 첫 수락 직후 MySQL 장애 | 합성 알림 3건 중 첫 알림이 Mock에 수락됐고 HTTP 응답은 진행 중이었다. MySQL에는 첫 알림이 `PROCESSING/STARTED`, 나머지 2건이 `QUEUED`로 남았다. 관측 시점에서 57ms 뒤 MySQL 컨테이너가 일시 중지된 상태를 확인했다. 완료 저장 시도 뒤 Worker가 정지했고, Kafka 끝은 `63·83·73`, 커밋은 `60·83·73`이었다. |
| EC2 중지·시작 | MySQL이 일시 중지됐고 Mock JVM은 살아 있는 상태에서 EC2 기본 중지를 요청했다. 콘솔에서 09:15:47 `중지됨`, 09:16:20 `실행 중`을 확인했다. 시각은 KST 기준이다. |
| 앱 기동 전 기록 대조 | Linux 부팅 ID가 바뀌었고, 수락 파일이 있는 루트 EBS 볼륨 ID와 파일시스템 UUID는 그대로였다. 기존 H2 파일을 읽기 전용으로 열어 첫 알림의 최초 수락 ID를 읽었다. MySQL의 `STARTED`·`QUEUED` 상태와 Kafka 끝·커밋은 중지 전과 같았다. 같은 파일로 시작한 Mock은 신규 수락 0회, 기존 저장 수락 1건을 보고했다. |
| Worker 재시작과 최종 대조 | 선점 기한 만료 후 Worker를 시작했다. 새 Mock은 HTTP 요청 3회에서 신규 수락 2회와 기존 수락 결과 재응답 1회를 기록했다. 첫 알림의 시도 이력은 `UNKNOWN → PROVIDER_ACCEPTED`였고, MySQL에 기록된 공급자 ID가 기존 EBS에서 읽은 최초 수락 ID와 일치했다. |

합성 알림 3건이 모두 완료됐다. 중지 전후를 합산한 HTTP 요청은 4회, 최초 수락은 3회, 기존 결과 재응답은 1회였다. 유실·중복 수락·Kafka 미처리·Retry·DLQ는 각각 0건이었고 최종 Kafka 끝·커밋은 `63·83·73`으로 일치했다. 같은 API 접수 키 3개도 기존 알림 ID를 반환했고 Mock 호출은 증가하지 않았다. 이 결과의 검증 범위는 동일 EBS를 유지한 EC2 기본 중지·시작이다. [EC2 중지·시작](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/Stop_Start.html) · [NVMe 장치의 EBS 볼륨 식별](https://docs.aws.amazon.com/ebs/latest/userguide/identify-nvme-ebs-device.html)

실험에 사용한 앱·Mock·Compose 서비스를 종료했고 MySQL·Kafka 볼륨과 H2 파일을 보존했다. EC2는 실행 중인 상태로 복구했다. SSH 인바운드는 원래의 단일 `/32`로 복원했고 임시 핫스팟 주소가 빠졌음을 콘솔에서 확인했다. 증거는 `build/reports/remote-smoke/aws-mock-ebs-20261119/`의 `before.log`, `after.log`, `ec2-stopped.json`, `ec2-started.json`, `ssh-rule-restored.json`에 있다.

## AWS가 필요한가

별도 PC/서버가 있으면 AWS 없이도 부하 발생기와 앱을 다른 머신에 배치할 수 있다.
별도 장비가 없다면 첫 선택은 **Mac 부하 발생기 + EC2 한 대의 앱/인프라**다.
AWS 서비스 수를 늘리는 대신 같은 코드를 원격 Linux에서 실행하고 네트워크 경계를 먼저 확인한다.

| 단계 | 구성 | 확인할 내용 | 보장 범위 밖 |
| --- | --- | --- | --- |
| A: 첫 원격 배포 | Mac → SSH 터널 → EC2 한 대의 앱·MySQL·Kafka·HTTP Mock | 원격 기동, 가상 요청의 접수/결과/offset, 네트워크 경유 실패 | 앱 서버 장애 시 서비스 지속, 운영 처리량 |
| B: 서버 안 역할 분리 | 같은 EC2의 API2/Worker2·Redis·HAProxy | Linux에서 기존 S9 구성을 재현하고 공유 quota·파티션 할당 대조 | 별도 앱 호스트 장애 복구 |
| C: 앱 호스트 분리 | 앱 복제본을 별도 EC2에 배치, 공유 인프라의 위치/생존 여부 명시 | 특정 앱 호스트 중단 후 다른 앱의 인수와 신규 처리 | 단일 DB/Kafka/Redis/LB의 고가용성, 멀티 AZ 전체 장애 복구 |

A는 부하 발생기와 앱이 다른 머신에 있는 실험이다. Worker가 여러 호스트에 분산된 실험은 아니다.
C에서 공유 인프라까지 함께 내려가는 호스트와 앱만 내려가는 호스트를 구분해야 한다.
앱 서버를 두 대로 늘려도 공유 인프라의 단일 장애점이 없어지지는 않는다.

## 첫 단계의 실행 형태

Java 21의 native JVM으로 앱과 HTTP Mock을 실행하고 `compose.yml`의 MySQL/Kafka를 같은 EC2에 둔다.
먼저 REALTIME 한 lane, Worker 기본1/활성1/대기2, HTTP Mock 경로의 작은 정상 실험을 준비한다.
앱에는 `--app.provider.mode=HTTP`를 명시해 별도 HTTP Mock을 호출하게 한다.
Redis quota와 여러 JVM은 B에서 기존 [초기화 계약](S6_REDIS_RECOVERY.md)과 [S9 검증](S9_LOCAL_TOPOLOGY.md)을 적용한다.
최초 실험에는 ALB/RDS/MSK/NAT Gateway를 추가하지 않는다.

| 현재 파일 | 원격 실행에 미치는 영향 |
| --- | --- |
| `src/main/resources/application.yml` | 앱은 `127.0.0.1:8080`. native JVM은 같은 호스트의 DB/Kafka에 접속 가능 |
| `src/mockServer/resources/mock-server.yml` | HTTP Mock은 `127.0.0.1:18081`. 별도 Mock JAR 필요 |
| `compose.yml` | DB/Kafka/선택 Redis의 호스트 바인딩 주소가 loopback. 첫 EC2 실행에서 DB와 Kafka의 기동과 접속을 확인 |
| `compose.topology.yml`, `deploy/local/haproxy.cfg` | Docker Desktop용 `host.docker.internal`을 사용. Linux의 native loopback API 접근을 검증한 배포 구성은 아직 없음 |

따라서 A에서는 앱에 직접 터널을 연결한다. B의 Linux용 LB 구성은 별도 구현·검증 대상으로 남긴다.
컨테이너 안에 앱을 넣으면 `127.0.0.1`과 Kafka advertised listener가 가리키는 대상이 달라지므로
이 native JVM 계획을 컨테이너 앱 배포에 그대로 적용하지 않는다.

EC2의 보안 그룹은 SSH22를 사용자의 공인 IP `/32`에 허용하는 형태로 계획한다.
앱/DB/Kafka/Redis/Mock 포트를 인터넷에 추가 공개하지 않고 현재 loopback 구성을 유지한다.
[AWS의 접속 규칙](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/security-group-rules-reference.html)을 근거로 한다.
이 프로젝트는 아직 공개 API용 인증·TLS를 구현하지 않았다.

Mac의 접속 예시는 다음과 같다. 주소와 로그인 계정은 실제 EC2 정보로 바꾸고 키는 로컬 SSH 설정에서 관리한다.
예시 주소는 예약된 `.invalid` 도메인이며 실제 접속 대상이 아니다.

```bash
ssh -NT \
  -L 127.0.0.1:18000:127.0.0.1:8080 \
  -L 127.0.0.1:18001:127.0.0.1:18081 \
  -o ExitOnForwardFailure=yes \
  -o ConnectTimeout=10 \
  -o ServerAliveInterval=30 \
  -o ServerAliveCountMax=3 \
  lab-user@ec2.example.invalid
# 별도 터미널, 실제 서버 기동·터널 연결 후:
curl --connect-timeout 2 --max-time 5 -fsS http://127.0.0.1:18000/actuator/health
```

[OpenSSH의 `-L`/`-N` 설명](https://man.openbsd.org/ssh.1)에 따른 로컬 포워딩이다.
Mac의 18000과 18001은 API와 Mock의 터널 입구다. 로컬 S9 LB가 같은 포트를 사용 중이면 먼저 충돌을 해결한다.
터널을 통한 지연에는 인터넷·암호화·포워딩 비용이 포함된다. S8의 같은 PC 처리량과 직접 비교해 성능 효과를 확정하지 않는다.
`ExitOnForwardFailure`는 포워딩 설정 실패를 검사하며 원격 앱의 health 성공을 대신하지 않는다.

## A 단계 실행 전에 정할 입력

- AWS 계정, 리전, 실험에 사용할 총예산/시간과 정리 시점.
- EC2 OS·CPU 아키텍처·메모리·디스크. 기존 컨테이너/JVM의 메모리를 합산하고 실제 사용량을 측정한 뒤 규격을 정한다.
  작은 인스턴스나 무료 사용량으로 전체 구성이 충분하다고 가정하지 않는다.
- 로그인 계정·키를 둔 로컬 경로·허용할 Mac 공인 IP. 비밀키나 인증 토큰을 저장소/채팅에 복사하지 않는다.
- 새 실험용 DB/토픽/group/volume과 배포 SHA/JAR SHA. 현재 Mac의 데이터/예산을 덮어쓰지 않는다.

EC2 실행 시간 외에 EBS, 공인 IPv4, 데이터 전송을 비용 계산에 포함한다.
인스턴스를 정지해도 EBS 등의 비용이 남을 수 있다.
[EC2 상태별 과금](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-lifecycle.html)과
[VPC 공인 IPv4 요금](https://aws.amazon.com/vpc/pricing/)을 확인하고 실제 선택한 리전/규격으로 견적을 정한다.
실험 종료 시 필요한 원본을 먼저 보존하고 인스턴스·볼륨·스냅샷·IP의 보유/정리 결과를 확인한다.

## A 단계 실행 절차

Java 21과 Docker Compose가 설치된 Linux 실험 서버를 준비한다. Mac의 저장소에서 아래 명령으로 앱과 HTTP Mock JAR를 만든다.
배포 대상 소스의 Git SHA, 두 JAR의 SHA-256, 서버 규격과 Java/Docker/Compose 버전을 실험 기록에 남긴다.
`compose.yml`, `.env.example`과 아래 두 JAR를 SSH로 서버의 전용 디렉터리에 복사하고 서버에서 두 JAR의 SHA-256이 일치하는지 확인한다.

```bash
./gradlew --no-daemon bootJar mockServerJar
git rev-parse HEAD
shasum -a 256 build/libs/notification-lab-0.3.0-SNAPSHOT.jar \
  build/libs/notification-lab-0.3.0-SNAPSHOT-http-mock.jar
```

서버의 전용 디렉터리에서 `.env.example` 형식으로 `.env`를 준비한다. 이번 실험의 비밀번호에는 각각 다른 임의의 16진수 문자열을 사용하고 파일 권한은 600으로 둔다. 예를 들어 OpenSSL에서 `openssl rand -hex 24`로 생성할 수 있다.
DB와 Kafka는 새 볼륨이 생성되는 Compose 프로젝트에서 기동한다. 기존에 이 프로젝트 이름을 사용했다면 이번 실험에 새 이름을 지정하고 아래 명령 전체에서 동일한 이름을 사용한다.

```bash
docker compose -p notification-remote-smoke up -d --wait --wait-timeout 180 mysql kafka
```

같은 디렉터리의 서버 터미널 두 개에서 각각 실행한다.

```bash
java -Xmx512m -jar notification-lab-0.3.0-SNAPSHOT-http-mock.jar
```

```bash
set -a
. ./.env
set +a
java -Xmx512m -jar notification-lab-0.3.0-SNAPSHOT.jar --app.provider.mode=HTTP
```

Mac에서 위 SSH 명령으로 두 포트를 연결한 뒤 합성 알림을 접수하고 Mock 집계를 읽는다.
실험 중 이 앱과 Mock에 다른 요청을 보내지 않아야 집계가 이번 실행만 반영된다.
첫 실행은 20건으로 하고, 확인이 끝나면 100건을 별도로 실행한다. 요청은 순차 전송하며 요청당 소켓 제한은 5초다.
도구는 20건 이하에 60초, 21~100건에 120초의 실행 예산을 적용한다.

```bash
mkdir -p build/reports/remote-smoke
python3 scripts/remote_smoke.py --count 20 --report build/reports/remote-smoke/aws-20.json
# 20건 검증 및 아래 Kafka 대조 후
python3 scripts/remote_smoke.py --count 100 --report build/reports/remote-smoke/aws-100.json
```

[검증 도구](../scripts/remote_smoke.py)는 HTTP 202 접수, 같은 키의 HTTP 200 재응답, 다른 본문의 HTTP 409,
DB에 저장된 알림 결과와 단일 시도 이력, HTTP Mock 요청·수락 집계를 확인한다.
결과 파일에는 진행 단계, 확인된 건수, 소요 시간과 실패 이유가 남는다. 오류나 timeout 뒤에는 DB·Mock·Kafka 상태를 먼저 확인한다.
실패 때 알려지지 않은 접수 결과가 있을 수 있고, 스크립트를 새로 실행하면 새 멱등 키를 사용한다.

Kafka는 각 실행 전후 서버에서 아래 명령으로 별도 대조한다. 요청 토픽의 파티션별 끝 오프셋 증가량 합이 실행 건수와 같아야 한다.
메시지가 있는 모든 요청 파티션에서 소비 그룹의 커밋값이 끝 오프셋에 도달했는지 확인한다.
비어 있는 파티션은 끝 오프셋 0과 커밋 미생성(`-`)을 허용한다. 새 환경에서 Retry/DLQ의 끝 오프셋은 계속 0이어야 한다.
HTTP 확인 직후 커밋이 진행 중이면 잠시 뒤 다시 조회하고 결과를 함께 보존한다.

```bash
docker compose -p notification-remote-smoke exec -T kafka \
  /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9094 \
  --topic 'notification\.requested\.v1(\.retry|\.dlq)?'
docker compose -p notification-remote-smoke exec -T kafka \
  /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9094 \
  --describe --group notification-worker-v1
```

20건·100건 단계는 네트워크를 경유한 접수와 정합성 확인에 사용한다. 10만 건·10분 목표는 캠페인 발송 구간을 별도로 측정한다.
정상 경로 검증 뒤에 SSH 터널 중단, 앱 프로세스 중단, 호스트 중단을 독립된 실험으로 진행한다.
결과와 서버 로그를 보존한 후 앱과 Mock을 종료하고 전용 Compose 프로젝트를 `down`한다.

이전 로컬 설정 해석과 원격 CI 증거는
[검증 기록](experiments/s9-remote-ci-and-deployment-preparation-2026-11-06.json)에 별도로 남긴다.
현재 소스172개/구성23개는 기준과 같고 Compose2개는 서비스 생성 없이 설정을 해석했다.
이전 기록 시점에는 SSH 예시를 `ssh -G`와 `sh -n`으로 검사했다. 2026-11-17에는 위와 같이 실제 터널을 열어 검증했다.
`./gradlew --no-daemon --console=plain test`는7초/종료0이지만 test가 `UP-TO-DATE`여서 새 JUnit 실행은0개다.
이를 앞선 원격398개 통과에 더하지 않는다.
