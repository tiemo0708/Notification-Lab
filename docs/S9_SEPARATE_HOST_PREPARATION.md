# S9 후속: 별도 머신과 AWS 실험 준비

상태는 **원격 실행 절차와 합성 알림 검증 도구 준비**다. EC2 실행과 별도 머신 실험은 대기 중이다.
기준 코드는 `b8f5043`이며 [원격 CI](https://github.com/tiemo0708/Notification-Lab/actions/runs/42173414421)의
기본 회귀398개 통과와 실제 AWS 실행을 구분한다.

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
| `compose.yml` | DB/Kafka/선택 Redis의 호스트 공개 주소가 loopback. EC2에서 그대로 해석할 수 있으나 실제 기동은 미검증 |
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
SSH 예시는 `ssh -G`의 옵션 해석과 `sh -n`의 문법만 검사했다. 실제 접속/터널 개통은 하지 않았다.
`./gradlew --no-daemon --console=plain test`는7초/종료0이지만 test가 `UP-TO-DATE`여서 새 JUnit 실행은0개다.
이를 앞선 원격398개 통과에 더하지 않는다.
