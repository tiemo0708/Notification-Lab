# S9 후속: 별도 머신과 AWS 실험 준비

상태: **준비 계획과 로컬 설정 해석 검증**. EC2 생성·접속·배포·별도 머신 실험은 아직 하지 않았다.
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
  -o ExitOnForwardFailure=yes \
  -o ConnectTimeout=10 \
  -o ServerAliveInterval=30 \
  -o ServerAliveCountMax=3 \
  lab-user@ec2.example.invalid
# 별도 터미널, 실제 서버 기동·터널 연결 후:
curl --connect-timeout 2 --max-time 5 -fsS http://127.0.0.1:18000/actuator/health
```

[OpenSSH의 `-L`/`-N` 설명](https://man.openbsd.org/ssh.1)에 따른 로컬 포워딩이다.
Mac의18000은 이 단계에서 터널 입구이며 HAProxy가 아니다. 로컬 S9 LB가 같은 포트를 사용 중이면 먼저 충돌을 해결한다.
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

## A 단계의 작은 완료 조건

1. 서버의 Java/Docker/Compose, 배포 SHA/JAR SHA와 기동 인수를 기록한다. `.env`는 서버에서 별도로 준비한다.
2. 새 실험 데이터로 DB/Kafka와 HTTP Mock, 앱을 기동하고 터널을 통해 health를 확인한다.
3. 가상 알림20개 이내, 동시 요청2개 이내, 요청별5초·전체60초의 제한된 smoke를 설계한다.
   같은 key 재접수와 다른 본문 충돌을 포함하고 결과/HTTP 호출/DB 작업·시도 이력/개별 offset을 대조한다.
4. 한 요청이라도 timeout/실패하면 입력 건수와 DB 가시성·HTTP 집계·offset·서버 지표를 보존한다.
   timeout을 접수 실패 확정으로 해석하거나 새 key로 무조건 재전송하지 않는다.
5. 작은 정상 경로 대조 뒤에만 네트워크 중단 실험을 별도로 실행한다.
   SSH 터널 종료, 앱 프로세스 종료, EC2 호스트 중단을 서로 다른 실험으로 구분한다.

위 smoke는 **완료 조건 설계**다. 외부 부하 발생기·정합성 수집 도구와 원격 실행은 아직 준비/실행하지 않았다.
기존 장시간 JUnit 실험은 앱·인프라를 같은 테스트 호스트에서 시작하므로 URL만 바꾸면 원격 실험이 된다고 가정하지 않는다.
100만 건을 다시 실행하거나 AWS를 생성한 결과가 아니다. Mock 수락은 단말 도착이나 외부 exactly-once가 아니다.

이번 로컬 설정 해석/기본 테스트 상태와 원격 CI 증거는
[검증 기록](experiments/s9-remote-ci-and-deployment-preparation-2026-11-06.json)에 별도로 남긴다.
현재 소스172개/구성23개는 기준과 같고 Compose2개는 서비스 생성 없이 설정을 해석했다.
SSH 예시는 `ssh -G`의 옵션 해석과 `sh -n`의 문법만 검사했다. 실제 접속/터널 개통은 하지 않았다.
`./gradlew --no-daemon --console=plain test`는7초/종료0이지만 test가 `UP-TO-DATE`여서 새 JUnit 실행은0개다.
이를 앞선 원격398개 통과에 더하지 않는다.
