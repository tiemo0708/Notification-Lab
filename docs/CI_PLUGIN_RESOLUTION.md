# Gradle 플러그인 의존성 저장소 조회 보완

2026-11-04. 사용자 승인 후 main의 로컬 4개 커밋을 정상 fast-forward 푸시했다.
원격 HEAD는 `790a02b772272cbd0bdbd3b6a1e8b1347d5ac887`이며 force push/병합/이력 재작성은 없었다.

## 이전 테스트 실패와 이번 실패

이전 c6cccc1의 [CI](https://github.com/tiemo0708/Notification-Lab/actions/runs/41757502204)는
349개 중 `WorkerConcurrencyTest` RECORD 조건 1개가 시간 창 관찰에서 실패했다.
[fixture 보완](CI_WORKER_CONCURRENCY.md)을 9a4ceec에 커밋했고, 790a02b 푸시에 포함했다.
푸시 전 마지막 로컬 전체 회귀는 46개 클래스/363개 통과였다.

새 790a02b의 [CI](https://github.com/tiemo0708/Notification-Lab/actions/runs/41780973224)는
**build 12초, 실행 테스트 0개**로 root project의 classpath 해석에서 실패했다.
이 실행으로 Worker 테스트의 수정 후 원격 성공/재실패를 판단할 수 없다.
실패 로그는 Plugin Portal 경로에서 아래 POM을 찾지 못했다고 기록한다.

- `org.springframework.boot:spring-boot-buildpack-platform:3.5.16`
- `org.springframework.boot:spring-boot-loader-tools:3.5.16`
- `org.apache.commons:commons-compress:1.27.1`
- `org.springframework:spring-core:6.2.19`

이후 로컬에서 동일 POM의 HEAD 요청은 Central 직접 주소와 Portal 주소 모두 200이었고,
Portal의 최종 주소는 Maven Central이었다. 원격 실패 때의 HTTP 상태/redirect 상세가 없어
일시적인 upstream/redirect 문제인지 정확한 원인은 확정하지 않는다. 존재하지 않는 버전이라고 결론 내리지 않는다.

## 변경과 검증

`settings.gradle` 첫 블록에 `pluginManagement.repositories`를 선언했다.
Maven Central을 먼저 조회하고 Plugin Portal을 다음으로 유지한다.
프로젝트 라이브러리용 `build.gradle.repositories`와 플러그인 해석 저장소는 별개다.
버전/의존성 목록/운영 Java·resources/CI 테스트 명령은 변경하지 않는다.

[Gradle 플러그인 저장소 안내](https://docs.gradle.org/current/userguide/plugins_intermediate.html#sec:custom_plugin_repositories)와
[Plugin Portal의 Maven Central 의존성 안내](https://plugins.gradle.org/docs/mirroring)를 확인했다.
사설 mirror나 `mavenLocal`, insecure protocol을 추가하지 않는다.

기존 아티팩트 캐시를 사용하지 않는 별도 Gradle user home에서 실행했다.

```bash
./gradlew --gradle-user-home /private/tmp/notification-gradle-cold-20261104 --no-daemon --console=plain --info buildEnvironment
./gradlew --gradle-user-home /private/tmp/notification-gradle-cold-20261104 --no-daemon --console=plain compileTestJava bootJar --rerun-tasks
```

첫 명령은 **14초/종료코드 0**이고, 문제의 네 모듈을 Central에서 직접 다운로드한 로그를 확인했다.
같은 새 환경에서 두 번째 명령은 **28초/종료코드 0**, 7개 task를 모두 실행했다.
이 검증은 producer 사전 실험 코드를 추가하기 전의 컴파일/JAR 생성이며, 기본 회귀 테스트 실행을 대신하지 않는다.
다시 콜드 검증할 때는 새 빈 user home 경로를 선택해야 한다. 위 경로를 재사용한 실행을 콜드 실행으로 세지 않는다.
[실패 단계·정확한 좌표·다운로드 URL·로그 SHA](experiments/ci-plugin-resolution-2026-11-04.json)를 보존한다.

producer 실험을 마친 뒤 현재 작업 트리의 `caffeinate -i ./gradlew --no-daemon --console=plain build`도 별도로 실행했다.
**19분59초/종료코드0**, 기본 test 새 실행 **46개 클래스/363개 통과, 실패·오류·스킵0**이었다.
콘솔/새 XML 총수를 대조했고 WorkerConcurrencyTest 11개도 모두 통과했다. 이 build의 bootJar는 UP-TO-DATE다.
새 producer opt-in은 기본 build에서 제외됐으며, 운영 Java/resources는 790a02b 대비 변경되지 않았다.
위 JSON에 전체 회귀 결과와 로그 SHA도 추가했다. 저장소 설정 보완 후 원격 CI는 아직 실행하지 않았다.
