# S7 100만 건 관찰 실험의 Worker 중단 분석

후속 [유한 복구와 진단 보완](S7_QUOTA_RECOVERY.md)을 구현·검증했다. 아래는 수정 전71688a8 실험과 분석 당시 기록이다.

2026-11-04 KST. 분석 대상은71688a8의2026-11-03 두 번째100만 건 관찰 실험이다.
첫 실패·최종 결과의 SHA와 실행 시작 때 고정한 소스160개를 대조했다. 코드 변경·장시간 재실행·커밋·푸시는 하지 않았다.

## 확인된 중단 경로

캠페인Worker → RedisRateLimiter.tryAcquire → UNAVAILABLE → WorkerQuota가unavailable 거절 →
NotificationWorker가런타임 오류를 일반 처리 실패로 전달 → WorkerPauseErrorHandler의 중단 처리 →
캠페인listenerRunning=false → 실험이Worker stopped: campaign을 감지하고FAIL 종료했다.

일반 예산 부족인LIMITED는 유한 지연 후pause/resume하는 별도 경로다. 이번에는unavailable1회로 분류됐다.
따라서190131건이 시스템의 용량 상한이라는 증거는 없다. 단일 불확실한 예산 조회에서 소비를 멈추는 현재 정책이 실험을 종료했다.
안전하게 허가를 확인하지 못한 상태에서 공급자를 호출하거나 실패 record를 성공 처리하지 않도록 설계한 동작이다.

| 기록 | 실제 값 |
| --- | --- |
| 실행 / 종료 | 11월3일21:46:01 ~ 23:13:45 KST(Gradle1시간27분50초) |
| 캠페인 사전 저장 | 2774.790초, 약46분15초 |
| 발송 구간의 실패 관찰 | 2448.709초, 약40분49초 |
| 관찰된 캠페인/실시간 Mock 수락 | 190131 / 10438 |
| Mock 요청/수락 | 200569 / 200569 |
| 캠페인 quotaUnavailable / quotaRejectedUnavailable | 1 / 1 |
| admissionFull | 0 |
| 첫 중단 분류 | INFRA_FAILURE |
| API 열린/완료 요청 | 10438 / 10438 |
| 최근32개 API 응답 / 최대클라이언트시간 | 모두202 / 483.995ms |
| 첫 실패 파일 | 66708bytes, 앱active/pool open에서 저장 |

이번 중단에는probeFailure가 없고 진행 중인API도 없었다. 이전 첫 실험의5초 접수 timeout과 같은 원인이 재현됐다고 표현할 수 없다.
위 건수는 관찰된 HTTP Mock 수락이며 단말 도착이 아니다. 최종 DB/offset 전수 대조는 중도 실패로 완료하지 못했다.
실험 전용 컨테이너는 종료 후 정리됐으며 남은 약81만 건을 현재 DB에서 이어 보내는 상태가 아니다.
기존 사용자 MySQL과volume은 유지됐다.

## 원인을 더 좁히지 못하는 이유

RedisRateLimiter는 슬롯 획득 거절, 연결 풀/연결 문제, Redis 요청timeout, Lua의예산 상태/정책 오류를 같은UNAVAILABLE로 합친다.
JedisException/IllegalStateException의 구체적인 이유와 단계·지연은 보존하지 않는다.
NotificationWorker도 원래 cause를 붙이지 않고 정제된 일반 오류를 던진다. 따라서 현재 스택만으로timeout을 확정할 수 없다.

역할별 Redis 연결3개/Worker3개는명시적실행설정이다. Redis timeout은코드/YAML기본250ms이며,
당시바인딩된실효값은report에없어환경변수등의override여부까지사후확정하지않는다. 코드상quota를 호출하는 경로는WorkerQuota이고,
pause 재개 스케줄러는container.resume만 호출한다. 별도4번째Redis 조회 스레드가 있다는 근거는 찾지 못했다.
이 구조에서는 단순히Worker3개라는 이유로연결3개가 부족하다고 단정하거나 pool을 늘릴 근거가 없다.

공유 호스트의 순간 지연이Redis 요청 한도(설정 기본250ms)를 넘은 상황은 확인할 후보지만 가설이다.
Redis가 죽었거나 토큰이 단순히 부족했다고 확정하지 않는다. 당시 Redis SLOWLOG/LATENCY/에러 분류는 보존되지 않았고
실험 컨테이너도 정리돼 사후 조회로 복원할 수 없다.

## 메모리와 DB 관찰 해석

마지막MySQL cgroup memory.current는2146406400bytes(약2047.0MiB), memory.events max는730이었다.
보존된MySQL표본 전체에서oom과oom_kill은0이었다. 마지막표본은실패 관찰 약1.72초 전이다.
직전에는Hikari active4/4와대기1개가 두표본에 나타났지만, 마지막표본은active2/4·대기0이었다.
이것은 자원 압박을 조사할 근거지만 Redis 실패와의 인과관계는 아니다. 조회 실패 시점의 풀 고갈이나 OOM도 확정하지 않는다.

Docker CLI 관찰은직전MySQL약954MiB/Redis약9.6MiB였다. Linux Docker CLI는cache를빼서표시하지만
memory.current는cgroup전체사용량이므로 직접 같은 수치로 비교하면 안 된다. 표본시각도 다르다.
[Docker 공식문서](https://docs.docker.com/reference/cli/docker/container/stats/)와
[cgroupv2 공식문서](https://docs.kernel.org/admin-guide/cgroup-v2.html)에따르면max이벤트는최대경계접근 횟수이며oom_kill과별개다.
따라서이값으로메모리누수·Redis OOM·MySQL 메모리증가가원인이라고결론내리지않는다. 순간GC/스케줄링지연도현재집계만으로배제하지못한다.

## 작은 검증과 다음 순서

기존테스트4개를새로실행해31초/종료코드0/실패·오류·스킵0을확인했다.
Redis 응답정지·연결거절·예산상태오류가같은UNAVAILABLE로반환되는것을확인했다.
Worker제어에서는Redis를1500ms정지시켜선점/시도/공급자호출/offset0을유지한채소비가중단되고,
정상상태복원후명시적재시작으로1회발송/offset1이되는것을검증했다.
Worker제어fixture의Redis timeout은100ms로실험의기본250ms와다르므로어제의원인을재현한것은아니다.
소스가바뀌지않았으므로전체341개회귀는이번에재실행하지않았고, 직전전체빌드이력과구분한다.

다음은원인을추측해timeout이나pool을먼저늘리는대신첫UNAVAILABLE에한정한유한진단을추가하는것이다.
슬롯/연결획득/명령실행/반환단계, 고정오류분류, 소요시간, pool사용량을본문·키·원문예외없이보관한다.
이후timeout·연결거절·pool고갈·예산상태오류제어군으로구분되는지검증한다.
네트워크의일시불확실성과정책/상태손상을구분한뒤중단/복구정책을판단해야한다.
현재실험성공을위해quota를우회하거나동일EVAL을무제한재시도하거나불확실한token을환불하지않는다.

[분석 근거·실행 명령](experiments/s7-observed-million-analysis-2026-11-04.json),
[최종 실험 결과](experiments/s7-delivery-million-2026-11-03-observed-attempt-2-result.json),
[첫 실패 기록](experiments/s7-delivery-million-2026-11-03-observed-attempt-2-first-failure.json)을 함께 참조한다.
