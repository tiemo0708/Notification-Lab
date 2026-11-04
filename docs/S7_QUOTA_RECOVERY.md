# S7 예산 조회 진단과 일시 오류의 유한 복구

2026-11-04 KST. 시작71688a8/main. [두 번째100만 건 실패 분석](S7_OBSERVED_MILLION_FAILURE.md)의
단일UNAVAILABLE가전체소비중단으로이어지는경로를보완했다. 당시실패의구체적인저수준원인을소급확정한것은아니다.

## 변경과 선택 범위

RedisRateLimiter.Decision에고정된FailureReason을추가했다. 기존2인자생성자는유지하며이전UNAVAILABLE는UNKNOWN으로다룬다.
프로세스내첫quota실패만원자적으로메모리에보존하고고정원인별counter를집계한다. 원문예외·cause·Redis키·수신자·본문·주소는수집하지않는다.
최초진단은UTC시각/단계/소요시간/실제로바인딩된timeout/연결상한/풀active·idle/점유slot만포함한다.
ACQUIRE/EVAL/RELEASE/SLOT단계를구분하고,정상허가counter는연결반환까지성공한뒤에만증가한다.

| 분류 | 복구 옵션ON의 처리 |
| --- | --- |
| TIMEOUT / CONNECTION / SLOT_BUSY | 같은미완료record를보관하고새허가획득을최대3번지연재시도 |
| STATE_ERROR / POOL_CLOSED / UNKNOWN | 기존대로중단·미완료offset유지 |
| LIMITED | 기존정상한도부족의pause/resume유지 |
| ALLOWED | DB상태재검증후발송시도시작 |

애플리케이션기본값은 `app.rate-limit.transient-recovery-enabled=false`다.
실험은 `-PtransientQuotaRecovery=ON`으로선택한다. timeout·pool크기·역할별예산·Worker/Relay수·RECORD는늘리거나바꾸지않는다.
복구는pause-enabled=true경로에서만적용한다. 이전bounded-wait비교모드와기본OFF의중단동작은유지한다.

각tryAcquire는한번의새획득이며내부EVAL재시도는없다. 응답이불확실한token은폐기하고환불하거나재사용하지않는다.
그뒤새획득에서ALLOWED가반환돼야공급자를호출한다. 불확실한요청이실제로token을소모했다면일부예산이낭비될수있다.
일시적장애중에provider를호출하거나quota를우회하지않는다.

## 재시도와 메모리 상한

같은topic/partition/offset에서실패하면250/500/1000ms로최대3번pause한다. 네번째실패는소비중단으로전파한다.
Spring3.3.16은오류처리기에공유부모컨테이너를전달하므로컨테이너하나에단일카운터를두지않는다.
컨테이너와topic/partition을묶고현재offset별횟수를유지한다. 다음offset은새예산이며,stop/close는자료를정리한다.
이 단계의 구현은 main/retry토픽각3개파티션의최대6개예산만보관했다. 초과하면중단하며동적파티션확장은포함하지않았다.
후속 [S8 복구 예산 검증](S8_PARTITION_RECOVERY.md)에서 기본 6을 유지한 명시적 최대 12 설정과 6+6/용량 초과 오류 경계를 추가했다.
기존pause예약은최대3개/스케줄러스레드1개를유지한다. Worker가대기하는동안Kafkapoll을유지하며실패record를성공커밋하지않는다.
명시적재시작/컨테이너생명주기변경은새예산이다. 이경계를넘는재시도까지전역적으로3회라고주장하지않는다.

[Spring3.3.16오류전달원본](https://github.com/spring-projects/spring-kafka/blob/v3.3.16/spring-kafka/src/main/java/org/springframework/kafka/listener/KafkaMessageListenerContainer.java),
[공식pause/resume](https://docs.spring.io/spring-kafka/reference/3.3/kafka/pause-resume.html),
[Jedis운영가이드](https://redis.io/docs/latest/develop/clients/jedis/produsage/)를확인했다.
컨테이너API로pause/resume하고스케줄러에서KafkaConsumer를직접호출하지않는다.

## 실제 제어 검증

초기집중검증은지속Redis정지제어의정리연결이시간초과돼46초에실패했다.
테스트용admin연결만15초대기로바꾸고실제Workerfixture의Redis100ms한도는유지했다. 서비스의기본250ms도유지한다.
후속Redis/Worker/진단41개검증은51초에, 부모공유에따른파티션분리를반영한Worker검증14개는55초에실패·오류·스킵0으로통과했다.

- 실제Redis응답지연을TIMEOUT/EVAL로기록하고SLOT_BUSY와구분했다. 복구와pool닫힘후에도최초진단이바뀌지않았다.
- 서로다른두record가각각예산을받아복구하고,허가전시도·HTTP·offset은증가하지않으며각각한번발송됐다.
- 지속정지는3번pause후중단하고,offset·시도·HTTP0을유지했다. 명시적재시작후한번발송했다.
- 잘못된예산상태는옵션ON에서도재시도없이중단했다.
- 실제3Consumer/3파티션동시실패도상한내중단했고,예산자료정리후재시작하여각각한번발송했다.

1천건리허설과전체회귀를확인한뒤사용자가승인한100만건재실험을시작한다.
재실험은새실험DB에서처음부터진행하며이전중간실패DB를이어쓰지않는다. 이전실패자료는보존한다.
성공/실패판정과실행시각은새실행의실제기록을따르며,Mock수락을단말도착으로표현하지않는다.

1천건REHEARSAL/복구ON은54초에통과했다. 캠페인1000+실시간116=1116건HTTP·DB·참조·offset을대조했고중복/잘못된참조0이었다.
양쪽Redis에서실효timeout250ms/연결상한3을확인했고이정상실험의조회실패는0이었다.
[고정소스](experiments/s7-quota-recovery-2026-11-04-source.json)와[검증메타데이터](experiments/s7-quota-recovery-2026-11-04-validation.json)를보존한다.

quota진단은정상결과의quotaDiagnostics와실패결과의application진단에포함된다. 기존첫실패파일은API고정창이며quota진단전용저널이아니다.
최종report쓰기실패나프로세스강제종료에서quota메모리진단까지디스크에남는다고보장하지않는다.

최종 `caffeinate -i ./gradlew --no-daemon --console=plain build`는16분34초/종료코드0이었다.
새44개 클래스 XML과 콘솔에서347개 통과/실패·오류·스킵0을 대조했다. 기존341개+새6개다.
기본 회귀를 실제로 재실행하고 실행JAR도 생성했다. 별도1천 건과 이전집중검증을 구분하며 원격CI 결과는 아니다.
고정소스160개의SHA가일치했다. 기본OFF기존중단과옵션ON복구를함께검증했고기존실패자료는변경하지않았다.
