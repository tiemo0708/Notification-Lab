# S7 100만 건 HTTP Mock 발송 검증 성공

2026-11-04 KST, 소스 e5e5b01. 사전 저장한 캠페인100만 건과 동시에 접수한 실시간54124건을 실제 MySQL·Kafka·Redis·HTTP Mock 경로로 처리했다.
총1054124건의 HTTP 수락과 DB/대상 매핑/offset 대조를 통과했다. 단말 도착이나 exactly-once 보장은 아니다.

| 결과 | 측정값 |
| --- | --- |
| 실행 | 11월4일02:06:26 ~ 06:25:37 KST |
| Gradle 전체 | 4시간19분16초 / 종료코드0 |
| 캠페인 사전 저장 | 2678.243초, 약44분38초 |
| 캠페인 발송 구간 | 12698.848초, 약3시간31분39초 |
| 캠페인 처리량 | 78.747건/초(사전 저장 제외) |
| Mock 캠페인 / 실시간 수락 | 1000000 / 54124 |
| 관찰 중복 / 잘못된 참조 | 0 / 0 |
| 혼합 실시간 API 접수 p95 / p99 / 최대 | 36.827 / 472.229 / 3439.614ms |
| 기준선 API 접수 p95 | 29.462ms(50개 표본) |

캠페인 대상100만 개 매핑, notification/delivery_task/outbox_event/delivery_attempt의 총1054124행과 단건 시도/수락을 검증했다.
캠페인 offset 합계1000000, 실시간54124였고 관찰한 모든 파티션에서committed=end였다.
재시도/DLQ를 통한 정상 처리 없이 Mock 요청/수락 모두1054124였다. 상세 검증은 실행한 harness의assertions와 원본을 따른다.

조건은 RECORD/앱 스케줄 Relay1/Worker3/역할별DB pool4/HTTP 연결3/Redis 연결3·timeout250ms,
캠페인 quota200/s·burst20/전체1000/s·burst100/발행 완료 저장1건이었다. JVM512MiB, MySQL2CPU·2GiB·buffer512MiB,
Kafka1CPU·1GiB, Redis256MiB의 격리된 Testcontainers를 사용했다. Java Amazon.com Inc.21.0.6/macOS15.7.7 aarch64다.
실패 관찰과일시 오류 복구 옵션은ON이었다. 양쪽Redis 조회실패는0회였으므로 새복구가 실제 발동해 성공했다고 해석하지 않는다.
복구는 별도Redis 지연·지속장애·상태손상·다중파티션 제어 테스트를 근거로 한다.

공유 호스트1회 측정이며 이전 실패 실험과의 엄밀한 성능 비교가 아니다. 최초5초 API timeout 원인은 미확정이다.
S7의100만 건발송 검증은완료했지만 이남은원인조사를완료로바꾸지않는다.

[작은 결과 요약](experiments/s7-delivery-million-2026-11-04-recovery-attempt-3-summary.json),
[전체 원본](experiments/s7-delivery-million-2026-11-04-recovery-attempt-3-result.json),
[종료·JUnit·SHA 대조](experiments/s7-delivery-million-2026-11-04-recovery-attempt-3-completion.json),
[시작 조건·소스160개SHA](experiments/s7-delivery-million-2026-11-04-recovery-attempt-3-start.json)를 보존한다.
기존 두FAIL 원본도 유지했다. 전체 회귀347개는 이소스의앞선수정검증이며,이번장시간task의JUnit1개PASS와구분한다.
결과정리단계에서는소스를바꾸지않아장시간실험이나전체회귀를다시실행하지않는다. 문서·원본SHA·설정·판정·링크를대조했다.
