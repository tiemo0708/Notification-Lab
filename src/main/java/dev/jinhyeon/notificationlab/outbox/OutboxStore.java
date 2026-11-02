package dev.jinhyeon.notificationlab.outbox;

import dev.jinhyeon.notificationlab.config.TrafficProperties;
import java.util.Optional;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.annotation.Isolation;

@Repository
@Profile("!memory & !sync")
public class OutboxStore {
    private final JdbcTemplate jdbc;
    private final TrafficProperties traffic;

    public OutboxStore(JdbcTemplate jdbc, TrafficProperties traffic) {
        this.jdbc = jdbc;
        this.traffic = traffic;
    }

    public record Claim(String eventId, String notificationId, String payload, String token, int attempt, String eventType) { }

    @Transactional(isolation = Isolation.READ_COMMITTED)
    public Optional<Claim> claim(int maxAttempts) {
        // Poll without next-key gaps that can block another lane's status transition.
        // Keep the index scan inside this lane even with stale optimizer statistics.
        // At most two candidates are returned; locking reads may lock more index records.
        var candidates = new ArrayList<Candidate>();
        candidates.addAll(candidate("PENDING"));
        candidates.addAll(candidate("PROCESSING"));
        if (candidates.isEmpty()) return Optional.empty();
        Claim claim = candidates.stream().min(Comparator.comparing(Candidate::availableAt)
                .thenComparing(c -> c.claim().eventId())).orElseThrow().claim();
        if (claim.attempt() > maxAttempts) {
            jdbc.update("UPDATE outbox_event SET status = 'FAILED', lease_token = NULL WHERE event_id = ?",
                    claim.eventId());
            return Optional.empty();
        }
        jdbc.update("""
                UPDATE outbox_event SET status = 'PROCESSING', lease_token = ?,
                    attempt_count = attempt_count + 1, available_at = TIMESTAMPADD(SECOND, 30, UTC_TIMESTAMP(6))
                WHERE event_id = ?
                """, claim.token(), claim.eventId());
        return Optional.of(claim);
    }

    private record Candidate(Claim claim, LocalDateTime availableAt) { }

    private List<Candidate> candidate(String status) {
        return jdbc.query("""
                SELECT event_id, notification_id, payload, attempt_count, available_at, event_type
                FROM outbox_event FORCE INDEX (idx_outbox_traffic_status_available)
                WHERE traffic_class = ? AND status = ? AND available_at <= UTC_TIMESTAMP(6)
                ORDER BY available_at, event_id LIMIT 1 FOR UPDATE SKIP LOCKED
                """, (rs, n) -> new Candidate(new Claim(rs.getString(1), rs.getString(2), rs.getString(3),
                UUID.randomUUID().toString(), rs.getInt(4) + 1, rs.getString(6)), rs.getObject(5, LocalDateTime.class)), traffic.lane().name(), status);
    }

    @Transactional
    public boolean published(Claim claim) {
        return jdbc.update("""
                UPDATE outbox_event SET status = 'PUBLISHED', published_at = UTC_TIMESTAMP(6), lease_token = NULL
                WHERE event_id = ? AND status = 'PROCESSING' AND lease_token = ?
                """, claim.eventId(), claim.token()) == 1;
    }

    @Transactional
    public void failed(Claim claim, int maxAttempts) {
        int backoffSeconds = Math.min(60, 1 << Math.min(claim.attempt() - 1, 6));
        jdbc.update("""
                UPDATE outbox_event SET status = ?, lease_token = NULL,
                    available_at = TIMESTAMPADD(SECOND, ?, UTC_TIMESTAMP(6))
                WHERE event_id = ? AND status = 'PROCESSING' AND lease_token = ?
                """, claim.attempt() >= maxAttempts ? "FAILED" : "PENDING", backoffSeconds,
                claim.eventId(), claim.token());
    }
}
