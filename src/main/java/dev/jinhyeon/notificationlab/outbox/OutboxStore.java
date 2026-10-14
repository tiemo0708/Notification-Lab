package dev.jinhyeon.notificationlab.outbox;

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

@Repository
@Profile("!memory & !sync")
public class OutboxStore {
    private final JdbcTemplate jdbc;

    public OutboxStore(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public record Claim(String eventId, String notificationId, String payload, String token, int attempt, String eventType) { }

    @Transactional
    public Optional<Claim> claim(int maxAttempts) {
        // Equality on the leading status column avoids sorting/scanning the whole backlog.
        // Lock at most two candidates in a consistent order, then pick the oldest due event.
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
                SELECT event_id, notification_id, payload, attempt_count, available_at, event_type FROM outbox_event
                WHERE status = ? AND available_at <= UTC_TIMESTAMP(6)
                ORDER BY available_at, event_id LIMIT 1 FOR UPDATE SKIP LOCKED
                """, (rs, n) -> new Candidate(new Claim(rs.getString(1), rs.getString(2), rs.getString(3),
                UUID.randomUUID().toString(), rs.getInt(4) + 1, rs.getString(6)), rs.getObject(5, LocalDateTime.class)), status);
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
