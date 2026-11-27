package dev.jinhyeon.notificationlab.campaign;

import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Repository
@Profile("!memory & !sync")
@ConditionalOnProperty(name = "app.traffic.lane", havingValue = "CAMPAIGN")
public class CampaignStore {
    private static final RowMapper<Campaign> MAPPER = (rs, row) ->
            new Campaign(UUID.fromString(rs.getString("campaign_id")), rs.getString("name"),
                rs.getString("title"), rs.getString("body"), rs.getString("audience"), rs.getInt("target_count"),
                Campaign.Status.valueOf(rs.getString("status")),
                instant(rs, "created_at"), instant(rs, "start_requested_at"),
                instant(rs, "scheduled_at"), instant(rs, "prepared_at"));

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        var value = rs.getObject(column, LocalDateTime.class);
        return value == null ? null : value.toInstant(ZoneOffset.UTC);
    }

    private final JdbcTemplate jdbc;

    public CampaignStore(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    @Transactional
    public Campaign create(CampaignRequest request, String key) {
        UUID id = UUID.randomUUID();
        jdbc.update("""
                INSERT INTO campaign (campaign_id, idempotency_key, name, title, body, audience,
                    target_count, status, created_at, scheduled_at)
                VALUES (?, ?, ?, ?, ?, 'SYNTHETIC_V1', ?, 'DRAFT', UTC_TIMESTAMP(6), ?)
                """, id.toString(), key.getBytes(StandardCharsets.UTF_8), request.name(), request.title(),
                request.body(), request.targetCount(), request.scheduledAt() == null ? null
                        : LocalDateTime.ofInstant(request.scheduledAt(), ZoneOffset.UTC));
        var created = find(id).orElseThrow();
        // Check only a new definition. An idempotent replay is valid after its deadline.
        if (created.scheduledAt() != null && !created.scheduledAt().isAfter(created.createdAt())) {
            throw new InvalidCampaignScheduleException();
        }
        return created;
    }

    public Optional<Campaign> find(UUID id) {
        return jdbc.query("SELECT * FROM campaign WHERE campaign_id = ?", MAPPER, id.toString()).stream().findFirst();
    }

    public Optional<Campaign> findByKey(String key) {
        return jdbc.query("SELECT * FROM campaign WHERE idempotency_key = ?", MAPPER,
                key.getBytes(StandardCharsets.UTF_8)).stream().findFirst();
    }

    public record Start(Campaign campaign, boolean replayed) { }

    // The durable start request IS this state transition. No in-memory job or external publish follows it here.
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public Start start(UUID id) {
        int changed = jdbc.update("""
                UPDATE campaign SET status = IF(scheduled_at IS NULL, 'START_REQUESTED', 'PREPARATION_REQUESTED'),
                    start_requested_at = UTC_TIMESTAMP(6)
                WHERE campaign_id = ? AND status = 'DRAFT'
                """, id.toString());
        // A current locking read also observes the winner after waiting on a concurrent UPDATE.
        Campaign campaign = jdbc.query("SELECT * FROM campaign WHERE campaign_id = ? FOR UPDATE", MAPPER,
                id.toString()).stream().findFirst().orElseThrow(CampaignNotFoundException::new);
        return new Start(campaign, changed == 0);
    }
}
