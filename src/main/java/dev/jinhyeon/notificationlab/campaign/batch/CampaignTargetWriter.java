package dev.jinhyeon.notificationlab.campaign.batch;

import dev.jinhyeon.notificationlab.campaign.Campaign;
import dev.jinhyeon.notificationlab.campaign.CampaignAudiencePage.Target;
import java.nio.charset.StandardCharsets;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionSynchronizationManager;

public class CampaignTargetWriter implements ItemWriter<Target> {
    private final Campaign campaign;
    private final JdbcTemplate jdbc;
    private final int maxAttempts;

    public CampaignTargetWriter(Campaign campaign, JdbcTemplate jdbc, int maxAttempts) {
        this.campaign = campaign;
        this.jdbc = jdbc;
        this.maxAttempts = maxAttempts;
    }

    @Override
    public void write(Chunk<? extends Target> chunk) {
        if (!TransactionSynchronizationManager.isActualTransactionActive()) {
            throw new IllegalStateException("Campaign writer requires a chunk transaction");
        }
        try {
            for (Target target : chunk) persist(target);
        } catch (DataAccessException failure) {
            // Spring Batch logs/persists failures. Never attach SQL/bound payloads as a cause.
            throw new IllegalStateException("Campaign chunk storage failed: " + failure.getClass().getSimpleName());
        }
    }

    private void persist(Target target) {
        if (target.ordinal() < 1 || target.ordinal() > campaign.targetCount()
                || !target.recipientId().equals("synthetic-v1-" + target.ordinal())) {
            throw new IllegalStateException("Invalid campaign target");
        }
        var ids = CampaignTargetIds.of(campaign.campaignId(), target.ordinal());
        var existing = jdbc.query("""
                SELECT c.notification_id, c.task_id, c.event_id, n.recipient_id, n.title, n.body, n.traffic_class
                FROM campaign_target c JOIN notification n ON n.notification_id = c.notification_id
                WHERE c.campaign_id = ? AND c.target_ordinal = ?
                """, (rs, row) -> ids.notificationId().toString().equals(rs.getString(1))
                && ids.taskId().toString().equals(rs.getString(2)) && ids.eventId().toString().equals(rs.getString(3))
                && target.recipientId().equals(rs.getString(4)) && campaign.title().equals(rs.getString(5))
                && campaign.body().equals(rs.getString(6)) && "CAMPAIGN".equals(rs.getString(7)),
                campaign.campaignId().toString(), target.ordinal());
        if (!existing.isEmpty()) {
            if (!existing.getFirst()) throw new IllegalStateException("Campaign target identity conflict");
            return; // Do not reset completed notifications, attempts, or published outbox events on replay.
        }
        // All inserts and the Batch checkpoint share one DataSource/transaction manager.
        // Primary/unique keys arbitrate races; the preliminary read is only a replay optimization.
        jdbc.update("""
                INSERT INTO notification (notification_id, recipient_id, title, body, status, traffic_class, created_at)
                VALUES (?, ?, ?, ?, 'QUEUED', 'CAMPAIGN', UTC_TIMESTAMP(6))
                """, ids.notificationId().toString(), target.recipientId(), campaign.title(), campaign.body());
        jdbc.update("""
                INSERT INTO delivery_task (task_id, notification_id, target_id, max_attempts, created_at)
                VALUES (?, ?, ?, ?, UTC_TIMESTAMP(6))
                """, ids.taskId().toString(), ids.notificationId().toString(),
                target.recipientId().getBytes(StandardCharsets.UTF_8), maxAttempts);
        jdbc.update("""
                INSERT INTO outbox_event (event_id, notification_id, event_type, payload, traffic_class, created_at, available_at)
                VALUES (?, ?, 'NOTIFICATION_REQUESTED', JSON_OBJECT('schemaVersion', 2,
                    'eventId', ?, 'notificationId', ?, 'taskId', ?), 'CAMPAIGN', UTC_TIMESTAMP(6), UTC_TIMESTAMP(6))
                """, ids.eventId().toString(), ids.notificationId().toString(), ids.eventId().toString(),
                ids.notificationId().toString(), ids.taskId().toString());
        jdbc.update("""
                INSERT INTO campaign_target (campaign_id, target_ordinal, notification_id, task_id, event_id)
                VALUES (?, ?, ?, ?, ?)
                """, campaign.campaignId().toString(), target.ordinal(), ids.notificationId().toString(),
                ids.taskId().toString(), ids.eventId().toString());
    }
}
