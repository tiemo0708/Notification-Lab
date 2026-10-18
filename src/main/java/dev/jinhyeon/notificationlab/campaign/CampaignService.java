package dev.jinhyeon.notificationlab.campaign;

import dev.jinhyeon.notificationlab.notification.IdempotencyConflictException;
import dev.jinhyeon.notificationlab.notification.InvalidIdempotencyKeyException;
import java.util.UUID;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Service
@Profile("!memory & !sync")
@ConditionalOnProperty(name = "app.traffic.lane", havingValue = "CAMPAIGN")
public class CampaignService {
    private final CampaignStore store;

    public CampaignService(CampaignStore store) { this.store = store; }

    public record Creation(Campaign campaign, boolean replayed) { }

    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    public Creation create(CampaignRequest request, String key) {
        if (key == null || !key.matches("[A-Za-z0-9._:-]{1,128}")) throw new InvalidIdempotencyKeyException();
        try {
            return new Creation(store.create(request, key), false);
        } catch (DuplicateKeyException duplicate) {
            Campaign existing = store.findByKey(key).orElseThrow(() -> duplicate);
            if (!existing.name().equals(request.name()) || !existing.title().equals(request.title())
                    || !existing.body().equals(request.body()) || existing.targetCount() != request.targetCount()) {
                throw new IdempotencyConflictException();
            }
            return new Creation(existing, true);
        }
    }

    public Campaign get(UUID id) { return store.find(id).orElseThrow(CampaignNotFoundException::new); }

    public CampaignStore.Start start(UUID id) { return store.start(id); }
}
