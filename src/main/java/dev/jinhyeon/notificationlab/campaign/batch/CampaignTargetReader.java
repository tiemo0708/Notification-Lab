package dev.jinhyeon.notificationlab.campaign.batch;

import dev.jinhyeon.notificationlab.campaign.Campaign;
import dev.jinhyeon.notificationlab.campaign.CampaignAudiencePage.Target;
import dev.jinhyeon.notificationlab.campaign.SyntheticCampaignAudience;
import java.util.Iterator;
import java.util.List;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamReader;

public class CampaignTargetReader implements ItemStreamReader<Target> {
    public static final String CHECKPOINT = "campaign.lastOrdinal";
    private final Campaign campaign;
    private final int pageSize;
    private long lastOrdinal;
    private Iterator<Target> page = List.<Target>of().iterator();

    public CampaignTargetReader(Campaign campaign, int pageSize) {
        // Validate the fixed definition and memory bound before any job processing.
        SyntheticCampaignAudience.read(campaign, 0, pageSize);
        this.campaign = campaign;
        this.pageSize = pageSize;
    }

    @Override
    public void open(ExecutionContext context) {
        lastOrdinal = context.getLong(CHECKPOINT, 0);
        if (lastOrdinal < 0 || lastOrdinal > campaign.targetCount()) {
            throw new IllegalStateException("Invalid campaign checkpoint");
        }
        page = List.<Target>of().iterator();
    }

    @Override
    public Target read() {
        if (lastOrdinal == campaign.targetCount()) return null;
        if (!page.hasNext()) page = SyntheticCampaignAudience.read(campaign, lastOrdinal, pageSize).items().iterator();
        Target target = page.next();
        lastOrdinal = target.ordinal();
        return target;
    }

    @Override
    public void update(ExecutionContext context) { context.putLong(CHECKPOINT, lastOrdinal); }

    @Override
    public void close() { page = List.<Target>of().iterator(); }
}
