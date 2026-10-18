package dev.jinhyeon.notificationlab.api;

import dev.jinhyeon.notificationlab.campaign.Campaign;
import dev.jinhyeon.notificationlab.campaign.CampaignRequest;
import dev.jinhyeon.notificationlab.campaign.CampaignService;
import dev.jinhyeon.notificationlab.notification.InvalidIdempotencyKeyException;
import jakarta.validation.Valid;
import java.net.URI;
import java.util.UUID;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Profile("!memory & !sync")
@ConditionalOnProperty(name = "app.traffic.lane", havingValue = "CAMPAIGN")
@RequestMapping("/campaigns")
public class CampaignController {
    private final CampaignService service;

    public CampaignController(CampaignService service) { this.service = service; }

    @PostMapping
    public ResponseEntity<Campaign> create(@Valid @RequestBody CampaignRequest request, @RequestHeader HttpHeaders headers) {
        var keys = headers.get("Idempotency-Key");
        if (keys == null || keys.size() != 1) throw new InvalidIdempotencyKeyException();
        var result = service.create(request, keys.getFirst());
        return ResponseEntity.status(result.replayed() ? 200 : 201)
                .location(location(result.campaign().campaignId())).body(result.campaign());
    }

    @PostMapping("/{id}/start")
    public ResponseEntity<Campaign> start(@PathVariable UUID id) {
        var result = service.start(id);
        return ResponseEntity.status(result.replayed() ? 200 : 202).location(location(id)).body(result.campaign());
    }

    @GetMapping("/{id}")
    public Campaign get(@PathVariable UUID id) { return service.get(id); }

    private URI location(UUID id) { return URI.create("/campaigns/" + id); }
}
