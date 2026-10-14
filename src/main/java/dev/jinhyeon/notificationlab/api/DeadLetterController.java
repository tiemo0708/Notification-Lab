package dev.jinhyeon.notificationlab.api;

import dev.jinhyeon.notificationlab.deadletter.DeadLetterService;
import dev.jinhyeon.notificationlab.deadletter.RedriveRequest;
import jakarta.validation.Valid;
import java.net.URI;
import java.util.UUID;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Profile("!memory & !sync")
@RequestMapping("/dead-letters")
public class DeadLetterController {
    private final DeadLetterService service;

    public DeadLetterController(DeadLetterService service) {
        this.service = service;
    }

    @GetMapping("/{eventId}")
    public DeadLetterService.DeadLetter get(@PathVariable UUID eventId) {
        return service.find(eventId);
    }

    @PostMapping("/{eventId}/redrive")
    public ResponseEntity<DeadLetterService.Redrive> redrive(@PathVariable UUID eventId,
                                                          @Valid @RequestBody RedriveRequest request) {
        var result = service.redrive(eventId, request);
        return ResponseEntity.status(result.replayed() ? 200 : 202)
                .location(URI.create("/notifications/" + result.redrive().targetNotificationId()))
                .body(result.redrive());
    }
}
