package dev.jinhyeon.notificationlab.api;

import dev.jinhyeon.notificationlab.notification.Notification;
import dev.jinhyeon.notificationlab.notification.NotificationRequest;
import dev.jinhyeon.notificationlab.notification.NotificationService;
import jakarta.validation.Valid;
import dev.jinhyeon.notificationlab.worker.DeliveryTaskStore;
import java.util.Optional;
import dev.jinhyeon.notificationlab.outbox.IdempotentSubmissionService;
import dev.jinhyeon.notificationlab.notification.InvalidIdempotencyKeyException;
import org.springframework.http.HttpHeaders;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.beans.factory.ObjectProvider;
import java.net.URI;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/notifications")
public class NotificationController {
    private final NotificationService service;
    private final ObjectProvider<DeliveryTaskStore> tasks;

    private final ObjectProvider<IdempotentSubmissionService> submissions;

    public NotificationController(NotificationService service, ObjectProvider<IdempotentSubmissionService> submissions,
                                  ObjectProvider<DeliveryTaskStore> tasks) {
        this.service = service;
        this.tasks = tasks;
        this.submissions = submissions;
    }

    @PostMapping
    public ResponseEntity<Notification> create(@Valid @RequestBody NotificationRequest request,
                                                @RequestHeader HttpHeaders headers) {
        var keys = headers.get("Idempotency-Key");
        if (keys != null && keys.size() != 1) throw new InvalidIdempotencyKeyException();
        String key = keys == null ? null : keys.getFirst();
        IdempotentSubmissionService submission = submissions.getIfAvailable();
        if (submission != null) {
            var result = submission.submit(request, key);
            return ResponseEntity.status(result.replayed() ? 200 : 202)
                    .location(URI.create("/notifications/" + result.notification().notificationId()))
                    .body(result.notification());
        }
        if (key != null) throw new InvalidIdempotencyKeyException();
        Notification result = service.send(request);
        // Synchronous baseline: 201 means a result resource exists, not that a device received it.
        return ResponseEntity.created(URI.create("/notifications/" + result.notificationId())).body(result);
    }

    @GetMapping("/{id}/delivery")
    public ResponseEntity<DeliveryTaskStore.Delivery> delivery(@PathVariable UUID id) {
        var store = tasks.getIfAvailable();
        return ResponseEntity.of(store == null ? Optional.empty() : store.find(id));
    }

    @GetMapping("/{id}")
    public Notification get(@PathVariable UUID id) {
        return service.get(id);
    }
}
