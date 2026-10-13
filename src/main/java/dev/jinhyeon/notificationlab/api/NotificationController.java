package dev.jinhyeon.notificationlab.api;

import dev.jinhyeon.notificationlab.notification.Notification;
import dev.jinhyeon.notificationlab.notification.NotificationRequest;
import dev.jinhyeon.notificationlab.notification.NotificationService;
import jakarta.validation.Valid;
import dev.jinhyeon.notificationlab.outbox.SubmissionService;
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

    private final ObjectProvider<SubmissionService> submissions;

    public NotificationController(NotificationService service, ObjectProvider<SubmissionService> submissions) {
        this.service = service;
        this.submissions = submissions;
    }

    @PostMapping
    public ResponseEntity<Notification> create(@Valid @RequestBody NotificationRequest request) {
        SubmissionService submission = submissions.getIfAvailable();
        if (submission != null) {
            Notification result = submission.accept(request);
            return ResponseEntity.accepted().location(URI.create("/notifications/" + result.notificationId()))
                    .body(result);
        }
        Notification result = service.send(request);
        // Synchronous baseline: 201 means a result resource exists, not that a device received it.
        return ResponseEntity.created(URI.create("/notifications/" + result.notificationId())).body(result);
    }

    @GetMapping("/{id}")
    public Notification get(@PathVariable UUID id) {
        return service.get(id);
    }
}
