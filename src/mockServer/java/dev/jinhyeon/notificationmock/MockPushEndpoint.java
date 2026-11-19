package dev.jinhyeon.notificationmock;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Validator;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class MockPushEndpoint {
    private static final int MAX_BODY_BYTES = 8192;
    private final HttpMockProperties settings;
    private final ObjectMapper mapper;
    private final Validator validator;
    private final MockAcceptanceStore acceptances;
    private final FixedWindowQuota quota;
    private final Semaphore slots;
    private final LongAdder requests = new LongAdder();
    private final LongAdder accepted = new LongAdder();
    private final LongAdder rateLimited = new LongAdder();
    private final LongAdder serverErrors = new LongAdder();
    private final LongAdder permanentFailures = new LongAdder();
    private final LongAdder busy = new LongAdder();
    private final AtomicInteger active = new AtomicInteger();
    private final AtomicInteger maxActive = new AtomicInteger();
    private final LongAdder idempotencyReplays = new LongAdder();
    private boolean storageFailed;
    private record Reply(ResponseEntity<?> response, boolean acquiredSlot) { }

    public MockPushEndpoint(HttpMockProperties settings, ObjectMapper mapper, Validator validator, MockAcceptanceStore acceptances) {
        this.settings = settings;
        this.mapper = mapper;
        this.validator = validator;
        this.acceptances = acceptances;
        this.quota = new FixedWindowQuota(settings.requestsPerWindow(), settings.windowMs(), System::nanoTime);
        this.slots = new Semaphore(settings.maxInFlight());
    }

    @PostMapping(path = "/pushes", consumes = "application/json", produces = "application/json")
    public ResponseEntity<?> push(HttpServletRequest request) throws IOException {
        if (request.getContentLengthLong() > MAX_BODY_BYTES) return error(413, "MOCK_PAYLOAD_TOO_LARGE");
        byte[] bytes = request.getInputStream().readNBytes(MAX_BODY_BYTES + 1);
        if (bytes.length > MAX_BODY_BYTES) return error(413, "MOCK_PAYLOAD_TOO_LARGE");
        Payload payload;
        try {
            payload = mapper.readerFor(Payload.class).with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS).readValue(bytes);
        } catch (IOException invalid) {
            return error(400, "MOCK_INVALID_REQUEST");
        }
        if (payload == null || !validator.validate(payload).isEmpty()) return error(400, "MOCK_INVALID_REQUEST");
        var keys = request.getHeaders("Idempotency-Key");
        UUID key = null;
        byte[] fingerprint = null;
        if (keys.hasMoreElements()) {
            if (!payload.notificationId().toString().equals(keys.nextElement()) || keys.hasMoreElements()) {
                return error(400, "MOCK_INVALID_IDEMPOTENCY_KEY");
            }
            key = payload.notificationId();
            fingerprint = fingerprint(payload);
        }
        requests.increment();
        Reply reply = begin(key, fingerprint);
        if (!reply.acquiredSlot()) return reply.response();
        try {
            Thread.sleep(settings.responseDelayMs());
            return reply.response();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return error(503, "MOCK_RESPONSE_INTERRUPTED");
        } finally {
            active.decrementAndGet();
            slots.release();
        }
    }

    private synchronized Reply begin(UUID key, byte[] fingerprint) {
        if (key != null) {
            if (storageFailed) return storageUnavailable();
            MockAcceptanceStore.Acceptance first;
            try {
                first = acceptances.find(key);
            } catch (SQLException failure) {
                return storageUnavailable();
            }
            if (first != null) {
                if (!MessageDigest.isEqual(first.fingerprint(), fingerprint)) {
                    return new Reply(error(409, "MOCK_IDEMPOTENCY_CONFLICT"), false);
                }
                idempotencyReplays.increment();
                return new Reply(accepted(first.providerId()), false);
            }
            if (settings.mode() == HttpMockProperties.Mode.SUCCESS && acceptances.count() >= settings.idempotencyCapacity()) {
                return new Reply(error(503, "MOCK_IDEMPOTENCY_CAPACITY"), false);
            }
        }
        long retryAfter = quota.acquire();
        if (retryAfter > 0) {
            rateLimited.increment();
            return new Reply(ResponseEntity.status(429).header("Retry-After", Long.toString(retryAfter))
                    .body(Map.of("errorCode", "MOCK_RATE_LIMITED")), false);
        }
        if (!slots.tryAcquire()) {
            busy.increment();
            return new Reply(error(503, "MOCK_BUSY"), false);
        }
        ResponseEntity<?> response;
        try {
            response = switch (settings.mode()) {
                case SUCCESS -> {
                    UUID providerId = UUID.randomUUID();
                    if (key != null) acceptances.save(key, fingerprint, providerId);
                    accepted.increment();
                    yield accepted(providerId);
                }
                case TOO_MANY_REQUESTS -> {
                    rateLimited.increment();
                    yield ResponseEntity.status(429).header("Retry-After", "1").body(Map.of("errorCode", "MOCK_RATE_LIMITED"));
                }
                case SERVER_ERROR -> {
                    serverErrors.increment();
                    yield error(500, "MOCK_UNAVAILABLE");
                }
                case INVALID_RECIPIENT -> {
                    permanentFailures.increment();
                    yield error(400, "MOCK_INVALID_RECIPIENT");
                }
            };
        } catch (SQLException failure) {
            slots.release();
            return storageUnavailable();
        }
        maxActive.accumulateAndGet(active.incrementAndGet(), Math::max);
        return new Reply(response, true);
    }

    private Reply storageUnavailable() {
        // A failed commit or sync can leave the receipt's durability uncertain until the store is reopened.
        storageFailed = true;
        return new Reply(error(503, "MOCK_IDEMPOTENCY_STORAGE_UNAVAILABLE"), false);
    }

    private byte[] fingerprint(Payload payload) throws IOException {
        try {
            // Hash the parsed fields so formatting does not affect equality; retain no message body.
            return MessageDigest.getInstance("SHA-256").digest(mapper.writeValueAsBytes(payload));
        } catch (NoSuchAlgorithmException missing) {
            throw new IllegalStateException("SHA-256 is unavailable", missing);
        }
    }

    private ResponseEntity<?> accepted(UUID providerId) {
        return ResponseEntity.accepted().body(Map.of("providerMessageId", "http-mock-" + providerId));
    }

    @GetMapping("/mock/stats")
    public Stats stats() {
        return new Stats(requests.sum(), accepted.sum(), rateLimited.sum(), serverErrors.sum(),
                permanentFailures.sum(), busy.sum(), active.get(), maxActive.get(), idempotencyReplays.sum(), acceptances.count());
    }

    @GetMapping("/mock/config")
    public HttpMockProperties config() { return settings; }

    private ResponseEntity<?> error(int status, String code) {
        return ResponseEntity.status(status).body(Map.of("errorCode", code));
    }

    public record Payload(@NotNull UUID notificationId, @NotBlank @Size(max = 100) String recipientId,
                          @NotBlank @Size(max = 100) String title, @NotBlank @Size(max = 500) String body) { }
    public record Stats(long requests, long accepted, long rateLimited, long serverErrors,
                        long permanentFailures, long busy, int active, int maxActive, long idempotencyReplays,
                        int storedAcceptances) { }
}
