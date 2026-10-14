package dev.jinhyeon.notificationmock;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Validator;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import java.io.IOException;
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

    public MockPushEndpoint(HttpMockProperties settings, ObjectMapper mapper, Validator validator) {
        this.settings = settings;
        this.mapper = mapper;
        this.validator = validator;
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
        requests.increment();
        long retryAfter = quota.acquire();
        if (retryAfter > 0) {
            rateLimited.increment();
            return ResponseEntity.status(429).header("Retry-After", Long.toString(retryAfter))
                    .body(Map.of("errorCode", "MOCK_RATE_LIMITED"));
        }
        if (!slots.tryAcquire()) {
            busy.increment();
            return error(503, "MOCK_BUSY");
        }
        maxActive.accumulateAndGet(active.incrementAndGet(), Math::max);
        try {
            ResponseEntity<?> result = switch (settings.mode()) {
                case SUCCESS -> {
                    // Record acceptance BEFORE delaying the response; client timeout does not undo it.
                    accepted.increment();
                    yield ResponseEntity.accepted().body(Map.of("providerMessageId", "http-mock-" + UUID.randomUUID()));
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
            try {
                Thread.sleep(settings.responseDelayMs());
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return error(503, "MOCK_RESPONSE_INTERRUPTED");
            }
            return result;
        } finally {
            active.decrementAndGet();
            slots.release();
        }
    }

    @GetMapping("/mock/stats")
    public Stats stats() {
        return new Stats(requests.sum(), accepted.sum(), rateLimited.sum(), serverErrors.sum(),
                permanentFailures.sum(), busy.sum(), active.get(), maxActive.get());
    }

    @GetMapping("/mock/config")
    public HttpMockProperties config() { return settings; }

    private ResponseEntity<?> error(int status, String code) {
        return ResponseEntity.status(status).body(Map.of("errorCode", code));
    }

    public record Payload(@NotNull UUID notificationId, @NotBlank @Size(max = 100) String recipientId,
                          @NotBlank @Size(max = 100) String title, @NotBlank @Size(max = 500) String body) { }
    public record Stats(long requests, long accepted, long rateLimited, long serverErrors,
                        long permanentFailures, long busy, int active, int maxActive) { }
}
