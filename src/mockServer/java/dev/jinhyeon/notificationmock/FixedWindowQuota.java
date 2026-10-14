package dev.jinhyeon.notificationmock;

import java.util.function.LongSupplier;

/** One process-wide quota. No per-recipient state, no unbounded key map. */
final class FixedWindowQuota {
    private final int limit;
    private final long windowNanos;
    private final LongSupplier nanoTime;
    private long startedAt;
    private int used;

    FixedWindowQuota(int limit, long windowMs, LongSupplier nanoTime) {
        if (limit < 1 || windowMs < 1) throw new IllegalArgumentException("Invalid quota");
        this.limit = limit;
        this.windowNanos = Math.multiplyExact(windowMs, 1_000_000);
        this.nanoTime = nanoTime;
        this.startedAt = nanoTime.getAsLong();
    }

    /** Returns zero on admission, or whole seconds until the current window ends. */
    synchronized long acquire() {
        long elapsed = nanoTime.getAsLong() - startedAt;
        if (elapsed >= windowNanos) {
            startedAt = nanoTime.getAsLong();
            used = 0;
            elapsed = 0;
        }
        if (used >= limit) return Math.max(1, (windowNanos - elapsed + 999_999_999) / 1_000_000_000);
        used++;
        return 0;
    }
}
