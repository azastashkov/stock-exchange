package com.exchange.loadclient;

/**
 * Token-bucket rate limiter using a monotonic {@code nanoTime} clock.
 * <p>
 * Holds up to {@code ratePerSec} tokens (one full second of burst capacity).
 * Tokens are replenished by {@link #bulkRefill(long)} based on the elapsed
 * nanos since the previous refill, and consumed one at a time by
 * {@link #tryAcquire()}.
 * <p>
 * The implementation is allocation-free; not thread-safe — callers serialize
 * access from a single scheduler thread.
 */
public final class RateLimiter {

    private final long ratePerSec;
    private long tokens;
    private long lastRefillNs;

    public RateLimiter(long ratePerSec) {
        this(ratePerSec, 0L);
    }

    public RateLimiter(long ratePerSec, long startNs) {
        if (ratePerSec <= 0) {
            throw new IllegalArgumentException("ratePerSec must be > 0: " + ratePerSec);
        }
        this.ratePerSec = ratePerSec;
        this.lastRefillNs = startNs;
        this.tokens = 0L;
    }

    /** Maximum tokens the bucket holds (= ratePerSec). */
    public long capacity() {
        return ratePerSec;
    }

    /** Current tokens available (for tests/diagnostics). */
    public long available() {
        return tokens;
    }

    /**
     * Refill tokens based on the time elapsed since the last refill. Tokens
     * are capped at {@link #capacity()}. Cheap; idempotent if {@code nowNs}
     * has not advanced.
     */
    public void bulkRefill(long nowNs) {
        long elapsed = nowNs - lastRefillNs;
        if (elapsed <= 0) return;
        // tokens to add = floor(elapsed * rate / 1e9). Avoid overflow using a 128-bit-ish trick:
        // for typical rates (<= 10M/s) the product fits in a long for any reasonable elapsed.
        long add = elapsed / 1_000_000_000L * ratePerSec
                + (elapsed % 1_000_000_000L) * ratePerSec / 1_000_000_000L;
        if (add <= 0) return;
        long t = tokens + add;
        if (t > ratePerSec) t = ratePerSec;
        tokens = t;
        lastRefillNs = nowNs;
    }

    /** Try to consume a single token; returns {@code true} on success. */
    public boolean tryAcquire() {
        if (tokens <= 0) return false;
        tokens--;
        return true;
    }

    /**
     * Attempt to consume a token after refilling at {@code nowNs}; combined
     * helper for tests.
     */
    public boolean tryAcquire(long nowNs) {
        bulkRefill(nowNs);
        return tryAcquire();
    }
}
