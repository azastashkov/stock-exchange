package com.exchange.fix;

/**
 * Simple token-bucket rate limiter. Bucket starts full
 * ({@code permitsPerSecond} tokens) and refills at a rate of one token every
 * {@code 1s / permitsPerSecond}. Allocation-free; not thread-safe.
 */
public final class RateLimiter {

    private final int permitsPerSecond;
    private final long nanosPerToken;
    private long tokens;
    private long lastRefillNanos = Long.MIN_VALUE;

    public RateLimiter(int permitsPerSecond) {
        if (permitsPerSecond <= 0) {
            throw new IllegalArgumentException("permitsPerSecond must be > 0");
        }
        this.permitsPerSecond = permitsPerSecond;
        this.nanosPerToken = 1_000_000_000L / permitsPerSecond;
        this.tokens = permitsPerSecond;
    }

    public boolean tryAcquire() {
        return tryAcquire(System.nanoTime());
    }

    public boolean tryAcquire(long nowNanos) {
        if (lastRefillNanos == Long.MIN_VALUE) {
            lastRefillNanos = nowNanos;
        } else {
            long elapsed = nowNanos - lastRefillNanos;
            if (elapsed > 0) {
                long add = elapsed / nanosPerToken;
                if (add > 0) {
                    tokens = Math.min(permitsPerSecond, tokens + add);
                    lastRefillNanos += add * nanosPerToken;
                }
            }
        }
        if (tokens > 0) {
            tokens--;
            return true;
        }
        return false;
    }

    public int permitsPerSecond() {
        return permitsPerSecond;
    }
}
