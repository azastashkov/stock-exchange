package com.exchange.fix;

/**
 * Per-session rate limiter factory. Each new FIX session asks for a fresh
 * limiter at logon time — the factory decides what permits to grant.
 */
@FunctionalInterface
public interface RateLimiterFactory {

    RateLimiter create(String senderCompId);

    /** Convenience: every session gets the same limit. */
    static RateLimiterFactory perSession(int permitsPerSecond) {
        return s -> new RateLimiter(permitsPerSecond);
    }
}
