package com.exchange.loadclient;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RateLimiterTest {

    @Test
    void rateIsEnforcedOver100msWindow() {
        // 1000/s -> in 100ms we should get exactly 100 tokens.
        RateLimiter rl = new RateLimiter(1_000L, 0L);
        rl.bulkRefill(100_000_000L); // +100ms
        int got = 0;
        while (rl.tryAcquire()) got++;
        assertThat(got).isEqualTo(100);
        // Another 100ms and we should refill.
        rl.bulkRefill(200_000_000L);
        int more = 0;
        while (rl.tryAcquire()) more++;
        assertThat(more).isEqualTo(100);
    }

    @Test
    void capsAtFullSecondBurst() {
        RateLimiter rl = new RateLimiter(1_000L, 0L);
        // 5 seconds of elapsed time should still cap at 1000 tokens (1s burst).
        rl.bulkRefill(5_000_000_000L);
        assertThat(rl.available()).isEqualTo(1_000L);
    }

    @Test
    void emptyBucketReturnsFalse() {
        RateLimiter rl = new RateLimiter(10L, 0L);
        assertThat(rl.tryAcquire()).isFalse();
    }

    @Test
    void rejectsNonPositiveRate() {
        try {
            new RateLimiter(0L);
            org.junit.jupiter.api.Assertions.fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) { /* ok */ }
        try {
            new RateLimiter(-1L);
            org.junit.jupiter.api.Assertions.fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) { /* ok */ }
    }

    @Test
    void replenishesGraduallyWithinSecond() {
        RateLimiter rl = new RateLimiter(10_000L, 0L);
        rl.bulkRefill(1_000_000L); // 1ms -> 10 tokens
        int got = 0;
        while (rl.tryAcquire()) got++;
        assertThat(got).isEqualTo(10);
    }

    @Test
    void rateIsEnforcedAtHighRateOver100ms() {
        RateLimiter rl = new RateLimiter(10_000L, 0L);
        rl.bulkRefill(100_000_000L);
        int got = 0;
        while (rl.tryAcquire()) got++;
        // 10000/s * 100ms = 1000 tokens, capped at 1s burst (10000).
        assertThat(got).isEqualTo(1_000);
    }
}
