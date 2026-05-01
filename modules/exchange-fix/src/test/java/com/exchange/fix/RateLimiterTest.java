package com.exchange.fix;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RateLimiterTest {

    @Test
    void allowsExactlyNPermitsWithinASecond() {
        RateLimiter rl = new RateLimiter(5);
        long now = 0L;
        for (int i = 0; i < 5; i++) {
            assertThat(rl.tryAcquire(now)).as("permit " + i).isTrue();
        }
        assertThat(rl.tryAcquire(now)).as("permit 6 at the same instant").isFalse();
    }

    @Test
    void replenishesOverTime() {
        RateLimiter rl = new RateLimiter(2);
        long t0 = 0L;
        assertThat(rl.tryAcquire(t0)).isTrue();
        assertThat(rl.tryAcquire(t0)).isTrue();
        assertThat(rl.tryAcquire(t0)).isFalse();
        // After 1s we have 2 permits/s available again
        long after = 1_000_000_000L;
        assertThat(rl.tryAcquire(after)).isTrue();
    }

    @Test
    void rejectsZeroOrNegativePermits() {
        try {
            new RateLimiter(0);
            org.junit.jupiter.api.Assertions.fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            // ok
        }
    }
}
