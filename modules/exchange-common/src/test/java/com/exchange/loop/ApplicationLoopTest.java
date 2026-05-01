package com.exchange.loop;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;

class ApplicationLoopTest {

    @Test
    void start_stop_invokes_pollOnce() {
        AtomicInteger calls = new AtomicInteger(0);
        ApplicationLoop loop = new ApplicationLoop("test-loop", false) {
            @Override
            protected boolean pollOnce() {
                calls.incrementAndGet();
                return true; // pretend we did work each iteration
            }
        };
        loop.start();
        await().atMost(ofSeconds(2)).until(() -> calls.get() > 100);
        loop.stop();
        int snapshot = calls.get();
        assertThat(snapshot).isGreaterThan(0);
        // After stop, no further increments
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        assertThat(calls.get()).isEqualTo(snapshot);
    }

    @Test
    void idle_loop_does_not_busy_burn() {
        ApplicationLoop loop = new ApplicationLoop("idle-loop", false) {
            @Override
            protected boolean pollOnce() {
                return false;
            }
        };
        loop.start();
        try {
            await().atMost(ofMillis(2_000))
                    .pollInterval(ofMillis(20))
                    .until(() -> loop.idleCounter() >= ApplicationLoop.IDLE_PARK_THRESHOLD);
            assertThat(loop.idleCounter()).isGreaterThanOrEqualTo(ApplicationLoop.IDLE_PARK_THRESHOLD);
        } finally {
            loop.stop();
        }
    }
}
