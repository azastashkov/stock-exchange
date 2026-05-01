package com.exchange.loadclient;

import org.HdrHistogram.Histogram;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

class LatencyRecorderTest {

    @Test
    void records100kSamplesAndComputesP99() {
        LatencyRecorder recorder = new LatencyRecorder();
        // Mix of fast and slow samples.
        for (int i = 0; i < 100_000; i++) {
            // 90% in 100us..1ms range, 10% in 1ms..10ms.
            long ns;
            if (i % 10 == 0) {
                ns = 1_000_000L + ThreadLocalRandom.current().nextLong(1_000_000L, 10_000_000L);
            } else {
                ns = 100_000L + ThreadLocalRandom.current().nextLong(0L, 900_000L);
            }
            recorder.recordRoundtripNanos(ns);
        }
        Histogram h = recorder.snapshot();
        assertThat(h).isNotNull();
        // After the snapshot, totals carries everything.
        assertThat(recorder.totalCount()).isGreaterThan(0L);
        double p99 = recorder.p99Ms();
        assertThat(p99).isGreaterThan(0.0);
        // The "10% slow" tail should put p99 above 1ms.
        assertThat(p99).isGreaterThan(1.0);
    }

    @Test
    void clampsOutOfRangeValues() {
        LatencyRecorder recorder = new LatencyRecorder();
        recorder.recordRoundtripNanos(10L);            // below 1us, clamped up
        recorder.recordRoundtripNanos(120L * 1_000_000_000L); // 120s, clamped down to 60s
        recorder.recordRoundtripNanos(500_000L);       // 0.5ms, untouched
        assertThat(recorder.totalCount()).isEqualTo(3L);
    }

    @Test
    void emptyHistogramReportsZeroCount() {
        LatencyRecorder recorder = new LatencyRecorder();
        assertThat(recorder.totalCount()).isEqualTo(0L);
    }

    @Test
    void printStatsPassesWhenP99BelowCeiling() {
        LatencyRecorder recorder = new LatencyRecorder();
        // All samples at 100us
        for (int i = 0; i < 1000; i++) recorder.recordRoundtripNanos(100_000L);
        boolean pass = recorder.printStatsAndCheck(5.0, 1, 1000L);
        assertThat(pass).isTrue();
    }

    @Test
    void printStatsFailsOnEmpty() {
        LatencyRecorder recorder = new LatencyRecorder();
        boolean pass = recorder.printStatsAndCheck(5.0, 1, 1000L);
        assertThat(pass).isFalse();
    }
}
