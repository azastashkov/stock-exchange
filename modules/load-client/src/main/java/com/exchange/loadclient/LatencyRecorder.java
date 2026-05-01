package com.exchange.loadclient;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

/**
 * Thread-safe wrapper over {@link Recorder}. All session threads call
 * {@link #recordRoundtripNanos(long)}; the orchestrator thread interrogates
 * the histogram at end of run.
 * <p>
 * Range: 1us .. 60s, 3 significant figures.
 */
public final class LatencyRecorder {

    /** 1 microsecond, in nanoseconds. */
    public static final long MIN_NS = 1_000L;
    /** 60 seconds, in nanoseconds. */
    public static final long MAX_NS = 60L * 1_000_000_000L;

    private final Recorder recorder;
    private final Histogram totals; // accumulated across the whole run

    public LatencyRecorder() {
        this.recorder = new Recorder(MIN_NS, MAX_NS, 3);
        this.totals = new Histogram(MIN_NS, MAX_NS, 3);
    }

    /** Record one observation, in nanoseconds. Safely clamped to the recorder range. */
    public void recordRoundtripNanos(long ns) {
        if (ns < MIN_NS) ns = MIN_NS;
        if (ns > MAX_NS) ns = MAX_NS;
        recorder.recordValue(ns);
    }

    /** Snapshot the current interval; merged into {@link #totals}. */
    public synchronized Histogram snapshot() {
        Histogram h = recorder.getIntervalHistogram();
        totals.add(h);
        return h;
    }

    /**
     * Print stats to {@code System.out} and return whether the run met the
     * ceiling: {@code count > 0 && p99 < ceilingMs}.
     */
    public boolean printStatsAndCheck(double p99CeilingMs,
                                       int sessions,
                                       long durationMs) {
        // Roll any remaining samples into totals.
        snapshot();
        long count = totals.getTotalCount();
        if (count == 0L) {
            System.out.println("Round-trip latency: NO SAMPLES");
            System.out.println("FAIL (count == 0)");
            return false;
        }
        double meanMs = totals.getMean() / 1_000_000.0;
        double p50Ms = totals.getValueAtPercentile(50.0) / 1_000_000.0;
        double p90Ms = totals.getValueAtPercentile(90.0) / 1_000_000.0;
        double p99Ms = totals.getValueAtPercentile(99.0) / 1_000_000.0;
        double p999Ms = totals.getValueAtPercentile(99.9) / 1_000_000.0;
        double p9999Ms = totals.getValueAtPercentile(99.99) / 1_000_000.0;
        double maxMs = totals.getMaxValue() / 1_000_000.0;
        double rate = durationMs > 0 ? (count * 1000.0) / durationMs : 0.0;

        System.out.printf("Round-trip latency (across %d sessions, %,d orders, duration %.1f s):%n",
                sessions, count, durationMs / 1000.0);
        System.out.printf("  count    : %,d%n", count);
        System.out.printf("  rate     : %,12.1f ord/s%n", rate);
        System.out.printf("  mean     : %12.2f ms%n", meanMs);
        System.out.printf("  p50      : %12.2f ms%n", p50Ms);
        System.out.printf("  p90      : %12.2f ms%n", p90Ms);
        System.out.printf("  p99      : %12.2f ms%n", p99Ms);
        System.out.printf("  p99.9    : %12.2f ms%n", p999Ms);
        System.out.printf("  p99.99   : %12.2f ms%n", p9999Ms);
        System.out.printf("  max      : %12.2f ms%n", maxMs);

        boolean pass = count > 0 && p99Ms < p99CeilingMs;
        if (pass) {
            System.out.printf("PASS (p99 %.2f ms < %.2f ms)%n", p99Ms, p99CeilingMs);
        } else {
            System.out.printf("FAIL (p99 %.2f ms >= %.2f ms)%n", p99Ms, p99CeilingMs);
        }
        return pass;
    }

    /** Snapshot total samples recorded so far (for tests/progress logs). */
    public long totalCount() {
        // Merge to keep a fresh number even mid-run.
        snapshot();
        return totals.getTotalCount();
    }

    /** Return current total p99 in ms (for tests). */
    public double p99Ms() {
        snapshot();
        if (totals.getTotalCount() == 0L) return Double.NaN;
        return totals.getValueAtPercentile(99.0) / 1_000_000.0;
    }

}
