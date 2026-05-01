package com.exchange.time;

/**
 * Tiny indirection over JDK clocks so call sites are testable and the
 * latency-sensitive choice (nanoTime vs. wall clock) is explicit.
 */
public final class Clocks {

    private Clocks() {
        // utility class
    }

    /**
     * Monotonic high-resolution timer suitable for measuring intervals.
     * Not comparable across JVMs.
     */
    public static long nanoTime() {
        return System.nanoTime();
    }

    /**
     * Wall-clock time in nanoseconds since the Unix epoch. Comparable across
     * processes/hosts; resolution is millisecond-bounded by the underlying
     * JDK clock.
     */
    public static long wallClockNanos() {
        return System.currentTimeMillis() * 1_000_000L;
    }
}
