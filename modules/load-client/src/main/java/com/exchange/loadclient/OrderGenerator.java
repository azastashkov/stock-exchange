package com.exchange.loadclient;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Generates a stream of {@link NewOrderSpec}s for a single
 * {@link SessionRunner}. Side alternates each call, prices walk randomly
 * around a per-symbol mid.
 * <p>
 * Tracks recently-sent orders so {@link SessionRunner} can choose to cancel
 * about 10% of them after a small delay.
 */
public final class OrderGenerator {

    /** Side codes are FIX char values: '1' = BUY, '2' = SELL. */
    public static final byte SIDE_BUY = (byte) '1';
    public static final byte SIDE_SELL = (byte) '2';

    /** Mid-price for each symbol, in fixed-point times 10000. */
    private final long[] midFp;
    /** Symbol names indexed in the same order as midFp. */
    private final String[] symbols;

    /** Per-symbol price walk amplitude, in fixed-point times 10000. */
    private final long stepFp;
    /** Bounding distance from priceBase to clamp mid. */
    private final long maxDriftFp;
    private final long priceBaseFp;

    /** Counter for ClOrdID generation. */
    private long seq;
    /** Toggles between BUY and SELL each call. */
    private boolean nextBuy = true;

    /** Recently-sent orders eligible for an upcoming cancel. */
    private final Deque<RestingOrder> resting = new ArrayDeque<>(64);
    private static final int MAX_RESTING = 1024;

    public OrderGenerator(String[] symbols, long priceBaseFp) {
        if (symbols == null || symbols.length == 0) {
            throw new IllegalArgumentException("symbols cannot be empty");
        }
        this.symbols = symbols.clone();
        this.midFp = new long[symbols.length];
        for (int i = 0; i < symbols.length; i++) {
            this.midFp[i] = priceBaseFp;
        }
        this.priceBaseFp = priceBaseFp;
        this.stepFp = Math.max(100L, priceBaseFp / 1000L);   // ~0.1% step
        this.maxDriftFp = Math.max(10_000L, priceBaseFp / 50L); // ~2% drift bound
    }

    /** Reset the internal seq number (used so per-session ClOrdIDs differ). */
    public void resetSeq(long start) {
        this.seq = start;
    }

    /**
     * Generate the next {@link NewOrderSpec}. The caller fills in the
     * ClOrdID byte buffer via {@link NewOrderSpec#clOrdId} (already padded
     * to 16 ASCII bytes).
     */
    public void next(NewOrderSpec out, int sessionIdx, long account) {
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        int idx = rng.nextInt(symbols.length);
        // Walk mid by ±stepFp/2; clamp to [priceBase - maxDrift, priceBase + maxDrift].
        long delta = rng.nextLong(-stepFp, stepFp + 1);
        long mid = midFp[idx] + delta;
        long lo = priceBaseFp - maxDriftFp;
        long hi = priceBaseFp + maxDriftFp;
        if (mid < lo) mid = lo;
        if (mid > hi) mid = hi;
        midFp[idx] = mid;

        boolean buy = nextBuy;
        nextBuy = !nextBuy;
        // Limit price: a few ticks (1c = 100 in fp10000) around mid, biased
        // to provide some matching.
        long jitter = rng.nextLong(-stepFp / 2L, stepFp / 2L + 1L);
        long price = mid + jitter;
        if (buy) {
            // BUY at slightly above mid to encourage matches against existing offers.
            price += stepFp / 4L;
        } else {
            price -= stepFp / 4L;
        }
        if (price < 100L) price = 100L; // never below 0.01

        long qty = 10L + rng.nextInt(191); // 10..200
        long clSeq = ++seq;
        out.symbol = symbols[idx];
        out.side = buy ? SIDE_BUY : SIDE_SELL;
        out.priceFp = price;
        out.qty = qty;
        out.account = account;
        out.sessionIdx = sessionIdx;
        out.clOrdSeq = clSeq;
        formatClOrdId(out.clOrdId, sessionIdx, clSeq);
        // Track for possible later cancel
        if (resting.size() >= MAX_RESTING) {
            resting.pollFirst();
        }
        RestingOrder r = new RestingOrder();
        r.clOrdSeq = clSeq;
        r.symbol = out.symbol;
        r.side = out.side;
        r.account = account;
        r.sentAtNs = System.nanoTime();
        resting.offerLast(r);
    }

    /**
     * Pop a previously-resting order whose age >= {@code minAgeNs}. Returns
     * {@code null} if none exists; the order is removed from the queue if
     * returned.
     */
    public RestingOrder pollCancellable(long minAgeNs) {
        if (resting.isEmpty()) return null;
        RestingOrder head = resting.peekFirst();
        if (head == null) return null;
        long age = System.nanoTime() - head.sentAtNs;
        if (age < minAgeNs) return null;
        return resting.pollFirst();
    }

    /** Symbols configured for this generator. */
    public String[] symbols() {
        return symbols.clone();
    }

    /** Current mid (fp10000) for the symbol at {@code idx}. */
    public long midFp(int idx) {
        return midFp[idx];
    }

    /** Spec for a NewOrderSingle to send. Mutable; reused. */
    public static final class NewOrderSpec {
        public final byte[] clOrdId = new byte[16];
        public String symbol;
        public byte side;
        public long priceFp;
        public long qty;
        public long account;
        public int sessionIdx;
        public long clOrdSeq;
    }

    /** Tracking record for a resting order eligible for cancellation. */
    public static final class RestingOrder {
        public long clOrdSeq;
        public String symbol;
        public byte side;
        public long account;
        public long sentAtNs;
    }

    /**
     * Encode "S<idx>-<seq>" into 16 ASCII bytes, zero-padded on the right.
     * Allocation-free.
     */
    static void formatClOrdId(byte[] dest, int sessionIdx, long seq) {
        if (dest.length != 16) {
            throw new IllegalArgumentException("dest must be 16 bytes");
        }
        int p = 0;
        dest[p++] = 'S';
        if (sessionIdx >= 100) {
            dest[p++] = (byte) ('0' + (sessionIdx / 100) % 10);
            dest[p++] = (byte) ('0' + (sessionIdx / 10) % 10);
            dest[p++] = (byte) ('0' + sessionIdx % 10);
        } else if (sessionIdx >= 10) {
            dest[p++] = (byte) ('0' + (sessionIdx / 10) % 10);
            dest[p++] = (byte) ('0' + sessionIdx % 10);
        } else if (sessionIdx >= 0) {
            dest[p++] = (byte) ('0' + sessionIdx);
        } else {
            dest[p++] = '0';
        }
        dest[p++] = '-';
        // Write seq using up to remaining (16 - p) digits; truncate if absurdly big.
        int remaining = 16 - p;
        long v = seq;
        if (v <= 0L) {
            dest[p++] = '0';
        } else {
            // Write digits backwards into a small scratch.
            byte[] tmp = new byte[20];
            int idx = 0;
            while (v > 0L && idx < tmp.length) {
                tmp[idx++] = (byte) ('0' + (int) (v % 10L));
                v /= 10L;
            }
            int writable = Math.min(idx, remaining);
            // We chose to keep the most-significant digits (truncate trailing 0s
            // in seq won't happen at large numbers; we instead drop low digits).
            // Simpler & correct: we only proceed if it fits.
            if (idx > remaining) {
                // Cycle the seq counter modulo (10^remaining - 1) externally; for
                // safety here, just emit the lower 'remaining' digits.
                for (int i = 0; i < remaining; i++) {
                    dest[p++] = tmp[i];
                }
                // reverse the digits we just wrote
                reverse(dest, 16 - remaining, 15);
            } else {
                for (int i = idx - 1; i >= 0; i--) {
                    dest[p++] = tmp[i];
                }
            }
        }
        // zero-pad
        for (int i = p; i < 16; i++) dest[i] = 0;
    }

    private static void reverse(byte[] b, int from, int to) {
        while (from < to) {
            byte t = b[from];
            b[from] = b[to];
            b[to] = t;
            from++;
            to--;
        }
    }
}
