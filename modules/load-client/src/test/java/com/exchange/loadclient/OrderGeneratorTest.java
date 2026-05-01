package com.exchange.loadclient;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class OrderGeneratorTest {

    private static final long PRICE_BASE_FP = 1_000_000L; // 100.0000

    @Test
    void producesAlternatingSides() {
        OrderGenerator g = new OrderGenerator(new String[]{"AAPL"}, PRICE_BASE_FP);
        OrderGenerator.NewOrderSpec spec = new OrderGenerator.NewOrderSpec();
        g.next(spec, 0, 1L);
        byte first = spec.side;
        g.next(spec, 0, 1L);
        byte second = spec.side;
        assertThat(first).isNotEqualTo(second);
        assertThat(first).isIn(OrderGenerator.SIDE_BUY, OrderGenerator.SIDE_SELL);
        assertThat(second).isIn(OrderGenerator.SIDE_BUY, OrderGenerator.SIDE_SELL);
    }

    @Test
    void priceWalksWithinConfiguredBand() {
        OrderGenerator g = new OrderGenerator(new String[]{"AAPL", "MSFT"}, PRICE_BASE_FP);
        OrderGenerator.NewOrderSpec spec = new OrderGenerator.NewOrderSpec();
        long maxDriftFp = Math.max(10_000L, PRICE_BASE_FP / 50L); // matches generator's clamp
        // Allow a generous extra slop for limit-side bias / jitter.
        long sideBias = Math.max(10_000L, PRICE_BASE_FP / 1000L);
        long lo = PRICE_BASE_FP - maxDriftFp - sideBias;
        long hi = PRICE_BASE_FP + maxDriftFp + sideBias;
        for (int i = 0; i < 50_000; i++) {
            g.next(spec, 0, 1L);
            assertThat(spec.priceFp).as("priceFp at iteration " + i).isBetween(lo, hi);
            assertThat(spec.qty).isBetween(10L, 200L);
            assertThat(spec.symbol).isIn("AAPL", "MSFT");
        }
    }

    @Test
    void clOrdIdHasSessionPrefixAndZeroPad() {
        OrderGenerator g = new OrderGenerator(new String[]{"AAPL"}, PRICE_BASE_FP);
        OrderGenerator.NewOrderSpec spec = new OrderGenerator.NewOrderSpec();
        g.next(spec, 7, 42L);
        // Format: "S7-1" then padded to 16 with zero bytes.
        assertThat(spec.clOrdId.length).isEqualTo(16);
        assertThat((char) spec.clOrdId[0]).isEqualTo('S');
        assertThat((char) spec.clOrdId[1]).isEqualTo('7');
        assertThat((char) spec.clOrdId[2]).isEqualTo('-');
        // First seq id is 1
        assertThat((char) spec.clOrdId[3]).isEqualTo('1');
        // remaining bytes are zero
        for (int i = 4; i < 16; i++) {
            assertThat(spec.clOrdId[i]).as("byte " + i).isEqualTo((byte) 0);
        }
    }

    @Test
    void cancellableQueueTracksRecentOrders() {
        OrderGenerator g = new OrderGenerator(new String[]{"AAPL"}, PRICE_BASE_FP);
        OrderGenerator.NewOrderSpec spec = new OrderGenerator.NewOrderSpec();
        for (int i = 0; i < 5; i++) g.next(spec, 1, 1L);
        // Immediately, no cancellable available (min age 1ms).
        assertThat(g.pollCancellable(1_000_000L)).isNull();
        // After waiting > 1ms, the head should pop.
        sleepMs(5L);
        OrderGenerator.RestingOrder r = g.pollCancellable(1_000_000L);
        assertThat(r).isNotNull();
        assertThat(r.symbol).isEqualTo("AAPL");
    }

    @Test
    void rejectsEmptySymbolArray() {
        try {
            new OrderGenerator(new String[]{}, PRICE_BASE_FP);
            org.junit.jupiter.api.Assertions.fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) { /* ok */ }
    }

    private static void sleepMs(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
    }
}
