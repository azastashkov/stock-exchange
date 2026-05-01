package com.exchange.engine;

import com.exchange.domain.OrdType;
import com.exchange.domain.Side;
import com.exchange.domain.TimeInForce;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Coarse JIT-warmed regression guard for the matching engine's throughput.
 * Not a precise benchmark — just a smoke test that we are nowhere near
 * regressed on the steady-state hot path.
 */
class ThroughputBenchmarkTest {

    @Test
    void hundredKAlternatingLimitsCompletesUnderTwoSeconds() throws IOException {
        // Slot count must be > total events: 100k accepts + ~50k trades
        try (EngineTestSupport s = new EngineTestSupport(200_000, "AAPL")) {
            // Warm-up: 5_000 round-trips to JIT-compile the hot path.
            warmup(s, 5_000);

            // Reset by re-creating fresh state for the timed run.
            try (EngineTestSupport timed = new EngineTestSupport(300_000, "AAPL")) {
                long start = System.nanoTime();
                runWorkload(timed, 100_000);
                long elapsedNs = System.nanoTime() - start;
                long elapsedMs = elapsedNs / 1_000_000L;
                System.out.println("Throughput test: 100k orders in " + elapsedMs + " ms");
                assertTrue(elapsedMs < 2000L,
                        "100k limit orders should complete in < 2s, took " + elapsedMs + "ms");
            }
        }
    }

    private static void warmup(EngineTestSupport s, int count) {
        runWorkload(s, count);
    }

    /** Submit {@code count} alternating buy/sell limits at a single price.
     * Each pair fully crosses, producing {@code count/2} trades. */
    private static void runWorkload(EngineTestSupport s, int count) {
        int symbolId = s.symbols.idOf("AAPL");
        long price = 1500000L;
        Order order = new Order();
        byte[] cl = new byte[16];

        for (int i = 0; i < count; i++) {
            byte side = (i % 2 == 0) ? Side.CODE_BUY : Side.CODE_SELL;
            // Encode "K" + i in clOrdId without allocating a String each loop.
            cl[0] = 'K';
            int v = i;
            // simple base-36 encoding into the buffer
            int p = 1;
            for (; p < 16 && v > 0; p++) {
                int d = v % 36;
                v /= 36;
                cl[p] = (byte) (d < 10 ? ('0' + d) : ('A' + d - 10));
            }
            for (; p < 16; p++) cl[p] = 0;

            order.set(0L, symbolId, side,
                    OrdType.CODE_LIMIT,
                    TimeInForce.CODE_DAY,
                    9000L,
                    1L + (i % 2),
                    cl,
                    1L,
                    price,
                    0L,
                    0L);
            s.engine.onNewOrder(order);
        }
    }
}
