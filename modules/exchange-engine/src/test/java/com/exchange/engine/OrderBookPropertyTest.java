package com.exchange.engine;

import com.exchange.events.OrderAcceptedEvent;
import com.exchange.events.OrderCancelledEvent;
import com.exchange.events.TradeEvent;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Random-sweep property tests. We submit a stream of random orders and
 * cancels then audit the resulting state by replaying every event from
 * the canonical event log.
 */
class OrderBookPropertyTest {

    /** Per-live-order metadata kept on the Java side to drive cancels. */
    private static final class Live {
        final long orderId;
        final long sender;
        final String clOrdId;
        Live(long orderId, long sender, String clOrdId) {
            this.orderId = orderId;
            this.sender = sender;
            this.clOrdId = clOrdId;
        }
    }

    @Test
    void randomMixDoesNotCrossBook_andQtyConservation() throws IOException {
        long seed = 0xCAFEBABE_DEADBEEFL;
        try (EngineTestSupport s = new EngineTestSupport(8192, "AAPL")) {
            Random rng = new Random(seed);
            List<Live> live = new ArrayList<>();

            int operations = 1000;
            for (int i = 0; i < operations; i++) {
                boolean cancel = !live.isEmpty() && rng.nextInt(10) >= 8;
                if (!cancel) {
                    byte side = rng.nextBoolean() ? EngineTestSupport.BUY : EngineTestSupport.SELL;
                    long price = 100_0000L + rng.nextInt(11) * 1_000L;
                    long qty = 1L + rng.nextInt(50);
                    long sender = 1L + rng.nextInt(5);
                    String cl = "C-" + i;
                    Order o = s.limit("AAPL", side, qty, price, sender, cl, 9000L + sender);
                    s.engine.onNewOrder(o);
                    if (o.orderId > 0L) {
                        live.add(new Live(o.orderId, sender, cl));
                    }
                } else {
                    int idx = rng.nextInt(live.size());
                    Live rec = live.get(idx);
                    s.engine.onCancel(rec.sender, EngineTestSupport.clOrdId(rec.clOrdId), 0L);
                    live.remove(idx);
                }
            }

            // Invariant 1: book is not crossed.
            OrderBook book = s.books.get(s.symbols.idOf("AAPL"));
            PriceLevel bestBid = book.bestBid();
            PriceLevel bestAsk = book.bestAsk();
            if (bestBid != null && bestAsk != null) {
                assertThat(bestBid.price).isLessThan(bestAsk.price);
            }

            // Invariant 2: per-level totalQty equals sum of remainingQty;
            // no negative or zero remaining qty on resting orders.
            for (PriceLevel lv : book.bidLevels()) {
                long sum = 0L;
                for (OrderEntry e = lv.peekHead(); e != null; e = e.next) {
                    assertThat(e.order.remainingQty).isGreaterThan(0L);
                    sum += e.order.remainingQty;
                }
                assertThat(sum).isEqualTo(lv.totalQty);
            }
            for (PriceLevel lv : book.askLevels()) {
                long sum = 0L;
                for (OrderEntry e = lv.peekHead(); e != null; e = e.next) {
                    assertThat(e.order.remainingQty).isGreaterThan(0L);
                    sum += e.order.remainingQty;
                }
                assertThat(sum).isEqualTo(lv.totalQty);
            }

            // Invariant 3: qty conservation across the event stream.
            // Each TRADE consumes the same qty from one resting order and
            // one taker (so 2*tradedQty of accepted qty is consumed).
            //   sum(accepted.qty) = 2*sum(trade.qty)
            //                     + sum(cancelled.remainingQty)
            //                     + sum over resting books of totalQty
            long acceptedQty = 0L;
            long cancelledRemaining = 0L;
            long tradedQty = 0L;

            List<EngineTestSupport.Recorded> all = s.drain();
            for (EngineTestSupport.Recorded r : all) {
                if (r.isAccepted()) {
                    OrderAcceptedEvent.MutableOrderAccepted m = r.decodeAccepted();
                    acceptedQty += m.qty;
                } else if (r.isCancelled()) {
                    OrderCancelledEvent.MutableOrderCancelled m = r.decodeCancelled();
                    cancelledRemaining += m.remainingQty;
                } else if (r.isTrade()) {
                    TradeEvent.MutableTrade m = r.decodeTrade();
                    tradedQty += m.qty;
                }
            }

            long restingQty = 0L;
            for (PriceLevel lv : book.bidLevels()) restingQty += lv.totalQty;
            for (PriceLevel lv : book.askLevels()) restingQty += lv.totalQty;

            assertThat(acceptedQty).isEqualTo(2L * tradedQty + cancelledRemaining + restingQty);
        }
    }
}
