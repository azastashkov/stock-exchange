package com.exchange.engine;

import com.exchange.domain.RejectReason;
import com.exchange.domain.Side;
import com.exchange.events.OrderAcceptedEvent;
import com.exchange.events.OrderCancelledEvent;
import com.exchange.events.OrderRejectedEvent;
import com.exchange.events.TradeEvent;
import com.exchange.store.EventType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Golden-vector tests for the matching engine. Each scenario submits one
 * or more commands then asserts on the events drained from the canonical
 * log.
 */
class MatchingEngineTest {

    @Test
    void singleLimitOrderRestsAndEmitsAccepted() throws IOException {
        try (EngineTestSupport s = new EngineTestSupport(64, "AAPL")) {
            Order o = s.limit("AAPL", EngineTestSupport.BUY, 100L, 1500000L, 1L, "C1", 9000L);
            s.engine.onNewOrder(o);

            List<EngineTestSupport.Recorded> events = s.drain();
            assertThat(events).hasSize(1);
            assertThat(events.get(0).isAccepted()).isTrue();
            OrderAcceptedEvent.MutableOrderAccepted acc = events.get(0).decodeAccepted();
            assertThat(acc.symbolId).isEqualTo(s.symbols.idOf("AAPL"));
            assertThat(acc.qty).isEqualTo(100L);
            assertThat(acc.price).isEqualTo(1500000L);
            assertThat(acc.orderId).isEqualTo(1L);
            assertThat(acc.side).isEqualTo(EngineTestSupport.BUY);

            assertThat(s.books.get(s.symbols.idOf("AAPL")).bestBid().totalQty).isEqualTo(100L);
            assertThat(s.books.get(s.symbols.idOf("AAPL")).bestAsk()).isNull();
        }
    }

    @Test
    void crossingLimitOrderProducesOneTrade() throws IOException {
        try (EngineTestSupport s = new EngineTestSupport(64, "AAPL")) {
            // Resting sell limit
            s.engine.onNewOrder(s.limit("AAPL", EngineTestSupport.SELL, 100L, 1500000L, 1L, "S1", 9001L));
            s.drain(); // consume initial accepted

            // Crossing buy at the same price
            s.engine.onNewOrder(s.limit("AAPL", EngineTestSupport.BUY, 100L, 1500000L, 2L, "B1", 9002L));
            List<EngineTestSupport.Recorded> events = s.drain();
            assertThat(events).hasSize(2);
            assertThat(events.get(0).type).isEqualTo(EventType.ORDER_ACCEPTED);
            assertThat(events.get(1).type).isEqualTo(EventType.TRADE);

            TradeEvent.MutableTrade t = events.get(1).decodeTrade();
            assertThat(t.symbolId).isEqualTo(s.symbols.idOf("AAPL"));
            assertThat(t.qty).isEqualTo(100L);
            assertThat(t.price).isEqualTo(1500000L);
            assertThat(t.makerOrderId).isEqualTo(1L); // resting sell
            assertThat(t.takerOrderId).isEqualTo(2L); // incoming buy
            assertThat(t.makerSide).isEqualTo(Side.CODE_SELL);

            assertThat(s.books.get(s.symbols.idOf("AAPL")).isEmpty()).isTrue();
        }
    }

    @Test
    void partialFill_takerRemainderRests() throws IOException {
        try (EngineTestSupport s = new EngineTestSupport(64, "AAPL")) {
            // Resting sell of 50
            s.engine.onNewOrder(s.limit("AAPL", EngineTestSupport.SELL, 50L, 1500000L, 1L, "S1", 9001L));
            s.drain();

            // Buy 80 — fills 50, rests 30
            s.engine.onNewOrder(s.limit("AAPL", EngineTestSupport.BUY, 80L, 1500000L, 2L, "B1", 9002L));
            List<EngineTestSupport.Recorded> events = s.drain();

            assertThat(events).hasSize(2);
            assertThat(events.get(0).isAccepted()).isTrue();
            assertThat(events.get(1).isTrade()).isTrue();
            TradeEvent.MutableTrade t = events.get(1).decodeTrade();
            assertThat(t.qty).isEqualTo(50L);

            OrderBook book = s.books.get(s.symbols.idOf("AAPL"));
            assertThat(book.bestAsk()).isNull();
            assertThat(book.bestBid().totalQty).isEqualTo(30L);
            assertThat(book.bestBid().peekHead().order.remainingQty).isEqualTo(30L);
        }
    }

    @Test
    void marketOrderAgainstEmptyBook_acceptedThenDiscarded() throws IOException {
        try (EngineTestSupport s = new EngineTestSupport(64, "AAPL")) {
            s.engine.onNewOrder(s.market("AAPL", EngineTestSupport.BUY, 100L, 1L, "M1", 9000L));
            List<EngineTestSupport.Recorded> events = s.drain();

            // Documented behavior: ORDER_ACCEPTED is emitted, the
            // market order finds no liquidity, and the unfilled remainder
            // is silently discarded — no further events.
            assertThat(events).hasSize(1);
            assertThat(events.get(0).isAccepted()).isTrue();

            assertThat(s.books.get(s.symbols.idOf("AAPL")).isEmpty()).isTrue();
        }
    }

    @Test
    void marketOrderEatsBestThenNextLevel() throws IOException {
        try (EngineTestSupport s = new EngineTestSupport(64, "AAPL")) {
            s.engine.onNewOrder(s.limit("AAPL", EngineTestSupport.SELL, 30L, 1000000L, 1L, "S1", 9001L));
            s.engine.onNewOrder(s.limit("AAPL", EngineTestSupport.SELL, 40L, 1010000L, 1L, "S2", 9001L));
            s.drain();

            s.engine.onNewOrder(s.market("AAPL", EngineTestSupport.BUY, 50L, 2L, "M1", 9000L));
            List<EngineTestSupport.Recorded> events = s.drain();

            assertThat(events).hasSize(3); // accepted + 2 trades
            assertThat(events.get(0).isAccepted()).isTrue();
            assertThat(events.get(1).isTrade()).isTrue();
            assertThat(events.get(2).isTrade()).isTrue();
            TradeEvent.MutableTrade t1 = events.get(1).decodeTrade();
            TradeEvent.MutableTrade t2 = events.get(2).decodeTrade();
            assertThat(t1.qty).isEqualTo(30L);
            assertThat(t1.price).isEqualTo(1000000L);
            assertThat(t2.qty).isEqualTo(20L);
            assertThat(t2.price).isEqualTo(1010000L);
        }
    }

    @Test
    void cancelRestingLimitEmitsCancelled() throws IOException {
        try (EngineTestSupport s = new EngineTestSupport(64, "AAPL")) {
            s.engine.onNewOrder(s.limit("AAPL", EngineTestSupport.BUY, 100L, 1500000L, 1L, "C1", 9000L));
            s.drain();

            assertThat(s.engine.onCancel(1L, EngineTestSupport.clOrdId("C1"), 12345L)).isTrue();
            List<EngineTestSupport.Recorded> events = s.drain();

            assertThat(events).hasSize(1);
            assertThat(events.get(0).isCancelled()).isTrue();
            OrderCancelledEvent.MutableOrderCancelled c = events.get(0).decodeCancelled();
            assertThat(c.orderId).isEqualTo(1L);
            assertThat(c.remainingQty).isEqualTo(100L);
            assertThat(c.senderId).isEqualTo(1L);
            assertThat(c.clientTsNs).isEqualTo(12345L);

            assertThat(s.books.get(s.symbols.idOf("AAPL")).isEmpty()).isTrue();
        }
    }

    @Test
    void cancelNonexistentEmitsRejectUnknownOrder() throws IOException {
        try (EngineTestSupport s = new EngineTestSupport(64, "AAPL")) {
            assertThat(s.engine.onCancel(1L, EngineTestSupport.clOrdId("ZZZ"), 0L)).isFalse();
            List<EngineTestSupport.Recorded> events = s.drain();
            assertThat(events).hasSize(1);
            assertThat(events.get(0).isRejected()).isTrue();
            OrderRejectedEvent.MutableOrderRejected r = events.get(0).decodeRejected();
            assertThat(r.reasonCode).isEqualTo(RejectReason.UNKNOWN_ORDER);
        }
    }

    @Test
    void cancelReplaceDecreasesQty_originalCancelledNewAccepted() throws IOException {
        try (EngineTestSupport s = new EngineTestSupport(64, "AAPL")) {
            s.engine.onNewOrder(s.limit("AAPL", EngineTestSupport.BUY, 100L, 1500000L, 1L, "C1", 9000L));
            s.drain();

            assertThat(s.engine.onCancelReplace(
                    1L,
                    EngineTestSupport.clOrdId("C1"),
                    EngineTestSupport.clOrdId("C2"),
                    /*newQty*/ 60L,
                    /*newPrice*/ 1500000L,
                    /*clientTs*/ 99999L)).isTrue();
            List<EngineTestSupport.Recorded> events = s.drain();

            assertThat(events).hasSize(2);
            assertThat(events.get(0).isCancelled()).isTrue();
            assertThat(events.get(1).isAccepted()).isTrue();
            OrderAcceptedEvent.MutableOrderAccepted acc = events.get(1).decodeAccepted();
            assertThat(acc.qty).isEqualTo(60L);
            assertThat(acc.orderId).isEqualTo(2L); // new orderId allocated

            OrderBook book = s.books.get(s.symbols.idOf("AAPL"));
            assertThat(book.bestBid().totalQty).isEqualTo(60L);
        }
    }

    @Test
    void duplicateClOrdIdEmitsRejectDuplicate() throws IOException {
        try (EngineTestSupport s = new EngineTestSupport(64, "AAPL")) {
            s.engine.onNewOrder(s.limit("AAPL", EngineTestSupport.BUY, 100L, 1500000L, 1L, "DUP", 9000L));
            s.drain();
            // Resubmit with same (sender, clOrdId) → duplicate
            s.engine.onNewOrder(s.limit("AAPL", EngineTestSupport.BUY, 50L, 1500000L, 1L, "DUP", 9000L));
            List<EngineTestSupport.Recorded> events = s.drain();
            assertThat(events).hasSize(1);
            assertThat(events.get(0).isRejected()).isTrue();
            OrderRejectedEvent.MutableOrderRejected r = events.get(0).decodeRejected();
            assertThat(r.reasonCode).isEqualTo(RejectReason.DUPLICATE_CLORDID);
        }
    }

    @Test
    void unknownSymbolEmitsRejectUnknownSymbol() throws IOException {
        try (EngineTestSupport s = new EngineTestSupport(64, "AAPL")) {
            // Build an order with an invalid symbolId without going through
            // Symbols#idOf so the engine sees an unknown symbol.
            Order o = new Order();
            byte[] cl = EngineTestSupport.clOrdId("X1");
            o.set(0L, /*symbolId*/ 99,
                    Side.CODE_BUY,
                    com.exchange.domain.OrdType.CODE_LIMIT,
                    com.exchange.domain.TimeInForce.CODE_DAY,
                    9000L, 1L, cl, 100L, 1500000L, 0L, 0L);
            s.engine.onNewOrder(o);
            List<EngineTestSupport.Recorded> events = s.drain();
            assertThat(events).hasSize(1);
            assertThat(events.get(0).isRejected()).isTrue();
            OrderRejectedEvent.MutableOrderRejected r = events.get(0).decodeRejected();
            assertThat(r.reasonCode).isEqualTo(RejectReason.UNKNOWN_SYMBOL);
        }
    }

    @Test
    void invalidQtyEmitsReject() throws IOException {
        try (EngineTestSupport s = new EngineTestSupport(64, "AAPL")) {
            Order o = s.limit("AAPL", EngineTestSupport.BUY, 0L, 1500000L, 1L, "Q0", 9000L);
            s.engine.onNewOrder(o);
            List<EngineTestSupport.Recorded> events = s.drain();
            assertThat(events).hasSize(1);
            assertThat(events.get(0).decodeRejected().reasonCode).isEqualTo(RejectReason.INVALID_QTY);
        }
    }

    @Test
    void invalidPriceForLimitEmitsReject() throws IOException {
        try (EngineTestSupport s = new EngineTestSupport(64, "AAPL")) {
            Order o = s.limit("AAPL", EngineTestSupport.BUY, 100L, 0L, 1L, "P0", 9000L);
            s.engine.onNewOrder(o);
            List<EngineTestSupport.Recorded> events = s.drain();
            assertThat(events).hasSize(1);
            assertThat(events.get(0).decodeRejected().reasonCode).isEqualTo(RejectReason.INVALID_PRICE);
        }
    }

    @Test
    void priceTimePriority_betterLevelFirst() throws IOException {
        try (EngineTestSupport s = new EngineTestSupport(64, "AAPL")) {
            // Two resting bids at different prices; better one (higher) wins
            s.engine.onNewOrder(s.limit("AAPL", EngineTestSupport.BUY, 30L, 1500000L, 1L, "B1", 9001L));
            s.engine.onNewOrder(s.limit("AAPL", EngineTestSupport.BUY, 30L, 1499000L, 2L, "B2", 9002L));
            s.drain();

            s.engine.onNewOrder(s.limit("AAPL", EngineTestSupport.SELL, 30L, 1499000L, 3L, "S1", 9003L));
            List<EngineTestSupport.Recorded> events = s.drain();
            assertThat(events).hasSize(2);
            TradeEvent.MutableTrade t = events.get(1).decodeTrade();
            assertThat(t.makerOrderId).isEqualTo(1L); // higher-priced bid filled first
            assertThat(t.price).isEqualTo(1500000L);
        }
    }
}
