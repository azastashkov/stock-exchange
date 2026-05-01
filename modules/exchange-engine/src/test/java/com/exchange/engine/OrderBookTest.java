package com.exchange.engine;

import com.exchange.domain.OrdType;
import com.exchange.domain.Side;
import com.exchange.domain.TimeInForce;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Direct unit tests of {@link OrderBook} invariants without going through
 * the matching engine. Exercises price-time priority, empty bests, and
 * cancel-cleans-empty-level.
 */
class OrderBookTest {

    private static Order makeOrder(long orderId, byte side, long qty, long price) {
        Order o = new Order();
        byte[] cl = new byte[16];
        cl[0] = 'X';
        o.set(orderId,
                /*symbolId*/ 1,
                side,
                OrdType.CODE_LIMIT,
                TimeInForce.CODE_DAY,
                /*account*/ 100L,
                /*senderId*/ 1L,
                cl,
                qty,
                price,
                /*entered*/ 0L,
                /*client*/ 0L);
        return o;
    }

    @Test
    void emptyBookReturnsNullBests() {
        OrderBook book = new OrderBook(1);
        assertThat(book.bestBid()).isNull();
        assertThat(book.bestAsk()).isNull();
        assertThat(book.isEmpty()).isTrue();
    }

    @Test
    void priceTimePriority_sameLevelFifo() {
        OrderBook book = new OrderBook(1);
        OrderEntryPool pool = new OrderEntryPool(8);

        Order o1 = makeOrder(1L, Side.CODE_BUY, 100L, 1000L);
        Order o2 = makeOrder(2L, Side.CODE_BUY, 50L, 1000L);
        Order o3 = makeOrder(3L, Side.CODE_BUY, 25L, 1000L);
        book.addOrder(o1, pool.acquire());
        book.addOrder(o2, pool.acquire());
        book.addOrder(o3, pool.acquire());

        PriceLevel best = book.bestBid();
        assertThat(best).isNotNull();
        assertThat(best.price).isEqualTo(1000L);
        assertThat(best.totalQty).isEqualTo(175L);
        assertThat(best.peekHead().order.orderId).isEqualTo(1L);
        assertThat(best.peekHead().next.order.orderId).isEqualTo(2L);
        assertThat(best.peekHead().next.next.order.orderId).isEqualTo(3L);
    }

    @Test
    void bestBidIsHighestPrice_bestAskIsLowestPrice() {
        OrderBook book = new OrderBook(1);
        OrderEntryPool pool = new OrderEntryPool(8);
        book.addOrder(makeOrder(1L, Side.CODE_BUY, 10L, 990L), pool.acquire());
        book.addOrder(makeOrder(2L, Side.CODE_BUY, 10L, 1000L), pool.acquire());
        book.addOrder(makeOrder(3L, Side.CODE_BUY, 10L, 980L), pool.acquire());
        book.addOrder(makeOrder(4L, Side.CODE_SELL, 10L, 1010L), pool.acquire());
        book.addOrder(makeOrder(5L, Side.CODE_SELL, 10L, 1005L), pool.acquire());
        book.addOrder(makeOrder(6L, Side.CODE_SELL, 10L, 1020L), pool.acquire());

        assertThat(book.bestBid().price).isEqualTo(1000L);
        assertThat(book.bestAsk().price).isEqualTo(1005L);
    }

    @Test
    void cancelRemovesOrderAndCleansEmptyLevel() {
        OrderBook book = new OrderBook(1);
        OrderEntryPool pool = new OrderEntryPool(8);

        book.addOrder(makeOrder(1L, Side.CODE_BUY, 10L, 990L), pool.acquire());
        book.addOrder(makeOrder(2L, Side.CODE_BUY, 10L, 1000L), pool.acquire());

        // Cancel order 2 — level @1000 should be removed entirely
        OrderEntry removed = book.removeOrderById(2L);
        assertThat(removed).isNotNull();
        assertThat(book.bestBid().price).isEqualTo(990L);
        assertThat(book.findEntry(2L)).isNull();

        // Cancel last order — book becomes empty
        OrderEntry alsoRemoved = book.removeOrderById(1L);
        assertThat(alsoRemoved).isNotNull();
        assertThat(book.bestBid()).isNull();
        assertThat(book.isEmpty()).isTrue();
    }

    @Test
    void removeNonexistentReturnsNull() {
        OrderBook book = new OrderBook(1);
        assertThat(book.removeOrderById(42L)).isNull();
    }

    @Test
    void cancelMiddleOrderPreservesOrderingOfRemaining() {
        OrderBook book = new OrderBook(1);
        OrderEntryPool pool = new OrderEntryPool(8);
        book.addOrder(makeOrder(1L, Side.CODE_SELL, 10L, 1000L), pool.acquire());
        book.addOrder(makeOrder(2L, Side.CODE_SELL, 20L, 1000L), pool.acquire());
        book.addOrder(makeOrder(3L, Side.CODE_SELL, 30L, 1000L), pool.acquire());

        book.removeOrderById(2L);

        PriceLevel level = book.bestAsk();
        assertThat(level.totalQty).isEqualTo(40L);
        assertThat(level.peekHead().order.orderId).isEqualTo(1L);
        assertThat(level.peekHead().next.order.orderId).isEqualTo(3L);
        assertThat(level.peekHead().next.next).isNull();
    }
}
