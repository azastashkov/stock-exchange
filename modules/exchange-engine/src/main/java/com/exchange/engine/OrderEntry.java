package com.exchange.engine;

/**
 * Doubly-linked list node for the FIFO inside a {@link PriceLevel}. The
 * book also stores entries by order id in an open-addressed map for O(1)
 * cancel.
 * <p>
 * Pool-managed via {@link OrderEntryPool}: after a fill or cancel the
 * entry is reset and returned to the pool's free-list.
 */
public final class OrderEntry {

    public Order order;
    public OrderEntry prev;
    public OrderEntry next;
    /** Back-reference so we can find our level on cancel without a lookup. */
    public PriceLevel level;

    public void reset() {
        this.order = null;
        this.prev = null;
        this.next = null;
        this.level = null;
    }
}
