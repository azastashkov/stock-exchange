package com.exchange.engine;

/**
 * Per-price FIFO of resting orders. Maintains head/tail pointers and a
 * running {@code totalQty}. Add appends to the tail (price-time priority);
 * remove unlinks in O(1) using the entry's {@code prev}/{@code next}
 * pointers.
 */
public final class PriceLevel {

    public long price;
    public OrderEntry head;
    public OrderEntry tail;
    public long totalQty;

    public PriceLevel() {
        // pre-allocated; reset() before reuse
    }

    public void init(long price) {
        this.price = price;
        this.head = null;
        this.tail = null;
        this.totalQty = 0L;
    }

    public void add(OrderEntry e) {
        e.level = this;
        e.prev = tail;
        e.next = null;
        if (tail == null) {
            head = e;
        } else {
            tail.next = e;
        }
        tail = e;
        totalQty += e.order.remainingQty;
    }

    public void remove(OrderEntry e) {
        OrderEntry p = e.prev;
        OrderEntry n = e.next;
        if (p != null) {
            p.next = n;
        } else {
            head = n;
        }
        if (n != null) {
            n.prev = p;
        } else {
            tail = p;
        }
        if (e.order != null) {
            totalQty -= e.order.remainingQty;
            if (totalQty < 0L) totalQty = 0L;
        }
        e.prev = null;
        e.next = null;
        e.level = null;
    }

    public OrderEntry peekHead() {
        return head;
    }

    public boolean isEmpty() {
        return head == null;
    }
}
