package com.exchange.engine;

import java.util.ArrayDeque;

/**
 * Free-list pool of {@link OrderEntry} nodes. The matching engine acquires
 * an entry per resting order and releases it on fill or cancel. Acquire is
 * O(1); fresh allocation only happens during warm-up or if the pool's high
 * water mark is exceeded.
 */
public final class OrderEntryPool {

    private final ArrayDeque<OrderEntry> free;

    public OrderEntryPool(int sizeHint) {
        this.free = new ArrayDeque<>(Math.max(16, sizeHint));
        for (int i = 0; i < sizeHint; i++) {
            free.push(new OrderEntry());
        }
    }

    /**
     * Returns a clean entry. Allocates if the free-list is empty.
     */
    public OrderEntry acquire() {
        OrderEntry e = free.pollFirst();
        if (e == null) {
            return new OrderEntry();
        }
        return e;
    }

    /**
     * Returns an entry to the pool after resetting it.
     */
    public void release(OrderEntry e) {
        if (e == null) return;
        e.reset();
        free.push(e);
    }

    public int freeSize() {
        return free.size();
    }
}
