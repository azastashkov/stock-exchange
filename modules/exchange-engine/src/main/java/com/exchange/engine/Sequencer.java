package com.exchange.engine;

/**
 * Single-threaded monotonically increasing order id generator.
 */
public final class Sequencer {

    private long next;

    public Sequencer() {
        this(1L);
    }

    public Sequencer(long start) {
        this.next = start;
    }

    public long nextOrderId() {
        return next++;
    }

    public long peek() {
        return next;
    }
}
