package com.exchange.store;

import java.nio.ByteBuffer;

/**
 * Mutable, reusable view of a single event read from the log. Populated by
 * {@link EventLogReader#poll(EventView)}; the {@link #payloadBuffer} is a
 * duplicate of the underlying mmap whose position/limit have been adjusted
 * to bracket the payload. The slice is only valid until the next call to
 * {@code poll} that reuses this view.
 */
public final class EventView {

    private long sequence;
    private long term;
    private long timestampNanos;
    private short type;
    private int payloadLength;
    private ByteBuffer payloadBuffer;

    public long sequence() { return sequence; }
    public long term() { return term; }
    public long timestampNanos() { return timestampNanos; }
    public short type() { return type; }
    public int payloadLength() { return payloadLength; }

    /**
     * Returns the payload as a {@link ByteBuffer} positioned at the start
     * of the payload bytes with limit set to position+payloadLength. The
     * buffer is a view into the mmap and MUST NOT be retained beyond the
     * next poll that reuses this view.
     */
    public ByteBuffer payloadBuffer() { return payloadBuffer; }

    /**
     * Populate this view. Called by the reader. The caller has already
     * configured the position/limit of {@code payloadBuffer}.
     */
    void set(long sequence, long term, long timestampNanos, short type, int payloadLength, ByteBuffer payloadBuffer) {
        this.sequence = sequence;
        this.term = term;
        this.timestampNanos = timestampNanos;
        this.type = type;
        this.payloadLength = payloadLength;
        this.payloadBuffer = payloadBuffer;
    }

    /** Reset to defaults; chiefly useful for tests. */
    public void reset() {
        this.sequence = 0L;
        this.term = 0L;
        this.timestampNanos = 0L;
        this.type = 0;
        this.payloadLength = 0;
        this.payloadBuffer = null;
    }
}
