package com.exchange.ipc;

import java.nio.ByteBuffer;

/**
 * Tiny, mutable holder for a slot read from a {@link MmapRing}. The
 * {@link #buffer} is a view into the underlying mmap whose position/limit
 * have been adjusted to bracket the {@link #length} payload bytes; it is
 * only valid until the next {@link RingConsumer#poll(MutableSlice)} call.
 */
public final class MutableSlice {

    public ByteBuffer buffer;
    public int length;

    public MutableSlice() {
        // empty
    }

    public void reset() {
        this.buffer = null;
        this.length = 0;
    }
}
