package com.exchange.ipc;

import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

import static com.exchange.ipc.MmapRing.LENGTH_PREFIX;
import static com.exchange.ipc.MmapRing.LONG_VIEW;
import static com.exchange.ipc.MmapRing.OFF_CONSUMER_SEQ;
import static com.exchange.ipc.MmapRing.OFF_PRODUCER_SEQ;

/**
 * Single-producer side of a {@link MmapRing}. Not thread-safe; only one
 * producer per ring is permitted.
 */
public final class RingProducer {

    private final MmapRing ring;
    private final MappedByteBuffer buffer;
    private final int slotCount;
    private final int payloadCapacity;
    private long producerSeqLocal;
    private long cachedConsumerSeq;

    RingProducer(MmapRing ring) {
        this.ring = ring;
        this.buffer = ring.buffer();
        this.slotCount = ring.slotCount();
        this.payloadCapacity = ring.payloadCapacity();
        this.producerSeqLocal = (long) LONG_VIEW.getAcquire(buffer, OFF_PRODUCER_SEQ);
        this.cachedConsumerSeq = (long) LONG_VIEW.getAcquire(buffer, OFF_CONSUMER_SEQ);
    }

    public boolean offer(byte[] data, int offset, int length) {
        if (length < 0 || length > payloadCapacity) {
            throw new IllegalArgumentException("length out of range: " + length + " (cap=" + payloadCapacity + ")");
        }
        if (offset < 0 || offset + length > data.length) {
            throw new IndexOutOfBoundsException("offset/length out of bounds for array of size " + data.length);
        }
        if (isFull()) return false;

        int base = ring.slotOffset(producerSeqLocal);
        // length first
        buffer.putInt(base, length);
        for (int i = 0; i < length; i++) {
            buffer.put(base + LENGTH_PREFIX + i, data[offset + i]);
        }
        VarHandle.releaseFence();
        long newSeq = producerSeqLocal + 1L;
        LONG_VIEW.setRelease(buffer, OFF_PRODUCER_SEQ, newSeq);
        producerSeqLocal = newSeq;
        return true;
    }

    public boolean offer(ByteBuffer payload) {
        int remaining = payload.remaining();
        if (remaining > payloadCapacity) {
            throw new IllegalArgumentException("payload too large: " + remaining + " (cap=" + payloadCapacity + ")");
        }
        if (isFull()) return false;

        int base = ring.slotOffset(producerSeqLocal);
        buffer.putInt(base, remaining);
        int srcPos = payload.position();
        for (int i = 0; i < remaining; i++) {
            buffer.put(base + LENGTH_PREFIX + i, payload.get(srcPos + i));
        }
        VarHandle.releaseFence();
        long newSeq = producerSeqLocal + 1L;
        LONG_VIEW.setRelease(buffer, OFF_PRODUCER_SEQ, newSeq);
        producerSeqLocal = newSeq;
        return true;
    }

    private boolean isFull() {
        // Use the cached consumer sequence first; only re-read with acquire
        // if the cache says we're full. This minimises cross-core traffic.
        if (producerSeqLocal - cachedConsumerSeq < slotCount) {
            return false;
        }
        cachedConsumerSeq = (long) LONG_VIEW.getAcquire(buffer, OFF_CONSUMER_SEQ);
        return producerSeqLocal - cachedConsumerSeq >= slotCount;
    }

    public long producerSequence() {
        return producerSeqLocal;
    }
}
