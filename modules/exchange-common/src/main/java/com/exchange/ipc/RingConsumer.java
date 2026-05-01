package com.exchange.ipc;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;

import static com.exchange.ipc.MmapRing.LENGTH_PREFIX;
import static com.exchange.ipc.MmapRing.LONG_VIEW;
import static com.exchange.ipc.MmapRing.OFF_CONSUMER_SEQ;
import static com.exchange.ipc.MmapRing.OFF_PRODUCER_SEQ;

/**
 * Single-consumer side of a {@link MmapRing}. Not thread-safe; only one
 * consumer per ring is permitted.
 */
public final class RingConsumer {

    private final MmapRing ring;
    private final MappedByteBuffer buffer;
    /** Pre-duplicated payload view; reused across polls. */
    private final ByteBuffer payloadView;
    private long consumerSeqLocal;
    private long cachedProducerSeq;

    RingConsumer(MmapRing ring) {
        this.ring = ring;
        this.buffer = ring.buffer();
        this.consumerSeqLocal = (long) LONG_VIEW.getAcquire(buffer, OFF_CONSUMER_SEQ);
        this.cachedProducerSeq = (long) LONG_VIEW.getAcquire(buffer, OFF_PRODUCER_SEQ);
        ByteBuffer dup = buffer.duplicate();
        dup.order(ByteOrder.LITTLE_ENDIAN);
        this.payloadView = dup;
    }

    public boolean poll(MutableSlice into) {
        if (consumerSeqLocal >= cachedProducerSeq) {
            cachedProducerSeq = (long) LONG_VIEW.getAcquire(buffer, OFF_PRODUCER_SEQ);
            if (consumerSeqLocal >= cachedProducerSeq) {
                return false;
            }
        }
        int base = ring.slotOffset(consumerSeqLocal);
        int length = buffer.getInt(base);
        int payloadStart = base + LENGTH_PREFIX;
        payloadView.limit(buffer.capacity());
        payloadView.position(payloadStart);
        payloadView.limit(payloadStart + length);
        into.buffer = payloadView;
        into.length = length;

        long newSeq = consumerSeqLocal + 1L;
        LONG_VIEW.setRelease(buffer, OFF_CONSUMER_SEQ, newSeq);
        consumerSeqLocal = newSeq;
        return true;
    }

    public long consumerSequence() {
        return consumerSeqLocal;
    }
}
