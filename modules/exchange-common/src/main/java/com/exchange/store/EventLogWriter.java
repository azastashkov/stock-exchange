package com.exchange.store;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

import static com.exchange.store.EventLog.INT_VIEW;
import static com.exchange.store.EventLog.LONG_VIEW;
import static com.exchange.store.EventLog.MAX_PAYLOAD;
import static com.exchange.store.EventLog.OFF_WRITER_SEQUENCE;
import static com.exchange.store.EventLog.OFF_WRITER_SLOT_INDEX;
import static com.exchange.store.EventLog.SLOT_LENGTH;
import static com.exchange.store.EventLog.SLOT_PAYLOAD;
import static com.exchange.store.EventLog.SLOT_SEQUENCE;
import static com.exchange.store.EventLog.SLOT_STATUS;
import static com.exchange.store.EventLog.SLOT_TERM;
import static com.exchange.store.EventLog.SLOT_TIMESTAMP;
import static com.exchange.store.EventLog.SLOT_TYPE;
import static com.exchange.store.EventLog.STATUS_COMMITTED;
import static com.exchange.store.EventLog.STATUS_EMPTY;
import static com.exchange.store.EventLog.STATUS_WRITING;
import static com.exchange.store.EventLog.slotOffset;

/**
 * Single-writer appender for an {@link EventLog}. The writer publishes new
 * events by:
 * <ol>
 *   <li>Locating the next slot via the header's {@code writerSlotIndex}.</li>
 *   <li>Asserting it is {@link EventLog#STATUS_EMPTY}; otherwise throwing
 *       {@link LogFullException}.</li>
 *   <li>Writing the slot status to {@code WRITING} and filling all metadata
 *       and payload bytes.</li>
 *   <li>Issuing a release fence and flipping status to {@code COMMITTED}
 *       with {@code setRelease}.</li>
 *   <li>Advancing the header's {@code writerSlotIndex} and
 *       {@code writerSequence} (release).</li>
 * </ol>
 */
public final class EventLogWriter {

    private final EventLog log;
    private final MappedByteBuffer buffer;
    private final long slotCount;
    private long nextSlotIndex;
    private long nextSequence;

    EventLogWriter(EventLog log) {
        this.log = log;
        this.buffer = log.buffer();
        this.slotCount = log.slotCountLong();
        // Resume from where the header says we left off.
        this.nextSlotIndex = log.lastSlotIndex();
        this.nextSequence = log.lastSequence() + 1L;
    }

    public long append(short type, long term, long timestampNanos, byte[] payload, int offset, int length) {
        if (length < 0) {
            throw new IllegalArgumentException("length must be non-negative");
        }
        if (length > MAX_PAYLOAD) {
            throw new IllegalArgumentException("payload exceeds MAX_PAYLOAD=" + MAX_PAYLOAD + ", got " + length);
        }
        if (offset < 0 || offset + length > payload.length) {
            throw new IndexOutOfBoundsException("offset/length out of bounds for payload of size " + payload.length);
        }
        long slotIdx = nextSlotIndex;
        if (slotIdx >= slotCount) {
            throw new LogFullException("Event log full: slot index " + slotIdx + " >= slotCount " + slotCount);
        }
        int base = slotOffset(slotIdx);
        int statusOff = base + SLOT_STATUS;
        int existing = (int) INT_VIEW.getAcquire(buffer, statusOff);
        if (existing != STATUS_EMPTY) {
            throw new LogFullException("Slot " + slotIdx + " not empty (status=" + existing + ")");
        }

        // Mark writing first (plain put).
        buffer.putInt(statusOff, STATUS_WRITING);

        // Header fields
        buffer.putInt(base + SLOT_LENGTH, length);
        buffer.putLong(base + SLOT_SEQUENCE, nextSequence);
        buffer.putLong(base + SLOT_TERM, term);
        buffer.putLong(base + SLOT_TIMESTAMP, timestampNanos);
        buffer.putShort(base + SLOT_TYPE, type);

        // Payload bytes
        int payloadOff = base + SLOT_PAYLOAD;
        for (int i = 0; i < length; i++) {
            buffer.put(payloadOff + i, payload[offset + i]);
        }

        // Publish: release-store the status to COMMITTED so any reader that
        // observes COMMITTED also observes the header/payload fields above.
        VarHandle_releaseFence();
        INT_VIEW.setRelease(buffer, statusOff, STATUS_COMMITTED);

        long assigned = nextSequence;
        nextSlotIndex = slotIdx + 1L;
        nextSequence = assigned + 1L;

        // Update header pointers (release so readers see them on acquire).
        LONG_VIEW.setRelease(buffer, OFF_WRITER_SEQUENCE, assigned);
        LONG_VIEW.setRelease(buffer, OFF_WRITER_SLOT_INDEX, nextSlotIndex);
        return assigned;
    }

    public long append(short type, long term, long timestampNanos, ByteBuffer payload) {
        int remaining = payload.remaining();
        if (remaining > MAX_PAYLOAD) {
            throw new IllegalArgumentException("payload exceeds MAX_PAYLOAD=" + MAX_PAYLOAD + ", got " + remaining);
        }
        long slotIdx = nextSlotIndex;
        if (slotIdx >= slotCount) {
            throw new LogFullException("Event log full: slot index " + slotIdx + " >= slotCount " + slotCount);
        }
        int base = slotOffset(slotIdx);
        int statusOff = base + SLOT_STATUS;
        int existing = (int) INT_VIEW.getAcquire(buffer, statusOff);
        if (existing != STATUS_EMPTY) {
            throw new LogFullException("Slot " + slotIdx + " not empty (status=" + existing + ")");
        }

        buffer.putInt(statusOff, STATUS_WRITING);
        buffer.putInt(base + SLOT_LENGTH, remaining);
        buffer.putLong(base + SLOT_SEQUENCE, nextSequence);
        buffer.putLong(base + SLOT_TERM, term);
        buffer.putLong(base + SLOT_TIMESTAMP, timestampNanos);
        buffer.putShort(base + SLOT_TYPE, type);

        // Copy payload bytes without mutating the caller's payload position.
        int payloadOff = base + SLOT_PAYLOAD;
        int srcPos = payload.position();
        for (int i = 0; i < remaining; i++) {
            buffer.put(payloadOff + i, payload.get(srcPos + i));
        }

        VarHandle_releaseFence();
        INT_VIEW.setRelease(buffer, statusOff, STATUS_COMMITTED);

        long assigned = nextSequence;
        nextSlotIndex = slotIdx + 1L;
        nextSequence = assigned + 1L;

        LONG_VIEW.setRelease(buffer, OFF_WRITER_SEQUENCE, assigned);
        LONG_VIEW.setRelease(buffer, OFF_WRITER_SLOT_INDEX, nextSlotIndex);
        return assigned;
    }

    /**
     * Like {@link #append(short, long, long, ByteBuffer)} but leaves the
     * slot in {@link EventLog#STATUS_WRITING}. Used by the Raft leader so
     * the entry is held back from downstream consumers until the
     * CommitApplier has confirmed majority replication and flips it to
     * COMMITTED. Returns the slot index that was written.
     */
    public long appendUncommitted(short type, long term, long timestampNanos, ByteBuffer payload) {
        int remaining = payload.remaining();
        if (remaining > MAX_PAYLOAD) {
            throw new IllegalArgumentException("payload exceeds MAX_PAYLOAD=" + MAX_PAYLOAD + ", got " + remaining);
        }
        long slotIdx = nextSlotIndex;
        if (slotIdx >= slotCount) {
            throw new LogFullException("Event log full: slot index " + slotIdx + " >= slotCount " + slotCount);
        }
        int base = slotOffset(slotIdx);
        int statusOff = base + SLOT_STATUS;
        int existing = (int) INT_VIEW.getAcquire(buffer, statusOff);
        if (existing != STATUS_EMPTY) {
            throw new LogFullException("Slot " + slotIdx + " not empty (status=" + existing + ")");
        }

        // Mark WRITING and stay there. The CommitApplier flips to COMMITTED.
        INT_VIEW.setRelease(buffer, statusOff, STATUS_WRITING);
        buffer.putInt(base + SLOT_LENGTH, remaining);
        buffer.putLong(base + SLOT_SEQUENCE, nextSequence);
        buffer.putLong(base + SLOT_TERM, term);
        buffer.putLong(base + SLOT_TIMESTAMP, timestampNanos);
        buffer.putShort(base + SLOT_TYPE, type);

        int payloadOff = base + SLOT_PAYLOAD;
        int srcPos = payload.position();
        for (int i = 0; i < remaining; i++) {
            buffer.put(payloadOff + i, payload.get(srcPos + i));
        }

        long assigned = nextSequence;
        nextSlotIndex = slotIdx + 1L;
        nextSequence = assigned + 1L;

        // Header pointers must advance even though status remains WRITING,
        // so a leader restart can find the durable tail.
        LONG_VIEW.setRelease(buffer, OFF_WRITER_SEQUENCE, assigned);
        LONG_VIEW.setRelease(buffer, OFF_WRITER_SLOT_INDEX, nextSlotIndex);
        return slotIdx;
    }

    public long nextSequence() {
        return nextSequence;
    }

    public long nextSlotIndex() {
        return nextSlotIndex;
    }

    /** Tiny indirection so the release fence is named explicitly. */
    private static void VarHandle_releaseFence() {
        java.lang.invoke.VarHandle.releaseFence();
    }

    EventLog log() {
        return log;
    }
}
