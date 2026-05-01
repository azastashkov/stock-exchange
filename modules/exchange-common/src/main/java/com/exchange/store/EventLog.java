package com.exchange.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;

/**
 * Append-only canonical event log backed by a memory-mapped file.
 * <p>
 * Layout: 64-byte header followed by {@code slotCount} fixed-size 256-byte
 * slots. One writer (the matching engine on the leader); many independent
 * readers each tracking their own cursor on disk.
 */
public final class EventLog implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(EventLog.class);

    static final int VERSION = 1;
    static final int SLOT_SIZE = 256;
    /** Public slot-size constant used by Raft transport to validate raw slot frames. */
    public static final int SLOT_SIZE_BYTES = SLOT_SIZE;
    static final int HEADER_SIZE = 64;

    /** Maximum payload bytes that fit in one slot (256 - 34 header bytes). */
    public static final int MAX_PAYLOAD = 222;

    // Header layout
    static final int OFF_MAGIC = 0;          // long
    static final int OFF_VERSION = 8;        // int
    static final int OFF_SLOT_SIZE = 12;     // int
    static final int OFF_SLOT_COUNT = 16;    // long
    static final int OFF_HEADER_SIZE = 24;   // int
    static final int OFF_RESERVED_28 = 28;   // int
    static final int OFF_WRITER_SEQUENCE = 32; // long
    static final int OFF_WRITER_SLOT_INDEX = 40; // long
    // 48..63 reserved

    // Slot layout (offsets within a slot)
    static final int SLOT_STATUS = 0;        // int (4)
    static final int SLOT_LENGTH = 4;        // int (4)
    static final int SLOT_SEQUENCE = 8;      // long (8)
    static final int SLOT_TERM = 16;         // long (8)
    static final int SLOT_TIMESTAMP = 24;    // long (8)
    static final int SLOT_TYPE = 32;         // short (2)
    static final int SLOT_PAYLOAD = 34;      // 222 bytes

    public static final int STATUS_EMPTY = 0;
    public static final int STATUS_WRITING = 1;
    public static final int STATUS_COMMITTED = 2;

    // VarHandles on the underlying ByteBuffer for release/acquire memory ordering.
    static final VarHandle INT_VIEW =
            MethodHandles.byteBufferViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);
    static final VarHandle LONG_VIEW =
            MethodHandles.byteBufferViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

    /**
     * Magic chosen to spell "EXCHGLOG" as 8 ASCII bytes when read as a
     * little-endian long. Recompute that here directly so we don't depend
     * on a hand-typed hex constant.
     */
    static long magicValue() {
        byte[] bytes = "EXCHGLOG".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
        long m = 0L;
        for (int i = 7; i >= 0; i--) {
            m = (m << 8) | (bytes[i] & 0xFFL);
        }
        return m;
    }

    private final Path path;
    private final FileChannel channel;
    private final MappedByteBuffer buffer;
    private final long slotCount;

    private final Map<String, EventLogReader> readers = new HashMap<>();
    private EventLogWriter writer;
    private boolean closed = false;

    private EventLog(Path path, FileChannel channel, MappedByteBuffer buffer, long slotCount) {
        this.path = path;
        this.channel = channel;
        this.buffer = buffer;
        this.slotCount = slotCount;
    }

    public static EventLog open(Path file, int slotCount) throws IOException {
        if (slotCount <= 0) {
            throw new IllegalArgumentException("slotCount must be positive");
        }
        long size = (long) HEADER_SIZE + (long) slotCount * SLOT_SIZE;

        if (Files.exists(file)) {
            return openExisting(file);
        }

        Path parent = file.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        FileChannel ch = FileChannel.open(file,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);
        try {
            MappedByteBuffer buf = ch.map(FileChannel.MapMode.READ_WRITE, 0, size);
            buf.order(ByteOrder.LITTLE_ENDIAN);
            // write header
            buf.putLong(OFF_MAGIC, magicValue());
            buf.putInt(OFF_VERSION, VERSION);
            buf.putInt(OFF_SLOT_SIZE, SLOT_SIZE);
            buf.putLong(OFF_SLOT_COUNT, slotCount);
            buf.putInt(OFF_HEADER_SIZE, HEADER_SIZE);
            buf.putInt(OFF_RESERVED_28, 0);
            buf.putLong(OFF_WRITER_SEQUENCE, 0L);
            buf.putLong(OFF_WRITER_SLOT_INDEX, 0L);
            buf.putLong(48, 0L);
            buf.putLong(56, 0L);
            buf.force();
            return new EventLog(file, ch, buf, slotCount);
        } catch (IOException | RuntimeException e) {
            try { ch.close(); } catch (IOException ignored) { /* best-effort */ }
            throw e;
        }
    }

    public static EventLog open(Path file) throws IOException {
        return openExisting(file);
    }

    private static EventLog openExisting(Path file) throws IOException {
        FileChannel ch = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE);
        try {
            long size = ch.size();
            if (size < HEADER_SIZE) {
                throw new IOException("Event log file too small: " + file);
            }
            MappedByteBuffer buf = ch.map(FileChannel.MapMode.READ_WRITE, 0, size);
            buf.order(ByteOrder.LITTLE_ENDIAN);
            long magic = buf.getLong(OFF_MAGIC);
            if (magic != magicValue()) {
                throw new IOException("Bad magic in event log " + file + ": 0x" + Long.toHexString(magic));
            }
            int version = buf.getInt(OFF_VERSION);
            if (version != VERSION) {
                throw new IOException("Unsupported event log version " + version + " in " + file);
            }
            int slotSize = buf.getInt(OFF_SLOT_SIZE);
            if (slotSize != SLOT_SIZE) {
                throw new IOException("Unexpected slot size " + slotSize + " in " + file);
            }
            long slotCount = buf.getLong(OFF_SLOT_COUNT);
            long expected = (long) HEADER_SIZE + slotCount * SLOT_SIZE;
            if (expected != size) {
                throw new IOException("Inconsistent file size: header expects " + expected + " but file is " + size);
            }
            return new EventLog(file, ch, buf, slotCount);
        } catch (IOException | RuntimeException e) {
            try { ch.close(); } catch (IOException ignored) { /* best-effort */ }
            throw e;
        }
    }

    public synchronized EventLogWriter writer() {
        ensureOpen();
        if (writer == null) {
            writer = new EventLogWriter(this);
        }
        return writer;
    }

    public synchronized EventLogReader reader(String consumerName) {
        ensureOpen();
        EventLogReader existing = readers.get(consumerName);
        if (existing != null) {
            return existing;
        }
        Path cursorPath = path.resolveSibling(path.getFileName() + ".cursor." + consumerName);
        try {
            EventLogReader reader = new EventLogReader(this, consumerName, cursorPath);
            readers.put(consumerName, reader);
            return reader;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to open cursor for consumer " + consumerName, e);
        }
    }

    public long lastSequence() {
        return (long) LONG_VIEW.getAcquire(buffer, OFF_WRITER_SEQUENCE);
    }

    public long lastSlotIndex() {
        return (long) LONG_VIEW.getAcquire(buffer, OFF_WRITER_SLOT_INDEX);
    }

    /**
     * Copy out the entire {@value #SLOT_SIZE} bytes of the slot at
     * {@code slotIndex}, regardless of its status. Used by the Raft
     * leader to ship raw slots over the wire.
     */
    public byte[] readSlotRaw(long slotIndex) {
        ensureOpen();
        if (slotIndex < 0L || slotIndex >= slotCount) {
            throw new IndexOutOfBoundsException("slotIndex out of range: " + slotIndex);
        }
        int base = slotOffset(slotIndex);
        byte[] out = new byte[SLOT_SIZE];
        for (int i = 0; i < SLOT_SIZE; i++) {
            out[i] = buffer.get(base + i);
        }
        return out;
    }

    /**
     * Read the {@code term} field of the slot at {@code slotIndex} without
     * disturbing reader cursors. Used by AppendEntries consistency checks.
     * Returns -1 for an out-of-range or empty slot.
     */
    public long readSlotTermAt(long slotIndex) {
        ensureOpen();
        if (slotIndex < 0L || slotIndex >= slotCount) {
            return -1L;
        }
        int base = slotOffset(slotIndex);
        int status = (int) INT_VIEW.getAcquire(buffer, base + SLOT_STATUS);
        if (status == STATUS_EMPTY) {
            return -1L;
        }
        return buffer.getLong(base + SLOT_TERM);
    }

    /**
     * Install a fully-formed 256-byte slot at {@code slotIndex}, overwriting
     * whatever was there. The status byte is forced to {@link #STATUS_WRITING}
     * regardless of the source bytes — commit comes from the CommitApplier.
     * Updates the writerSequence/writerSlotIndex header pointers if the
     * supplied index extends the log forward.
     */
    public void installSlot(long slotIndex, byte[] rawSlotBytes) {
        ensureOpen();
        if (slotIndex < 0L || slotIndex >= slotCount) {
            throw new IndexOutOfBoundsException("slotIndex out of range: " + slotIndex);
        }
        if (rawSlotBytes == null || rawSlotBytes.length != SLOT_SIZE) {
            throw new IllegalArgumentException("rawSlotBytes must be exactly " + SLOT_SIZE + " bytes");
        }
        int base = slotOffset(slotIndex);
        // Mark the slot in-progress before writing payload, so partial writes
        // are never observable as COMMITTED.
        INT_VIEW.setRelease(buffer, base + SLOT_STATUS, STATUS_EMPTY);
        // Skip the first 4 bytes (status); we overwrite the rest verbatim.
        for (int i = 4; i < SLOT_SIZE; i++) {
            buffer.put(base + i, rawSlotBytes[i]);
        }
        // Force status to WRITING — commit must be a separate publish step.
        INT_VIEW.setRelease(buffer, base + SLOT_STATUS, STATUS_WRITING);

        // Bump writer pointers forward if this install extended the log.
        long sequence = buffer.getLong(base + SLOT_SEQUENCE);
        long currentLastIdx = lastSlotIndex();
        if (slotIndex + 1L > currentLastIdx) {
            LONG_VIEW.setRelease(buffer, OFF_WRITER_SLOT_INDEX, slotIndex + 1L);
        }
        if (sequence > lastSequence()) {
            LONG_VIEW.setRelease(buffer, OFF_WRITER_SEQUENCE, sequence);
        }
    }

    /**
     * Discard everything strictly after {@code slotIndex}: writer-pointers
     * roll back to {@code slotIndex+1}, every later slot is zeroed to
     * {@link #STATUS_EMPTY}. {@code slotIndex == -1} truncates back to an
     * empty log.
     */
    public void truncateAfter(long slotIndex) {
        ensureOpen();
        if (slotIndex < -1L || slotIndex >= slotCount) {
            throw new IndexOutOfBoundsException("slotIndex out of range: " + slotIndex);
        }
        long firstToWipe = slotIndex + 1L;
        for (long i = firstToWipe; i < slotCount; i++) {
            int base = slotOffset(i);
            int status = (int) INT_VIEW.getAcquire(buffer, base + SLOT_STATUS);
            if (status == STATUS_EMPTY) {
                continue;
            }
            // Zero the metadata so a later resume cannot observe stale data.
            buffer.putInt(base + SLOT_LENGTH, 0);
            buffer.putLong(base + SLOT_SEQUENCE, 0L);
            buffer.putLong(base + SLOT_TERM, 0L);
            buffer.putLong(base + SLOT_TIMESTAMP, 0L);
            buffer.putShort(base + SLOT_TYPE, (short) 0);
            INT_VIEW.setRelease(buffer, base + SLOT_STATUS, STATUS_EMPTY);
        }
        LONG_VIEW.setRelease(buffer, OFF_WRITER_SLOT_INDEX, firstToWipe);
        // Recompute last sequence as the sequence of the last surviving slot.
        long newLastSeq = 0L;
        if (firstToWipe > 0L) {
            int prevBase = slotOffset(firstToWipe - 1L);
            int prevStatus = (int) INT_VIEW.getAcquire(buffer, prevBase + SLOT_STATUS);
            if (prevStatus != STATUS_EMPTY) {
                newLastSeq = buffer.getLong(prevBase + SLOT_SEQUENCE);
            }
        }
        LONG_VIEW.setRelease(buffer, OFF_WRITER_SEQUENCE, newLastSeq);
    }

    /**
     * Flip the slot at {@code slotIndex} from {@link #STATUS_WRITING} to
     * {@link #STATUS_COMMITTED} with a release store. Idempotent: a no-op
     * if the slot is already committed. Throws if the slot is empty.
     */
    public void commit(long slotIndex) {
        ensureOpen();
        if (slotIndex < 0L || slotIndex >= slotCount) {
            throw new IndexOutOfBoundsException("slotIndex out of range: " + slotIndex);
        }
        int base = slotOffset(slotIndex);
        int status = (int) INT_VIEW.getAcquire(buffer, base + SLOT_STATUS);
        if (status == STATUS_COMMITTED) {
            return;
        }
        if (status != STATUS_WRITING) {
            throw new IllegalStateException("Cannot commit slot " + slotIndex + " in status " + status);
        }
        java.lang.invoke.VarHandle.releaseFence();
        INT_VIEW.setRelease(buffer, base + SLOT_STATUS, STATUS_COMMITTED);
    }

    /**
     * Return the status of a slot. Used by tests and the CommitApplier.
     * Returns {@link #STATUS_EMPTY} for out-of-range indices.
     */
    public int slotStatus(long slotIndex) {
        ensureOpen();
        if (slotIndex < 0L || slotIndex >= slotCount) {
            return STATUS_EMPTY;
        }
        int base = slotOffset(slotIndex);
        return (int) INT_VIEW.getAcquire(buffer, base + SLOT_STATUS);
    }

    public int slotCount() {
        return Math.toIntExact(slotCount);
    }

    public long slotCountLong() {
        return slotCount;
    }

    public int slotSize() {
        return SLOT_SIZE;
    }

    public Path path() {
        return path;
    }

    /** Internal: shared mmap buffer for writer/reader. */
    MappedByteBuffer buffer() {
        return buffer;
    }

    static int slotOffset(long index) {
        return Math.toIntExact(HEADER_SIZE + index * SLOT_SIZE);
    }

    @Override
    public synchronized void close() {
        if (closed) return;
        closed = true;
        for (EventLogReader r : readers.values()) {
            try { r.close(); } catch (Exception e) { LOG.warn("Reader close failed", e); }
        }
        readers.clear();
        try {
            buffer.force();
        } catch (Throwable t) {
            LOG.warn("force() on event log {} failed: {}", path, t.toString());
        }
        try {
            channel.close();
        } catch (IOException e) {
            LOG.warn("channel close failed for {}: {}", path, e.toString());
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("EventLog is closed: " + path);
        }
    }
}
