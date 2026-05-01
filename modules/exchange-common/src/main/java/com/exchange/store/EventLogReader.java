package com.exchange.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static com.exchange.store.EventLog.INT_VIEW;
import static com.exchange.store.EventLog.LONG_VIEW;
import static com.exchange.store.EventLog.SLOT_LENGTH;
import static com.exchange.store.EventLog.SLOT_PAYLOAD;
import static com.exchange.store.EventLog.SLOT_SEQUENCE;
import static com.exchange.store.EventLog.SLOT_STATUS;
import static com.exchange.store.EventLog.SLOT_TERM;
import static com.exchange.store.EventLog.SLOT_TIMESTAMP;
import static com.exchange.store.EventLog.SLOT_TYPE;
import static com.exchange.store.EventLog.STATUS_COMMITTED;
import static com.exchange.store.EventLog.slotOffset;

/**
 * Independent reader over an {@link EventLog}. Each reader has its own
 * persistent cursor stored in {@code <log>.cursor.<name>} (an 8-byte mmap
 * holding the last-consumed slot index).
 * <p>
 * Allocation-free in steady state: {@link #poll(EventView)} reuses a
 * pre-duplicated payload {@link ByteBuffer} as the slice into the mmap.
 */
public final class EventLogReader implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(EventLogReader.class);

    private final EventLog log;
    private final String consumerName;
    private final MappedByteBuffer buffer;
    private final long slotCount;
    private final Path cursorPath;
    private final FileChannel cursorChannel;
    private final MappedByteBuffer cursorBuffer;
    /**
     * Pre-duplicated view over the underlying mmap. We mutate
     * position/limit on each poll instead of allocating a fresh slice.
     */
    private final ByteBuffer payloadView;

    /** Last consumed slot index; -1 means "none yet". */
    private long cursor;
    private boolean closed = false;

    EventLogReader(EventLog log, String consumerName, Path cursorPath) throws IOException {
        this.log = log;
        this.consumerName = consumerName;
        this.buffer = log.buffer();
        this.slotCount = log.slotCountLong();
        this.cursorPath = cursorPath;
        boolean newCursor = !Files.exists(cursorPath);
        Path parent = cursorPath.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        this.cursorChannel = FileChannel.open(cursorPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);
        try {
            if (cursorChannel.size() < 8) {
                cursorChannel.truncate(0);
                cursorChannel.position(0);
                ByteBuffer init = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
                init.putLong(-1L);
                init.flip();
                while (init.hasRemaining()) {
                    cursorChannel.write(init);
                }
            }
            this.cursorBuffer = cursorChannel.map(FileChannel.MapMode.READ_WRITE, 0, 8);
            this.cursorBuffer.order(ByteOrder.LITTLE_ENDIAN);
            this.cursor = newCursor ? -1L : (long) LONG_VIEW.getAcquire(cursorBuffer, 0);
        } catch (IOException | RuntimeException e) {
            try { cursorChannel.close(); } catch (IOException ignored) { /* best-effort */ }
            throw e;
        }

        // One-shot duplicate of the underlying mmap; reused for all polls.
        ByteBuffer dup = buffer.duplicate();
        dup.order(ByteOrder.LITTLE_ENDIAN);
        this.payloadView = dup;
    }

    /**
     * Poll for the next event. If a committed event is available it is
     * read into {@code into} and the cursor is advanced and persisted.
     * The {@code into.payloadBuffer()} slice is valid only until the next
     * call.
     *
     * @return true if an event was read, false otherwise.
     */
    public boolean poll(EventView into) {
        if (closed) {
            throw new IllegalStateException("Reader is closed");
        }
        long next = cursor + 1L;
        if (next >= slotCount) {
            return false;
        }
        int base = slotOffset(next);
        int status = (int) INT_VIEW.getAcquire(buffer, base + SLOT_STATUS);
        if (status != STATUS_COMMITTED) {
            return false;
        }
        // Acquire of status pairs with the writer's release; reads below
        // are guaranteed to see the writer's stores.
        int length = buffer.getInt(base + SLOT_LENGTH);
        long sequence = buffer.getLong(base + SLOT_SEQUENCE);
        long term = buffer.getLong(base + SLOT_TERM);
        long timestamp = buffer.getLong(base + SLOT_TIMESTAMP);
        short type = buffer.getShort(base + SLOT_TYPE);

        int payloadStart = base + SLOT_PAYLOAD;
        // Reuse the duplicated buffer as the payload slice. Position is
        // payloadStart, limit is payloadStart+length. Caller must consume
        // before next poll.
        payloadView.limit(buffer.capacity()); // ensure limit >= start+length
        payloadView.position(payloadStart);
        payloadView.limit(payloadStart + length);
        into.set(sequence, term, timestamp, type, length, payloadView);

        cursor = next;
        // Persist cursor with release.
        LONG_VIEW.setRelease(cursorBuffer, 0, cursor);
        return true;
    }

    public long cursor() {
        return cursor;
    }

    public String consumerName() {
        return consumerName;
    }

    public Path cursorPath() {
        return cursorPath;
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        try {
            cursorBuffer.force();
        } catch (Throwable t) {
            LOG.warn("force() on cursor {} failed: {}", cursorPath, t.toString());
        }
        try {
            cursorChannel.close();
        } catch (IOException e) {
            LOG.warn("Cursor channel close failed for {}: {}", cursorPath, e.toString());
        }
    }

    EventLog log() {
        return log;
    }
}
