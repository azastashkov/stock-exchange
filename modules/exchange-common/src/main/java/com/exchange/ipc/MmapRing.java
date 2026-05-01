package com.exchange.ipc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Single-producer / single-consumer ring buffer over a memory-mapped file.
 * <p>
 * The buffer is sized at construction; {@code slotCount} must be a power
 * of two so the index wrap is a cheap mask. Each slot is a fixed
 * {@code slotSize} bytes, with the first 4 bytes reserved for an int32 LE
 * payload length and the remaining {@code slotSize-4} bytes for data.
 * <p>
 * Producer and consumer track sequence numbers in the header; full and
 * empty are detected by comparing them.
 */
public final class MmapRing implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(MmapRing.class);

    static final int HEADER_SIZE = 64;
    static final int LENGTH_PREFIX = 4;

    static final int OFF_MAGIC = 0;          // long
    static final int OFF_VERSION = 8;        // int
    static final int OFF_SLOT_SIZE = 12;     // int
    static final int OFF_SLOT_COUNT = 16;    // int
    static final int OFF_RESERVED_20 = 20;   // int (padding to 8-byte align next)
    static final int OFF_PRODUCER_SEQ = 24;  // long
    static final int OFF_CONSUMER_SEQ = 32;  // long
    // 40..63 padding

    static final int VERSION = 1;

    static final VarHandle INT_VIEW =
            MethodHandles.byteBufferViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);
    static final VarHandle LONG_VIEW =
            MethodHandles.byteBufferViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

    static long magicValue() {
        // 'EXCHGRNG' as 8 ASCII bytes read as a little-endian long.
        byte[] bytes = "EXCHGRNG".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
        long m = 0L;
        for (int i = 7; i >= 0; i--) {
            m = (m << 8) | (bytes[i] & 0xFFL);
        }
        return m;
    }

    private final Path path;
    private final FileChannel channel;
    private final MappedByteBuffer buffer;
    private final int slotSize;
    private final int slotCount;
    private final int slotMask;
    private final int payloadCapacity;

    private RingProducer producer;
    private RingConsumer consumer;
    private boolean closed = false;

    private MmapRing(Path path, FileChannel channel, MappedByteBuffer buffer, int slotSize, int slotCount) {
        this.path = path;
        this.channel = channel;
        this.buffer = buffer;
        this.slotSize = slotSize;
        this.slotCount = slotCount;
        this.slotMask = slotCount - 1;
        this.payloadCapacity = slotSize - LENGTH_PREFIX;
    }

    public static MmapRing create(Path path, int slotSize, int slotCount) throws IOException {
        if (slotCount <= 0 || (slotCount & (slotCount - 1)) != 0) {
            throw new IllegalArgumentException("slotCount must be a power of 2: " + slotCount);
        }
        if (slotSize < 8 || (slotSize % 8) != 0) {
            throw new IllegalArgumentException("slotSize must be a multiple of 8 and >= 8: " + slotSize);
        }
        long size = (long) HEADER_SIZE + (long) slotCount * slotSize;

        if (Files.exists(path)) {
            return openExisting(path);
        }

        Path parent = path.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        FileChannel ch = FileChannel.open(path,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);
        try {
            MappedByteBuffer buf = ch.map(FileChannel.MapMode.READ_WRITE, 0, size);
            buf.order(ByteOrder.LITTLE_ENDIAN);
            buf.putLong(OFF_MAGIC, magicValue());
            buf.putInt(OFF_VERSION, VERSION);
            buf.putInt(OFF_SLOT_SIZE, slotSize);
            buf.putInt(OFF_SLOT_COUNT, slotCount);
            buf.putInt(OFF_RESERVED_20, 0);
            buf.putLong(OFF_PRODUCER_SEQ, 0L);
            buf.putLong(OFF_CONSUMER_SEQ, 0L);
            buf.force();
            return new MmapRing(path, ch, buf, slotSize, slotCount);
        } catch (IOException | RuntimeException e) {
            try { ch.close(); } catch (IOException ignored) { /* best-effort */ }
            throw e;
        }
    }

    private static MmapRing openExisting(Path path) throws IOException {
        FileChannel ch = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
        try {
            long size = ch.size();
            if (size < HEADER_SIZE) {
                throw new IOException("Ring file too small: " + path);
            }
            MappedByteBuffer buf = ch.map(FileChannel.MapMode.READ_WRITE, 0, size);
            buf.order(ByteOrder.LITTLE_ENDIAN);
            long magic = buf.getLong(OFF_MAGIC);
            if (magic != magicValue()) {
                throw new IOException("Bad magic in ring " + path + ": 0x" + Long.toHexString(magic));
            }
            int version = buf.getInt(OFF_VERSION);
            if (version != VERSION) {
                throw new IOException("Unsupported ring version " + version + " in " + path);
            }
            int slotSize = buf.getInt(OFF_SLOT_SIZE);
            int slotCount = buf.getInt(OFF_SLOT_COUNT);
            long expected = (long) HEADER_SIZE + (long) slotCount * slotSize;
            if (expected != size) {
                throw new IOException("Inconsistent ring file size for " + path);
            }
            return new MmapRing(path, ch, buf, slotSize, slotCount);
        } catch (IOException | RuntimeException e) {
            try { ch.close(); } catch (IOException ignored) { /* best-effort */ }
            throw e;
        }
    }

    public synchronized RingProducer producer() {
        ensureOpen();
        if (producer == null) {
            producer = new RingProducer(this);
        }
        return producer;
    }

    public synchronized RingConsumer consumer() {
        ensureOpen();
        if (consumer == null) {
            consumer = new RingConsumer(this);
        }
        return consumer;
    }

    public int slotSize() { return slotSize; }
    public int slotCount() { return slotCount; }
    public int payloadCapacity() { return payloadCapacity; }
    public Path path() { return path; }

    /**
     * Current value of the producer sequence as observed via an acquire-load
     * of the header. Intended for monitoring/gauges (depth = producer -
     * consumer); not for control flow on the hot path.
     */
    public long producerCursor() {
        return (long) LONG_VIEW.getAcquire(buffer, OFF_PRODUCER_SEQ);
    }

    /**
     * Current value of the consumer sequence as observed via an acquire-load
     * of the header. Intended for monitoring/gauges; not for control flow.
     */
    public long consumerCursor() {
        return (long) LONG_VIEW.getAcquire(buffer, OFF_CONSUMER_SEQ);
    }

    int slotMask() { return slotMask; }
    MappedByteBuffer buffer() { return buffer; }

    int slotOffset(long sequence) {
        int idx = (int) (sequence & slotMask);
        return HEADER_SIZE + idx * slotSize;
    }

    @Override
    public synchronized void close() {
        if (closed) return;
        closed = true;
        try {
            buffer.force();
        } catch (Throwable t) {
            LOG.warn("force() on ring {} failed: {}", path, t.toString());
        }
        try {
            channel.close();
        } catch (IOException e) {
            LOG.warn("ring channel close failed for {}: {}", path, e.toString());
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("MmapRing is closed: " + path);
        }
    }
}
