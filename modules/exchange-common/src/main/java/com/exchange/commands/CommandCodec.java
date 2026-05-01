package com.exchange.commands;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Per-thread scratch buffers used by services that produce binary commands
 * onto an {@code MmapRing}. Keeps allocations off the hot path; returns
 * direct {@link ByteBuffer}s in little-endian order, ready to pass to
 * {@code RingProducer.offer(ByteBuffer)}.
 */
public final class CommandCodec {

    /** Holds at minimum the largest command (ExecutionReportCommand at 200 bytes). */
    public static final int MAX_COMMAND_SIZE = 256;

    private static final ThreadLocal<ByteBuffer> SCRATCH = ThreadLocal.withInitial(() -> {
        ByteBuffer b = ByteBuffer.allocateDirect(MAX_COMMAND_SIZE);
        b.order(ByteOrder.LITTLE_ENDIAN);
        return b;
    });

    private CommandCodec() {
        // utility class
    }

    /** Returns the cleared thread-local scratch buffer (limit=capacity, position=0). */
    public static ByteBuffer scratch() {
        ByteBuffer b = SCRATCH.get();
        b.clear();
        return b;
    }

    /** Read the discriminator byte (type) of a command at {@code offset}. */
    public static byte peekType(ByteBuffer in, int offset) {
        return in.get(offset);
    }
}
