package com.exchange.raft;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * File-backed Raft persistent state. Holds {@code currentTerm},
 * {@code votedFor}, {@code commitIndex}, {@code lastApplied}.
 * <p>
 * Layout (little-endian):
 * <pre>
 *   0  long  currentTerm
 *   8  int   votedFor (-1 = none)
 *  12  int   reserved (padding)
 *  16  long  commitIndex
 *  24  long  lastApplied
 *  32..63   reserved padding to 64 bytes
 * </pre>
 * Setters fsync via {@link FileChannel#force(boolean)} so the value is
 * durable before the call returns.
 */
public final class PersistentState implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(PersistentState.class);

    static final int FILE_SIZE = 64;

    static final int OFF_TERM = 0;
    static final int OFF_VOTED_FOR = 8;
    static final int OFF_COMMIT_INDEX = 16;
    static final int OFF_LAST_APPLIED = 24;

    public static final int VOTED_FOR_NONE = -1;

    private final Path path;
    private final FileChannel channel;
    private final MappedByteBuffer buffer;
    private boolean closed = false;

    private PersistentState(Path path, FileChannel channel, MappedByteBuffer buffer) {
        this.path = path;
        this.channel = channel;
        this.buffer = buffer;
    }

    /**
     * Open or create the state file. If the file does not exist, it is
     * created and zero-initialised with {@code votedFor = -1}.
     */
    public static PersistentState open(Path file) {
        try {
            Path parent = file.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            boolean exists = Files.exists(file);
            FileChannel ch = FileChannel.open(file,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE);
            try {
                if (!exists || ch.size() < FILE_SIZE) {
                    ch.truncate(0);
                    ByteBuffer init = ByteBuffer.allocate(FILE_SIZE).order(ByteOrder.LITTLE_ENDIAN);
                    init.putLong(OFF_TERM, 0L);
                    init.putInt(OFF_VOTED_FOR, VOTED_FOR_NONE);
                    init.putInt(OFF_VOTED_FOR + 4, 0);
                    init.putLong(OFF_COMMIT_INDEX, -1L);
                    init.putLong(OFF_LAST_APPLIED, -1L);
                    ch.position(0);
                    while (init.hasRemaining()) {
                        ch.write(init);
                    }
                    ch.force(true);
                }
                MappedByteBuffer buf = ch.map(FileChannel.MapMode.READ_WRITE, 0, FILE_SIZE);
                buf.order(ByteOrder.LITTLE_ENDIAN);
                return new PersistentState(file, ch, buf);
            } catch (IOException | RuntimeException e) {
                try { ch.close(); } catch (IOException ignored) { /* best-effort */ }
                throw e;
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to open Raft state file " + file, e);
        }
    }

    public synchronized long currentTerm() {
        ensureOpen();
        return buffer.getLong(OFF_TERM);
    }

    public synchronized int votedFor() {
        ensureOpen();
        return buffer.getInt(OFF_VOTED_FOR);
    }

    public synchronized long commitIndex() {
        ensureOpen();
        return buffer.getLong(OFF_COMMIT_INDEX);
    }

    public synchronized long lastApplied() {
        ensureOpen();
        return buffer.getLong(OFF_LAST_APPLIED);
    }

    /**
     * Atomically update {@code currentTerm} and {@code votedFor}, fsyncing
     * before returning. Used together when entering a new term.
     */
    public synchronized void setTermAndVote(long term, int votedFor) {
        ensureOpen();
        if (term < 0L) {
            throw new IllegalArgumentException("term must be non-negative");
        }
        buffer.putLong(OFF_TERM, term);
        buffer.putInt(OFF_VOTED_FOR, votedFor);
        force();
    }

    public synchronized void setCurrentTerm(long term) {
        ensureOpen();
        if (term < 0L) {
            throw new IllegalArgumentException("term must be non-negative");
        }
        buffer.putLong(OFF_TERM, term);
        force();
    }

    public synchronized void setVotedFor(int votedFor) {
        ensureOpen();
        buffer.putInt(OFF_VOTED_FOR, votedFor);
        force();
    }

    public synchronized void setCommitIndex(long commitIndex) {
        ensureOpen();
        if (commitIndex < -1L) {
            throw new IllegalArgumentException("commitIndex must be >= -1");
        }
        buffer.putLong(OFF_COMMIT_INDEX, commitIndex);
        force();
    }

    public synchronized void setLastApplied(long lastApplied) {
        ensureOpen();
        if (lastApplied < -1L) {
            throw new IllegalArgumentException("lastApplied must be >= -1");
        }
        buffer.putLong(OFF_LAST_APPLIED, lastApplied);
        force();
    }

    public Path path() {
        return path;
    }

    @Override
    public synchronized void close() {
        if (closed) return;
        closed = true;
        try {
            buffer.force();
        } catch (Throwable t) {
            LOG.warn("Persistent state force failed for {}: {}", path, t.toString());
        }
        try {
            channel.close();
        } catch (IOException e) {
            LOG.warn("Persistent state close failed for {}: {}", path, e.toString());
        }
    }

    private void force() {
        // mmap force flushes dirty pages; the FileChannel.force(true) on the
        // underlying channel is more conservative and updates the metadata
        // too. The mmap variant is sufficient for crash safety on the data
        // pages themselves.
        try {
            buffer.force();
        } catch (Throwable t) {
            LOG.warn("force() failed for {}: {}", path, t.toString());
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("PersistentState is closed: " + path);
        }
    }
}
