package com.exchange.fix;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * Per-FIX-session state. One instance per logged-in client.
 * <p>
 * Sequence numbers are persisted to {@link #seqFile} after every
 * successful inbound consume and every outbound send, so a crash followed
 * by a restart leaves the session in a recoverable state.
 */
public final class FixSession {

    /** First sequence number used by both directions. */
    public static final int INITIAL_SEQ_NUM = 1;

    public final String senderCompIdAsClient;   // client's SenderCompID
    public final String targetCompIdAsClient;   // server's SenderCompID
    public final byte[] senderCompIdBytes;      // bytes used as our 56=
    public final byte[] targetCompIdBytes;      // bytes used as our 49=
    public final long sessionId;
    public final Path seqFile;

    public final SocketChannel channel;
    /** 64 KiB direct read buffer; owns its own state. */
    public final ByteBuffer readBuffer;
    /** Direct write buffer; written-to under {@link #lock}. Sized large enough
     *  to absorb several seconds of outbound burst at &gt; 10k orders/sec. */
    public final ByteBuffer writeBuffer;
    public final RateLimiter rateLimiter;

    /** Lock guarding writeBuffer access between gateway-io and gateway-out. */
    public final Object lock = new Object();

    public int inSeqNumExpected = INITIAL_SEQ_NUM;
    public int outSeqNum = INITIAL_SEQ_NUM;
    public int heartBtInt = 30;
    public long lastInboundNanos;
    public long lastOutboundNanos;
    public boolean loggedOn;
    public boolean testRequestSent;
    public boolean closing;
    public byte[] pendingTestReqId;

    /** Throttle for {@link #persistThrottled()}: persist at most once per N ms. */
    private static final long PERSIST_INTERVAL_NANOS = 1_000_000_000L; // 1 second
    private long lastPersistNanos = 0L;
    private boolean persistDirty = false;

    public FixSession(String senderCompIdAsClient,
                      String targetCompIdAsClient,
                      SocketChannel channel,
                      RateLimiter rateLimiter,
                      Path seqFile) {
        this.senderCompIdAsClient = senderCompIdAsClient;
        this.targetCompIdAsClient = targetCompIdAsClient;
        this.senderCompIdBytes = targetCompIdAsClient.getBytes(StandardCharsets.US_ASCII);
        this.targetCompIdBytes = senderCompIdAsClient.getBytes(StandardCharsets.US_ASCII);
        this.sessionId = senderCompIdHash(senderCompIdAsClient);
        this.channel = channel;
        this.rateLimiter = rateLimiter;
        this.seqFile = seqFile;
        this.readBuffer = ByteBuffer.allocateDirect(65536);
        this.writeBuffer = ByteBuffer.allocateDirect(4 * 1024 * 1024);
        this.lastInboundNanos = System.nanoTime();
        this.lastOutboundNanos = System.nanoTime();
    }

    /**
     * Long hash of a SenderCompID, used as a session identifier on the
     * inbound ring. Same algorithm everywhere so rounds-trips are stable.
     */
    public static long senderCompIdHash(String senderCompId) {
        long h = 0xcbf29ce484222325L;
        for (int i = 0; i < senderCompId.length(); i++) {
            char c = senderCompId.charAt(i);
            h ^= (c & 0xFFL);
            h *= 0x100000001b3L;
        }
        return h;
    }

    /** Read sequence numbers from {@link #seqFile} if it exists. */
    public void load() throws IOException {
        if (!Files.exists(seqFile)) {
            inSeqNumExpected = INITIAL_SEQ_NUM;
            outSeqNum = INITIAL_SEQ_NUM;
            return;
        }
        byte[] bytes = Files.readAllBytes(seqFile);
        String s = new String(bytes, StandardCharsets.US_ASCII).trim();
        // Format: "in,out"
        int comma = s.indexOf(',');
        if (comma < 0) {
            inSeqNumExpected = INITIAL_SEQ_NUM;
            outSeqNum = INITIAL_SEQ_NUM;
            return;
        }
        try {
            this.inSeqNumExpected = Integer.parseInt(s.substring(0, comma).trim());
            this.outSeqNum = Integer.parseInt(s.substring(comma + 1).trim());
        } catch (NumberFormatException nfe) {
            inSeqNumExpected = INITIAL_SEQ_NUM;
            outSeqNum = INITIAL_SEQ_NUM;
        }
    }

    /**
     * Throttled persist: marks state dirty and writes to disk no more than
     * once per {@link #PERSIST_INTERVAL_NANOS}. The hot path on the gateway
     * cannot afford a per-message {@code rename(2)} but we still want
     * crash-recovery to work; one second of message loss on a crash is the
     * trade-off. Call {@link #persist()} on session close to force a flush.
     */
    public void persistThrottled() throws IOException {
        long now = System.nanoTime();
        persistDirty = true;
        if (now - lastPersistNanos < PERSIST_INTERVAL_NANOS) {
            return;
        }
        lastPersistNanos = now;
        persist();
        persistDirty = false;
    }

    /** True if there are dirty seq nums waiting to be flushed. */
    public boolean hasPendingPersist() {
        return persistDirty;
    }

    /** Persist seq nums atomically by writing to a temp file and renaming. */
    public void persist() throws IOException {
        Path parent = seqFile.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        String text = inSeqNumExpected + "," + outSeqNum;
        Path tmp = (parent == null ? seqFile.resolveSibling(seqFile.getFileName() + ".tmp")
                : parent.resolve(seqFile.getFileName() + ".tmp"));
        Files.write(tmp, text.getBytes(StandardCharsets.US_ASCII));
        try {
            Files.move(tmp, seqFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (java.nio.file.AtomicMoveNotSupportedException amnse) {
            Files.move(tmp, seqFile, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    public void incInSeq() {
        inSeqNumExpected++;
    }

    public int nextOutSeq() {
        return outSeqNum++;
    }

    public boolean checkAndConsumeRate() {
        return rateLimiter == null || rateLimiter.tryAcquire();
    }
}
