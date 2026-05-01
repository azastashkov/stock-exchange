package com.exchange.raft;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Wire codec for Raft RPC messages.
 * <p>
 * Frame layout: {@code [u32 length] [u8 type] [body]}.
 * The 4-byte length is the size of {@code [type+body]}; little-endian.
 * Bodies use little-endian encoding for primitives and UTF-8 length-prefixed
 * for byte arrays where applicable. {@code byte[][] entries} is encoded as
 * {@code u32 count} followed by {@code u32 size} + {@code bytes} per entry.
 */
public final class RpcCodec {

    public static final int LENGTH_PREFIX_BYTES = 4;

    private RpcCodec() {
        // utility class
    }

    /**
     * Encode {@code msg} into a fresh ByteBuffer. The returned buffer is
     * ready-to-write (position=0, limit=size, includes length prefix).
     */
    public static ByteBuffer encode(RaftRpc.Message msg) {
        ByteBuffer body = encodeBody(msg);
        int bodyLen = body.remaining();
        int frameLen = LENGTH_PREFIX_BYTES + 1 + bodyLen;
        ByteBuffer out = ByteBuffer.allocate(frameLen).order(ByteOrder.LITTLE_ENDIAN);
        out.putInt(1 + bodyLen);
        out.put(msg.type());
        out.put(body);
        out.flip();
        return out;
    }

    /**
     * Decode a single message from {@code in}. Caller must have already
     * positioned/limited {@code in} to the {@code [type+body]} bytes — i.e.
     * the length prefix has been stripped. Returns null on unknown type.
     */
    public static RaftRpc.Message decode(ByteBuffer in) {
        ByteBuffer src = in.order() == ByteOrder.LITTLE_ENDIAN
                ? in
                : in.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        byte type = src.get();
        return switch (type) {
            case RaftRpc.TYPE_REQUEST_VOTE -> decodeRequestVote(src);
            case RaftRpc.TYPE_REQUEST_VOTE_RESP -> decodeRequestVoteResp(src);
            case RaftRpc.TYPE_APPEND_ENTRIES -> decodeAppendEntries(src);
            case RaftRpc.TYPE_APPEND_ENTRIES_RESP -> decodeAppendEntriesResp(src);
            case RaftRpc.TYPE_INSTALL_SNAPSHOT -> decodeInstallSnapshot(src);
            case RaftRpc.TYPE_INSTALL_SNAPSHOT_RESP -> decodeInstallSnapshotResp(src);
            default -> null;
        };
    }

    // ---------- bodies ----------

    private static ByteBuffer encodeBody(RaftRpc.Message msg) {
        return switch (msg) {
            case RaftRpc.RequestVote rv -> encodeRequestVote(rv);
            case RaftRpc.RequestVoteResp rvr -> encodeRequestVoteResp(rvr);
            case RaftRpc.AppendEntries ae -> encodeAppendEntries(ae);
            case RaftRpc.AppendEntriesResp aer -> encodeAppendEntriesResp(aer);
            case RaftRpc.InstallSnapshot is -> encodeInstallSnapshot(is);
            case RaftRpc.InstallSnapshotResp isr -> encodeInstallSnapshotResp(isr);
        };
    }

    private static ByteBuffer encodeRequestVote(RaftRpc.RequestVote rv) {
        ByteBuffer b = ByteBuffer.allocate(8 + 4 + 8 + 8).order(ByteOrder.LITTLE_ENDIAN);
        b.putLong(rv.term());
        b.putInt(rv.candidateId());
        b.putLong(rv.lastLogIndex());
        b.putLong(rv.lastLogTerm());
        b.flip();
        return b;
    }

    private static RaftRpc.RequestVote decodeRequestVote(ByteBuffer src) {
        long term = src.getLong();
        int candidateId = src.getInt();
        long lastLogIndex = src.getLong();
        long lastLogTerm = src.getLong();
        return new RaftRpc.RequestVote(term, candidateId, lastLogIndex, lastLogTerm);
    }

    private static ByteBuffer encodeRequestVoteResp(RaftRpc.RequestVoteResp r) {
        ByteBuffer b = ByteBuffer.allocate(8 + 4 + 1).order(ByteOrder.LITTLE_ENDIAN);
        b.putLong(r.term());
        b.putInt(r.voterId());
        b.put(r.voteGranted() ? (byte) 1 : (byte) 0);
        b.flip();
        return b;
    }

    private static RaftRpc.RequestVoteResp decodeRequestVoteResp(ByteBuffer src) {
        long term = src.getLong();
        int voterId = src.getInt();
        boolean granted = src.get() != 0;
        return new RaftRpc.RequestVoteResp(term, voterId, granted);
    }

    private static ByteBuffer encodeAppendEntries(RaftRpc.AppendEntries ae) {
        byte[][] entries = ae.entries();
        int count = entries == null ? 0 : entries.length;
        int totalEntryBytes = 0;
        for (int i = 0; i < count; i++) {
            totalEntryBytes += 4 + (entries[i] == null ? 0 : entries[i].length);
        }
        int size = 8 + 4 + 8 + 8 + 4 + totalEntryBytes + 8;
        ByteBuffer b = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
        b.putLong(ae.term());
        b.putInt(ae.leaderId());
        b.putLong(ae.prevLogIndex());
        b.putLong(ae.prevLogTerm());
        b.putInt(count);
        for (int i = 0; i < count; i++) {
            byte[] e = entries[i] == null ? new byte[0] : entries[i];
            b.putInt(e.length);
            b.put(e);
        }
        b.putLong(ae.leaderCommit());
        b.flip();
        return b;
    }

    private static RaftRpc.AppendEntries decodeAppendEntries(ByteBuffer src) {
        long term = src.getLong();
        int leaderId = src.getInt();
        long prevLogIndex = src.getLong();
        long prevLogTerm = src.getLong();
        int count = src.getInt();
        if (count < 0) {
            throw new IllegalArgumentException("AppendEntries entry count negative: " + count);
        }
        byte[][] entries = new byte[count][];
        for (int i = 0; i < count; i++) {
            int len = src.getInt();
            if (len < 0) {
                throw new IllegalArgumentException("AppendEntries entry size negative");
            }
            byte[] e = new byte[len];
            src.get(e);
            entries[i] = e;
        }
        long leaderCommit = src.getLong();
        return new RaftRpc.AppendEntries(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit);
    }

    private static ByteBuffer encodeAppendEntriesResp(RaftRpc.AppendEntriesResp r) {
        ByteBuffer b = ByteBuffer.allocate(8 + 4 + 1 + 8 + 8).order(ByteOrder.LITTLE_ENDIAN);
        b.putLong(r.term());
        b.putInt(r.followerId());
        b.put(r.success() ? (byte) 1 : (byte) 0);
        b.putLong(r.matchIndex());
        b.putLong(r.conflictIndex());
        b.flip();
        return b;
    }

    private static RaftRpc.AppendEntriesResp decodeAppendEntriesResp(ByteBuffer src) {
        long term = src.getLong();
        int followerId = src.getInt();
        boolean success = src.get() != 0;
        long matchIndex = src.getLong();
        long conflictIndex = src.getLong();
        return new RaftRpc.AppendEntriesResp(term, followerId, success, matchIndex, conflictIndex);
    }

    private static ByteBuffer encodeInstallSnapshot(RaftRpc.InstallSnapshot is) {
        byte[] data = is.data() == null ? new byte[0] : is.data();
        ByteBuffer b = ByteBuffer.allocate(8 + 4 + 8 + 8 + 8 + 4 + data.length + 1)
                .order(ByteOrder.LITTLE_ENDIAN);
        b.putLong(is.term());
        b.putInt(is.leaderId());
        b.putLong(is.lastIncludedIndex());
        b.putLong(is.lastIncludedTerm());
        b.putLong(is.offset());
        b.putInt(data.length);
        b.put(data);
        b.put(is.done() ? (byte) 1 : (byte) 0);
        b.flip();
        return b;
    }

    private static RaftRpc.InstallSnapshot decodeInstallSnapshot(ByteBuffer src) {
        long term = src.getLong();
        int leaderId = src.getInt();
        long lastIncludedIndex = src.getLong();
        long lastIncludedTerm = src.getLong();
        long offset = src.getLong();
        int len = src.getInt();
        byte[] data = new byte[len];
        src.get(data);
        boolean done = src.get() != 0;
        return new RaftRpc.InstallSnapshot(term, leaderId, lastIncludedIndex, lastIncludedTerm,
                offset, data, done);
    }

    private static ByteBuffer encodeInstallSnapshotResp(RaftRpc.InstallSnapshotResp r) {
        ByteBuffer b = ByteBuffer.allocate(8 + 4).order(ByteOrder.LITTLE_ENDIAN);
        b.putLong(r.term());
        b.putInt(r.followerId());
        b.flip();
        return b;
    }

    private static RaftRpc.InstallSnapshotResp decodeInstallSnapshotResp(ByteBuffer src) {
        long term = src.getLong();
        int followerId = src.getInt();
        return new RaftRpc.InstallSnapshotResp(term, followerId);
    }
}
