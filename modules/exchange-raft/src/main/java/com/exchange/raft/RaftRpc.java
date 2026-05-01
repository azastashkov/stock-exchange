package com.exchange.raft;

/**
 * Container for the Raft RPC message records. All four message types are
 * value-objects; they carry no behaviour and are encoded by {@link RpcCodec}.
 */
public final class RaftRpc {

    private RaftRpc() {
        // namespace
    }

    public static final byte TYPE_REQUEST_VOTE = 1;
    public static final byte TYPE_REQUEST_VOTE_RESP = 2;
    public static final byte TYPE_APPEND_ENTRIES = 3;
    public static final byte TYPE_APPEND_ENTRIES_RESP = 4;
    public static final byte TYPE_INSTALL_SNAPSHOT = 5;
    public static final byte TYPE_INSTALL_SNAPSHOT_RESP = 6;

    /** Marker interface implemented by every Raft message. */
    public sealed interface Message permits
            RequestVote,
            RequestVoteResp,
            AppendEntries,
            AppendEntriesResp,
            InstallSnapshot,
            InstallSnapshotResp {
        byte type();
    }

    public record RequestVote(long term,
                              int candidateId,
                              long lastLogIndex,
                              long lastLogTerm) implements Message {
        @Override public byte type() { return TYPE_REQUEST_VOTE; }
    }

    public record RequestVoteResp(long term,
                                  int voterId,
                                  boolean voteGranted) implements Message {
        @Override public byte type() { return TYPE_REQUEST_VOTE_RESP; }
    }

    public record AppendEntries(long term,
                                int leaderId,
                                long prevLogIndex,
                                long prevLogTerm,
                                byte[][] entries,
                                long leaderCommit) implements Message {
        @Override public byte type() { return TYPE_APPEND_ENTRIES; }
    }

    public record AppendEntriesResp(long term,
                                    int followerId,
                                    boolean success,
                                    long matchIndex,
                                    long conflictIndex) implements Message {
        @Override public byte type() { return TYPE_APPEND_ENTRIES_RESP; }
    }

    /**
     * Stub for a future snapshot transfer. v1 does not implement this — it
     * is defined so the codec can frame it without a runtime fallthrough.
     */
    public record InstallSnapshot(long term,
                                  int leaderId,
                                  long lastIncludedIndex,
                                  long lastIncludedTerm,
                                  long offset,
                                  byte[] data,
                                  boolean done) implements Message {
        @Override public byte type() { return TYPE_INSTALL_SNAPSHOT; }
    }

    public record InstallSnapshotResp(long term,
                                      int followerId) implements Message {
        @Override public byte type() { return TYPE_INSTALL_SNAPSHOT_RESP; }
    }
}
