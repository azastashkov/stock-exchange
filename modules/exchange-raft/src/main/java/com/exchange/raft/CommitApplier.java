package com.exchange.raft;

/**
 * Marker class for the commit-applier subsystem.
 * <p>
 * <strong>Design decision:</strong> the commit applier is folded into the
 * {@link RaftNode#pollOnce()} tick alongside replication. On each tick
 * after RPC processing and outbound replication, the loop iterates from
 * {@code lastApplied + 1} to {@code commitIndex} and flips each slot's
 * status from {@code WRITING} → {@code COMMITTED} via
 * {@link com.exchange.store.EventLog#commit(long)}. The downstream consumers
 * (Reporter, MDP, PositionKeeper) only see {@code COMMITTED} entries, so
 * the flip is the sole gate between Raft acknowledgement and downstream
 * visibility. {@code lastApplied} is then persisted to disk so a restart
 * resumes from the right slot.
 * <p>
 * On a follower, the same path runs: {@code installSlot} writes {@code
 * WRITING}, {@code AppendEntries.leaderCommit} updates the local
 * {@code commitIndex}, and the next tick's commit applier flips
 * {@code WRITING} → {@code COMMITTED}.
 * <p>
 * This class is intentionally empty — it exists to give the role a stable
 * type name that callers can reference for documentation.
 */
public final class CommitApplier {

    private final RaftNode node;

    public CommitApplier(RaftNode node) {
        this.node = node;
    }

    public RaftNode node() {
        return node;
    }
}
