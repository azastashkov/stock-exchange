package com.exchange.raft;

/**
 * Marker class for the leader replication subsystem.
 * <p>
 * <strong>Design decision:</strong> the replicator is integrated into
 * {@link RaftNode}'s single {@code raft-loop} thread rather than running as
 * its own {@code ApplicationLoop}. The loop's tick performs three steps in
 * order:
 * <ol>
 *   <li>Drain inbound RPC messages.</li>
 *   <li>If leader and the heartbeat interval elapsed,
 *       {@code replicateAll(...)} — package up batches of new entries from
 *       the local {@code EventLog} and dispatch one
 *       {@code AppendEntries} per peer; advance {@code commitIndex} when a
 *       majority of {@code matchIndex[peer]} clears the threshold.</li>
 *   <li>Apply newly-committed entries by flipping their slot status from
 *       {@code WRITING} to {@code COMMITTED} (the
 *       {@link CommitApplier} role).</li>
 * </ol>
 * Folding the replicator into the same thread that owns the rest of the
 * state machine means there are no cross-thread synchronisation points
 * inside the Raft node and no risk of inconsistent reads of
 * {@code nextIndex}/{@code matchIndex}/{@code commitIndex}.
 * <p>
 * This class is intentionally empty — it exists to make the architectural
 * role explicit in the file layout and to give callers a stable type they
 * can reference for documentation purposes.
 */
public final class Replicator {

    private final RaftNode node;

    public Replicator(RaftNode node) {
        this.node = node;
    }

    public RaftNode node() {
        return node;
    }
}
