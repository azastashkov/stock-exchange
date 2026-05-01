package com.exchange.raft;

import com.exchange.loop.ApplicationLoop;
import com.exchange.store.EventLog;
import com.exchange.store.EventLogWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;

/**
 * Raft state machine. One {@code raft-loop} thread per node serializes:
 * <ul>
 *   <li>Inbound RPC messages (drained from the transport's queue).</li>
 *   <li>Election timeout / heartbeat scheduling.</li>
 *   <li>Outbound RPC sends (heartbeats, AppendEntries, vote requests).</li>
 *   <li>Commit applier (advancing slot status WRITING → COMMITTED on
 *       leader and follower as commitIndex moves).</li>
 * </ul>
 * Public methods other than {@link #appendEvent}, {@link #isLeader},
 * {@link #leaderId}, {@link #currentTerm} are not safe to call from
 * external threads. {@link #appendEvent} writes to the local EventLog
 * via the writer (single-writer guarantee preserved because callers
 * serialise on the engine's own thread).
 */
public final class RaftNode extends ApplicationLoop {

    private static final Logger LOG = LoggerFactory.getLogger(RaftNode.class);

    /** Maximum entries per AppendEntries batch. */
    public static final int MAX_BATCH = 32;
    /** Heartbeat interval in nanoseconds. */
    public static final long HEARTBEAT_INTERVAL_NANOS = 50_000_000L; // 50ms

    /** Election timeout default range, in milliseconds. */
    public static final int ELECTION_TIMEOUT_MIN_MS = 150;
    public static final int ELECTION_TIMEOUT_MAX_MS = 300;
    /** Warm-node election timeout range (shorter so it wins). */
    public static final int WARM_ELECTION_TIMEOUT_MIN_MS = 100;
    public static final int WARM_ELECTION_TIMEOUT_MAX_MS = 200;

    public interface Clock {
        long nowNanos();
    }

    public static final Clock SYSTEM_CLOCK = System::nanoTime;

    // -----------------------------------------------------------------
    // Wiring
    // -----------------------------------------------------------------

    private final int nodeId;
    private final int warmNodeId;
    private final PeerConfig peers;
    private final EventLog log;
    private final EventLogWriter writer;
    private final PersistentState persistent;
    private final RpcTransport transport;
    private final Clock clock;
    private final Random rng;
    /**
     * Optional drainer hook for transports that buffer inbound on a
     * concurrent queue. Implementations run from the raft loop tick.
     */
    private final TransportPump transportPump;

    // -----------------------------------------------------------------
    // Volatile / state-machine fields (only mutated from raft-loop)
    // -----------------------------------------------------------------

    private RaftRole role = RaftRole.FOLLOWER;
    private int leaderId = -1;
    /** Last time we observed a leader's heartbeat or granted a vote, in nanos. */
    private long lastHeartbeatNanos;
    /** Election timeout for the *current* follower/candidate cycle, in nanos. */
    private long currentElectionTimeoutNanos;
    /** Last time the leader sent heartbeats out, in nanos. */
    private long lastHeartbeatSentNanos;

    /** Per-peer Raft state on the leader. {@code matchIndex} is a
     *  {@link ConcurrentHashMap} so the metrics thread can read it
     *  concurrently with raft-loop updates. */
    private final Map<Integer, Long> nextIndex = new HashMap<>();
    private final java.util.concurrent.ConcurrentHashMap<Integer, Long> matchIndex =
            new java.util.concurrent.ConcurrentHashMap<>();
    /**
     * Per-peer last-contact timestamp (nanos): updated when this leader
     * receives a successful AppendEntriesResp. Used to detect
     * minority-partition isolation and step down.
     */
    private final Map<Integer, Long> peerLastResponse = new HashMap<>();
    /** Time the current leader term started — used for the leader-stepdown deadline. */
    private long leaderTermStartNanos;

    /** Vote tally for the active election (candidate-only). */
    private final Set<Integer> votesReceived = new HashSet<>();

    /** External queue from non-raft-loop threads: append events & ticks. */
    private final ConcurrentLinkedQueue<RaftRpc.Message> inboundFallback = new ConcurrentLinkedQueue<>();

    /** When set, suppress sends — emulate a network black hole for tests. */
    private final AtomicBoolean sendsBlackholed = new AtomicBoolean(false);

    // -----------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------

    public RaftNode(int nodeId,
                    int warmNodeId,
                    PeerConfig peers,
                    EventLog log,
                    EventLogWriter writer,
                    PersistentState persistent,
                    RpcTransport transport,
                    TransportPump transportPump,
                    long randomSeed,
                    Clock clock) {
        super("raft-loop-" + nodeId, false);
        this.nodeId = nodeId;
        this.warmNodeId = warmNodeId;
        this.peers = peers;
        this.log = log;
        this.writer = writer;
        this.persistent = persistent;
        this.transport = transport;
        this.transportPump = transportPump;
        this.clock = clock == null ? SYSTEM_CLOCK : clock;
        this.rng = new Random(randomSeed);

        long now = this.clock.nowNanos();
        this.lastHeartbeatNanos = now;
        this.currentElectionTimeoutNanos = pickElectionTimeoutNanos();

        transport.setListener(this::deliverInbound);
    }

    // -----------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------

    public boolean isLeader() {
        return role == RaftRole.LEADER;
    }

    public RaftRole role() {
        return role;
    }

    public int leaderId() {
        return leaderId;
    }

    public int nodeId() {
        return nodeId;
    }

    public long currentTerm() {
        return persistent.currentTerm();
    }

    public long commitIndex() {
        return persistent.commitIndex();
    }

    public long lastApplied() {
        return persistent.lastApplied();
    }

    /** Last log index in the local event log (-1 if empty). Public for metrics. */
    public long lastLogIndexSnapshot() {
        return lastLogIndex();
    }

    /** matchIndex for the given peer (-1 if unknown / not leader / not started yet). */
    public long matchIndexFor(int peerId) {
        return matchIndex.getOrDefault(peerId, -1L);
    }

    public LongSupplier termSupplier() {
        return persistent::currentTerm;
    }

    /**
     * Leader-only entry point used by the engine. Appends an event with
     * status WRITING into the local log; the replicator picks it up
     * on the next tick. Returns true on success, false if not leader
     * or if the log is full.
     */
    public boolean appendEvent(short type, ByteBuffer payload) {
        if (role != RaftRole.LEADER) return false;
        try {
            long term = persistent.currentTerm();
            long ts = System.nanoTime();
            writer.appendUncommitted(type, term, ts, payload);
            return true;
        } catch (Throwable t) {
            LOG.warn("appendEvent failed on node {}: {}", nodeId, t.toString());
            return false;
        }
    }

    public PeerConfig peers() {
        return peers;
    }

    public EventLog log() {
        return log;
    }

    /** Test helper: pretend the network is gone. */
    public void setNetworkBlackholed(boolean blackholed) {
        sendsBlackholed.set(blackholed);
    }

    // -----------------------------------------------------------------
    // Loop tick
    // -----------------------------------------------------------------

    @Override
    protected boolean pollOnce() {
        boolean did = false;
        // 1. Drain inbound from the transport pump (NIO/in-memory queues).
        if (transportPump != null) {
            int n = transportPump.drain(64);
            if (n > 0) did = true;
        }
        // 2. Drain inbound fallback (used when a synchronous transport
        //    delivers from this thread already, the queue stays empty).
        for (int i = 0; i < 64; i++) {
            RaftRpc.Message m = inboundFallback.poll();
            if (m == null) break;
            handleMessage(m);
            did = true;
        }
        // 3. Tick state-machine: election timeout / heartbeat / commit.
        long now = clock.nowNanos();
        if (role == RaftRole.LEADER) {
            if (now - lastHeartbeatSentNanos >= HEARTBEAT_INTERVAL_NANOS) {
                replicateAll(now);
                lastHeartbeatSentNanos = now;
                did = true;
            }
            // Step down if we can't see a majority (minority partition).
            if (!majorityRespondedRecently(now)) {
                LOG.info("node {} stepping down LEADER (no majority within timeout)", nodeId);
                role = RaftRole.FOLLOWER;
                leaderId = -1;
                lastHeartbeatNanos = now;
                currentElectionTimeoutNanos = pickElectionTimeoutNanos();
            }
        } else {
            if (now - lastHeartbeatNanos >= currentElectionTimeoutNanos) {
                startElection(now);
                did = true;
            }
        }
        // 4. Apply newly-committed entries (flip WRITING → COMMITTED).
        if (applyCommittedEntries()) did = true;
        return did;
    }

    // -----------------------------------------------------------------
    // Inbound delivery
    // -----------------------------------------------------------------

    /**
     * Called by the transport. When the transport delivers from another
     * thread we MUST queue; the raft loop drains the queue. With pumps
     * (NIO/in-memory transports), the transport already queues inbound;
     * the listener bypasses fallback. Otherwise we fall back to the
     * inboundFallback queue.
     */
    private void deliverInbound(RaftRpc.Message m) {
        if (transportPump != null) {
            handleMessage(m);
        } else {
            inboundFallback.add(m);
        }
    }

    private void handleMessage(RaftRpc.Message msg) {
        switch (msg) {
            case RaftRpc.RequestVote rv -> onRequestVote(rv);
            case RaftRpc.RequestVoteResp rvr -> onRequestVoteResp(rvr);
            case RaftRpc.AppendEntries ae -> onAppendEntries(ae);
            case RaftRpc.AppendEntriesResp aer -> onAppendEntriesResp(aer);
            default -> LOG.debug("node {} ignoring {}", nodeId, msg.getClass().getSimpleName());
        }
    }

    // -----------------------------------------------------------------
    // Term handling
    // -----------------------------------------------------------------

    private void observeTerm(long observedTerm) {
        if (observedTerm > persistent.currentTerm()) {
            persistent.setTermAndVote(observedTerm, PersistentState.VOTED_FOR_NONE);
            if (role != RaftRole.FOLLOWER) {
                LOG.info("node {} stepping down to FOLLOWER (saw term {})", nodeId, observedTerm);
            }
            role = RaftRole.FOLLOWER;
            leaderId = -1;
            votesReceived.clear();
        }
    }

    // -----------------------------------------------------------------
    // Election
    // -----------------------------------------------------------------

    private void startElection(long nowNanos) {
        long newTerm = persistent.currentTerm() + 1L;
        persistent.setTermAndVote(newTerm, nodeId);
        role = RaftRole.CANDIDATE;
        leaderId = -1;
        votesReceived.clear();
        votesReceived.add(nodeId); // self-vote
        lastHeartbeatNanos = nowNanos;
        currentElectionTimeoutNanos = pickElectionTimeoutNanos();
        LOG.info("node {} starting election for term {}", nodeId, newTerm);

        long lastIdx = lastLogIndex();
        long lastTerm = lastIdx >= 0 ? log.readSlotTermAt(lastIdx) : 0L;
        RaftRpc.RequestVote rv = new RaftRpc.RequestVote(newTerm, nodeId, lastIdx, lastTerm);
        for (int peer : peers.peerIds()) {
            sendTo(peer, rv);
        }
        // Single-node degenerate case: become leader immediately.
        if (peers.peerIds().isEmpty() && peers.total() == 1) {
            becomeLeader();
        } else if (votesReceived.size() >= peers.majority()) {
            becomeLeader();
        }
    }

    private void onRequestVote(RaftRpc.RequestVote rv) {
        observeTerm(rv.term());
        long curTerm = persistent.currentTerm();
        boolean grant = false;
        if (rv.term() < curTerm) {
            grant = false;
        } else {
            int voted = persistent.votedFor();
            boolean canVote = (voted == PersistentState.VOTED_FOR_NONE || voted == rv.candidateId());
            // log up-to-date check
            long ourLastIdx = lastLogIndex();
            long ourLastTerm = ourLastIdx >= 0 ? log.readSlotTermAt(ourLastIdx) : 0L;
            boolean candidateUpToDate =
                    rv.lastLogTerm() > ourLastTerm
                            || (rv.lastLogTerm() == ourLastTerm && rv.lastLogIndex() >= ourLastIdx);
            if (canVote && candidateUpToDate) {
                persistent.setVotedFor(rv.candidateId());
                grant = true;
                lastHeartbeatNanos = clock.nowNanos();
                currentElectionTimeoutNanos = pickElectionTimeoutNanos();
            }
        }
        sendTo(rv.candidateId(), new RaftRpc.RequestVoteResp(curTerm, nodeId, grant));
    }

    private void onRequestVoteResp(RaftRpc.RequestVoteResp r) {
        observeTerm(r.term());
        if (role != RaftRole.CANDIDATE) return;
        if (r.term() != persistent.currentTerm()) return;
        if (r.voteGranted()) {
            votesReceived.add(r.voterId());
            if (votesReceived.size() >= peers.majority()) {
                becomeLeader();
            }
        }
    }

    private void becomeLeader() {
        role = RaftRole.LEADER;
        leaderId = nodeId;
        long lastIdx = lastLogIndex();
        long nextIdx = lastIdx + 1L;
        nextIndex.clear();
        matchIndex.clear();
        peerLastResponse.clear();
        long now = clock.nowNanos();
        for (int p : peers.peerIds()) {
            nextIndex.put(p, nextIdx);
            matchIndex.put(p, -1L);
            peerLastResponse.put(p, now);
        }
        leaderTermStartNanos = now;
        LOG.info("node {} became LEADER for term {} (lastIdx={})", nodeId, persistent.currentTerm(), lastIdx);
        // Send immediate heartbeat to assert leadership.
        replicateAll(now);
        lastHeartbeatSentNanos = now;
    }

    // -----------------------------------------------------------------
    // AppendEntries — leader replication
    // -----------------------------------------------------------------

    private void replicateAll(long nowNanos) {
        long curTerm = persistent.currentTerm();
        long leaderCommit = persistent.commitIndex();
        long lastIdx = lastLogIndex();
        for (int peer : peers.peerIds()) {
            long ni = nextIndex.getOrDefault(peer, 0L);
            long prevIdx = ni - 1L;
            long prevTerm = (prevIdx >= 0) ? log.readSlotTermAt(prevIdx) : 0L;
            if (prevTerm < 0L) prevTerm = 0L;

            byte[][] entries;
            if (ni > lastIdx) {
                entries = new byte[0][];
            } else {
                long count = Math.min(MAX_BATCH, lastIdx - ni + 1L);
                entries = new byte[(int) count][];
                for (int i = 0; i < count; i++) {
                    entries[i] = log.readSlotRaw(ni + i);
                }
            }
            RaftRpc.AppendEntries ae = new RaftRpc.AppendEntries(
                    curTerm, nodeId, prevIdx, prevTerm, entries, leaderCommit);
            sendTo(peer, ae);
        }
        // Self-ack: in a single-node cluster commitIndex must advance even
        // without any peers responding.
        recomputeCommitIndex();
    }

    private void onAppendEntries(RaftRpc.AppendEntries ae) {
        observeTerm(ae.term());
        long curTerm = persistent.currentTerm();
        if (ae.term() < curTerm) {
            sendTo(ae.leaderId(), new RaftRpc.AppendEntriesResp(curTerm, nodeId, false, -1L, 0L));
            return;
        }
        // Valid leader for this term — reset election timer.
        leaderId = ae.leaderId();
        if (role != RaftRole.FOLLOWER) {
            role = RaftRole.FOLLOWER;
        }
        lastHeartbeatNanos = clock.nowNanos();
        currentElectionTimeoutNanos = pickElectionTimeoutNanos();

        // Consistency check on prevLogIndex/prevLogTerm.
        if (ae.prevLogIndex() >= 0) {
            long ourLastIdx = lastLogIndex();
            if (ae.prevLogIndex() > ourLastIdx) {
                long conflict = ourLastIdx + 1L; // we want entries from here
                sendTo(ae.leaderId(),
                        new RaftRpc.AppendEntriesResp(curTerm, nodeId, false, -1L, conflict));
                return;
            }
            long ourPrevTerm = log.readSlotTermAt(ae.prevLogIndex());
            if (ourPrevTerm != ae.prevLogTerm()) {
                // Find the start of the conflicting term to fast-back-up.
                long c = ae.prevLogIndex();
                while (c > 0L && log.readSlotTermAt(c - 1L) == ourPrevTerm) {
                    c--;
                }
                // Truncate the conflicting tail so leader can repopulate.
                log.truncateAfter(c - 1L);
                sendTo(ae.leaderId(),
                        new RaftRpc.AppendEntriesResp(curTerm, nodeId, false, -1L, c));
                return;
            }
        }
        // Install entries.
        byte[][] entries = ae.entries();
        long writeIdx = ae.prevLogIndex() + 1L;
        for (int i = 0; i < entries.length; i++) {
            long target = writeIdx + i;
            byte[] raw = entries[i];
            if (raw == null || raw.length != EventLog.SLOT_SIZE_BYTES) {
                // Defensive: shape mismatch; drop and abort.
                sendTo(ae.leaderId(),
                        new RaftRpc.AppendEntriesResp(curTerm, nodeId, false, -1L, target));
                return;
            }
            long ourTerm = log.readSlotTermAt(target);
            if (ourTerm >= 0L && ourTerm != extractTerm(raw)) {
                // Conflict: truncate and reinstall.
                log.truncateAfter(target - 1L);
            }
            log.installSlot(target, raw);
        }
        long matchIdx = writeIdx + entries.length - 1L; // -1 if no entries
        // Apply commit.
        long newCommit = Math.min(ae.leaderCommit(), Math.max(matchIdx, ae.prevLogIndex()));
        if (newCommit > persistent.commitIndex()) {
            persistent.setCommitIndex(newCommit);
        }
        sendTo(ae.leaderId(),
                new RaftRpc.AppendEntriesResp(curTerm, nodeId, true, matchIdx, 0L));
    }

    private static long extractTerm(byte[] rawSlot) {
        // Slot layout: status(4) + length(4) + sequence(8) + term(8) + ...
        // term offset 16, little-endian.
        long v = 0L;
        for (int i = 7; i >= 0; i--) {
            v = (v << 8) | (rawSlot[16 + i] & 0xFFL);
        }
        return v;
    }

    private void onAppendEntriesResp(RaftRpc.AppendEntriesResp r) {
        observeTerm(r.term());
        if (role != RaftRole.LEADER) return;
        if (r.term() != persistent.currentTerm()) return;
        int peer = r.followerId();
        peerLastResponse.put(peer, clock.nowNanos());
        if (r.success()) {
            long mi = Math.max(matchIndex.getOrDefault(peer, -1L), r.matchIndex());
            matchIndex.put(peer, mi);
            nextIndex.put(peer, mi + 1L);
            recomputeCommitIndex();
        } else {
            long hint = r.conflictIndex();
            long cur = nextIndex.getOrDefault(peer, 0L);
            long backed = (hint > 0L) ? hint : Math.max(0L, cur - 1L);
            nextIndex.put(peer, backed);
        }
    }

    /**
     * Returns true if the leader has received a positive response from a
     * majority of peers within roughly two election timeouts. Used so the
     * leader steps down on a minority partition.
     */
    private boolean majorityRespondedRecently(long now) {
        long cutoff = 2L * (long) ELECTION_TIMEOUT_MAX_MS * 1_000_000L; // ~600ms
        // Don't step down before we've had a chance to gather any responses.
        if (now - leaderTermStartNanos < cutoff) return true;
        int responding = 1; // self counts
        for (int p : peers.peerIds()) {
            long last = peerLastResponse.getOrDefault(p, 0L);
            if (now - last < cutoff) responding++;
        }
        return responding >= peers.majority();
    }

    private void recomputeCommitIndex() {
        long lastIdx = lastLogIndex();
        if (lastIdx < 0) return;
        // Single-node cluster: anything we have is committed.
        if (peers.total() == 1) {
            if (lastIdx > persistent.commitIndex()) {
                persistent.setCommitIndex(lastIdx);
            }
            return;
        }
        long curTerm = persistent.currentTerm();
        // Find the highest N such that a majority of matchIndex[i] >= N
        // and log[N].term == currentTerm (Raft commit safety).
        long N = persistent.commitIndex();
        for (long candidate = lastIdx; candidate > N; candidate--) {
            long termAtCandidate = log.readSlotTermAt(candidate);
            if (termAtCandidate != curTerm) continue;
            int count = 1; // self
            for (int peer : peers.peerIds()) {
                if (matchIndex.getOrDefault(peer, -1L) >= candidate) count++;
            }
            if (count >= peers.majority()) {
                persistent.setCommitIndex(candidate);
                break;
            }
        }
    }

    // -----------------------------------------------------------------
    // Commit applier — flips WRITING → COMMITTED for committed slots.
    // -----------------------------------------------------------------

    private boolean applyCommittedEntries() {
        long commit = persistent.commitIndex();
        long applied = persistent.lastApplied();
        if (applied >= commit) return false;
        long target = commit;
        for (long idx = applied + 1L; idx <= target; idx++) {
            int status = log.slotStatus(idx);
            if (status == EventLog.STATUS_EMPTY) {
                // Slot not yet installed; defer.
                return idx > applied + 1L;
            }
            if (status == EventLog.STATUS_WRITING) {
                log.commit(idx);
            }
        }
        persistent.setLastApplied(target);
        return true;
    }

    // -----------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------

    private long lastLogIndex() {
        long lsi = log.lastSlotIndex();
        return lsi - 1L;
    }

    private long pickElectionTimeoutNanos() {
        int min, max;
        if (nodeId == warmNodeId) {
            min = WARM_ELECTION_TIMEOUT_MIN_MS;
            max = WARM_ELECTION_TIMEOUT_MAX_MS;
        } else {
            min = ELECTION_TIMEOUT_MIN_MS;
            max = ELECTION_TIMEOUT_MAX_MS;
        }
        int pick = min + rng.nextInt(max - min + 1);
        return pick * 1_000_000L;
    }

    private void sendTo(int peer, RaftRpc.Message msg) {
        if (sendsBlackholed.get()) return;
        transport.send(peer, msg);
    }

    /** Hook the Raft loop calls each tick to drain a queue-based transport. */
    @FunctionalInterface
    public interface TransportPump {
        int drain(int max);
    }
}
