package com.exchange.raft;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Process-local Raft transport used by tests. All transports for a cluster
 * share a single {@link Registry}. Sends are synchronous (no copy in flight)
 * but the listener contract — that messages are processed on the Raft loop
 * thread — is upheld by an inbound queue drained on each tick.
 * <p>
 * Supports network partitioning for testing: a {@link Registry#partition}
 * map of "blocked" pair sets cause messages between those node ids to be
 * dropped silently.
 */
public final class InMemoryRpcTransport implements RpcTransport {

    private static final Logger LOG = LoggerFactory.getLogger(InMemoryRpcTransport.class);

    private final int nodeId;
    private final Registry registry;
    private final ConcurrentLinkedQueue<RaftRpc.Message> inbound = new ConcurrentLinkedQueue<>();
    private volatile Listener listener;
    private volatile boolean closed = false;

    public InMemoryRpcTransport(int nodeId, Registry registry) {
        this.nodeId = nodeId;
        this.registry = registry;
        registry.register(nodeId, this);
    }

    @Override
    public void setListener(Listener listener) {
        this.listener = listener;
    }

    @Override
    public boolean send(int peerId, RaftRpc.Message msg) {
        if (closed) return false;
        if (registry.isPartitioned(nodeId, peerId)) {
            return true; // silent drop
        }
        InMemoryRpcTransport target = registry.lookup(peerId);
        if (target == null || target.closed) return false;
        target.inbound.add(msg);
        return true;
    }

    @Override
    public void start() {
        // nothing to start; messages are drained from the Raft loop tick.
    }

    @Override
    public void close() {
        closed = true;
        registry.unregister(nodeId);
        inbound.clear();
    }

    /**
     * Drain pending inbound messages, invoking the listener for each. Called
     * from the Raft node's tick loop. Returns the number of messages
     * delivered.
     */
    public int drain(int max) {
        if (listener == null) return 0;
        int n = 0;
        while (n < max) {
            RaftRpc.Message m = inbound.poll();
            if (m == null) break;
            try {
                listener.onMessage(m);
            } catch (Throwable t) {
                LOG.error("listener error on node {}", nodeId, t);
            }
            n++;
        }
        return n;
    }

    public int nodeId() {
        return nodeId;
    }

    /** Shared registry across all in-process transports for a cluster. */
    public static final class Registry {
        private final Map<Integer, InMemoryRpcTransport> nodes = new HashMap<>();
        // Bidirectional partitions keyed by ordered (min,max) id pair
        private final Set<Long> partitions = new HashSet<>();

        public synchronized void register(int id, InMemoryRpcTransport t) {
            nodes.put(id, t);
        }

        public synchronized void unregister(int id) {
            nodes.remove(id);
        }

        public synchronized InMemoryRpcTransport lookup(int id) {
            return nodes.get(id);
        }

        public synchronized boolean isPartitioned(int a, int b) {
            return partitions.contains(pairKey(a, b));
        }

        public synchronized void partition(int a, int b) {
            partitions.add(pairKey(a, b));
        }

        public synchronized void heal(int a, int b) {
            partitions.remove(pairKey(a, b));
        }

        public synchronized void healAll() {
            partitions.clear();
        }

        /**
         * Partition the cluster into two groups: every pair where one id is
         * in {@code groupA} and the other is in {@code groupB} drops both
         * directions.
         */
        public synchronized void partitionGroups(Set<Integer> groupA, Set<Integer> groupB) {
            for (int a : groupA) {
                for (int b : groupB) {
                    partitions.add(pairKey(a, b));
                }
            }
        }

        private static long pairKey(int a, int b) {
            int lo = Math.min(a, b);
            int hi = Math.max(a, b);
            return (((long) lo) << 32) | (hi & 0xFFFFFFFFL);
        }
    }
}
