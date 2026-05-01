package com.exchange.raft;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Immutable cluster topology view. Carries this node's id and the
 * address of every peer (including this one).
 */
public final class PeerConfig {

    private final int nodeId;
    private final Map<Integer, NetAddress> peers;
    private final Set<Integer> peerIds;

    public PeerConfig(int nodeId, Map<Integer, NetAddress> peers) {
        if (peers == null || peers.isEmpty()) {
            throw new IllegalArgumentException("peers must be non-empty");
        }
        if (!peers.containsKey(nodeId)) {
            throw new IllegalArgumentException("peers must contain self id " + nodeId);
        }
        this.nodeId = nodeId;
        this.peers = Collections.unmodifiableMap(new HashMap<>(peers));
        Set<Integer> others = new HashSet<>(this.peers.keySet());
        others.remove(nodeId);
        this.peerIds = Collections.unmodifiableSet(others);
    }

    public int nodeId() {
        return nodeId;
    }

    public NetAddress addressOf(int id) {
        return peers.get(id);
    }

    public NetAddress selfAddress() {
        return peers.get(nodeId);
    }

    /** All peer ids excluding self. */
    public Set<Integer> peerIds() {
        return peerIds;
    }

    /** All node ids including self. */
    public Set<Integer> allIds() {
        return peers.keySet();
    }

    public int total() {
        return peers.size();
    }

    public int majority() {
        return total() / 2 + 1;
    }
}
