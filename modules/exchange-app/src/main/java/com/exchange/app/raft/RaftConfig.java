package com.exchange.app.raft;

import com.exchange.raft.NetAddress;
import com.exchange.raft.PeerConfig;
import com.exchange.raft.RaftNode;
import com.exchange.raft.RpcTransport;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Configuration bundle for Raft integration in {@code ExchangeWiring}.
 * <p>
 * When {@link #enabled} is false the wiring stays in the legacy
 * single-node mode (matching engine writes COMMITTED entries directly).
 * When true, all five Raft pieces are constructed and wired into the
 * engine path.
 */
public final class RaftConfig {

    /**
     * Factory used by the wiring to obtain an {@link RpcTransport}. The
     * argument is the {@link PeerConfig} for the node being built. This
     * indirection lets tests inject an {@code InMemoryRpcTransport}
     * sharing a registry without changing wiring.
     */
    public interface TransportFactory extends Function<PeerConfig, RpcTransport> {
    }

    public final boolean enabled;
    public final int nodeId;
    public final int warmNodeId;
    public final PeerConfig peers;
    public final Path stateDir;
    public final TransportFactory transportFactory;

    private RaftConfig(boolean enabled, int nodeId, int warmNodeId,
                       PeerConfig peers, Path stateDir, TransportFactory tf) {
        this.enabled = enabled;
        this.nodeId = nodeId;
        this.warmNodeId = warmNodeId;
        this.peers = peers;
        this.stateDir = stateDir;
        this.transportFactory = tf;
    }

    /** Build a "Raft disabled" config. */
    public static RaftConfig disabled() {
        return new RaftConfig(false, -1, -1, null, null, null);
    }

    /** Build a five-node Raft config rooted at {@code stateDir}. */
    public static RaftConfig of(int nodeId, int warmNodeId,
                                Map<Integer, NetAddress> peerMap,
                                Path stateDir,
                                TransportFactory transportFactory) {
        if (peerMap == null || peerMap.isEmpty()) {
            throw new IllegalArgumentException("peerMap must be non-empty");
        }
        if (transportFactory == null) {
            throw new IllegalArgumentException("transportFactory must be non-null");
        }
        Map<Integer, NetAddress> copy = new HashMap<>(peerMap);
        PeerConfig pc = new PeerConfig(nodeId, copy);
        return new RaftConfig(true, nodeId, warmNodeId, pc, stateDir, transportFactory);
    }
}
