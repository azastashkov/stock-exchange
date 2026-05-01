package com.exchange.raft;

import java.io.Closeable;

/**
 * Bi-directional Raft RPC transport. Implementations:
 * <ul>
 *   <li>{@code NioRpcTransport} — production, one TCP connection per peer.</li>
 *   <li>{@code InMemoryRpcTransport} — tests; routes messages within JVM
 *       through a shared registry, with optional partition simulation.</li>
 * </ul>
 * The transport delivers inbound messages by invoking {@link Listener#onMessage}
 * on the receiving node's listener. The Raft node's tick loop owns the
 * single thread that processes inbound messages; the transport must marshal
 * inbound calls onto that thread (via a queue) — the listener's contract is
 * that {@code onMessage} is invoked from the Raft loop only.
 * <p>
 * Outbound {@link #send(int, RaftRpc.Message)} is fire-and-forget; failure
 * to deliver is silent (Raft tolerates dropped packets).
 */
public interface RpcTransport extends Closeable {

    /** Subscribe to inbound messages. Replaces any previous listener. */
    void setListener(Listener listener);

    /**
     * Best-effort fire-and-forget send. Returns true if the message was
     * accepted for transport (queued), false if the transport could not
     * accept it (closed, peer unknown, etc.).
     */
    boolean send(int peerId, RaftRpc.Message msg);

    /** Start any background networking threads. Idempotent. */
    void start();

    @Override
    void close();

    /** Receiver-side listener invoked from the Raft node's loop thread. */
    @FunctionalInterface
    interface Listener {
        void onMessage(RaftRpc.Message msg);
    }
}
