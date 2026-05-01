package com.exchange.app.reporter;

import com.exchange.domain.ClOrdId;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;

/**
 * Cross-loop map from {@code (senderId, clOrdIdHash)} to {@code sessionId}.
 * Written by the OrderManager when a NEW_ORDER (or CANCEL_REPLACE) is
 * forwarded; read by the Reporter when expanding events into Execution
 * Reports so they can be routed back to the originating session.
 * <p>
 * Updates and reads are bounded-rate so {@code synchronized} on this is
 * adequate for v1.
 */
public final class SessionRegistry {

    private final Long2LongOpenHashMap map;

    public SessionRegistry() {
        this.map = new Long2LongOpenHashMap(256);
        this.map.defaultReturnValue(0L);
    }

    public synchronized void put(long senderId, byte[] clOrdId16, long sessionId) {
        map.put(key(senderId, clOrdId16), sessionId);
    }

    public synchronized long get(long senderId, byte[] clOrdId16) {
        return map.get(key(senderId, clOrdId16));
    }

    /** Forget a (senderId, clOrdId) once the order is terminal. */
    public synchronized void remove(long senderId, byte[] clOrdId16) {
        map.remove(key(senderId, clOrdId16));
    }

    public synchronized int size() {
        return map.size();
    }

    private static long key(long senderId, byte[] clOrdId16) {
        return ClOrdId.hash64(clOrdId16) ^ (senderId * 0x9E3779B97F4A7C15L);
    }
}
