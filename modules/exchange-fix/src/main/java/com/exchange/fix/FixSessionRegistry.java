package com.exchange.fix;

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Maintains the live FIX sessions. Reads/writes from the gateway-io thread
 * are unsynchronised; a {@link ConcurrentHashMap} mirror is used by the
 * gateway-out thread to look up sessions by id without grabbing the io
 * thread's bookkeeping.
 */
public final class FixSessionRegistry {

    private final Long2ObjectOpenHashMap<FixSession> bySessionId = new Long2ObjectOpenHashMap<>(64);
    private final Map<String, FixSession> bySenderCompId = new ConcurrentHashMap<>();
    private final Map<Long, FixSession> bySessionIdConcurrent = new ConcurrentHashMap<>();

    public synchronized void register(FixSession session) {
        bySessionId.put(session.sessionId, session);
        bySenderCompId.put(session.senderCompIdAsClient, session);
        bySessionIdConcurrent.put(session.sessionId, session);
    }

    public synchronized FixSession remove(String senderCompId) {
        FixSession s = bySenderCompId.remove(senderCompId);
        if (s != null) {
            bySessionId.remove(s.sessionId);
            bySessionIdConcurrent.remove(s.sessionId);
        }
        return s;
    }

    public synchronized FixSession bySenderCompId(String senderCompId) {
        return bySenderCompId.get(senderCompId);
    }

    /** Concurrent lookup safe to call from any thread (used by outLoop). */
    public FixSession bySessionId(long sessionId) {
        return bySessionIdConcurrent.get(sessionId);
    }

    public synchronized int size() {
        return bySessionId.size();
    }

    /**
     * Snapshot iteration. Allocation: array per call. Used by the io loop
     * for liveness checks; bounded by #sessions which is typically small.
     */
    public synchronized FixSession[] snapshot() {
        FixSession[] out = new FixSession[bySessionId.size()];
        int i = 0;
        for (FixSession s : bySessionId.values()) {
            out[i++] = s;
        }
        return out;
    }
}
