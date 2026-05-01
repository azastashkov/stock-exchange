package com.exchange.app.reporter;

import com.exchange.domain.ClOrdId;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Order-id keyed registry of in-flight order metadata. Owned by the
 * Reporter loop (single writer); other readers can be added later.
 * <p>
 * Records are pooled to keep the hot path allocation-free after warm-up.
 */
public final class OrderRegistry {

    public static final class OrderRecord {
        public long orderId;
        public long sessionId;
        public long senderId;
        public long account;
        public int symbolId;
        public byte side;
        public long origQty;
        public long filledQty;
        public long price;
        public long clientTsNs;
        public final byte[] clOrdId = new byte[ClOrdId.LENGTH];

        public void reset() {
            orderId = 0;
            sessionId = 0;
            senderId = 0;
            account = 0;
            symbolId = 0;
            side = 0;
            origQty = 0;
            filledQty = 0;
            price = 0;
            clientTsNs = 0;
            for (int i = 0; i < clOrdId.length; i++) clOrdId[i] = 0;
        }

        public long remainingQty() {
            return Math.max(0L, origQty - filledQty);
        }
    }

    private final Long2ObjectOpenHashMap<OrderRecord> byOrderId;
    /** Reverse index: hash(senderId, clOrdId) → orderId. */
    private final Long2LongOpenHashMap byClOrdId;
    private final Deque<OrderRecord> pool;

    public OrderRegistry(int sizeHint) {
        this.byOrderId = new Long2ObjectOpenHashMap<>(Math.max(64, sizeHint));
        this.byClOrdId = new Long2LongOpenHashMap(Math.max(64, sizeHint));
        this.byClOrdId.defaultReturnValue(0L);
        this.pool = new ArrayDeque<>(64);
        for (int i = 0; i < Math.min(sizeHint, 1024); i++) {
            pool.push(new OrderRecord());
        }
    }

    /** Acquire a record from the pool (or allocate fresh if drained). */
    public OrderRecord acquire() {
        OrderRecord r = pool.pollFirst();
        if (r == null) return new OrderRecord();
        r.reset();
        return r;
    }

    /** Insert a record into the registry. Caller previously {@link #acquire}d it. */
    public void register(OrderRecord r) {
        byOrderId.put(r.orderId, r);
        byClOrdId.put(senderClHash(r.senderId, r.clOrdId), r.orderId);
    }

    public OrderRecord byOrderId(long orderId) {
        return byOrderId.get(orderId);
    }

    public long orderIdFor(long senderId, byte[] clOrdId16) {
        return byClOrdId.get(senderClHash(senderId, clOrdId16));
    }

    /**
     * Apply a fill: increase filledQty. Returns the (possibly fully-filled)
     * record; if fully filled, the record is also removed from the registry
     * and returned to the pool, so callers must finish using it before the
     * next call.
     */
    public OrderRecord applyFill(long orderId, long fillQty) {
        OrderRecord r = byOrderId.get(orderId);
        if (r == null) return null;
        r.filledQty += fillQty;
        return r;
    }

    /** Remove and return record to pool after order is terminal. */
    public void retire(long orderId) {
        OrderRecord r = byOrderId.remove(orderId);
        if (r == null) return;
        byClOrdId.remove(senderClHash(r.senderId, r.clOrdId));
        pool.push(r);
    }

    public int liveOrderCount() {
        return byOrderId.size();
    }

    private static long senderClHash(long senderId, byte[] clOrdId16) {
        long h = ClOrdId.hash64(clOrdId16);
        // Mix in senderId so different senders with the same clOrdId don't collide.
        return h ^ (senderId * 0x9E3779B97F4A7C15L);
    }
}
