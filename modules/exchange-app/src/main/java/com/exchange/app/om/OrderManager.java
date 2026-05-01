package com.exchange.app.om;

import com.exchange.domain.ClOrdId;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

/**
 * Per-sender duplicate-clOrdId tracker. The OrderManager is single-threaded
 * (one writer per process) so all maps are unsynchronized; if multiple OM
 * threads are ever introduced, callers must shard by senderId.
 * <p>
 * v1 design: track only the set of seen clOrdId hashes per sender. Cancels
 * intentionally do <em>not</em> evict the entry — a re-use of the same
 * clOrdId after cancel is still treated as a duplicate, which matches FIX
 * 4.4 semantics for ClOrdID uniqueness.
 */
public final class OrderManager {

    private final Long2ObjectOpenHashMap<LongOpenHashSet> bySender;

    public OrderManager() {
        this.bySender = new Long2ObjectOpenHashMap<>(64);
    }

    /** Returns true if {@code clOrdId16} has been seen for {@code senderId}. */
    public boolean isDuplicate(long senderId, byte[] clOrdId16) {
        LongOpenHashSet set = bySender.get(senderId);
        if (set == null) return false;
        return set.contains(ClOrdId.hash64(clOrdId16));
    }

    /** Record {@code clOrdId16} as seen for {@code senderId}. */
    public void register(long senderId, byte[] clOrdId16) {
        LongOpenHashSet set = bySender.get(senderId);
        if (set == null) {
            set = new LongOpenHashSet(64);
            bySender.put(senderId, set);
        }
        set.add(ClOrdId.hash64(clOrdId16));
    }

    /** Number of senders currently tracked. */
    public int senderCount() {
        return bySender.size();
    }

    /** Reset everything (testing helper). */
    public void clear() {
        bySender.clear();
    }
}
