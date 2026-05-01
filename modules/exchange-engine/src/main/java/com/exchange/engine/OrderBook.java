package com.exchange.engine;

import com.exchange.domain.Side;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectRBTreeMap;
import it.unimi.dsi.fastutil.longs.LongComparators;

import java.util.ArrayDeque;

/**
 * Per-symbol two-sided order book. Bids are stored in descending price
 * order so iteration starts at the best bid; asks ascend so iteration
 * starts at the best ask. {@link Long2ObjectOpenHashMap ordersById} gives
 * O(1) lookup-by-orderId for cancels.
 * <p>
 * Empty {@link PriceLevel}s are pooled in a small free-list so price
 * creation/destruction does not allocate on the hot path after warm-up.
 */
public final class OrderBook {

    private final int symbolId;
    private final Long2ObjectRBTreeMap<PriceLevel> bids;
    private final Long2ObjectRBTreeMap<PriceLevel> asks;
    private final Long2ObjectOpenHashMap<OrderEntry> ordersById;
    private final ArrayDeque<PriceLevel> levelPool;

    public OrderBook(int symbolId) {
        this.symbolId = symbolId;
        this.bids = new Long2ObjectRBTreeMap<>(LongComparators.OPPOSITE_COMPARATOR);
        this.asks = new Long2ObjectRBTreeMap<>(LongComparators.NATURAL_COMPARATOR);
        this.ordersById = new Long2ObjectOpenHashMap<>(1024);
        this.levelPool = new ArrayDeque<>(64);
    }

    public int symbolId() {
        return symbolId;
    }

    /**
     * Insert a resting order at the appropriate price level.
     */
    public void addOrder(Order o, OrderEntry e) {
        e.order = o;
        o.entry = e;
        Long2ObjectRBTreeMap<PriceLevel> side = (o.side == Side.CODE_BUY) ? bids : asks;
        PriceLevel level = side.get(o.price);
        if (level == null) {
            level = acquireLevel(o.price);
            side.put(o.price, level);
        }
        level.add(e);
        ordersById.put(o.orderId, e);
    }

    /**
     * Remove an order by id. Returns the entry (now unlinked) or {@code null}
     * if no such order exists.
     */
    public OrderEntry removeOrderById(long orderId) {
        OrderEntry e = ordersById.remove(orderId);
        if (e == null) return null;
        PriceLevel level = e.level;
        if (level != null) {
            level.remove(e);
            if (level.isEmpty()) {
                Long2ObjectRBTreeMap<PriceLevel> side =
                        (e.order != null && e.order.side == Side.CODE_BUY) ? bids : asks;
                side.remove(level.price);
                releaseLevel(level);
            }
        }
        return e;
    }

    /**
     * Best bid level (highest buy price), or {@code null} if no bids.
     */
    public PriceLevel bestBid() {
        if (bids.isEmpty()) return null;
        return bids.get(bids.firstLongKey());
    }

    /**
     * Best ask level (lowest sell price), or {@code null} if no asks.
     */
    public PriceLevel bestAsk() {
        if (asks.isEmpty()) return null;
        return asks.get(asks.firstLongKey());
    }

    /** True if both sides are empty. */
    public boolean isEmpty() {
        return bids.isEmpty() && asks.isEmpty();
    }

    /** Used by the matching engine to remove a level whose qty has gone to zero. */
    void removeLevel(byte sideCode, long price) {
        Long2ObjectRBTreeMap<PriceLevel> side = (sideCode == Side.CODE_BUY) ? bids : asks;
        PriceLevel level = side.remove(price);
        if (level != null) {
            releaseLevel(level);
        }
    }

    /** Track the entry under its order id (engine bypasses addOrder when it's already mapped). */
    void registerEntry(long orderId, OrderEntry e) {
        ordersById.put(orderId, e);
    }

    /** Removes the id mapping (used after a fill consumes the entry). */
    void unregisterById(long orderId) {
        ordersById.remove(orderId);
    }

    /** Number of resting orders (across both sides). For tests/asserts. */
    public int restingOrderCount() {
        return ordersById.size();
    }

    /** Walk all bid levels in descending order. For asserts/tests. */
    public Iterable<PriceLevel> bidLevels() {
        return bids.values();
    }

    /** Walk all ask levels in ascending order. For asserts/tests. */
    public Iterable<PriceLevel> askLevels() {
        return asks.values();
    }

    /** Look up a resting entry by id. */
    public OrderEntry findEntry(long orderId) {
        return ordersById.get(orderId);
    }

    private PriceLevel acquireLevel(long price) {
        PriceLevel level = levelPool.pollFirst();
        if (level == null) {
            level = new PriceLevel();
        }
        level.init(price);
        return level;
    }

    private void releaseLevel(PriceLevel level) {
        level.head = null;
        level.tail = null;
        level.totalQty = 0L;
        level.price = 0L;
        levelPool.push(level);
    }
}
