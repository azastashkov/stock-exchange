package com.exchange.app.risk;

import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

/**
 * In-process exposure book per (account, symbol). Tracks signed net
 * position quantity, signed net notional, and volume-weighted average
 * price.
 * <p>
 * Single writer (the PositionKeeper loop); readers are the Risk loop and
 * monitoring. We use the cheapest possible synchronization that's still
 * correct: {@code synchronized} on this. Updates are at trade-rate (~10k/s),
 * so contention is low; the read path is also infrequent because we only
 * check on accept of a NEW order.
 */
public final class AccountExposure {

    /** Snapshot of one (account,symbol) point. Mutated only under the lock. */
    public static final class Snapshot {
        public long netQty;
        public long netNotional;
        public long lastPrice;
        public long vwap;
        public long tradedQtyAbs;

        public void copyFrom(Snapshot o) {
            this.netQty = o.netQty;
            this.netNotional = o.netNotional;
            this.lastPrice = o.lastPrice;
            this.vwap = o.vwap;
            this.tradedQtyAbs = o.tradedQtyAbs;
        }
    }

    private final Long2ObjectOpenHashMap<Int2ObjectOpenHashMap<Snapshot>> byAccount;
    /** Last-trade-price per symbol (for fat-finger band). */
    private final Int2LongOpenHashMap lastPriceBySymbol;

    public AccountExposure() {
        this.byAccount = new Long2ObjectOpenHashMap<>(64);
        this.lastPriceBySymbol = new Int2LongOpenHashMap(32);
        this.lastPriceBySymbol.defaultReturnValue(0L);
    }

    /**
     * Apply a fill: qty signed (positive for taker buy fills, negative for
     * taker sell fills). Adjust the signed account position.
     */
    public synchronized void onFill(long account, int symbolId, long signedQty, long price) {
        Int2ObjectOpenHashMap<Snapshot> map = byAccount.get(account);
        if (map == null) {
            map = new Int2ObjectOpenHashMap<>(16);
            byAccount.put(account, map);
        }
        Snapshot s = map.get(symbolId);
        if (s == null) {
            s = new Snapshot();
            map.put(symbolId, s);
        }
        s.netQty += signedQty;
        s.netNotional += signedQty * price;
        long newAbs = s.tradedQtyAbs + Math.abs(signedQty);
        if (newAbs > 0L) {
            // VWAP weighted by total traded qty (absolute, not net)
            s.vwap = (s.vwap * s.tradedQtyAbs + price * Math.abs(signedQty)) / newAbs;
        }
        s.tradedQtyAbs = newAbs;
        s.lastPrice = price;

        lastPriceBySymbol.put(symbolId, price);
    }

    /** Read-only signed net qty for (account,symbol). 0 if unknown. */
    public synchronized long netQty(long account, int symbolId) {
        Int2ObjectOpenHashMap<Snapshot> map = byAccount.get(account);
        if (map == null) return 0L;
        Snapshot s = map.get(symbolId);
        return s == null ? 0L : s.netQty;
    }

    /**
     * Total absolute open notional for an account: sum of |netQty| * mark
     * across symbols, where mark is last trade price (fallback to VWAP).
     * Approximate but adequate for risk gating.
     */
    public synchronized long openNotional(long account) {
        Int2ObjectOpenHashMap<Snapshot> map = byAccount.get(account);
        if (map == null) return 0L;
        long total = 0L;
        for (Int2ObjectMap.Entry<Snapshot> e : map.int2ObjectEntrySet()) {
            Snapshot s = e.getValue();
            long mark = s.lastPrice == 0L ? s.vwap : s.lastPrice;
            total += Math.abs(s.netQty) * mark;
        }
        return total;
    }

    /** Snapshot copy of (account,symbol) into the caller-supplied holder. */
    public synchronized void copyInto(long account, int symbolId, Snapshot dst) {
        dst.netQty = 0;
        dst.netNotional = 0;
        dst.lastPrice = 0;
        dst.vwap = 0;
        dst.tradedQtyAbs = 0;
        Int2ObjectOpenHashMap<Snapshot> map = byAccount.get(account);
        if (map == null) return;
        Snapshot s = map.get(symbolId);
        if (s == null) return;
        dst.copyFrom(s);
    }

    public synchronized long lastTradePrice(int symbolId) {
        return lastPriceBySymbol.get(symbolId);
    }

    public synchronized int accountCount() {
        return byAccount.size();
    }
}
