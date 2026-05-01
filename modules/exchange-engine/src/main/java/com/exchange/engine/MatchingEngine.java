package com.exchange.engine;

import com.exchange.domain.ClOrdId;
import com.exchange.domain.OrdType;
import com.exchange.domain.RejectReason;
import com.exchange.domain.Side;
import com.exchange.domain.Symbols;
import com.exchange.domain.TimeInForce;
import com.exchange.store.EventLogWriter;
import com.exchange.store.EventType;
import com.exchange.time.Clocks;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;

import java.nio.ByteBuffer;
import java.util.function.LongSupplier;

/**
 * Single-threaded price-time matching engine.
 * <p>
 * Public methods correspond to the validated incoming commands the gateway
 * delivers. They run synchronously; emitting one or more events to the
 * canonical {@link EventLogWriter} as a side effect.
 * <p>
 * Allocation policy: after warm-up the steady-state hot path is
 * allocation-free. The only collections that may grow on demand are the
 * per-sender duplicate-detection maps and the price-level pool, both of
 * which are amortized.
 */
public final class MatchingEngine {

    private final EventLogWriter writer;
    private final Symbols symbols;
    private final Int2ObjectOpenHashMap<OrderBook> books;
    private final OrderEntryPool entryPool;
    private final Sequencer sequencer;
    private final LongSupplier termSupplier;
    /**
     * When true, events are appended with status {@code WRITING} and only
     * become visible to downstream consumers after the Raft CommitApplier
     * flips them to {@code COMMITTED}. When false (default, single-node
     * mode), events are appended as already-committed.
     */
    private final boolean raftMode;

    /**
     * Per-sender map from {@code hash64(clOrdId16)} to {@code orderId}.
     * Used for duplicate-clOrdId detection and for cancel-by-clOrdId
     * lookup. {@code defaultReturnValue} is 0L so a miss is unambiguous
     * (we never assign orderId 0).
     */
    private final it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap<Long2LongOpenHashMap> clOrdIndex;

    /** Scratch buffer for cancel id passed to ORDER_CANCELLED encoder. */
    private final byte[] zeroOrigClOrdId = new byte[ClOrdId.LENGTH];

    public MatchingEngine(EventLogWriter writer,
                          Symbols symbols,
                          Int2ObjectOpenHashMap<OrderBook> books,
                          OrderEntryPool entryPool,
                          Sequencer sequencer,
                          LongSupplier termSupplier) {
        this(writer, symbols, books, entryPool, sequencer, termSupplier, false);
    }

    public MatchingEngine(EventLogWriter writer,
                          Symbols symbols,
                          Int2ObjectOpenHashMap<OrderBook> books,
                          OrderEntryPool entryPool,
                          Sequencer sequencer,
                          LongSupplier termSupplier,
                          boolean raftMode) {
        this.writer = writer;
        this.symbols = symbols;
        this.books = books;
        this.entryPool = entryPool;
        this.sequencer = sequencer;
        this.termSupplier = termSupplier == null ? () -> 0L : termSupplier;
        this.raftMode = raftMode;
        this.clOrdIndex = new it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap<>(64);
    }

    /**
     * Append helper that respects {@link #raftMode}: when raftMode is on
     * the entry is left in {@code WRITING} status until the Raft
     * CommitApplier promotes it.
     */
    private void appendEvent(short type, ByteBuffer payload) {
        long term = termSupplier.getAsLong();
        long ts = Clocks.wallClockNanos();
        if (raftMode) {
            writer.appendUncommitted(type, term, ts, payload);
        } else {
            writer.append(type, term, ts, payload);
        }
    }

    public Sequencer sequencer() {
        return sequencer;
    }

    public OrderBook bookFor(int symbolId) {
        return books.get(symbolId);
    }

    // --------------------------------------------------------------------
    // Public command API
    // --------------------------------------------------------------------

    /**
     * Process an incoming new-order request. The caller has populated
     * {@code incoming} with all client-supplied fields (no orderId yet);
     * we assign the orderId, validate, run matching, and emit events.
     */
    public void onNewOrder(Order incoming) {
        // ---------------- Validation ----------------
        OrderBook book = books.get(incoming.symbolId);
        if (book == null) {
            emitReject(incoming.symbolId,
                    RejectReason.UNKNOWN_SYMBOL,
                    incoming.senderId,
                    incoming.clOrdIdBytes,
                    incoming.account,
                    incoming.clientTsNs);
            return;
        }
        if (incoming.qty <= 0L) {
            emitReject(incoming.symbolId,
                    RejectReason.INVALID_QTY,
                    incoming.senderId,
                    incoming.clOrdIdBytes,
                    incoming.account,
                    incoming.clientTsNs);
            return;
        }
        if (incoming.ordType == OrdType.CODE_LIMIT) {
            if (incoming.price <= 0L) {
                emitReject(incoming.symbolId,
                        RejectReason.INVALID_PRICE,
                        incoming.senderId,
                        incoming.clOrdIdBytes,
                        incoming.account,
                        incoming.clientTsNs);
                return;
            }
        } else if (incoming.ordType == OrdType.CODE_MARKET) {
            if (incoming.price != 0L) {
                emitReject(incoming.symbolId,
                        RejectReason.INVALID_PRICE,
                        incoming.senderId,
                        incoming.clOrdIdBytes,
                        incoming.account,
                        incoming.clientTsNs);
                return;
            }
        } else {
            emitReject(incoming.symbolId,
                    RejectReason.INVALID_PRICE,
                    incoming.senderId,
                    incoming.clOrdIdBytes,
                    incoming.account,
                    incoming.clientTsNs);
            return;
        }
        if (incoming.side != Side.CODE_BUY && incoming.side != Side.CODE_SELL) {
            emitReject(incoming.symbolId,
                    RejectReason.INVALID_QTY,
                    incoming.senderId,
                    incoming.clOrdIdBytes,
                    incoming.account,
                    incoming.clientTsNs);
            return;
        }

        long clHash = ClOrdId.hash64(incoming.clOrdIdBytes);
        Long2LongOpenHashMap perSender = clOrdIndex.get(incoming.senderId);
        if (perSender != null && perSender.containsKey(clHash)) {
            emitReject(incoming.symbolId,
                    RejectReason.DUPLICATE_CLORDID,
                    incoming.senderId,
                    incoming.clOrdIdBytes,
                    incoming.account,
                    incoming.clientTsNs);
            return;
        }

        // ---------------- Assign orderId, emit ACCEPTED ----------------
        long orderId = sequencer.nextOrderId();
        incoming.orderId = orderId;
        if (incoming.enteredTsNs == 0L) {
            incoming.enteredTsNs = Clocks.nanoTime();
        }
        if (perSender == null) {
            perSender = new Long2LongOpenHashMap(16);
            perSender.defaultReturnValue(0L);
            clOrdIndex.put(incoming.senderId, perSender);
        }
        perSender.put(clHash, orderId);

        emitAccepted(incoming);

        // ---------------- Matching ----------------
        match(book, incoming);

        // ---------------- Rest or discard remainder ----------------
        if (incoming.remainingQty > 0L) {
            if (incoming.ordType == OrdType.CODE_LIMIT && incoming.tif == TimeInForce.CODE_DAY) {
                OrderEntry entry = entryPool.acquire();
                book.addOrder(incoming, entry);
            } else {
                // MARKET (or IOC remainder when wired) — discard with no
                // additional event; the ACCEPTED + any TRADEs are the trail.
                // Map remains so the orderId stays "known" for later lookups.
            }
        }
    }

    /**
     * Cancel a resting order by {@code (senderId, clOrdId)}. Returns true
     * on success, false if no such order exists (in which case an
     * ORDER_REJECTED with UNKNOWN_ORDER is also emitted).
     */
    public boolean onCancel(long senderId, byte[] clOrdId16, long clientTsNs) {
        long clHash = ClOrdId.hash64(clOrdId16);
        Long2LongOpenHashMap perSender = clOrdIndex.get(senderId);
        long orderId = (perSender == null) ? 0L : perSender.get(clHash);
        if (orderId == 0L) {
            emitReject(0,
                    RejectReason.UNKNOWN_ORDER,
                    senderId,
                    clOrdId16,
                    0L,
                    clientTsNs);
            return false;
        }
        // Find the resting entry across all books
        OrderEntry entry = findRestingEntryById(orderId);
        if (entry == null || entry.order == null) {
            // The order id was once known but is no longer resting — already
            // filled or already cancelled. Reject as UNKNOWN_ORDER.
            emitReject(0,
                    RejectReason.UNKNOWN_ORDER,
                    senderId,
                    clOrdId16,
                    0L,
                    clientTsNs);
            return false;
        }
        Order order = entry.order;
        OrderBook book = books.get(order.symbolId);
        if (book != null) {
            book.removeOrderById(order.orderId);
        }
        // Forget the (senderId, clOrdId) → orderId mapping so a subsequent
        // new with the same id is allowed.
        if (perSender != null) {
            perSender.remove(clHash);
        }
        emitCancelled(order, zeroOrigClOrdId, clientTsNs);
        order.entry = null;
        entryPool.release(entry);
        return true;
    }

    /**
     * Cancel-replace: cancel the order known by {@code (senderId, origClOrdId)}
     * and submit a new order with {@code (senderId, newClOrdId)}, new qty, new
     * price (limit) under a fresh orderId. The original cancel emits an
     * {@code ORDER_CANCELLED} with {@code origClOrdId} carried in the
     * {@code origClOrdId} field of the cancel event.
     */
    public boolean onCancelReplace(long senderId,
                                   byte[] origClOrdId16,
                                   byte[] newClOrdId16,
                                   long newQty,
                                   long newPrice,
                                   long clientTsNs) {
        long origHash = ClOrdId.hash64(origClOrdId16);
        Long2LongOpenHashMap perSender = clOrdIndex.get(senderId);
        long origOrderId = (perSender == null) ? 0L : perSender.get(origHash);
        if (origOrderId == 0L) {
            emitReject(0,
                    RejectReason.UNKNOWN_ORDER,
                    senderId,
                    origClOrdId16,
                    0L,
                    clientTsNs);
            return false;
        }
        OrderEntry entry = findRestingEntryById(origOrderId);
        if (entry == null || entry.order == null) {
            emitReject(0,
                    RejectReason.UNKNOWN_ORDER,
                    senderId,
                    origClOrdId16,
                    0L,
                    clientTsNs);
            return false;
        }
        Order orig = entry.order;
        OrderBook book = books.get(orig.symbolId);
        if (book != null) {
            book.removeOrderById(orig.orderId);
        }
        // Cancel event carries the original clOrdId in origClOrdId field
        // and the original's clOrdId in clOrdId field. We emit BEFORE
        // building the replacement so the audit trail is in order.
        emitCancelled(orig, origClOrdId16, clientTsNs);
        if (perSender != null) {
            perSender.remove(origHash);
        }

        // Build replacement order, reusing fields of the original
        Order replacement = new Order();
        replacement.set(0L,
                orig.symbolId,
                orig.side,
                orig.ordType,
                orig.tif,
                orig.account,
                orig.senderId,
                newClOrdId16,
                newQty,
                newPrice,
                Clocks.nanoTime(),
                clientTsNs);

        // Release original entry first, then the original Order can also be
        // dropped (caller-owned; we don't pool Order here).
        orig.entry = null;
        entryPool.release(entry);

        onNewOrder(replacement);
        return true;
    }

    // --------------------------------------------------------------------
    // Matching
    // --------------------------------------------------------------------

    private void match(OrderBook book, Order taker) {
        if (taker.side == Side.CODE_BUY) {
            // BUY taker eats from asks (lowest first)
            while (taker.remainingQty > 0L) {
                PriceLevel level = book.bestAsk();
                if (level == null) break;
                if (taker.ordType == OrdType.CODE_LIMIT && taker.price < level.price) break;
                if (!fillLevel(book, level, taker, Side.CODE_SELL)) break;
            }
        } else {
            // SELL taker eats from bids (highest first)
            while (taker.remainingQty > 0L) {
                PriceLevel level = book.bestBid();
                if (level == null) break;
                if (taker.ordType == OrdType.CODE_LIMIT && taker.price > level.price) break;
                if (!fillLevel(book, level, taker, Side.CODE_BUY)) break;
            }
        }
    }

    /**
     * Drain {@code level} into {@code taker} until either runs out. Returns
     * true if the taker can keep going (i.e., the level was fully consumed
     * but the taker still has qty); false if matching for this taker should
     * stop (taker filled).
     */
    private boolean fillLevel(OrderBook book, PriceLevel level, Order taker, byte makerSideCode) {
        while (level.head != null && taker.remainingQty > 0L) {
            OrderEntry entry = level.head;
            Order maker = entry.order;
            long fillQty = Math.min(taker.remainingQty, maker.remainingQty);

            long tradeTs = Clocks.nanoTime();
            ByteBuffer payload = EngineEventCodec.encodeTrade(
                    book.symbolId(),
                    maker.orderId,
                    taker.orderId,
                    fillQty,
                    level.price,
                    makerSideCode,
                    tradeTs,
                    taker.clientTsNs);
            appendEvent(EventType.TRADE, payload);

            maker.remainingQty -= fillQty;
            taker.remainingQty -= fillQty;
            level.totalQty -= fillQty;
            if (level.totalQty < 0L) level.totalQty = 0L;

            if (maker.remainingQty == 0L) {
                level.remove(entry);
                book.unregisterById(maker.orderId);
                // Forget maker's (senderId, clOrdId) since it's fully filled
                Long2LongOpenHashMap mkSender = clOrdIndex.get(maker.senderId);
                if (mkSender != null) {
                    long h = ClOrdId.hash64(maker.clOrdIdBytes);
                    mkSender.remove(h);
                }
                maker.entry = null;
                entryPool.release(entry);
            }
        }
        if (level.isEmpty()) {
            book.removeLevel(makerSideCode, level.price);
        }
        return taker.remainingQty > 0L;
    }

    // --------------------------------------------------------------------
    // Lookups
    // --------------------------------------------------------------------

    private OrderEntry findRestingEntryById(long orderId) {
        // The engine doesn't keep a global orderId → book map. We could,
        // but given a single live order at a time per id we just probe
        // each book. Number of books is small (~symbols), so this is O(S).
        // For high-symbol-count deployments we'd add an Long2ObjectOpenHashMap.
        for (Int2ObjectOpenHashMap.Entry<OrderBook> e : books.int2ObjectEntrySet()) {
            OrderEntry entry = e.getValue().findEntry(orderId);
            if (entry != null) return entry;
        }
        return null;
    }

    // --------------------------------------------------------------------
    // Event emit helpers
    // --------------------------------------------------------------------

    private void emitAccepted(Order o) {
        ByteBuffer payload = EngineEventCodec.encodeOrderAccepted(o);
        appendEvent(EventType.ORDER_ACCEPTED, payload);
    }

    private void emitCancelled(Order o, byte[] origClOrdId16, long clientTsNs) {
        ByteBuffer payload = EngineEventCodec.encodeOrderCancelled(o, origClOrdId16, clientTsNs);
        appendEvent(EventType.ORDER_CANCELLED, payload);
    }

    private void emitReject(int symbolId,
                            byte reason,
                            long senderId,
                            byte[] clOrdId16,
                            long account,
                            long clientTsNs) {
        ByteBuffer payload = EngineEventCodec.encodeOrderRejected(
                symbolId, reason, senderId, clOrdId16, account, clientTsNs);
        appendEvent(EventType.ORDER_REJECTED, payload);
    }
}
