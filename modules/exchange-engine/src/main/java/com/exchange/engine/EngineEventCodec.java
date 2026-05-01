package com.exchange.engine;

import com.exchange.events.OrderAcceptedEvent;
import com.exchange.events.OrderCancelledEvent;
import com.exchange.events.OrderRejectedEvent;
import com.exchange.events.TradeEvent;
import com.exchange.store.EventLog;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Allocation-free encoding helpers for the four engine event payloads. Each
 * method writes into a thread-local direct {@link ByteBuffer} (capacity
 * {@link EventLog#MAX_PAYLOAD}) and returns it positioned at 0 with limit
 * set to the payload size, ready to pass to
 * {@code EventLogWriter.append(short, long, long, ByteBuffer)}.
 */
public final class EngineEventCodec {

    /**
     * Per-engine-thread reusable scratch. The matching engine is single-threaded
     * but we use ThreadLocal anyway so tests / future call sites are safe.
     */
    private static final ThreadLocal<ByteBuffer> SCRATCH = ThreadLocal.withInitial(() -> {
        ByteBuffer b = ByteBuffer.allocateDirect(EventLog.MAX_PAYLOAD);
        b.order(ByteOrder.LITTLE_ENDIAN);
        return b;
    });

    private EngineEventCodec() {
        // utility class
    }

    /** Returns the (cleared) thread-local scratch buffer. */
    public static ByteBuffer scratch() {
        ByteBuffer b = SCRATCH.get();
        b.clear();
        return b;
    }

    /**
     * Encode an ORDER_ACCEPTED payload from {@code o} into the thread-local
     * scratch buffer; returns the buffer positioned at 0 and limited to
     * {@link OrderAcceptedEvent#SIZE}.
     */
    public static ByteBuffer encodeOrderAccepted(Order o) {
        ByteBuffer buf = scratch();
        OrderAcceptedEvent.encode(buf, 0,
                o.orderId,
                o.symbolId,
                o.side,
                o.ordType,
                o.tif,
                o.qty,
                o.price,
                o.account,
                o.senderId,
                o.clOrdIdBytes,
                o.clientTsNs,
                o.enteredTsNs);
        buf.position(0).limit(OrderAcceptedEvent.SIZE);
        return buf;
    }

    public static ByteBuffer encodeTrade(int symbolId,
                                         long makerOrderId,
                                         long takerOrderId,
                                         long qty,
                                         long price,
                                         byte makerSide,
                                         long tradeTsNs,
                                         long clientTsNs) {
        ByteBuffer buf = scratch();
        TradeEvent.encode(buf, 0,
                symbolId,
                makerOrderId,
                takerOrderId,
                qty,
                price,
                makerSide,
                tradeTsNs,
                clientTsNs);
        buf.position(0).limit(TradeEvent.SIZE);
        return buf;
    }

    public static ByteBuffer encodeOrderCancelled(Order o,
                                                  byte[] origClOrdId16,
                                                  long clientTsNs) {
        ByteBuffer buf = scratch();
        OrderCancelledEvent.encode(buf, 0,
                o.orderId,
                o.symbolId,
                o.remainingQty,
                o.senderId,
                o.clOrdIdBytes,
                origClOrdId16,
                o.account,
                clientTsNs);
        buf.position(0).limit(OrderCancelledEvent.SIZE);
        return buf;
    }

    public static ByteBuffer encodeOrderRejected(int symbolId,
                                                 byte reason,
                                                 long senderId,
                                                 byte[] clOrdId16,
                                                 long account,
                                                 long clientTsNs) {
        ByteBuffer buf = scratch();
        OrderRejectedEvent.encode(buf, 0,
                symbolId,
                reason,
                senderId,
                clOrdId16,
                account,
                clientTsNs);
        buf.position(0).limit(OrderRejectedEvent.SIZE);
        return buf;
    }
}
