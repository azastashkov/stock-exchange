package com.exchange.app.reporter;

import com.exchange.app.gateway.GatewayOutEmitter;
import com.exchange.commands.CommandCodec;
import com.exchange.commands.ExecType;
import com.exchange.commands.ExecutionReportCommand;
import com.exchange.domain.ClOrdId;
import com.exchange.domain.OrderStatus;
import com.exchange.events.OrderAcceptedEvent;
import com.exchange.events.OrderCancelledEvent;
import com.exchange.events.OrderRejectedEvent;
import com.exchange.events.TradeEvent;
import com.exchange.loop.ApplicationLoop;
import com.exchange.store.EventLogReader;
import com.exchange.store.EventType;
import com.exchange.store.EventView;
import com.exchange.time.Clocks;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/**
 * Reporter loop. Reads from the canonical event log via its own cursor
 * ({@code reporter}) and emits {@link ExecutionReportCommand}s onto the
 * gateway-out ring, with routing by {@code sessionId}.
 * <p>
 * Maintains an {@link OrderRegistry} so trade events can be expanded into
 * maker+taker pairs with full context (clOrdId, sessionId, etc.).
 */
public final class ReporterService extends ApplicationLoop {

    private final EventLogReader reader;
    private final GatewayOutEmitter gatewayOut;
    private final OrderRegistry registry;
    private final SessionRegistry sessions;

    private final EventView view = new EventView();
    private final OrderAcceptedEvent.MutableOrderAccepted mutAccepted = new OrderAcceptedEvent.MutableOrderAccepted();
    private final TradeEvent.MutableTrade mutTrade = new TradeEvent.MutableTrade();
    private final OrderCancelledEvent.MutableOrderCancelled mutCancelled = new OrderCancelledEvent.MutableOrderCancelled();
    private final OrderRejectedEvent.MutableOrderRejected mutRejected = new OrderRejectedEvent.MutableOrderRejected();
    private final byte[] textBuf = new byte[ExecutionReportCommand.TEXT_LENGTH];

    private final Timer roundtripLatency;
    private final Counter execAcceptedCounter;
    private final Counter execTradeCounter;
    private final Counter execCancelledCounter;
    private final Counter execRejectedCounter;

    private static final byte[] ZERO_CL = new byte[ClOrdId.LENGTH];

    public ReporterService(EventLogReader reader,
                           GatewayOutEmitter gatewayOut,
                           OrderRegistry registry,
                           SessionRegistry sessions,
                           MeterRegistry mr) {
        super("reporter", false);
        this.reader = reader;
        this.gatewayOut = gatewayOut;
        this.registry = registry;
        this.sessions = sessions;
        this.roundtripLatency = mr == null ? null
                : Timer.builder("exchange.roundtrip.latency")
                .publishPercentileHistogram()
                .register(mr);
        this.execAcceptedCounter = mr == null ? null : Counter.builder("exchange.reporter.accepted").register(mr);
        this.execTradeCounter = mr == null ? null : Counter.builder("exchange.reporter.trades").register(mr);
        this.execCancelledCounter = mr == null ? null : Counter.builder("exchange.reporter.cancelled").register(mr);
        this.execRejectedCounter = mr == null ? null : Counter.builder("exchange.reporter.rejected").register(mr);
    }

    @Override
    protected boolean pollOnce() {
        if (!reader.poll(view)) return false;
        ByteBuffer payload = view.payloadBuffer();
        int basePos = payload.position();
        short type = view.type();
        switch (type) {
            case EventType.ORDER_ACCEPTED -> handleAccepted(payload, basePos);
            case EventType.TRADE -> handleTrade(payload, basePos);
            case EventType.ORDER_CANCELLED -> handleCancelled(payload, basePos);
            case EventType.ORDER_REJECTED -> handleRejected(payload, basePos);
            default -> {
                // Other events (BOOK_OPEN, BOOK_CLOSE, RISK_REJECTED) ignored.
            }
        }
        return true;
    }

    private void handleAccepted(ByteBuffer payload, int basePos) {
        OrderAcceptedEvent.decode(payload, basePos, mutAccepted);
        OrderRegistry.OrderRecord r = registry.acquire();
        r.orderId = mutAccepted.orderId;
        r.sessionId = sessions == null ? 0L
                : sessions.get(mutAccepted.senderId, mutAccepted.clOrdId);
        r.senderId = mutAccepted.senderId;
        r.account = mutAccepted.account;
        r.symbolId = mutAccepted.symbolId;
        r.side = mutAccepted.side;
        r.origQty = mutAccepted.qty;
        r.filledQty = 0L;
        r.price = mutAccepted.price;
        r.clientTsNs = mutAccepted.clientTsNs;
        for (int i = 0; i < ClOrdId.LENGTH; i++) {
            r.clOrdId[i] = mutAccepted.clOrdId[i];
        }
        registry.register(r);

        emitExecReport(
                /*sessionId*/ r.sessionId,
                r.senderId,
                r.orderId,
                r.symbolId,
                r.side,
                ExecType.NEW,
                (byte) OrderStatus.NEW.ordinal(),
                /*rejectReason*/ (byte) 0,
                r.origQty,
                /*filledQty*/ 0L,
                r.origQty,
                r.price,
                /*lastQty*/ 0L,
                /*lastPx*/ 0L,
                r.account,
                r.clientTsNs,
                r.clOrdId,
                ZERO_CL,
                emptyText());
        if (execAcceptedCounter != null) execAcceptedCounter.increment();
    }

    private void handleTrade(ByteBuffer payload, int basePos) {
        TradeEvent.decode(payload, basePos, mutTrade);
        OrderRegistry.OrderRecord maker = registry.byOrderId(mutTrade.makerOrderId);
        OrderRegistry.OrderRecord taker = registry.byOrderId(mutTrade.takerOrderId);
        long lastQty = mutTrade.qty;
        long lastPx = mutTrade.price;

        if (maker != null) {
            maker.filledQty += lastQty;
            byte execType = maker.filledQty >= maker.origQty ? ExecType.FILL : ExecType.PARTIAL_FILL;
            byte ordStatus = maker.filledQty >= maker.origQty
                    ? (byte) OrderStatus.FILLED.ordinal()
                    : (byte) OrderStatus.PARTIALLY_FILLED.ordinal();
            emitExecReport(
                    maker.sessionId,
                    maker.senderId,
                    maker.orderId,
                    maker.symbolId,
                    maker.side,
                    execType,
                    ordStatus,
                    /*rejectReason*/ (byte) 0,
                    maker.origQty,
                    maker.filledQty,
                    Math.max(0L, maker.origQty - maker.filledQty),
                    maker.price,
                    lastQty,
                    lastPx,
                    maker.account,
                    maker.clientTsNs,
                    maker.clOrdId,
                    ZERO_CL,
                    emptyText());
            if (execTradeCounter != null) execTradeCounter.increment();
            if (maker.filledQty >= maker.origQty) {
                if (sessions != null) sessions.remove(maker.senderId, maker.clOrdId);
                registry.retire(maker.orderId);
            }
        }
        if (taker != null) {
            taker.filledQty += lastQty;
            byte execType = taker.filledQty >= taker.origQty ? ExecType.FILL : ExecType.PARTIAL_FILL;
            byte ordStatus = taker.filledQty >= taker.origQty
                    ? (byte) OrderStatus.FILLED.ordinal()
                    : (byte) OrderStatus.PARTIALLY_FILLED.ordinal();
            emitExecReport(
                    taker.sessionId,
                    taker.senderId,
                    taker.orderId,
                    taker.symbolId,
                    taker.side,
                    execType,
                    ordStatus,
                    /*rejectReason*/ (byte) 0,
                    taker.origQty,
                    taker.filledQty,
                    Math.max(0L, taker.origQty - taker.filledQty),
                    taker.price,
                    lastQty,
                    lastPx,
                    taker.account,
                    taker.clientTsNs,
                    taker.clOrdId,
                    ZERO_CL,
                    emptyText());
            if (execTradeCounter != null) execTradeCounter.increment();
            if (taker.filledQty >= taker.origQty) {
                if (sessions != null) sessions.remove(taker.senderId, taker.clOrdId);
                registry.retire(taker.orderId);
            }
        }
    }

    private void handleCancelled(ByteBuffer payload, int basePos) {
        OrderCancelledEvent.decode(payload, basePos, mutCancelled);
        OrderRegistry.OrderRecord r = registry.byOrderId(mutCancelled.orderId);
        long sessionId = r != null ? r.sessionId : 0L;
        long senderId = r != null ? r.senderId : mutCancelled.senderId;
        long account = r != null ? r.account : mutCancelled.account;
        int symbolId = r != null ? r.symbolId : mutCancelled.symbolId;
        byte side = r != null ? r.side : 0;
        long origQty = r != null ? r.origQty : mutCancelled.remainingQty;
        long filledQty = r != null ? r.filledQty : 0L;
        long price = r != null ? r.price : 0L;
        emitExecReport(
                sessionId,
                senderId,
                mutCancelled.orderId,
                symbolId,
                side,
                ExecType.CANCELED,
                (byte) OrderStatus.CANCELLED.ordinal(),
                /*rejectReason*/ (byte) 0,
                origQty,
                filledQty,
                mutCancelled.remainingQty,
                price,
                /*lastQty*/ 0L,
                /*lastPx*/ 0L,
                account,
                mutCancelled.clientTsNs,
                mutCancelled.clOrdId,
                mutCancelled.origClOrdId,
                emptyText());
        if (execCancelledCounter != null) execCancelledCounter.increment();
        if (r != null) {
            if (sessions != null) sessions.remove(r.senderId, r.clOrdId);
            registry.retire(mutCancelled.orderId);
        }
    }

    private void handleRejected(ByteBuffer payload, int basePos) {
        OrderRejectedEvent.decode(payload, basePos, mutRejected);
        emitExecReport(
                /*sessionId*/ 0L,
                mutRejected.senderId,
                /*orderId*/ 0L,
                mutRejected.symbolId,
                /*side*/ (byte) 0,
                ExecType.REJECTED,
                (byte) OrderStatus.REJECTED.ordinal(),
                mutRejected.reasonCode,
                /*qty*/ 0L,
                /*filledQty*/ 0L,
                /*remainingQty*/ 0L,
                /*price*/ 0L,
                /*lastQty*/ 0L,
                /*lastPx*/ 0L,
                mutRejected.account,
                mutRejected.clientTsNs,
                mutRejected.clOrdId,
                ZERO_CL,
                emptyText());
        if (execRejectedCounter != null) execRejectedCounter.increment();
    }

    private byte[] emptyText() {
        for (int i = 0; i < textBuf.length; i++) textBuf[i] = 0;
        return textBuf;
    }

    private void emitExecReport(long sessionId,
                                long senderId,
                                long orderId,
                                int symbolId,
                                byte side,
                                byte execType,
                                byte ordStatus,
                                byte rejectReason,
                                long qty,
                                long filledQty,
                                long remainingQty,
                                long price,
                                long lastQty,
                                long lastPx,
                                long account,
                                long clientTsNs,
                                byte[] clOrdId16,
                                byte[] origClOrdId16,
                                byte[] textAscii) {
        long now = Clocks.nanoTime();
        ByteBuffer out = CommandCodec.scratch();
        ExecutionReportCommand.encode(out, 0,
                sessionId,
                senderId,
                orderId,
                symbolId,
                side,
                execType,
                ordStatus,
                rejectReason,
                qty,
                filledQty,
                remainingQty,
                price,
                lastQty,
                lastPx,
                account,
                clientTsNs,
                now,
                clOrdId16,
                origClOrdId16,
                textAscii);
        out.position(0);
        out.limit(ExecutionReportCommand.SIZE);
        gatewayOut.offerSpin(out);
        if (roundtripLatency != null && clientTsNs > 0L) {
            long delta = now - clientTsNs;
            if (delta > 0L) {
                roundtripLatency.record(delta, TimeUnit.NANOSECONDS);
            }
        }
    }
}
