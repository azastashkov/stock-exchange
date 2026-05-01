package com.exchange.app.risk;

import com.exchange.app.gateway.GatewayOutEmitter;
import com.exchange.commands.CancelReplaceCommand;
import com.exchange.commands.CommandCodec;
import com.exchange.commands.CommandType;
import com.exchange.commands.ExecType;
import com.exchange.commands.ExecutionReportCommand;
import com.exchange.commands.NewOrderCommand;
import com.exchange.domain.ClOrdId;
import com.exchange.domain.OrdType;
import com.exchange.domain.OrderStatus;
import com.exchange.domain.RejectReason;
import com.exchange.domain.Symbols;
import com.exchange.ipc.MutableSlice;
import com.exchange.ipc.RingConsumer;
import com.exchange.ipc.RingProducer;
import com.exchange.loop.ApplicationLoop;
import com.exchange.time.Clocks;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

import java.nio.ByteBuffer;

/**
 * Pre-trade risk gate. Reads from {@code om-out}; on accept forwards the
 * raw command bytes to {@code risk-out}; on reject emits an
 * {@code ExecutionReportCommand} REJECTED with reason {@code RISK_LIMIT}
 * directly on the gateway-out ring.
 * <p>
 * Cancels are passed through unchecked.
 */
public final class AggregatedRiskCheck extends ApplicationLoop {

    private final RingConsumer omOut;
    private final RingProducer riskOut;
    private final GatewayOutEmitter gatewayOut;
    private final RiskLimits limits;
    private final AccountExposure exposures;
    private final Symbols symbols;

    private final MutableSlice slice = new MutableSlice();
    private final NewOrderCommand.MutableNewOrder mutNew = new NewOrderCommand.MutableNewOrder();
    private final CancelReplaceCommand.MutableCancelReplace mutCxR = new CancelReplaceCommand.MutableCancelReplace();
    private final byte[] textBuf = new byte[ExecutionReportCommand.TEXT_LENGTH];

    private final Counter newOrderCounter;
    private final Counter cancelCounter;
    private final Counter cxrCounter;
    private final Counter rejectCounter;

    public AggregatedRiskCheck(RingConsumer omOut,
                               RingProducer riskOut,
                               GatewayOutEmitter gatewayOut,
                               RiskLimits limits,
                               AccountExposure exposures,
                               Symbols symbols,
                               MeterRegistry mr) {
        super("risk", false);
        this.omOut = omOut;
        this.riskOut = riskOut;
        this.gatewayOut = gatewayOut;
        this.limits = limits;
        this.exposures = exposures;
        this.symbols = symbols;
        this.newOrderCounter = mr == null ? null : Counter.builder("exchange.risk.new_orders").register(mr);
        this.cancelCounter = mr == null ? null : Counter.builder("exchange.risk.cancels").register(mr);
        this.cxrCounter = mr == null ? null : Counter.builder("exchange.risk.cancel_replaces").register(mr);
        this.rejectCounter = mr == null ? null : Counter.builder("exchange.risk.rejects").register(mr);
    }

    @Override
    protected boolean pollOnce() {
        if (!omOut.poll(slice)) return false;
        ByteBuffer payload = slice.buffer;
        int basePos = payload.position();
        int len = slice.length;
        byte type = CommandCodec.peekType(payload, basePos);

        switch (type) {
            case CommandType.NEW_ORDER -> {
                if (newOrderCounter != null) newOrderCounter.increment();
                NewOrderCommand.decode(payload, basePos, mutNew);
                String reason = checkNewOrder(mutNew);
                if (reason != null) {
                    if (rejectCounter != null) rejectCounter.increment();
                    emitRiskReject(mutNew.sessionId, mutNew.senderId, mutNew.symbolId,
                            mutNew.side, mutNew.qty, mutNew.price, mutNew.account,
                            mutNew.clOrdId, mutNew.clientTsNs, reason);
                    return true;
                }
                forward(payload, basePos, len);
            }
            case CommandType.CANCEL_REPLACE -> {
                if (cxrCounter != null) cxrCounter.increment();
                CancelReplaceCommand.decode(payload, basePos, mutCxR);
                String reason = checkCancelReplace(mutCxR);
                if (reason != null) {
                    if (rejectCounter != null) rejectCounter.increment();
                    emitRiskReject(mutCxR.sessionId, mutCxR.senderId, mutCxR.symbolId,
                            mutCxR.side, mutCxR.qty, mutCxR.price, mutCxR.account,
                            mutCxR.clOrdId, mutCxR.clientTsNs, reason);
                    return true;
                }
                forward(payload, basePos, len);
            }
            case CommandType.CANCEL -> {
                if (cancelCounter != null) cancelCounter.increment();
                forward(payload, basePos, len);
            }
            default -> forward(payload, basePos, len);
        }
        return true;
    }

    private String checkNewOrder(NewOrderCommand.MutableNewOrder n) {
        if (!symbols.contains(n.symbolId)) {
            return null; // engine will reject; risk doesn't filter on symbol id alone
        }
        // Per-symbol per-account qty
        long curQty = Math.abs(exposures.netQty(n.account, n.symbolId));
        if (curQty + n.qty > limits.maxQtyPerSymbolPerAccount()) {
            return "MAX_QTY_PER_SYMBOL";
        }
        // Open notional across account
        long extra = n.qty * (n.price == 0L ? Math.max(1L, exposures.lastTradePrice(n.symbolId)) : n.price);
        long current = exposures.openNotional(n.account);
        if (current + extra > limits.maxOpenNotionalPerAccount()) {
            return "MAX_OPEN_NOTIONAL";
        }
        // Fat-finger band on LIMIT orders: only if we have a last trade price
        if (n.ordType == OrdType.CODE_LIMIT && n.price > 0L) {
            long lastPx = exposures.lastTradePrice(n.symbolId);
            if (lastPx > 0L) {
                double band = limits.fatFingerBandPct();
                long lowerBound = (long) Math.floor(lastPx * (1.0 - band));
                long upperBound = (long) Math.ceil(lastPx * (1.0 + band));
                if (n.price < lowerBound || n.price > upperBound) {
                    return "FAT_FINGER_BAND";
                }
            }
        }
        return null;
    }

    private String checkCancelReplace(CancelReplaceCommand.MutableCancelReplace n) {
        long curQty = Math.abs(exposures.netQty(n.account, n.symbolId));
        if (curQty + n.qty > limits.maxQtyPerSymbolPerAccount()) {
            return "MAX_QTY_PER_SYMBOL";
        }
        long extra = n.qty * (n.price == 0L ? Math.max(1L, exposures.lastTradePrice(n.symbolId)) : n.price);
        long current = exposures.openNotional(n.account);
        if (current + extra > limits.maxOpenNotionalPerAccount()) {
            return "MAX_OPEN_NOTIONAL";
        }
        if (n.price > 0L) {
            long lastPx = exposures.lastTradePrice(n.symbolId);
            if (lastPx > 0L) {
                double band = limits.fatFingerBandPct();
                long lowerBound = (long) Math.floor(lastPx * (1.0 - band));
                long upperBound = (long) Math.ceil(lastPx * (1.0 + band));
                if (n.price < lowerBound || n.price > upperBound) {
                    return "FAT_FINGER_BAND";
                }
            }
        }
        return null;
    }

    private void forward(ByteBuffer payload, int basePos, int len) {
        ByteBuffer out = CommandCodec.scratch();
        for (int i = 0; i < len; i++) {
            out.put(i, payload.get(basePos + i));
        }
        out.position(0);
        out.limit(len);
        while (!riskOut.offer(out)) {
            Thread.onSpinWait();
        }
    }

    private void emitRiskReject(long sessionId,
                                long senderId,
                                int symbolId,
                                byte side,
                                long qty,
                                long price,
                                long account,
                                byte[] clOrdId16,
                                long clientTsNs,
                                String reason) {
        // Fill text buf
        for (int i = 0; i < textBuf.length; i++) textBuf[i] = 0;
        int textLen = Math.min(reason.length(), textBuf.length);
        for (int i = 0; i < textLen; i++) {
            char c = reason.charAt(i);
            textBuf[i] = (byte) (c & 0x7F);
        }
        ByteBuffer out = CommandCodec.scratch();
        ExecutionReportCommand.encode(out, 0,
                sessionId,
                senderId,
                /*orderId*/ 0L,
                symbolId,
                side,
                ExecType.REJECTED,
                (byte) OrderStatus.REJECTED.ordinal(),
                RejectReason.RISK_LIMIT,
                qty,
                /*filledQty*/ 0L,
                /*remainingQty*/ 0L,
                price,
                /*lastQty*/ 0L,
                /*lastPx*/ 0L,
                account,
                clientTsNs,
                Clocks.nanoTime(),
                clOrdId16,
                ZERO_CL,
                textBuf);
        out.position(0);
        out.limit(ExecutionReportCommand.SIZE);
        gatewayOut.offerSpin(out);
    }

    private static final byte[] ZERO_CL = new byte[ClOrdId.LENGTH];
}
