package com.exchange.app.om;

import com.exchange.app.gateway.GatewayOutEmitter;
import com.exchange.app.reporter.SessionRegistry;
import com.exchange.commands.CommandCodec;
import com.exchange.commands.CommandType;
import com.exchange.commands.ExecType;
import com.exchange.commands.ExecutionReportCommand;
import com.exchange.commands.NewOrderCommand;
import com.exchange.domain.ClOrdId;
import com.exchange.domain.OrderStatus;
import com.exchange.domain.RejectReason;
import com.exchange.ipc.MutableSlice;
import com.exchange.ipc.RingConsumer;
import com.exchange.ipc.RingProducer;
import com.exchange.loop.ApplicationLoop;
import com.exchange.time.Clocks;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

import java.nio.ByteBuffer;

/**
 * OrderManager loop. Polls the inbound ring (raw binary commands from the
 * gateway), runs a duplicate-clOrdId check, and either passes the slot
 * through to the {@code om-out} ring or short-circuits with an immediate
 * REJECTED ExecutionReport on the gateway-out ring.
 * <p>
 * The OM is intentionally stateless beyond the dedup map: order lifecycle
 * tracking lives in the Reporter, which observes the canonical event log.
 */
public final class OrderManagerService extends ApplicationLoop {

    private final RingConsumer inbound;
    private final RingProducer omOut;
    private final GatewayOutEmitter gatewayOut;
    private final OrderManager om;
    private final SessionRegistry sessions;

    private final MutableSlice slice = new MutableSlice();
    private final NewOrderCommand.MutableNewOrder mutNew = new NewOrderCommand.MutableNewOrder();
    private final byte[] dupTextBytes;

    private final Counter inboundCounter;
    private final Counter dupRejectCounter;
    private final Counter forwardedCounter;

    public OrderManagerService(RingConsumer inbound,
                               RingProducer omOut,
                               GatewayOutEmitter gatewayOut,
                               OrderManager om,
                               SessionRegistry sessions,
                               MeterRegistry meterRegistry) {
        super("om", false);
        this.inbound = inbound;
        this.omOut = omOut;
        this.gatewayOut = gatewayOut;
        this.om = om;
        this.sessions = sessions;
        byte[] tb = "DUPLICATE_CLORDID".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
        this.dupTextBytes = tb;
        this.inboundCounter = meterRegistry == null ? null
                : Counter.builder("exchange.om.inbound").register(meterRegistry);
        this.dupRejectCounter = meterRegistry == null ? null
                : Counter.builder("exchange.om.duplicate_rejects").register(meterRegistry);
        this.forwardedCounter = meterRegistry == null ? null
                : Counter.builder("exchange.om.forwarded").register(meterRegistry);
    }

    @Override
    protected boolean pollOnce() {
        if (!inbound.poll(slice)) return false;
        if (inboundCounter != null) inboundCounter.increment();

        ByteBuffer payload = slice.buffer;
        int basePos = payload.position();
        int len = slice.length;
        byte type = CommandCodec.peekType(payload, basePos);

        switch (type) {
            case CommandType.NEW_ORDER -> handleNewOrder(payload, basePos, len);
            case CommandType.CANCEL -> handleCancel(payload, basePos, len);
            case CommandType.CANCEL_REPLACE -> handleCancelReplace(payload, basePos, len);
            default -> {
                // Unknown — pass through; engine will not match anything.
                forwardSlice(payload, basePos, len);
            }
        }
        return true;
    }

    private void handleNewOrder(ByteBuffer payload, int basePos, int len) {
        NewOrderCommand.decode(payload, basePos, mutNew);
        if (om.isDuplicate(mutNew.senderId, mutNew.clOrdId)) {
            emitDuplicateReject();
            if (dupRejectCounter != null) dupRejectCounter.increment();
            return;
        }
        om.register(mutNew.senderId, mutNew.clOrdId);
        if (sessions != null) {
            sessions.put(mutNew.senderId, mutNew.clOrdId, mutNew.sessionId);
        }
        forwardSlice(payload, basePos, len);
        if (forwardedCounter != null) forwardedCounter.increment();
    }

    private void handleCancel(ByteBuffer payload, int basePos, int len) {
        // Cancels are passed through; the engine resolves the underlying order.
        forwardSlice(payload, basePos, len);
        if (forwardedCounter != null) forwardedCounter.increment();
    }

    private void handleCancelReplace(ByteBuffer payload, int basePos, int len) {
        // Cancel-replace also uses a fresh clOrdId; treat it like a new order
        // for dedup purposes.
        long senderId = payload.getLong(basePos + 32);
        long sessionId = payload.getLong(basePos + 40);
        // clOrdId at offset 48; reuse mutNew.clOrdId as scratch for the hash check.
        for (int i = 0; i < ClOrdId.LENGTH; i++) {
            mutNew.clOrdId[i] = payload.get(basePos + 48 + i);
        }
        if (om.isDuplicate(senderId, mutNew.clOrdId)) {
            // Populate enough of mutNew for the reject (the rest stays from prior decode).
            mutNew.senderId = senderId;
            mutNew.sessionId = sessionId;
            emitDuplicateReject();
            if (dupRejectCounter != null) dupRejectCounter.increment();
            return;
        }
        om.register(senderId, mutNew.clOrdId);
        if (sessions != null) {
            sessions.put(senderId, mutNew.clOrdId, sessionId);
        }
        forwardSlice(payload, basePos, len);
        if (forwardedCounter != null) forwardedCounter.increment();
    }

    private void forwardSlice(ByteBuffer payload, int basePos, int len) {
        ByteBuffer out = CommandCodec.scratch();
        for (int i = 0; i < len; i++) {
            out.put(i, payload.get(basePos + i));
        }
        out.position(0);
        out.limit(len);
        // Spin until producer succeeds; ring is sized so this should rarely
        // happen in practice.
        while (!omOut.offer(out)) {
            Thread.onSpinWait();
        }
    }

    private void emitDuplicateReject() {
        // Use the just-decoded mutNew fields. Note: for CANCEL_REPLACE we
        // populated clOrdId only; senderId, sessionId, etc. need re-reading
        // for that path — but the most important fields are already present.
        ByteBuffer out = CommandCodec.scratch();
        ExecutionReportCommand.encode(out, 0,
                mutNew.sessionId,
                mutNew.senderId,
                /*orderId*/ 0L,
                mutNew.symbolId,
                mutNew.side,
                ExecType.REJECTED,
                (byte) OrderStatus.REJECTED.ordinal(),
                RejectReason.DUPLICATE_CLORDID,
                mutNew.qty,
                /*filledQty*/ 0L,
                /*remainingQty*/ 0L,
                mutNew.price,
                /*lastQty*/ 0L,
                /*lastPx*/ 0L,
                mutNew.account,
                mutNew.clientTsNs,
                Clocks.nanoTime(),
                mutNew.clOrdId,
                ZERO_CL,
                dupTextBytes);
        out.position(0);
        out.limit(ExecutionReportCommand.SIZE);
        gatewayOut.offerSpin(out);
    }

    private static final byte[] ZERO_CL = new byte[ClOrdId.LENGTH];
}
