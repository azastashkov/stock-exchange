package com.exchange.app.raft;

import com.exchange.app.gateway.GatewayOutEmitter;
import com.exchange.commands.CancelCommand;
import com.exchange.commands.CancelReplaceCommand;
import com.exchange.commands.CommandCodec;
import com.exchange.commands.CommandType;
import com.exchange.commands.ExecType;
import com.exchange.commands.ExecutionReportCommand;
import com.exchange.commands.NewOrderCommand;
import com.exchange.domain.ClOrdId;
import com.exchange.domain.OrderStatus;
import com.exchange.domain.RejectReason;
import com.exchange.engine.MatchingEngine;
import com.exchange.engine.Order;
import com.exchange.ipc.MutableSlice;
import com.exchange.ipc.RingConsumer;
import com.exchange.loop.ApplicationLoop;
import com.exchange.raft.RaftNode;
import com.exchange.time.Clocks;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/**
 * Raft-aware engine loop. Drains the {@code risk-out} ring exactly like the
 * vanilla {@link com.exchange.app.engine.EngineService}, but before invoking
 * the engine it checks {@link RaftNode#isLeader()}. If this node is not the
 * leader, the command is rejected with an {@code ExecutionReport} carrying
 * a "NOT LEADER" text back to the gateway-out ring, and the engine is
 * skipped entirely so no event is appended to the local log.
 * <p>
 * The engine itself was constructed with {@code raftMode=true}, so any
 * appends it does perform are written with status {@code WRITING} and
 * become visible to downstream consumers only after the Raft CommitApplier
 * promotes them.
 */
public final class RaftAwareEngineService extends ApplicationLoop {

    private final RingConsumer riskOut;
    private final MatchingEngine engine;
    private final RaftNode raftNode;
    private final GatewayOutEmitter gatewayOut;

    private final MutableSlice slice = new MutableSlice();
    private final NewOrderCommand.MutableNewOrder mutNew = new NewOrderCommand.MutableNewOrder();
    private final CancelCommand.MutableCancel mutCancel = new CancelCommand.MutableCancel();
    private final CancelReplaceCommand.MutableCancelReplace mutCxR = new CancelReplaceCommand.MutableCancelReplace();
    private final byte[] zeroOrigClOrdId = new byte[ClOrdId.LENGTH];

    private final Timer matchLatency;
    private final Counter newOrderCounter;
    private final Counter cancelCounter;
    private final Counter cxrCounter;
    private final Counter notLeaderRejectCounter;

    public RaftAwareEngineService(RingConsumer riskOut,
                                  MatchingEngine engine,
                                  RaftNode raftNode,
                                  GatewayOutEmitter gatewayOut,
                                  MeterRegistry mr) {
        super("engine-raft", false);
        this.riskOut = riskOut;
        this.engine = engine;
        this.raftNode = raftNode;
        this.gatewayOut = gatewayOut;
        this.matchLatency = mr == null ? null
                : Timer.builder("exchange.engine.match.latency")
                .publishPercentileHistogram()
                .register(mr);
        this.newOrderCounter = mr == null ? null : Counter.builder("exchange.engine.new_orders").register(mr);
        this.cancelCounter = mr == null ? null : Counter.builder("exchange.engine.cancels").register(mr);
        this.cxrCounter = mr == null ? null : Counter.builder("exchange.engine.cancel_replaces").register(mr);
        this.notLeaderRejectCounter = mr == null ? null
                : Counter.builder("exchange.engine.not_leader_rejects").register(mr);
    }

    @Override
    protected boolean pollOnce() {
        if (!riskOut.poll(slice)) return false;
        ByteBuffer payload = slice.buffer;
        int basePos = payload.position();
        byte type = CommandCodec.peekType(payload, basePos);

        // First, the leader gate: not-leader → reject and don't touch the engine.
        if (!raftNode.isLeader()) {
            rejectNotLeader(payload, basePos, type);
            if (notLeaderRejectCounter != null) notLeaderRejectCounter.increment();
            return true;
        }

        long startNs = matchLatency != null ? Clocks.nanoTime() : 0L;
        switch (type) {
            case CommandType.NEW_ORDER -> {
                NewOrderCommand.decode(payload, basePos, mutNew);
                Order o = new Order();
                o.set(0L,
                        mutNew.symbolId,
                        mutNew.side,
                        mutNew.ordType,
                        mutNew.tif,
                        mutNew.account,
                        mutNew.senderId,
                        mutNew.clOrdId,
                        mutNew.qty,
                        mutNew.price,
                        Clocks.nanoTime(),
                        mutNew.clientTsNs);
                engine.onNewOrder(o);
                if (newOrderCounter != null) newOrderCounter.increment();
            }
            case CommandType.CANCEL -> {
                CancelCommand.decode(payload, basePos, mutCancel);
                engine.onCancel(mutCancel.senderId, mutCancel.clOrdId, mutCancel.clientTsNs);
                if (cancelCounter != null) cancelCounter.increment();
            }
            case CommandType.CANCEL_REPLACE -> {
                CancelReplaceCommand.decode(payload, basePos, mutCxR);
                engine.onCancelReplace(mutCxR.senderId,
                        mutCxR.origClOrdId,
                        mutCxR.clOrdId,
                        mutCxR.qty,
                        mutCxR.price,
                        mutCxR.clientTsNs);
                if (cxrCounter != null) cxrCounter.increment();
            }
            default -> {
                // Unknown — drop.
            }
        }
        if (matchLatency != null) {
            matchLatency.record(Clocks.nanoTime() - startNs, TimeUnit.NANOSECONDS);
        }
        return true;
    }

    private void rejectNotLeader(ByteBuffer payload, int basePos, byte type) {
        long sessionId, senderId, account, qty, price, clientTsNs;
        int symbolId;
        byte side;
        byte[] clOrdId;
        switch (type) {
            case CommandType.NEW_ORDER -> {
                NewOrderCommand.decode(payload, basePos, mutNew);
                sessionId = mutNew.sessionId;
                senderId = mutNew.senderId;
                symbolId = mutNew.symbolId;
                side = mutNew.side;
                qty = mutNew.qty;
                price = mutNew.price;
                account = mutNew.account;
                clientTsNs = mutNew.clientTsNs;
                clOrdId = mutNew.clOrdId;
            }
            case CommandType.CANCEL -> {
                CancelCommand.decode(payload, basePos, mutCancel);
                sessionId = mutCancel.sessionId;
                senderId = mutCancel.senderId;
                symbolId = 0;
                side = 0;
                qty = 0L;
                price = 0L;
                account = 0L;
                clientTsNs = mutCancel.clientTsNs;
                clOrdId = mutCancel.clOrdId;
            }
            case CommandType.CANCEL_REPLACE -> {
                CancelReplaceCommand.decode(payload, basePos, mutCxR);
                sessionId = mutCxR.sessionId;
                senderId = mutCxR.senderId;
                symbolId = 0;
                side = 0;
                qty = mutCxR.qty;
                price = mutCxR.price;
                account = 0L;
                clientTsNs = mutCxR.clientTsNs;
                clOrdId = mutCxR.clOrdId;
            }
            default -> {
                return; // unknown types are silently dropped
            }
        }

        int leader = raftNode.leaderId();
        String text = leader >= 0 ? ("NOT LEADER, use node " + leader) : "NOT LEADER";
        byte[] textBytes = text.getBytes(java.nio.charset.StandardCharsets.US_ASCII);
        ByteBuffer out = CommandCodec.scratch();
        ExecutionReportCommand.encode(out, 0,
                sessionId,
                senderId,
                /*orderId*/ 0L,
                symbolId,
                side,
                ExecType.REJECTED,
                (byte) OrderStatus.REJECTED.ordinal(),
                RejectReason.MARKET_NOT_OPEN, // closest fit; MARKET_NOT_OPEN doubles as "node not active"
                qty,
                /*filledQty*/ 0L,
                /*remainingQty*/ 0L,
                price,
                /*lastQty*/ 0L,
                /*lastPx*/ 0L,
                account,
                clientTsNs,
                Clocks.nanoTime(),
                clOrdId,
                zeroOrigClOrdId,
                textBytes);
        out.position(0);
        out.limit(ExecutionReportCommand.SIZE);
        gatewayOut.offerSpin(out);
    }
}
