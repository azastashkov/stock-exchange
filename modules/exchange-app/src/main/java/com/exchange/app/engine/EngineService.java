package com.exchange.app.engine;

import com.exchange.commands.CancelCommand;
import com.exchange.commands.CancelReplaceCommand;
import com.exchange.commands.CommandCodec;
import com.exchange.commands.CommandType;
import com.exchange.commands.NewOrderCommand;
import com.exchange.engine.MatchingEngine;
import com.exchange.engine.Order;
import com.exchange.ipc.MutableSlice;
import com.exchange.ipc.RingConsumer;
import com.exchange.loop.ApplicationLoop;
import com.exchange.time.Clocks;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/**
 * Engine loop: drains the {@code risk-out} ring, decodes one command per
 * iteration, and dispatches to {@link MatchingEngine}. Engine writes events
 * directly to the canonical event log; downstream loops (Reporter,
 * PositionKeeper, MarketData) read from the log via independent cursors.
 */
public final class EngineService extends ApplicationLoop {

    private final RingConsumer riskOut;
    private final MatchingEngine engine;

    private final MutableSlice slice = new MutableSlice();
    private final NewOrderCommand.MutableNewOrder mutNew = new NewOrderCommand.MutableNewOrder();
    private final CancelCommand.MutableCancel mutCancel = new CancelCommand.MutableCancel();
    private final CancelReplaceCommand.MutableCancelReplace mutCxR = new CancelReplaceCommand.MutableCancelReplace();

    private final Timer matchLatency;
    private final Counter newOrderCounter;
    private final Counter cancelCounter;
    private final Counter cxrCounter;

    public EngineService(RingConsumer riskOut,
                         MatchingEngine engine,
                         MeterRegistry mr) {
        super("engine", false);
        this.riskOut = riskOut;
        this.engine = engine;
        this.matchLatency = mr == null ? null
                : Timer.builder("exchange.engine.match.latency")
                .publishPercentileHistogram()
                .register(mr);
        this.newOrderCounter = mr == null ? null : Counter.builder("exchange.engine.new_orders").register(mr);
        this.cancelCounter = mr == null ? null : Counter.builder("exchange.engine.cancels").register(mr);
        this.cxrCounter = mr == null ? null : Counter.builder("exchange.engine.cancel_replaces").register(mr);
    }

    @Override
    protected boolean pollOnce() {
        if (!riskOut.poll(slice)) return false;
        ByteBuffer payload = slice.buffer;
        int basePos = payload.position();
        byte type = CommandCodec.peekType(payload, basePos);
        long startNs = matchLatency != null ? Clocks.nanoTime() : 0L;
        switch (type) {
            case CommandType.NEW_ORDER -> {
                NewOrderCommand.decode(payload, basePos, mutNew);
                // Fresh Order is needed because the engine retains a reference
                // when the order rests on the book. See MatchingEngine#onNewOrder.
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
}
