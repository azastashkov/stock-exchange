package com.exchange.app.position;

import com.exchange.app.risk.AccountExposure;
import com.exchange.domain.Side;
import com.exchange.events.OrderAcceptedEvent;
import com.exchange.events.TradeEvent;
import com.exchange.loop.ApplicationLoop;
import com.exchange.store.EventLogReader;
import com.exchange.store.EventType;
import com.exchange.store.EventView;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.nio.ByteBuffer;

/**
 * PositionKeeper loop. Owns its own EventLog cursor ({@code position}) and
 * applies TRADE events to the {@link AccountExposure} ledger.
 * <p>
 * The loop is the sole writer to {@code AccountExposure}. To translate a
 * fill into per-account positions we need to know each side's account; the
 * trade event itself only carries the maker/taker order ids, so we
 * maintain a tiny local map {@code orderId → (account, side)} populated
 * from {@code ORDER_ACCEPTED} events and torn down lazily.
 */
public final class PositionKeeperService extends ApplicationLoop {

    private final EventLogReader reader;
    private final AccountExposure exposures;

    private final EventView view = new EventView();
    private final OrderAcceptedEvent.MutableOrderAccepted mutAccepted = new OrderAcceptedEvent.MutableOrderAccepted();
    private final TradeEvent.MutableTrade mutTrade = new TradeEvent.MutableTrade();

    /** orderId → (account, side, remainingQty). */
    private final Long2ObjectOpenHashMap<OrderInfo> byOrderId = new Long2ObjectOpenHashMap<>(1024);

    private final Counter tradeCounter;
    private final Counter acceptedCounter;

    public PositionKeeperService(EventLogReader reader,
                                 AccountExposure exposures,
                                 MeterRegistry mr) {
        super("position", false);
        this.reader = reader;
        this.exposures = exposures;
        this.tradeCounter = mr == null ? null : Counter.builder("exchange.position.trades").register(mr);
        this.acceptedCounter = mr == null ? null : Counter.builder("exchange.position.accepted").register(mr);
    }

    @Override
    protected boolean pollOnce() {
        if (!reader.poll(view)) return false;
        ByteBuffer payload = view.payloadBuffer();
        int basePos = payload.position();
        short type = view.type();
        if (type == EventType.ORDER_ACCEPTED) {
            OrderAcceptedEvent.decode(payload, basePos, mutAccepted);
            OrderInfo info = byOrderId.get(mutAccepted.orderId);
            if (info == null) {
                info = new OrderInfo();
                byOrderId.put(mutAccepted.orderId, info);
            }
            info.account = mutAccepted.account;
            info.side = mutAccepted.side;
            info.remainingQty = mutAccepted.qty;
            if (acceptedCounter != null) acceptedCounter.increment();
            return true;
        }
        if (type == EventType.TRADE) {
            TradeEvent.decode(payload, basePos, mutTrade);
            OrderInfo maker = byOrderId.get(mutTrade.makerOrderId);
            OrderInfo taker = byOrderId.get(mutTrade.takerOrderId);
            if (maker != null) {
                long signed = (maker.side == Side.CODE_BUY) ? mutTrade.qty : -mutTrade.qty;
                exposures.onFill(maker.account, mutTrade.symbolId, signed, mutTrade.price);
                maker.remainingQty -= mutTrade.qty;
                if (maker.remainingQty <= 0L) {
                    byOrderId.remove(mutTrade.makerOrderId);
                }
            } else {
                // Fallback: still update last-trade price marker
                exposures.onFill(0L, mutTrade.symbolId, 0L, mutTrade.price);
            }
            if (taker != null) {
                long signed = (taker.side == Side.CODE_BUY) ? mutTrade.qty : -mutTrade.qty;
                exposures.onFill(taker.account, mutTrade.symbolId, signed, mutTrade.price);
                taker.remainingQty -= mutTrade.qty;
                if (taker.remainingQty <= 0L) {
                    byOrderId.remove(mutTrade.takerOrderId);
                }
            }
            if (tradeCounter != null) tradeCounter.increment();
            return true;
        }
        // Other event types ignored; cursor still advances.
        return true;
    }

    private static final class OrderInfo {
        long account;
        byte side;
        long remainingQty;
    }
}
