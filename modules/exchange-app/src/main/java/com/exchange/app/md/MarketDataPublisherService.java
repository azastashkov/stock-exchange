package com.exchange.app.md;

import com.exchange.events.TradeEvent;
import com.exchange.loop.ApplicationLoop;
import com.exchange.store.EventLogReader;
import com.exchange.store.EventType;
import com.exchange.store.EventView;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * MarketData loop. Consumes TRADE events from the event log via its own
 * cursor ({@code marketdata}) and pushes a wire-format trade tick to all
 * connected TCP subscribers (via {@link MdServer}).
 */
public final class MarketDataPublisherService extends ApplicationLoop {

    private final EventLogReader reader;
    private final MdServer server;

    private final EventView view = new EventView();
    private final TradeEvent.MutableTrade mutTrade = new TradeEvent.MutableTrade();
    private final ByteBuffer scratch;
    private long tradeCount = 0L;

    private final Counter tradeCounter;

    public MarketDataPublisherService(EventLogReader reader,
                                      MdServer server,
                                      MeterRegistry mr) {
        super("marketdata", false);
        this.reader = reader;
        this.server = server;
        this.scratch = ByteBuffer.allocateDirect(TradeMdMessage.SIZE).order(ByteOrder.LITTLE_ENDIAN);
        this.tradeCounter = mr == null ? null : Counter.builder("exchange.md.trades").register(mr);
    }

    @Override
    protected boolean pollOnce() {
        if (!reader.poll(view)) return false;
        if (view.type() != EventType.TRADE) return true;
        ByteBuffer payload = view.payloadBuffer();
        TradeEvent.decode(payload, payload.position(), mutTrade);
        scratch.clear();
        TradeMdMessage.encode(scratch, 0,
                mutTrade.symbolId,
                mutTrade.makerSide,
                mutTrade.qty,
                mutTrade.price,
                mutTrade.tradeTsNs);
        scratch.position(0).limit(TradeMdMessage.SIZE);
        if (server != null && server.subscriberCount() > 0) {
            server.broadcast(scratch);
        }
        tradeCount++;
        if (tradeCounter != null) tradeCounter.increment();
        return true;
    }

    public long tradeCount() {
        return tradeCount;
    }

    public int subscriberCount() {
        return server == null ? 0 : server.subscriberCount();
    }
}
