package com.exchange.app.metrics;

import com.exchange.domain.Symbols;
import com.exchange.engine.OrderBook;
import com.exchange.engine.PriceLevel;
import com.exchange.fix.FixServer;
import com.exchange.fix.FixSessionRegistry;
import com.exchange.store.EventLog;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;

import java.util.List;

/**
 * Auxiliary gauges for FIX gateway, event log, and per-symbol book state.
 * <p>
 * The {@link RingDepthGauges} class registers ring-depth gauges; this class
 * fills in the remaining metrics required by the Grafana dashboard.
 */
public final class ExchangeGauges {

    private ExchangeGauges() {
        // utility class
    }

    /**
     * Register gateway metrics derived from a {@link FixServer} and its
     * {@link FixSessionRegistry}.
     */
    public static void registerFixGateway(MeterRegistry mr, FixServer server, FixSessionRegistry sessions) {
        if (mr == null) return;
        if (sessions != null) {
            Gauge.builder("exchange.gateway.session.count", sessions, FixSessionRegistry::size)
                    .register(mr);
        }
        if (server != null) {
            FunctionCounter.builder("exchange.gateway.inbound.accepted", server, FixServer::inboundOrdersAccepted)
                    .register(mr);
            FunctionCounter.builder("exchange.gateway.backpressure.rejected", server, FixServer::backpressureRejected)
                    .register(mr);
        }
    }

    /** Register a gauge that exposes the last committed event-log sequence. */
    public static void registerEventLogSequence(MeterRegistry mr, EventLog log) {
        if (mr == null || log == null) return;
        Gauge.builder("exchange.event.log.sequence", log, l -> (double) l.lastSequence())
                .register(mr);
    }

    /**
     * Register top-of-book gauges (best bid / best ask / spread) per symbol,
     * plus a depth gauge counting resting orders.
     */
    public static void registerOrderBooks(MeterRegistry mr, Symbols symbols, Int2ObjectMap<OrderBook> books) {
        if (mr == null || books == null || symbols == null) return;
        for (Int2ObjectMap.Entry<OrderBook> e : books.int2ObjectEntrySet()) {
            int symbolId = e.getIntKey();
            OrderBook book = e.getValue();
            String name = symbols.nameOf(symbolId);
            List<Tag> tags = List.of(Tag.of("symbol", name));
            Gauge.builder("exchange.book.bid", book, b -> {
                        PriceLevel l = b.bestBid();
                        return l == null ? 0.0 : l.price / 10000.0;
                    })
                    .tags(tags).register(mr);
            Gauge.builder("exchange.book.ask", book, b -> {
                        PriceLevel l = b.bestAsk();
                        return l == null ? 0.0 : l.price / 10000.0;
                    })
                    .tags(tags).register(mr);
            Gauge.builder("exchange.book.depth.bid", book, b -> {
                        PriceLevel l = b.bestBid();
                        return l == null ? 0.0 : (double) l.totalQty;
                    })
                    .tags(tags).register(mr);
            Gauge.builder("exchange.book.depth.ask", book, b -> {
                        PriceLevel l = b.bestAsk();
                        return l == null ? 0.0 : (double) l.totalQty;
                    })
                    .tags(tags).register(mr);
        }
    }
}
