package com.exchange.engine;

import com.exchange.domain.ClOrdId;
import com.exchange.domain.OrdType;
import com.exchange.domain.Side;
import com.exchange.domain.Symbols;
import com.exchange.domain.TimeInForce;
import com.exchange.events.OrderAcceptedEvent;
import com.exchange.events.OrderCancelledEvent;
import com.exchange.events.OrderRejectedEvent;
import com.exchange.events.TradeEvent;
import com.exchange.store.EventLog;
import com.exchange.store.EventLogReader;
import com.exchange.store.EventType;
import com.exchange.store.EventView;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/** Shared helpers for matching-engine tests: wires up a temp EventLog,
 * a Symbols registry, OrderBooks, and an engine. */
final class EngineTestSupport implements AutoCloseable {

    final Path tempDir;
    final Path logPath;
    final EventLog log;
    final EventLogReader reader;
    final Symbols symbols;
    final Int2ObjectOpenHashMap<OrderBook> books;
    final OrderEntryPool pool;
    final Sequencer sequencer;
    final MatchingEngine engine;

    EngineTestSupport(int slotCount, String... symbolNames) throws IOException {
        this.tempDir = Files.createTempDirectory("engine-test-" + UUID.randomUUID());
        this.logPath = tempDir.resolve("events.log");
        this.log = EventLog.open(logPath, slotCount);
        this.reader = log.reader("test");
        this.symbols = Symbols.register(symbolNames);
        this.books = new Int2ObjectOpenHashMap<>();
        for (String name : symbolNames) {
            int id = symbols.idOf(name);
            books.put(id, new OrderBook(id));
        }
        this.pool = new OrderEntryPool(64);
        this.sequencer = new Sequencer();
        this.engine = new MatchingEngine(log.writer(), symbols, books, pool, sequencer, () -> 0L);
    }

    static byte[] clOrdId(String s) {
        byte[] b = new byte[ClOrdId.LENGTH];
        ClOrdId.writeAscii(b, s);
        return b;
    }

    Order limit(String symbol, byte side, long qty, long price, long sender, String cl, long account) {
        Order o = new Order();
        o.set(0L,
                symbols.idOf(symbol),
                side,
                OrdType.CODE_LIMIT,
                TimeInForce.CODE_DAY,
                account,
                sender,
                clOrdId(cl),
                qty,
                price,
                0L,
                0L);
        return o;
    }

    Order market(String symbol, byte side, long qty, long sender, String cl, long account) {
        Order o = new Order();
        o.set(0L,
                symbols.idOf(symbol),
                side,
                OrdType.CODE_MARKET,
                TimeInForce.CODE_DAY,
                account,
                sender,
                clOrdId(cl),
                qty,
                0L,
                0L,
                0L);
        return o;
    }

    /** Drain all pending events from the log into a list of records. */
    List<Recorded> drain() {
        List<Recorded> out = new ArrayList<>();
        EventView view = new EventView();
        while (reader.poll(view)) {
            out.add(Recorded.copy(view));
        }
        return out;
    }

    @Override
    public void close() {
        try { reader.close(); } catch (Exception ignored) { /* best-effort */ }
        try { log.close(); } catch (Exception ignored) { /* best-effort */ }
        try {
            Files.walk(tempDir)
                    .sorted(java.util.Comparator.reverseOrder())
                    .forEach(p -> { try { Files.deleteIfExists(p); } catch (IOException ignored) { /* best-effort */ } });
        } catch (IOException ignored) {
            /* best-effort */
        }
    }

    /** A copy of an EventView contents; safe to retain across polls. */
    static final class Recorded {
        final long sequence;
        final long timestampNanos;
        final short type;
        final byte[] payload;

        private Recorded(long sequence, long timestampNanos, short type, byte[] payload) {
            this.sequence = sequence;
            this.timestampNanos = timestampNanos;
            this.type = type;
            this.payload = payload;
        }

        static Recorded copy(EventView v) {
            int len = v.payloadLength();
            byte[] copy = new byte[len];
            int pos = v.payloadBuffer().position();
            for (int i = 0; i < len; i++) {
                copy[i] = v.payloadBuffer().get(pos + i);
            }
            return new Recorded(v.sequence(), v.timestampNanos(), v.type(), copy);
        }

        boolean isAccepted() { return type == EventType.ORDER_ACCEPTED; }
        boolean isTrade() { return type == EventType.TRADE; }
        boolean isCancelled() { return type == EventType.ORDER_CANCELLED; }
        boolean isRejected() { return type == EventType.ORDER_REJECTED; }

        OrderAcceptedEvent.MutableOrderAccepted decodeAccepted() {
            OrderAcceptedEvent.MutableOrderAccepted m = new OrderAcceptedEvent.MutableOrderAccepted();
            OrderAcceptedEvent.decode(java.nio.ByteBuffer.wrap(payload).order(java.nio.ByteOrder.LITTLE_ENDIAN), 0, m);
            return m;
        }

        TradeEvent.MutableTrade decodeTrade() {
            TradeEvent.MutableTrade m = new TradeEvent.MutableTrade();
            TradeEvent.decode(java.nio.ByteBuffer.wrap(payload).order(java.nio.ByteOrder.LITTLE_ENDIAN), 0, m);
            return m;
        }

        OrderCancelledEvent.MutableOrderCancelled decodeCancelled() {
            OrderCancelledEvent.MutableOrderCancelled m = new OrderCancelledEvent.MutableOrderCancelled();
            OrderCancelledEvent.decode(java.nio.ByteBuffer.wrap(payload).order(java.nio.ByteOrder.LITTLE_ENDIAN), 0, m);
            return m;
        }

        OrderRejectedEvent.MutableOrderRejected decodeRejected() {
            OrderRejectedEvent.MutableOrderRejected m = new OrderRejectedEvent.MutableOrderRejected();
            OrderRejectedEvent.decode(java.nio.ByteBuffer.wrap(payload).order(java.nio.ByteOrder.LITTLE_ENDIAN), 0, m);
            return m;
        }
    }

    static final byte BUY = Side.CODE_BUY;
    static final byte SELL = Side.CODE_SELL;
}
