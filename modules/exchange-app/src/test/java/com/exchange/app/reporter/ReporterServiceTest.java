package com.exchange.app.reporter;

import com.exchange.app.gateway.GatewayOutEmitter;
import com.exchange.commands.ExecType;
import com.exchange.commands.ExecutionReportCommand;
import com.exchange.domain.ClOrdId;
import com.exchange.domain.OrdType;
import com.exchange.domain.OrderStatus;
import com.exchange.domain.Side;
import com.exchange.domain.TimeInForce;
import com.exchange.events.OrderAcceptedEvent;
import com.exchange.events.OrderCancelledEvent;
import com.exchange.events.OrderRejectedEvent;
import com.exchange.events.TradeEvent;
import com.exchange.ipc.MmapRing;
import com.exchange.ipc.MutableSlice;
import com.exchange.store.EventLog;
import com.exchange.store.EventLogReader;
import com.exchange.store.EventLogWriter;
import com.exchange.store.EventType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class ReporterServiceTest {

    @Test
    void acceptedThenTrade_emitsThreeReports(@TempDir Path tmp) throws IOException {
        try (EventLog log = EventLog.open(tmp.resolve("events.log"), 1024);
             MmapRing gw = MmapRing.create(tmp.resolve("gw.dat"), 256, 64)) {

            EventLogWriter writer = log.writer();
            EventLogReader reader = log.reader("reporter");
            OrderRegistry registry = new OrderRegistry(64);
            SessionRegistry sessions = new SessionRegistry();

            byte[] cl1 = clOrdId("ORD1");
            byte[] cl2 = clOrdId("ORD2");
            sessions.put(7L, cl1, 100L);
            sessions.put(8L, cl2, 200L);

            // ACCEPTED for maker (orderId=1, sender=7, side=SELL, qty=100)
            ByteBuffer p = scratch();
            OrderAcceptedEvent.encode(p, 0,
                    1L, 42, Side.CODE_SELL, OrdType.CODE_LIMIT, TimeInForce.CODE_DAY,
                    100L, 1500000L, 9000L, 7L, cl1, 555L, 1000L);
            p.position(0).limit(OrderAcceptedEvent.SIZE);
            writer.append(EventType.ORDER_ACCEPTED, 0L, 1000L, p);

            // ACCEPTED for taker (orderId=2, sender=8, side=BUY, qty=60)
            p = scratch();
            OrderAcceptedEvent.encode(p, 0,
                    2L, 42, Side.CODE_BUY, OrdType.CODE_LIMIT, TimeInForce.CODE_DAY,
                    60L, 1500000L, 9001L, 8L, cl2, 666L, 2000L);
            p.position(0).limit(OrderAcceptedEvent.SIZE);
            writer.append(EventType.ORDER_ACCEPTED, 0L, 2000L, p);

            // TRADE: maker=1 (sell), taker=2 (buy), qty=60, price=1500000
            p = scratch();
            TradeEvent.encode(p, 0, 42, 1L, 2L, 60L, 1500000L, Side.CODE_SELL, 3000L, 666L);
            p.position(0).limit(TradeEvent.SIZE);
            writer.append(EventType.TRADE, 0L, 3000L, p);

            GatewayOutEmitter emitter = new GatewayOutEmitter(gw.producer());
            ReporterService rep = new ReporterService(reader, emitter, registry, sessions, null);
            // Drive 4 polls: 2 ACCEPTED + 1 TRADE = 4 events emitted (2 NEW + 2 trade reports)
            for (int i = 0; i < 3; i++) invokePoll(rep);

            // Drain gw ring
            MutableSlice s = new MutableSlice();
            int count = 0;
            ExecutionReportCommand.MutableExecutionReport er = new ExecutionReportCommand.MutableExecutionReport();
            boolean foundMakerNew = false, foundTakerNew = false, foundMakerFill = false, foundTakerFill = false;
            while (gw.consumer().poll(s)) {
                count++;
                ExecutionReportCommand.decode(s.buffer, s.buffer.position(), er);
                if (er.execType == ExecType.NEW && er.orderId == 1L) {
                    foundMakerNew = true;
                    assertThat(er.sessionId).isEqualTo(100L);
                    assertThat(er.senderId).isEqualTo(7L);
                    assertThat(er.qty).isEqualTo(100L);
                    assertThat(er.remainingQty).isEqualTo(100L);
                    assertThat(er.ordStatus).isEqualTo((byte) OrderStatus.NEW.ordinal());
                } else if (er.execType == ExecType.NEW && er.orderId == 2L) {
                    foundTakerNew = true;
                    assertThat(er.sessionId).isEqualTo(200L);
                } else if (er.orderId == 1L && (er.execType == ExecType.PARTIAL_FILL || er.execType == ExecType.FILL)) {
                    foundMakerFill = true;
                    assertThat(er.lastQty).isEqualTo(60L);
                    assertThat(er.lastPx).isEqualTo(1500000L);
                    assertThat(er.filledQty).isEqualTo(60L);
                    // 100 - 60 = 40 remaining
                    assertThat(er.remainingQty).isEqualTo(40L);
                    assertThat(er.execType).isEqualTo(ExecType.PARTIAL_FILL);
                    assertThat(er.sessionId).isEqualTo(100L);
                } else if (er.orderId == 2L && (er.execType == ExecType.PARTIAL_FILL || er.execType == ExecType.FILL)) {
                    foundTakerFill = true;
                    // 60 - 60 = 0; FILL
                    assertThat(er.execType).isEqualTo(ExecType.FILL);
                    assertThat(er.sessionId).isEqualTo(200L);
                }
            }
            assertThat(count).isEqualTo(4);
            assertThat(foundMakerNew).isTrue();
            assertThat(foundTakerNew).isTrue();
            assertThat(foundMakerFill).isTrue();
            assertThat(foundTakerFill).isTrue();
        }
    }

    @Test
    void cancelEvent_emitsCanceledReport(@TempDir Path tmp) throws IOException {
        try (EventLog log = EventLog.open(tmp.resolve("events.log"), 64);
             MmapRing gw = MmapRing.create(tmp.resolve("gw.dat"), 256, 32)) {

            EventLogWriter writer = log.writer();
            EventLogReader reader = log.reader("reporter");
            OrderRegistry registry = new OrderRegistry(64);
            SessionRegistry sessions = new SessionRegistry();

            byte[] cl = clOrdId("CXL1");
            sessions.put(7L, cl, 1L);

            // ACCEPTED first so registry knows about it
            ByteBuffer p = scratch();
            OrderAcceptedEvent.encode(p, 0,
                    9L, 42, Side.CODE_BUY, OrdType.CODE_LIMIT, TimeInForce.CODE_DAY,
                    100L, 1500000L, 9000L, 7L, cl, 555L, 1000L);
            p.position(0).limit(OrderAcceptedEvent.SIZE);
            writer.append(EventType.ORDER_ACCEPTED, 0L, 1000L, p);

            // CANCELLED
            p = scratch();
            byte[] zero = new byte[ClOrdId.LENGTH];
            OrderCancelledEvent.encode(p, 0,
                    9L, 42, 100L, 7L, cl, zero, 9000L, 555L);
            p.position(0).limit(OrderCancelledEvent.SIZE);
            writer.append(EventType.ORDER_CANCELLED, 0L, 2000L, p);

            GatewayOutEmitter emitter = new GatewayOutEmitter(gw.producer());
            ReporterService rep = new ReporterService(reader, emitter, registry, sessions, null);
            invokePoll(rep);
            invokePoll(rep);

            MutableSlice s = new MutableSlice();
            // Drain NEW
            assertThat(gw.consumer().poll(s)).isTrue();
            ExecutionReportCommand.MutableExecutionReport er = new ExecutionReportCommand.MutableExecutionReport();
            ExecutionReportCommand.decode(s.buffer, s.buffer.position(), er);
            assertThat(er.execType).isEqualTo(ExecType.NEW);

            // Drain CANCELED
            assertThat(gw.consumer().poll(s)).isTrue();
            ExecutionReportCommand.decode(s.buffer, s.buffer.position(), er);
            assertThat(er.execType).isEqualTo(ExecType.CANCELED);
            assertThat(er.orderId).isEqualTo(9L);
            assertThat(er.ordStatus).isEqualTo((byte) OrderStatus.CANCELLED.ordinal());
            assertThat(er.sessionId).isEqualTo(1L);
        }
    }

    @Test
    void rejectEvent_emitsRejectedReport(@TempDir Path tmp) throws IOException {
        try (EventLog log = EventLog.open(tmp.resolve("events.log"), 64);
             MmapRing gw = MmapRing.create(tmp.resolve("gw.dat"), 256, 32)) {

            EventLogWriter writer = log.writer();
            EventLogReader reader = log.reader("reporter");
            OrderRegistry registry = new OrderRegistry(64);
            SessionRegistry sessions = new SessionRegistry();

            byte[] cl = clOrdId("BAD1");
            ByteBuffer p = scratch();
            OrderRejectedEvent.encode(p, 0,
                    42, com.exchange.domain.RejectReason.INVALID_QTY, 7L, cl, 9000L, 555L);
            p.position(0).limit(OrderRejectedEvent.SIZE);
            writer.append(EventType.ORDER_REJECTED, 0L, 1000L, p);

            GatewayOutEmitter emitter = new GatewayOutEmitter(gw.producer());
            ReporterService rep = new ReporterService(reader, emitter, registry, sessions, null);
            invokePoll(rep);

            MutableSlice s = new MutableSlice();
            assertThat(gw.consumer().poll(s)).isTrue();
            ExecutionReportCommand.MutableExecutionReport er = new ExecutionReportCommand.MutableExecutionReport();
            ExecutionReportCommand.decode(s.buffer, s.buffer.position(), er);
            assertThat(er.execType).isEqualTo(ExecType.REJECTED);
            assertThat(er.rejectReason).isEqualTo(com.exchange.domain.RejectReason.INVALID_QTY);
        }
    }

    private static byte[] clOrdId(String s) {
        byte[] b = new byte[ClOrdId.LENGTH];
        ClOrdId.writeAscii(b, s);
        return b;
    }

    private static ByteBuffer scratch() {
        return ByteBuffer.allocate(EventLog.MAX_PAYLOAD).order(java.nio.ByteOrder.LITTLE_ENDIAN);
    }

    private static void invokePoll(ReporterService svc) {
        try {
            java.lang.reflect.Method m = svc.getClass().getSuperclass().getDeclaredMethod("pollOnce");
            m.setAccessible(true);
            m.invoke(svc);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }
}
