package com.exchange.app.om;

import com.exchange.app.gateway.GatewayOutEmitter;
import com.exchange.app.reporter.SessionRegistry;
import com.exchange.commands.CommandCodec;
import com.exchange.commands.ExecType;
import com.exchange.commands.ExecutionReportCommand;
import com.exchange.commands.NewOrderCommand;
import com.exchange.domain.ClOrdId;
import com.exchange.domain.OrdType;
import com.exchange.domain.RejectReason;
import com.exchange.domain.Side;
import com.exchange.domain.TimeInForce;
import com.exchange.ipc.MmapRing;
import com.exchange.ipc.MutableSlice;
import com.exchange.ipc.RingProducer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class OrderManagerServiceTest {

    @Test
    void duplicateClOrdId_isRejected_andNotForwarded(@TempDir Path tmp) throws IOException {
        try (MmapRing inbound = MmapRing.create(tmp.resolve("in.dat"), 256, 32);
             MmapRing omOut = MmapRing.create(tmp.resolve("om-out.dat"), 256, 32);
             MmapRing gwOut = MmapRing.create(tmp.resolve("gw-out.dat"), 256, 32)) {

            OrderManager om = new OrderManager();
            SessionRegistry sessions = new SessionRegistry();
            GatewayOutEmitter gw = new GatewayOutEmitter(gwOut.producer());
            OrderManagerService svc = new OrderManagerService(
                    inbound.consumer(), omOut.producer(), gw, om, sessions, null);

            // First NEW: should pass through.
            offerNewOrder(inbound.producer(), 1L, 7L, "DUP", 100L, 1500000L);
            assertThat(svc.idleCounter()).isEqualTo(0); // not yet started; idle remains 0
            // Drive the loop manually:
            invokePoll(svc); // drains first slot
            // omOut should have 1 message
            MutableSlice slice = new MutableSlice();
            assertThat(omOut.consumer().poll(slice)).isTrue();
            assertThat(slice.length).isEqualTo(NewOrderCommand.SIZE);
            // gwOut should still be empty
            assertThat(gwOut.consumer().poll(slice)).isFalse();

            // Second NEW with same (sender, clOrdId): should be rejected.
            offerNewOrder(inbound.producer(), 1L, 7L, "DUP", 50L, 1500000L);
            invokePoll(svc);
            assertThat(omOut.consumer().poll(slice)).isFalse(); // not forwarded
            assertThat(gwOut.consumer().poll(slice)).isTrue();
            assertThat(slice.length).isEqualTo(ExecutionReportCommand.SIZE);

            ExecutionReportCommand.MutableExecutionReport er = new ExecutionReportCommand.MutableExecutionReport();
            ExecutionReportCommand.decode(slice.buffer, slice.buffer.position(), er);
            assertThat(er.execType).isEqualTo(ExecType.REJECTED);
            assertThat(er.rejectReason).isEqualTo(RejectReason.DUPLICATE_CLORDID);
            assertThat(er.senderId).isEqualTo(7L);
            assertThat(er.sessionId).isEqualTo(1L);
        }
    }

    @Test
    void distinctClOrdIds_passThrough(@TempDir Path tmp) throws IOException {
        try (MmapRing inbound = MmapRing.create(tmp.resolve("in.dat"), 256, 32);
             MmapRing omOut = MmapRing.create(tmp.resolve("om-out.dat"), 256, 32);
             MmapRing gwOut = MmapRing.create(tmp.resolve("gw-out.dat"), 256, 32)) {

            OrderManager om = new OrderManager();
            SessionRegistry sessions = new SessionRegistry();
            GatewayOutEmitter gw = new GatewayOutEmitter(gwOut.producer());
            OrderManagerService svc = new OrderManagerService(
                    inbound.consumer(), omOut.producer(), gw, om, sessions, null);

            offerNewOrder(inbound.producer(), 1L, 7L, "ABC", 100L, 1500000L);
            offerNewOrder(inbound.producer(), 1L, 7L, "DEF", 50L, 1510000L);
            invokePoll(svc);
            invokePoll(svc);

            MutableSlice s = new MutableSlice();
            assertThat(omOut.consumer().poll(s)).isTrue();
            assertThat(omOut.consumer().poll(s)).isTrue();
            assertThat(omOut.consumer().poll(s)).isFalse();
            assertThat(gwOut.consumer().poll(s)).isFalse();

            assertThat(sessions.size()).isEqualTo(2);
        }
    }

    private static void offerNewOrder(RingProducer p, long sessionId, long senderId, String cl,
                                      long qty, long price) {
        ByteBuffer buf = ByteBuffer.allocate(NewOrderCommand.SIZE).order(ByteOrder.LITTLE_ENDIAN);
        byte[] clBytes = new byte[ClOrdId.LENGTH];
        ClOrdId.writeAscii(clBytes, cl);
        NewOrderCommand.encode(buf, 0,
                Side.CODE_BUY,
                OrdType.CODE_LIMIT,
                TimeInForce.CODE_DAY,
                42,
                qty,
                price,
                9000L,
                senderId,
                sessionId,
                clBytes,
                12345L);
        buf.position(0).limit(NewOrderCommand.SIZE);
        while (!p.offer(buf)) {
            Thread.onSpinWait();
        }
    }

    /** Reflectively invoke the protected pollOnce; avoids starting the thread. */
    private static void invokePoll(OrderManagerService svc) {
        try {
            java.lang.reflect.Method m = svc.getClass().getSuperclass().getDeclaredMethod("pollOnce");
            m.setAccessible(true);
            m.invoke(svc);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }
}
