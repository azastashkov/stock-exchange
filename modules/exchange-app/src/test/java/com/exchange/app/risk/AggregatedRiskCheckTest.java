package com.exchange.app.risk;

import com.exchange.app.gateway.GatewayOutEmitter;
import com.exchange.commands.ExecType;
import com.exchange.commands.ExecutionReportCommand;
import com.exchange.commands.NewOrderCommand;
import com.exchange.domain.ClOrdId;
import com.exchange.domain.OrdType;
import com.exchange.domain.RejectReason;
import com.exchange.domain.Side;
import com.exchange.domain.Symbols;
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

class AggregatedRiskCheckTest {

    @Test
    void overNotional_isRejected(@TempDir Path tmp) throws IOException {
        try (MmapRing omOut = MmapRing.create(tmp.resolve("om-out.dat"), 256, 32);
             MmapRing riskOut = MmapRing.create(tmp.resolve("risk-out.dat"), 256, 32);
             MmapRing gwOut = MmapRing.create(tmp.resolve("gw-out.dat"), 256, 32)) {

            RiskLimits limits = RiskLimits.builder()
                    .maxOpenNotionalPerAccount(1_000_000L)
                    .maxQtyPerSymbolPerAccount(1_000_000L)
                    .fatFingerBandPct(2.0)
                    .build();
            AccountExposure exposures = new AccountExposure();
            Symbols symbols = Symbols.register(new String[]{"AAPL"});
            int aapl = symbols.idOf("AAPL");
            GatewayOutEmitter gw = new GatewayOutEmitter(gwOut.producer());
            AggregatedRiskCheck risk = new AggregatedRiskCheck(
                    omOut.consumer(), riskOut.producer(), gw,
                    limits, exposures, symbols, null);

            // qty * price = 1000 * 1500000 = 1.5B → over 1M
            offerNewOrder(omOut.producer(), 1L, 7L, "OVR1", aapl, 1000L, 1500000L);
            invokePoll(risk);

            MutableSlice s = new MutableSlice();
            assertThat(riskOut.consumer().poll(s)).isFalse();
            assertThat(gwOut.consumer().poll(s)).isTrue();

            ExecutionReportCommand.MutableExecutionReport er = new ExecutionReportCommand.MutableExecutionReport();
            ExecutionReportCommand.decode(s.buffer, s.buffer.position(), er);
            assertThat(er.execType).isEqualTo(ExecType.REJECTED);
            assertThat(er.rejectReason).isEqualTo(RejectReason.RISK_LIMIT);
        }
    }

    @Test
    void withinLimits_passesThrough(@TempDir Path tmp) throws IOException {
        try (MmapRing omOut = MmapRing.create(tmp.resolve("om-out.dat"), 256, 32);
             MmapRing riskOut = MmapRing.create(tmp.resolve("risk-out.dat"), 256, 32);
             MmapRing gwOut = MmapRing.create(tmp.resolve("gw-out.dat"), 256, 32)) {

            RiskLimits limits = RiskLimits.permissive();
            AccountExposure exposures = new AccountExposure();
            Symbols symbols = Symbols.register(new String[]{"AAPL"});
            int aapl = symbols.idOf("AAPL");
            GatewayOutEmitter gw = new GatewayOutEmitter(gwOut.producer());
            AggregatedRiskCheck risk = new AggregatedRiskCheck(
                    omOut.consumer(), riskOut.producer(), gw,
                    limits, exposures, symbols, null);

            offerNewOrder(omOut.producer(), 1L, 7L, "OK1", aapl, 100L, 1500000L);
            invokePoll(risk);

            MutableSlice s = new MutableSlice();
            assertThat(riskOut.consumer().poll(s)).isTrue();
            assertThat(gwOut.consumer().poll(s)).isFalse();
        }
    }

    @Test
    void fatFingerBand_isRejected_whenLastPriceKnown(@TempDir Path tmp) throws IOException {
        try (MmapRing omOut = MmapRing.create(tmp.resolve("om-out.dat"), 256, 32);
             MmapRing riskOut = MmapRing.create(tmp.resolve("risk-out.dat"), 256, 32);
             MmapRing gwOut = MmapRing.create(tmp.resolve("gw-out.dat"), 256, 32)) {

            RiskLimits limits = RiskLimits.builder()
                    .maxOpenNotionalPerAccount(Long.MAX_VALUE / 2)
                    .maxQtyPerSymbolPerAccount(Long.MAX_VALUE / 2)
                    .fatFingerBandPct(0.1) // ±10%
                    .build();
            AccountExposure exposures = new AccountExposure();
            Symbols symbols = Symbols.register(new String[]{"AAPL"});
            int aapl = symbols.idOf("AAPL");
            // Seed a "last trade price" so the band kicks in.
            exposures.onFill(0L, aapl, 0L, 1000000L);
            GatewayOutEmitter gw = new GatewayOutEmitter(gwOut.producer());
            AggregatedRiskCheck risk = new AggregatedRiskCheck(
                    omOut.consumer(), riskOut.producer(), gw,
                    limits, exposures, symbols, null);

            // 2 000 000 is double the last trade price → rejected.
            offerNewOrder(omOut.producer(), 1L, 7L, "F1", aapl, 100L, 2_000_000L);
            invokePoll(risk);

            MutableSlice s = new MutableSlice();
            assertThat(riskOut.consumer().poll(s)).isFalse();
            assertThat(gwOut.consumer().poll(s)).isTrue();
        }
    }

    private static void offerNewOrder(RingProducer p, long sessionId, long senderId, String cl,
                                      int symbolId, long qty, long price) {
        ByteBuffer buf = ByteBuffer.allocate(NewOrderCommand.SIZE).order(ByteOrder.LITTLE_ENDIAN);
        byte[] clBytes = new byte[ClOrdId.LENGTH];
        ClOrdId.writeAscii(clBytes, cl);
        NewOrderCommand.encode(buf, 0,
                Side.CODE_BUY,
                OrdType.CODE_LIMIT,
                TimeInForce.CODE_DAY,
                symbolId,
                qty,
                price,
                9000L,
                senderId,
                sessionId,
                clBytes,
                System.nanoTime());
        buf.position(0).limit(NewOrderCommand.SIZE);
        while (!p.offer(buf)) Thread.onSpinWait();
    }

    private static void invokePoll(AggregatedRiskCheck svc) {
        try {
            java.lang.reflect.Method m = svc.getClass().getSuperclass().getDeclaredMethod("pollOnce");
            m.setAccessible(true);
            m.invoke(svc);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }
}
