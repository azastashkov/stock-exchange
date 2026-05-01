package com.exchange.app.pipeline;

import com.exchange.app.config.ExchangeWiring;
import com.exchange.app.risk.RiskLimits;
import com.exchange.commands.ExecType;
import com.exchange.commands.ExecutionReportCommand;
import com.exchange.commands.NewOrderCommand;
import com.exchange.domain.ClOrdId;
import com.exchange.domain.OrdType;
import com.exchange.domain.Side;
import com.exchange.domain.TimeInForce;
import com.exchange.ipc.MutableSlice;
import com.exchange.ipc.RingProducer;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class EndToEndPipelineTest {

    @Test
    void hundredOrdersAlternatingSidesProduceTradesAndPositionUpdates(@TempDir Path tmp) throws IOException {
        MeterRegistry mr = new SimpleMeterRegistry();
        ExchangeWiring wiring = new ExchangeWiring(
                tmp,
                List.of("AAPL"),
                256,
                4096,
                65536,
                /*cpuPinning*/ false,
                /*mdPort*/ 0,
                RiskLimits.permissive(),
                mr);

        try {
            wiring.start();

            int aapl = wiring.symbols().idOf("AAPL");
            RingProducer inbound = wiring.gatewayStub().inboundProducer();

            int ordersTotal = 100;
            long basePrice = 1500000L;
            // Alternate buy/sell at the same crossing price to maximize fills.
            for (int i = 0; i < ordersTotal; i++) {
                byte side = (i % 2 == 0) ? Side.CODE_BUY : Side.CODE_SELL;
                long account = (i % 2 == 0) ? 1001L : 1002L;
                long sender = (i % 2 == 0) ? 11L : 22L;
                long session = (i % 2 == 0) ? 100L : 200L;
                String cl = "C" + i;
                offerNewOrder(inbound, sender, session, account, side, aapl, 10L, basePrice, cl);
            }

            // Each NEW results in at least 1 ExecutionReport (NEW). Buys after the first
            // sell hit will trigger fills, so we expect > 100 reports.
            // We wait for at least the 100 NEW reports + some fills.
            int expectedMin = ordersTotal;
            List<ExecutionReportCommand.MutableExecutionReport> drained = new ArrayList<>();
            Awaitility.await()
                    .atMost(Duration.ofSeconds(5))
                    .pollInterval(Duration.ofMillis(20))
                    .until(() -> {
                        drainGwOut(wiring, drained);
                        return drained.size() >= expectedMin;
                    });
            // Wait for the PositionKeeper to consume the same trades (it has
            // its own cursor and lags slightly behind the Reporter).
            Awaitility.await()
                    .atMost(Duration.ofSeconds(5))
                    .pollInterval(Duration.ofMillis(20))
                    .until(() -> wiring.accountExposure().lastTradePrice(aapl) > 0L);
            // Drain any straggler reports.
            try { Thread.sleep(50); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            drainGwOut(wiring, drained);

            // Count NEWs and fills
            Map<Long, Integer> newsByOrderId = new HashMap<>();
            int totalFills = 0;
            int newCount = 0;
            for (ExecutionReportCommand.MutableExecutionReport er : drained) {
                if (er.execType == ExecType.NEW) {
                    newCount++;
                    newsByOrderId.merge(er.orderId, 1, Integer::sum);
                } else if (er.execType == ExecType.PARTIAL_FILL || er.execType == ExecType.FILL) {
                    totalFills++;
                }
            }
            assertThat(newCount).isEqualTo(ordersTotal);
            // Trades pair maker+taker so totalFills should be even.
            assertThat(totalFills % 2).isEqualTo(0);
            assertThat(totalFills).isGreaterThan(0);

            // Position assertion: each filled buy by account 1001 increases its qty,
            // each sell by account 1002 decreases. Buys and sells are paired so
            // we should see equal-and-opposite positions. Some orders may still
            // rest unfilled, but lastTradePrice must reflect the executed trades.
            assertThat(wiring.accountExposure().lastTradePrice(aapl)).isEqualTo(basePrice);
            long buyerNet = wiring.accountExposure().netQty(1001L, aapl);
            long sellerNet = wiring.accountExposure().netQty(1002L, aapl);
            assertThat(buyerNet).isPositive();
            assertThat(sellerNet).isNegative();
            // |buyer| == |seller| because the matching is conservation-of-quantity.
            assertThat(buyerNet).isEqualTo(-sellerNet);

            // Roundtrip latency histogram should have at least one observation.
            Timer rt = mr.find("exchange.roundtrip.latency").timer();
            assertThat(rt).isNotNull();
            assertThat(rt.count()).isGreaterThan(0L);

            // Engine match latency
            Timer em = mr.find("exchange.engine.match.latency").timer();
            assertThat(em).isNotNull();
            assertThat(em.count()).isGreaterThan(0L);

            // MarketData service consumed trades
            Awaitility.await()
                    .atMost(Duration.ofSeconds(2))
                    .pollInterval(Duration.ofMillis(10))
                    .until(() -> wiring.marketDataService().tradeCount() > 0L);
            assertThat(wiring.marketDataService().tradeCount()).isGreaterThan(0L);
            assertThat(wiring.marketDataService().subscriberCount()).isEqualTo(0);

            // Ring-depth gauges are registered for all four rings.
            Gauge inboundDepth = mr.find("exchange.ring.depth").tag("ring", "inbound").gauge();
            Gauge gwOutDepth = mr.find("exchange.ring.depth").tag("ring", "gateway-out").gauge();
            assertThat(inboundDepth).isNotNull();
            assertThat(gwOutDepth).isNotNull();
        } finally {
            wiring.close();
        }
    }

    private static void drainGwOut(ExchangeWiring wiring,
                                   List<ExecutionReportCommand.MutableExecutionReport> out) {
        MutableSlice slice = new MutableSlice();
        while (wiring.gatewayStub().gatewayOutConsumer().poll(slice)) {
            ExecutionReportCommand.MutableExecutionReport er = new ExecutionReportCommand.MutableExecutionReport();
            ExecutionReportCommand.decode(slice.buffer, slice.buffer.position(), er);
            out.add(er);
        }
    }

    private static void offerNewOrder(RingProducer p,
                                      long senderId,
                                      long sessionId,
                                      long account,
                                      byte side,
                                      int symbolId,
                                      long qty,
                                      long price,
                                      String cl) {
        ByteBuffer buf = ByteBuffer.allocate(NewOrderCommand.SIZE).order(ByteOrder.LITTLE_ENDIAN);
        byte[] clBytes = new byte[ClOrdId.LENGTH];
        ClOrdId.writeAscii(clBytes, cl);
        long clientTs = System.nanoTime();
        NewOrderCommand.encode(buf, 0,
                side,
                OrdType.CODE_LIMIT,
                TimeInForce.CODE_DAY,
                symbolId,
                qty,
                price,
                account,
                senderId,
                sessionId,
                clBytes,
                clientTs);
        buf.position(0).limit(NewOrderCommand.SIZE);
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (!p.offer(buf)) {
            if (System.nanoTime() > deadlineNanos) {
                throw new RuntimeException("inbound ring back-pressured for too long");
            }
            Thread.onSpinWait();
        }
    }
}
