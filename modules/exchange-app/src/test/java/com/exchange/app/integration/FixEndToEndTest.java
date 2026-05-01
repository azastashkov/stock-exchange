package com.exchange.app.integration;

import com.exchange.app.config.ExchangeWiring;
import com.exchange.app.risk.RiskLimits;
import com.exchange.fix.FixField;
import com.exchange.fix.FixMessage;
import com.exchange.fix.test.TestFixClient;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Boots the entire wiring including the FIX server and verifies that a FIX
 * client can submit a New Order Single and receive an ExecutionReport(NEW)
 * back through the full pipeline.
 */
class FixEndToEndTest {

    @Test
    void newOrderRoundTripsThroughFixGateway(@TempDir Path tmp) throws Exception {
        int fixPort = freePort();
        SimpleMeterRegistry mr = new SimpleMeterRegistry();
        ExchangeWiring wiring = new ExchangeWiring(
                tmp,
                List.of("AAPL"),
                256,
                4096,
                65536,
                /*cpuPinning*/ false,
                /*mdPort*/ 0,
                /*fixEnabled*/ true,
                fixPort,
                "EXCHANGE",
                /*rateLimitMsgsPerSec*/ 1000,
                RiskLimits.permissive(),
                mr);
        try {
            wiring.start();
            try (TestFixClient client = new TestFixClient("127.0.0.1", fixPort, "TRADER01", "EXCHANGE")) {
                client.sendLogon(30, true);
                FixMessage logonResp = client.receive(2000);
                assertThat(logonResp).isNotNull();
                assertThat(logonResp.msgType()).isEqualTo("A");

                client.sendNewOrderSingle("CL1", "AAPL", '1', 100L, 1500000L, '2', 1001L);

                FixMessage er = client.receive(3000);
                assertThat(er).isNotNull();
                assertThat(er.msgType()).isEqualTo("8");
                assertThat(er.getString(FixField.SYMBOL)).isEqualTo("AAPL");
                assertThat(er.getString(FixField.CL_ORD_ID)).isEqualTo("CL1");
                assertThat(er.getChar(FixField.EXEC_TYPE)).isEqualTo((byte) '0');
                assertThat(er.getChar(FixField.ORD_STATUS)).isEqualTo((byte) '0');
            }
        } finally {
            wiring.close();
            wiring.wipe();
        }
    }

    @Test
    void crossingOrdersProduceFills(@TempDir Path tmp) throws Exception {
        int fixPort = freePort();
        SimpleMeterRegistry mr = new SimpleMeterRegistry();
        ExchangeWiring wiring = new ExchangeWiring(
                tmp,
                List.of("AAPL"),
                256,
                4096,
                65536,
                /*cpuPinning*/ false,
                /*mdPort*/ 0,
                /*fixEnabled*/ true,
                fixPort,
                "EXCHANGE",
                /*rateLimitMsgsPerSec*/ 1000,
                RiskLimits.permissive(),
                mr);
        try {
            wiring.start();
            try (TestFixClient seller = new TestFixClient("127.0.0.1", fixPort, "SELLER", "EXCHANGE");
                 TestFixClient buyer  = new TestFixClient("127.0.0.1", fixPort, "BUYER",  "EXCHANGE")) {
                seller.sendLogon(30, true);
                FixMessage sLogon = seller.receive(2000);
                assertThat(sLogon).isNotNull();
                buyer.sendLogon(30, true);
                FixMessage bLogon = buyer.receive(2000);
                assertThat(bLogon).isNotNull();

                // Seller posts a sell at 150
                seller.sendNewOrderSingle("S1", "AAPL", '2', 100L, 1500000L, '2', 2001L);
                FixMessage sNew = seller.receive(3000);
                assertThat(sNew).isNotNull();
                assertThat(sNew.getChar(FixField.EXEC_TYPE)).isEqualTo((byte) '0');

                // Buyer crosses
                buyer.sendNewOrderSingle("B1", "AAPL", '1', 100L, 1500000L, '2', 1001L);
                FixMessage bNew = buyer.receive(3000);
                assertThat(bNew).isNotNull();
                assertThat(bNew.getChar(FixField.EXEC_TYPE)).isEqualTo((byte) '0');

                // Now both should receive a fill (Trade ER)
                FixMessage sFill = seller.receive(3000);
                FixMessage bFill = buyer.receive(3000);
                assertThat(sFill).isNotNull();
                assertThat(bFill).isNotNull();
                // FIX 4.4 fill is ExecType=F (Trade)
                assertThat(sFill.getChar(FixField.EXEC_TYPE)).isEqualTo((byte) 'F');
                assertThat(bFill.getChar(FixField.EXEC_TYPE)).isEqualTo((byte) 'F');
            }
        } finally {
            wiring.close();
            wiring.wipe();
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
