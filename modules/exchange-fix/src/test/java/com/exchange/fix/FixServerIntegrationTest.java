package com.exchange.fix;

import com.exchange.commands.CommandCodec;
import com.exchange.commands.CommandType;
import com.exchange.commands.ExecType;
import com.exchange.commands.ExecutionReportCommand;
import com.exchange.commands.NewOrderCommand;
import com.exchange.domain.ClOrdId;
import com.exchange.domain.OrderStatus;
import com.exchange.domain.Side;
import com.exchange.domain.Symbols;
import com.exchange.fix.test.TestFixClient;
import com.exchange.ipc.MmapRing;
import com.exchange.ipc.MutableSlice;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class FixServerIntegrationTest {

    private FixServer server;
    private MmapRing inboundRing;
    private MmapRing gatewayOutRing;

    @AfterEach
    void tearDown() {
        if (server != null) server.stop();
        if (inboundRing != null) inboundRing.close();
        if (gatewayOutRing != null) gatewayOutRing.close();
    }

    @Test
    void logonAndNewOrderEndToEnd(@TempDir Path tmp) throws Exception {
        int port = freePort();
        Symbols sym = Symbols.register(new String[]{"AAPL", "MSFT"});
        Path dataDir = tmp.resolve("data");
        java.nio.file.Files.createDirectories(dataDir);
        inboundRing = MmapRing.create(dataDir.resolve("ring-inbound.dat"), 256, 1024);
        gatewayOutRing = MmapRing.create(dataDir.resolve("ring-gateway-out.dat"), 256, 1024);
        FixSessionRegistry reg = new FixSessionRegistry();
        Validator val = new Validator(sym);
        server = new FixServer(
                port,
                "EXCHANGE".getBytes(java.nio.charset.StandardCharsets.US_ASCII),
                reg,
                sym,
                val,
                inboundRing.producer(),
                gatewayOutRing.consumer(),
                RateLimiterFactory.perSession(1000),
                tmp.resolve("fix-seq"));
        server.start();

        try (TestFixClient client = new TestFixClient("127.0.0.1", port, "CLIENT01", "EXCHANGE")) {
            client.sendLogon(30, true);
            FixMessage logonResp = client.receive(2000);
            assertThat(logonResp).isNotNull();
            assertThat(logonResp.msgType()).isEqualTo("A");
            assertThat(logonResp.getInt(FixField.HEART_BT_INT)).isEqualTo(30);

            // Send a New Order Single
            client.sendNewOrderSingle("ORDC1", "AAPL", '1', 100L, 1500000L, '2', 1001L);

            // Wait for the inbound ring to have a NewOrderCommand
            MutableSlice slice = new MutableSlice();
            Awaitility.await()
                    .atMost(Duration.ofSeconds(3))
                    .pollInterval(Duration.ofMillis(20))
                    .until(() -> inboundRing.consumer().poll(slice));
            ByteBuffer payload = slice.buffer;
            int basePos = payload.position();
            assertThat(CommandCodec.peekType(payload, basePos)).isEqualTo(CommandType.NEW_ORDER);
            NewOrderCommand.MutableNewOrder mut = new NewOrderCommand.MutableNewOrder();
            NewOrderCommand.decode(payload, basePos, mut);
            assertThat(mut.qty).isEqualTo(100L);
            assertThat(mut.side).isEqualTo(Side.CODE_BUY);
            assertThat(mut.symbolId).isEqualTo(sym.idOf("AAPL"));

            // Inject a synthetic ExecutionReportCommand into gateway-out and verify the
            // FIX client receives an ExecutionReport.
            injectExecutionReport(gatewayOutRing.producer(), mut.sessionId, mut.senderId,
                    sym.idOf("AAPL"), Side.CODE_BUY,
                    ExecType.NEW, OrderStatus.NEW, 100L, 100L, 1500000L,
                    1001L, "ORDC1", System.nanoTime());

            FixMessage er = client.receive(3000);
            assertThat(er).isNotNull();
            assertThat(er.msgType()).isEqualTo("8");
            assertThat(er.getString(FixField.SYMBOL)).isEqualTo("AAPL");
            assertThat(er.getString(FixField.CL_ORD_ID)).isEqualTo("ORDC1");
            assertThat(er.getChar(FixField.EXEC_TYPE)).isEqualTo((byte) '0');

            client.sendLogout("bye");
            FixMessage logoutResp = client.receive(2000);
            assertThat(logoutResp).isNotNull();
            assertThat(logoutResp.msgType()).isEqualTo("5");
        }
    }

    @Test
    void seqNumPersistsAcrossReconnect(@TempDir Path tmp) throws Exception {
        int port = freePort();
        Symbols sym = Symbols.register(new String[]{"AAPL"});
        Path dataDir = tmp.resolve("data");
        java.nio.file.Files.createDirectories(dataDir);
        inboundRing = MmapRing.create(dataDir.resolve("ring-inbound.dat"), 256, 1024);
        gatewayOutRing = MmapRing.create(dataDir.resolve("ring-gateway-out.dat"), 256, 1024);
        FixSessionRegistry reg = new FixSessionRegistry();
        Validator val = new Validator(sym);
        Path seqDir = tmp.resolve("fix-seq");
        server = new FixServer(
                port,
                "EXCHANGE".getBytes(java.nio.charset.StandardCharsets.US_ASCII),
                reg, sym, val,
                inboundRing.producer(), gatewayOutRing.consumer(),
                RateLimiterFactory.perSession(1000),
                seqDir);
        server.start();

        // First session
        int nextOutSeq;
        try (TestFixClient client = new TestFixClient("127.0.0.1", port, "CLIENT01", "EXCHANGE")) {
            client.sendLogon(30, false);
            FixMessage logonResp = client.receive(2000);
            assertThat(logonResp).isNotNull();
            assertThat(logonResp.getInt(FixField.MSG_SEQ_NUM)).isEqualTo(1);
            client.sendLogout("bye");
            FixMessage logoutResp = client.receive(2000);
            assertThat(logoutResp).isNotNull();
            nextOutSeq = client.outSeqNum; // = 3 after logon + logout
        }

        // Second session — server should remember its outbound seq
        Awaitility.await().atMost(Duration.ofSeconds(2)).until(() -> reg.size() == 0);

        try (TestFixClient client = new TestFixClient("127.0.0.1", port, "CLIENT01", "EXCHANGE")) {
            client.outSeqNum = nextOutSeq;
            client.sendLogon(30, false);
            FixMessage logonResp = client.receive(2000);
            assertThat(logonResp).isNotNull();
            // Server's outbound seq num should be > 1 — it persisted state
            assertThat(logonResp.getInt(FixField.MSG_SEQ_NUM)).isGreaterThan(1);
        }
    }

    @Test
    void unknownSymbolGetsBusinessReject(@TempDir Path tmp) throws Exception {
        int port = freePort();
        Symbols sym = Symbols.register(new String[]{"AAPL"});
        Path dataDir = tmp.resolve("data");
        java.nio.file.Files.createDirectories(dataDir);
        inboundRing = MmapRing.create(dataDir.resolve("ring-inbound.dat"), 256, 1024);
        gatewayOutRing = MmapRing.create(dataDir.resolve("ring-gateway-out.dat"), 256, 1024);
        FixSessionRegistry reg = new FixSessionRegistry();
        Validator val = new Validator(sym);
        server = new FixServer(
                port,
                "EXCHANGE".getBytes(java.nio.charset.StandardCharsets.US_ASCII),
                reg, sym, val,
                inboundRing.producer(), gatewayOutRing.consumer(),
                RateLimiterFactory.perSession(1000),
                tmp.resolve("fix-seq"));
        server.start();
        try (TestFixClient client = new TestFixClient("127.0.0.1", port, "CLIENT02", "EXCHANGE")) {
            client.sendLogon(30, true);
            client.receive(2000);
            client.sendNewOrderSingle("X1", "ZZZZ", '1', 100L, 1500000L, '2', 1001L);
            FixMessage rej = client.receive(2000);
            assertThat(rej).isNotNull();
            assertThat(rej.msgType()).isEqualTo("j");
        }
    }

    private void injectExecutionReport(com.exchange.ipc.RingProducer producer,
                                       long sessionId, long senderId,
                                       int symbolId, byte side,
                                       byte execType, OrderStatus status,
                                       long qty, long remaining, long priceFp,
                                       long account, String clOrdId, long clientTs) {
        ByteBuffer buf = ByteBuffer.allocate(ExecutionReportCommand.SIZE)
                .order(java.nio.ByteOrder.LITTLE_ENDIAN);
        byte[] cl = new byte[ClOrdId.LENGTH];
        ClOrdId.writeAscii(cl, clOrdId);
        byte[] zero = new byte[ClOrdId.LENGTH];
        byte[] text = new byte[ExecutionReportCommand.TEXT_LENGTH];
        ExecutionReportCommand.encode(buf, 0,
                sessionId, senderId, /*orderId*/ 1L, symbolId, side, execType,
                (byte) status.ordinal(), (byte) 0,
                qty, 0L, remaining, priceFp, 0L, 0L,
                account, clientTs, System.nanoTime(),
                cl, zero, text);
        buf.position(0).limit(ExecutionReportCommand.SIZE);
        long deadline = System.nanoTime() + 2_000_000_000L;
        while (!producer.offer(buf)) {
            if (System.nanoTime() > deadline) {
                throw new RuntimeException("gateway-out ring back-pressured");
            }
            Thread.onSpinWait();
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
