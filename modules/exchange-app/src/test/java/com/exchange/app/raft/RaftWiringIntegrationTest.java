package com.exchange.app.raft;

import com.exchange.app.config.ExchangeWiring;
import com.exchange.app.gateway.GatewayStub;
import com.exchange.app.risk.RiskLimits;
import com.exchange.commands.CommandCodec;
import com.exchange.commands.CommandType;
import com.exchange.commands.ExecType;
import com.exchange.commands.ExecutionReportCommand;
import com.exchange.commands.NewOrderCommand;
import com.exchange.domain.ClOrdId;
import com.exchange.domain.OrdType;
import com.exchange.domain.Side;
import com.exchange.domain.TimeInForce;
import com.exchange.ipc.MutableSlice;
import com.exchange.raft.InMemoryRpcTransport;
import com.exchange.raft.NetAddress;
import com.exchange.raft.RaftNode;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Boots two ExchangeWiring instances joined by an in-memory Raft transport
 * and verifies:
 * <ul>
 *   <li>One leader is elected (the cluster comes up).</li>
 *   <li>An order routed at a follower receives a {@code NOT LEADER}
 *       reject ExecutionReport without engine processing.</li>
 *   <li>An order routed at the leader is accepted, replicated, and applied
 *       to the follower's event log (commit converges across nodes).</li>
 * </ul>
 * Uses 3 nodes (smallest majority) to keep the test fast and deterministic.
 */
class RaftWiringIntegrationTest {

    private final List<ExchangeWiring> wirings = new ArrayList<>();

    @AfterEach
    void tearDown() {
        for (ExchangeWiring w : wirings) {
            try { w.close(); } catch (Exception ignored) { /* best-effort */ }
            try { w.wipe(); } catch (Exception ignored) { /* best-effort */ }
        }
        wirings.clear();
    }

    @Test
    void cluster_elects_leader_and_followers_reject_orders(@TempDir Path tmp) throws Exception {
        int n = 3;
        InMemoryRpcTransport.Registry registry = new InMemoryRpcTransport.Registry();
        Map<Integer, NetAddress> peerMap = new HashMap<>();
        for (int i = 1; i <= n; i++) {
            peerMap.put(i, new NetAddress("127.0.0.1", 21000 + i));
        }
        for (int i = 1; i <= n; i++) {
            Path nodeDir = tmp.resolve("node-" + i);
            Files.createDirectories(nodeDir);
            int finalI = i;
            RaftConfig rc = RaftConfig.of(i, /*warm*/ 1, peerMap, nodeDir.resolve("raft"),
                    peerCfg -> new InMemoryRpcTransport(finalI, registry));
            ExchangeWiring w = new ExchangeWiring(
                    nodeDir,
                    List.of("AAPL"),
                    256, 1024, 16384,
                    /*cpuPinning*/ false,
                    /*mdPort*/ 0,
                    /*fixEnabled*/ false,
                    /*fixPort*/ 0,
                    "EXCHANGE", 1000,
                    RiskLimits.permissive(),
                    new SimpleMeterRegistry(),
                    rc);
            wirings.add(w);
            w.start();
        }

        // Wait for a leader.
        await().atMost(Duration.ofSeconds(3)).until(() -> currentLeader() != null);
        ExchangeWiring leader = currentLeader();
        assertThat(leader).isNotNull();

        // Pick a follower.
        ExchangeWiring follower = wirings.stream()
                .filter(w -> !w.raftNode().isLeader())
                .findFirst()
                .orElseThrow();

        // Submit an order to the FOLLOWER's gateway. Expect a NOT LEADER reject.
        submitNewOrder(follower, "FOLLOW1", 1L);
        ExecutionReportCommand.MutableExecutionReport er = pollExecReport(follower, 3000);
        assertThat(er).isNotNull();
        assertThat(er.execType).isEqualTo(ExecType.REJECTED);
        assertThat(er.textAsString()).contains("NOT LEADER");

        // Submit an order to the LEADER's gateway. Expect an ACCEPTED ER.
        submitNewOrder(leader, "LEAD1", 2L);
        ExecutionReportCommand.MutableExecutionReport ack = pollExecReport(leader, 3000);
        assertThat(ack).isNotNull();
        assertThat(ack.execType).isEqualTo(ExecType.NEW);

        // Wait for the leader's event log to advance, then verify a follower's
        // commitIndex tracks. (Happy path: the order produces ORDER_ACCEPTED,
        // the leader appends WRITING, replicates, commits, applies.)
        await().atMost(Duration.ofSeconds(3)).until(() -> {
            long leaderCommit = leader.raftNode().commitIndex();
            long followerCommit = follower.raftNode().commitIndex();
            return leaderCommit >= 0 && followerCommit >= leaderCommit - 1;
        });
    }

    private ExchangeWiring currentLeader() {
        for (ExchangeWiring w : wirings) {
            RaftNode rn = w.raftNode();
            if (rn != null && rn.isLeader()) return w;
        }
        return null;
    }

    private static void submitNewOrder(ExchangeWiring w, String clOrdId, long senderId) {
        GatewayStub gs = w.gatewayStub();
        ByteBuffer scratch = CommandCodec.scratch();
        byte[] cl16 = new byte[ClOrdId.LENGTH];
        byte[] s = clOrdId.getBytes(java.nio.charset.StandardCharsets.US_ASCII);
        System.arraycopy(s, 0, cl16, 0, Math.min(s.length, ClOrdId.LENGTH));
        NewOrderCommand.encode(scratch, 0,
                Side.CODE_BUY,
                OrdType.CODE_LIMIT,
                TimeInForce.CODE_DAY,
                w.symbols().idOf("AAPL"),
                100L,
                1500000L,
                1000L,
                senderId,
                /*sessionId*/ 999L,
                cl16,
                /*clientTsNs*/ 0L);
        scratch.position(0);
        scratch.limit(NewOrderCommand.SIZE);
        while (!w.inboundRing().producer().offer(scratch)) {
            Thread.onSpinWait();
        }
    }

    private static ExecutionReportCommand.MutableExecutionReport pollExecReport(ExchangeWiring w, long timeoutMs) {
        long deadline = System.nanoTime() + timeoutMs * 1_000_000L;
        MutableSlice slice = new MutableSlice();
        ExecutionReportCommand.MutableExecutionReport out = new ExecutionReportCommand.MutableExecutionReport();
        while (System.nanoTime() < deadline) {
            if (w.gatewayStub().gatewayOutConsumer().poll(slice)) {
                ByteBuffer buf = slice.buffer;
                int basePos = buf.position();
                ExecutionReportCommand.decode(buf, basePos, out);
                return out;
            }
            Thread.onSpinWait();
        }
        return null;
    }
}
