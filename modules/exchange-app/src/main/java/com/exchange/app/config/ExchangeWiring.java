package com.exchange.app.config;

import com.exchange.app.engine.EngineService;
import com.exchange.app.gateway.GatewayOutEmitter;
import com.exchange.app.gateway.GatewayStub;
import com.exchange.app.md.MarketDataPublisherService;
import com.exchange.app.md.MdServer;
import com.exchange.app.metrics.RingDepthGauges;
import com.exchange.app.om.OrderManager;
import com.exchange.app.om.OrderManagerService;
import com.exchange.app.position.PositionKeeperService;
import com.exchange.app.raft.RaftAwareEngineService;
import com.exchange.app.raft.RaftConfig;
import com.exchange.app.reporter.OrderRegistry;
import com.exchange.app.reporter.ReporterService;
import com.exchange.app.reporter.SessionRegistry;
import com.exchange.app.risk.AccountExposure;
import com.exchange.app.risk.AggregatedRiskCheck;
import com.exchange.app.risk.RiskLimits;
import com.exchange.domain.Symbols;
import com.exchange.engine.MatchingEngine;
import com.exchange.engine.OrderBook;
import com.exchange.engine.OrderEntryPool;
import com.exchange.engine.Sequencer;
import com.exchange.fix.FixServer;
import com.exchange.fix.FixSessionRegistry;
import com.exchange.fix.RateLimiterFactory;
import com.exchange.fix.Validator;
import com.exchange.ipc.MmapRing;
import com.exchange.loop.ApplicationLoop;
import com.exchange.raft.NioRpcTransport;
import com.exchange.raft.PersistentState;
import com.exchange.raft.RaftNode;
import com.exchange.raft.RpcTransport;
import com.exchange.store.EventLog;
import com.exchange.store.EventLogReader;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Builds the in-process trading pipeline: rings, event log, matching engine,
 * and all per-loop services.
 * <p>
 * Designed to be testable without a Spring context — instantiate directly
 * and call {@link #start()} / {@link #stop()}. The Spring side
 * ({@link ExchangeWiringConfig}) is a thin {@code @Configuration} that
 * wraps a {@code ExchangeWiring} bean.
 */
public final class ExchangeWiring implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ExchangeWiring.class);

    public static final String INBOUND_RING = "inbound";
    public static final String OM_OUT_RING = "om-out";
    public static final String RISK_OUT_RING = "risk-out";
    public static final String GATEWAY_OUT_RING = "gateway-out";

    public static final String LOG_FILE = "events.log";

    private final Path dataDir;
    private final int slotSize;
    private final int slotCount;
    private final int eventLogSlots;
    private final boolean cpuPinning;
    private final int mdPort;
    private final int fixPort;
    private final boolean fixEnabled;
    private final String fixSenderCompId;
    private final int fixRateLimitPerSec;
    private final RiskLimits riskLimits;
    private final MeterRegistry meterRegistry;
    private final RaftConfig raftConfig;

    // Components
    private MmapRing inboundRing;
    private MmapRing omOutRing;
    private MmapRing riskOutRing;
    private MmapRing gatewayOutRing;
    private EventLog eventLog;
    private EventLogReader reporterReader;
    private EventLogReader positionReader;
    private EventLogReader marketDataReader;

    private Symbols symbols;
    private Int2ObjectOpenHashMap<OrderBook> books;
    private OrderEntryPool entryPool;
    private Sequencer sequencer;
    private MatchingEngine matchingEngine;
    private OrderManager orderManager;
    private SessionRegistry sessionRegistry;
    private OrderRegistry orderRegistry;
    private AccountExposure accountExposure;

    private OrderManagerService orderManagerService;
    private AggregatedRiskCheck riskService;
    private EngineService engineService;
    private RaftAwareEngineService raftEngineService;
    private ReporterService reporterService;
    private PositionKeeperService positionKeeperService;
    private MarketDataPublisherService marketDataService;
    private MdServer mdServer;
    private GatewayStub gatewayStub;
    private FixServer fixServer;
    private FixSessionRegistry fixSessionRegistry;

    // Raft (only populated when raftConfig.enabled).
    private RaftNode raftNode;
    private RpcTransport raftTransport;
    private PersistentState raftState;

    private final List<ApplicationLoop> loops = new ArrayList<>();
    private boolean started = false;

    public ExchangeWiring(Path dataDir,
                          List<String> symbolNames,
                          int slotSize,
                          int slotCount,
                          int eventLogSlots,
                          boolean cpuPinning,
                          int mdPort,
                          RiskLimits riskLimits,
                          MeterRegistry meterRegistry) {
        this(dataDir, symbolNames, slotSize, slotCount, eventLogSlots, cpuPinning,
                mdPort, false, 0, "EXCHANGE", 5000, riskLimits, meterRegistry, RaftConfig.disabled());
    }

    public ExchangeWiring(Path dataDir,
                          List<String> symbolNames,
                          int slotSize,
                          int slotCount,
                          int eventLogSlots,
                          boolean cpuPinning,
                          int mdPort,
                          boolean fixEnabled,
                          int fixPort,
                          String fixSenderCompId,
                          int fixRateLimitPerSec,
                          RiskLimits riskLimits,
                          MeterRegistry meterRegistry) {
        this(dataDir, symbolNames, slotSize, slotCount, eventLogSlots, cpuPinning,
                mdPort, fixEnabled, fixPort, fixSenderCompId, fixRateLimitPerSec,
                riskLimits, meterRegistry, RaftConfig.disabled());
    }

    public ExchangeWiring(Path dataDir,
                          List<String> symbolNames,
                          int slotSize,
                          int slotCount,
                          int eventLogSlots,
                          boolean cpuPinning,
                          int mdPort,
                          boolean fixEnabled,
                          int fixPort,
                          String fixSenderCompId,
                          int fixRateLimitPerSec,
                          RiskLimits riskLimits,
                          MeterRegistry meterRegistry,
                          RaftConfig raftConfig) {
        this.dataDir = dataDir;
        this.slotSize = slotSize;
        this.slotCount = slotCount;
        this.eventLogSlots = eventLogSlots;
        this.cpuPinning = cpuPinning;
        this.mdPort = mdPort;
        this.fixEnabled = fixEnabled;
        this.fixPort = fixPort;
        this.fixSenderCompId = fixSenderCompId;
        this.fixRateLimitPerSec = fixRateLimitPerSec;
        this.riskLimits = riskLimits;
        this.meterRegistry = meterRegistry;
        this.raftConfig = raftConfig == null ? RaftConfig.disabled() : raftConfig;
        bootstrap(symbolNames);
    }

    private void bootstrap(List<String> symbolNames) {
        try {
            Files.createDirectories(dataDir);

            inboundRing = MmapRing.create(dataDir.resolve("ring-inbound.dat"), slotSize, slotCount);
            omOutRing = MmapRing.create(dataDir.resolve("ring-om-out.dat"), slotSize, slotCount);
            riskOutRing = MmapRing.create(dataDir.resolve("ring-risk-out.dat"), slotSize, slotCount);
            gatewayOutRing = MmapRing.create(dataDir.resolve("ring-gateway-out.dat"), slotSize, slotCount);

            eventLog = EventLog.open(dataDir.resolve(LOG_FILE), eventLogSlots);

            String[] arr = symbolNames.toArray(new String[0]);
            symbols = Symbols.register(arr);
            books = new Int2ObjectOpenHashMap<>();
            for (String name : arr) {
                int id = symbols.idOf(name);
                books.put(id, new OrderBook(id));
            }
            entryPool = new OrderEntryPool(1024);
            sequencer = new Sequencer();

            // Raft: build persistent state, transport, RaftNode BEFORE the
            // matching engine so the engine can be wired with the Raft term
            // supplier. The engine's writer is shared with Raft's installer:
            // the leader's writes go via appendUncommitted and the follower's
            // writes go via installSlot from RaftNode.onAppendEntries.
            java.util.function.LongSupplier termSupplier = () -> 0L;
            if (raftConfig.enabled) {
                Path raftDir = raftConfig.stateDir != null
                        ? raftConfig.stateDir
                        : dataDir.resolve("raft");
                Files.createDirectories(raftDir);
                raftState = PersistentState.open(raftDir.resolve("state.bin"));
                raftTransport = raftConfig.transportFactory.apply(raftConfig.peers);
                if (raftTransport == null) {
                    throw new IllegalStateException(
                            "raftConfig.transportFactory returned null transport");
                }
                RaftNode.TransportPump pump = pumpFor(raftTransport);
                raftNode = new RaftNode(
                        raftConfig.nodeId,
                        raftConfig.warmNodeId,
                        raftConfig.peers,
                        eventLog,
                        eventLog.writer(),
                        raftState,
                        raftTransport,
                        pump,
                        System.nanoTime() ^ raftConfig.nodeId,
                        RaftNode.SYSTEM_CLOCK);
                termSupplier = raftNode.termSupplier();
            }

            matchingEngine = new MatchingEngine(eventLog.writer(), symbols, books, entryPool, sequencer,
                    termSupplier, raftConfig.enabled);

            orderManager = new OrderManager();
            sessionRegistry = new SessionRegistry();
            orderRegistry = new OrderRegistry(1024);
            accountExposure = new AccountExposure();

            reporterReader = eventLog.reader("reporter");
            positionReader = eventLog.reader("position");
            marketDataReader = eventLog.reader("marketdata");

            mdServer = new MdServer(mdPort);

            // Single emitter for the gateway-out ring; serialises concurrent
            // writes from OM/Risk/Reporter onto the (single-producer) ring.
            GatewayOutEmitter gwOut = new GatewayOutEmitter(gatewayOutRing.producer());

            orderManagerService = new OrderManagerService(
                    inboundRing.consumer(),
                    omOutRing.producer(),
                    gwOut,
                    orderManager,
                    sessionRegistry,
                    meterRegistry);
            riskService = new AggregatedRiskCheck(
                    omOutRing.consumer(),
                    riskOutRing.producer(),
                    gwOut,
                    riskLimits,
                    accountExposure,
                    symbols,
                    meterRegistry);
            if (raftConfig.enabled) {
                engineService = null; // replaced by RaftAwareEngineService below
            } else {
                engineService = new EngineService(
                        riskOutRing.consumer(),
                        matchingEngine,
                        meterRegistry);
            }
            reporterService = new ReporterService(
                    reporterReader,
                    gwOut,
                    orderRegistry,
                    sessionRegistry,
                    meterRegistry);
            positionKeeperService = new PositionKeeperService(
                    positionReader,
                    accountExposure,
                    meterRegistry);
            marketDataService = new MarketDataPublisherService(
                    marketDataReader,
                    mdServer,
                    meterRegistry);

            // The gateway-out ring is consumed by the FIX gateway when enabled;
            // otherwise its consumer end is handed to GatewayStub for tests.
            if (fixEnabled) {
                fixSessionRegistry = new FixSessionRegistry();
                Validator validator = new Validator(symbols);
                Path fixSeqDir = dataDir.resolve("fix");
                fixServer = new FixServer(
                        fixPort,
                        fixSenderCompId.getBytes(java.nio.charset.StandardCharsets.US_ASCII),
                        fixSessionRegistry,
                        symbols,
                        validator,
                        inboundRing.producer(),
                        gatewayOutRing.consumer(),
                        RateLimiterFactory.perSession(fixRateLimitPerSec),
                        fixSeqDir);
                gatewayStub = new GatewayStub(null, null);
            } else {
                gatewayStub = new GatewayStub(inboundRing.producer(), gatewayOutRing.consumer());
            }

            loops.add(orderManagerService);
            loops.add(riskService);
            if (raftConfig.enabled) {
                raftEngineService = new RaftAwareEngineService(
                        riskOutRing.consumer(),
                        matchingEngine,
                        raftNode,
                        gwOut,
                        meterRegistry);
                loops.add(raftEngineService);
                loops.add(raftNode);
            } else {
                loops.add(engineService);
            }
            loops.add(reporterService);
            loops.add(positionKeeperService);
            loops.add(marketDataService);

            // Register depth gauges
            if (meterRegistry != null) {
                RingDepthGauges.register(meterRegistry, INBOUND_RING, inboundRing);
                RingDepthGauges.register(meterRegistry, OM_OUT_RING, omOutRing);
                RingDepthGauges.register(meterRegistry, RISK_OUT_RING, riskOutRing);
                RingDepthGauges.register(meterRegistry, GATEWAY_OUT_RING, gatewayOutRing);
                com.exchange.app.metrics.ExchangeGauges.registerEventLogSequence(meterRegistry, eventLog);
                com.exchange.app.metrics.ExchangeGauges.registerOrderBooks(meterRegistry, symbols, books);
                com.exchange.app.metrics.ExchangeGauges.registerFixGateway(meterRegistry, fixServer, fixSessionRegistry);
                if (raftNode != null) {
                    Gauge.builder("exchange.raft.role", raftNode,
                            n -> switch (n.role()) {
                                case FOLLOWER -> 0.0;
                                case CANDIDATE -> 1.0;
                                case LEADER -> 2.0;
                            }).register(meterRegistry);
                    Gauge.builder("exchange.raft.term", raftNode,
                            n -> (double) n.currentTerm()).register(meterRegistry);
                    Gauge.builder("exchange.raft.commit_index", raftNode,
                            n -> (double) n.commitIndex()).register(meterRegistry);
                    Gauge.builder("exchange.raft.last_applied", raftNode,
                            n -> (double) n.lastApplied()).register(meterRegistry);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to bootstrap exchange wiring at " + dataDir, e);
        }
    }

    public synchronized void start() {
        if (started) return;
        started = true;
        try {
            mdServer.start();
        } catch (IOException e) {
            LOG.warn("MD server failed to start: {}", e.toString());
        }
        if (raftTransport != null) {
            raftTransport.start();
        }
        for (ApplicationLoop loop : loops) {
            loop.start();
        }
        if (fixServer != null) {
            try {
                fixServer.start();
            } catch (IOException e) {
                LOG.warn("FIX server failed to start: {}", e.toString());
            }
        }
    }

    public synchronized void stop() {
        if (!started) return;
        started = false;
        if (fixServer != null) {
            try { fixServer.stop(); } catch (Exception e) { LOG.warn("fix server stop failed", e); }
        }
        // Stop in reverse order so downstream loops drain before producers exit.
        for (int i = loops.size() - 1; i >= 0; i--) {
            try { loops.get(i).stop(); } catch (Exception e) { LOG.warn("loop stop failed", e); }
        }
        if (raftTransport != null) {
            try { raftTransport.close(); } catch (Exception e) { LOG.warn("raft transport stop failed", e); }
        }
        try { mdServer.close(); } catch (Exception e) { LOG.warn("md server close failed", e); }
    }

    @Override
    public void close() {
        try { stop(); } catch (Exception ignored) { /* best-effort */ }
        try { reporterReader.close(); } catch (Exception ignored) { /* best-effort */ }
        try { positionReader.close(); } catch (Exception ignored) { /* best-effort */ }
        try { marketDataReader.close(); } catch (Exception ignored) { /* best-effort */ }
        try { eventLog.close(); } catch (Exception ignored) { /* best-effort */ }
        try { inboundRing.close(); } catch (Exception ignored) { /* best-effort */ }
        try { omOutRing.close(); } catch (Exception ignored) { /* best-effort */ }
        try { riskOutRing.close(); } catch (Exception ignored) { /* best-effort */ }
        try { gatewayOutRing.close(); } catch (Exception ignored) { /* best-effort */ }
        if (raftState != null) {
            try { raftState.close(); } catch (Exception ignored) { /* best-effort */ }
        }
    }

    /**
     * Helper: pick the transport's drain method via reflection-free dispatch.
     * Both {@code NioRpcTransport} and {@code InMemoryRpcTransport} expose a
     * {@code drain(int)} that returns the count of inbound messages
     * delivered.
     */
    private static RaftNode.TransportPump pumpFor(RpcTransport t) {
        if (t instanceof NioRpcTransport nio) {
            return nio::drain;
        }
        if (t instanceof com.exchange.raft.InMemoryRpcTransport inmem) {
            return inmem::drain;
        }
        // Generic fallback: assume the transport delivers synchronously and
        // queue is empty.
        return max -> 0;
    }

    /** Recursively delete the temp data directory. Useful for tests. */
    public void wipe() {
        try {
            if (Files.exists(dataDir)) {
                Files.walk(dataDir)
                        .sorted(Comparator.reverseOrder())
                        .forEach(p -> {
                            try { Files.deleteIfExists(p); } catch (IOException ignored) { /* best-effort */ }
                        });
            }
        } catch (IOException ignored) { /* best-effort */ }
    }

    // -------------- accessors used by tests / Spring context --------------

    public boolean cpuPinningEnabled() { return cpuPinning; }
    public Path dataDir() { return dataDir; }
    public Symbols symbols() { return symbols; }
    public MmapRing inboundRing() { return inboundRing; }
    public MmapRing omOutRing() { return omOutRing; }
    public MmapRing riskOutRing() { return riskOutRing; }
    public MmapRing gatewayOutRing() { return gatewayOutRing; }
    public EventLog eventLog() { return eventLog; }
    public OrderManager orderManager() { return orderManager; }
    public SessionRegistry sessionRegistry() { return sessionRegistry; }
    public OrderRegistry orderRegistry() { return orderRegistry; }
    public AccountExposure accountExposure() { return accountExposure; }
    public MatchingEngine matchingEngine() { return matchingEngine; }
    public OrderManagerService orderManagerService() { return orderManagerService; }
    public AggregatedRiskCheck riskService() { return riskService; }
    public EngineService engineService() { return engineService; }
    public ReporterService reporterService() { return reporterService; }
    public PositionKeeperService positionKeeperService() { return positionKeeperService; }
    public MarketDataPublisherService marketDataService() { return marketDataService; }
    public MdServer mdServer() { return mdServer; }
    public GatewayStub gatewayStub() { return gatewayStub; }
    public FixServer fixServer() { return fixServer; }
    public FixSessionRegistry fixSessionRegistry() { return fixSessionRegistry; }
    public RaftNode raftNode() { return raftNode; }
    public RpcTransport raftTransport() { return raftTransport; }
    public RaftAwareEngineService raftEngineService() { return raftEngineService; }
}
