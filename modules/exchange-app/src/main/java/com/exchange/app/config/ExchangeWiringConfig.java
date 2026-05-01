package com.exchange.app.config;

import com.exchange.app.raft.RaftConfig;
import com.exchange.app.risk.RiskLimits;
import com.exchange.raft.NetAddress;
import com.exchange.raft.NioRpcTransport;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * Spring {@code @Configuration} that builds the {@link ExchangeWiring} from
 * {@link ExchangeConfig}. The wiring's start/stop lifecycle is bound directly
 * to the bean's {@code initMethod} / {@code destroyMethod} so it runs after
 * the bean is constructed.
 */
@Configuration
@EnableConfigurationProperties(ExchangeConfig.class)
public class ExchangeWiringConfig {

    private static final Logger LOG = LoggerFactory.getLogger(ExchangeWiringConfig.class);

    private final ExchangeConfig config;

    @Autowired
    public ExchangeWiringConfig(ExchangeConfig config) {
        this.config = config;
    }

    @Bean
    public MeterRegistry fallbackMeterRegistry() {
        return new SimpleMeterRegistry();
    }

    @Bean(initMethod = "start", destroyMethod = "close")
    public ExchangeWiring exchangeWiring(MeterRegistry meterRegistry) {
        Path dataDir = Paths.get(config.getDataDir());
        RaftConfig raftConfig = buildRaftConfig(dataDir);
        LOG.info("Building exchange wiring: dataDir={}, fix.enabled={}, raft.enabled={}, node={}, peers={}",
                dataDir, config.getFix().isEnabled(), config.getRaft().isEnabled(),
                config.getNodeId(), config.getCluster().getPeers());

        return new ExchangeWiring(
                dataDir,
                config.getSymbolsList(),
                config.getRings().getSlotSize(),
                config.getRings().getSlotCount(),
                config.getRings().getEventLogSlots(),
                config.getCpuPinning().isEnabled(),
                config.getMarketData().getPort(),
                config.getFix().isEnabled(),
                config.getFix().getPort(),
                config.getFix().getSenderCompId(),
                config.getFix().getRateLimitMsgsPerSec(),
                RiskLimits.permissive(),
                meterRegistry,
                raftConfig);
    }

    private RaftConfig buildRaftConfig(Path dataDir) {
        if (!config.getRaft().isEnabled()) {
            return RaftConfig.disabled();
        }
        Map<Integer, NetAddress> peers = parsePeers(config.getCluster().getPeers());
        if (peers.isEmpty()) {
            LOG.warn("exchange.raft.enabled=true but exchange.cluster.peers is empty; staying single-node");
            return RaftConfig.disabled();
        }
        int nodeId = (int) config.getNodeId();
        int warmNodeId = (int) config.getWarmNodeId();
        Path stateDir = dataDir.resolve("raft");
        return RaftConfig.of(nodeId, warmNodeId, peers, stateDir, NioRpcTransport::new);
    }

    private static Map<Integer, NetAddress> parsePeers(String peersSpec) {
        Map<Integer, NetAddress> out = new HashMap<>();
        if (peersSpec == null || peersSpec.isBlank()) return out;
        for (String entry : peersSpec.split(",")) {
            String[] kv = entry.trim().split("=", 2);
            if (kv.length != 2) continue;
            int id;
            try {
                id = Integer.parseInt(kv[0].trim());
            } catch (NumberFormatException e) {
                continue;
            }
            String[] hp = kv[1].trim().split(":", 2);
            if (hp.length != 2) continue;
            try {
                out.put(id, new NetAddress(hp[0].trim(), Integer.parseInt(hp[1].trim())));
            } catch (NumberFormatException e) {
                // skip malformed entry
            }
        }
        return out;
    }
}
