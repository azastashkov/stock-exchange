package com.exchange.app.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * Spring configuration POJO for the {@code exchange.*} property tree. This
 * mirrors the layout of {@code application.yml}; nested classes have plain
 * setters so Spring's relaxed binding can fill them in.
 */
@ConfigurationProperties(prefix = "exchange")
public final class ExchangeConfig {

    private long nodeId = 1L;
    private long warmNodeId = 2L;
    private String dataDir = "./data";
    private String symbols = "AAPL,MSFT,GOOG,AMZN,TSLA";
    private final Cluster cluster = new Cluster();
    private final Fix fix = new Fix();
    private final MarketData marketData = new MarketData();
    private final Raft raft = new Raft();
    private final CpuPinning cpuPinning = new CpuPinning();
    private final Rings rings = new Rings();

    public long getNodeId() { return nodeId; }
    public void setNodeId(long nodeId) { this.nodeId = nodeId; }

    public long getWarmNodeId() { return warmNodeId; }
    public void setWarmNodeId(long warmNodeId) { this.warmNodeId = warmNodeId; }

    public String getDataDir() { return dataDir; }
    public void setDataDir(String dataDir) { this.dataDir = dataDir; }

    public String getSymbols() { return symbols; }
    public void setSymbols(String symbols) { this.symbols = symbols; }

    /** Parse the comma-separated symbols string into a trimmed list. */
    public List<String> getSymbolsList() {
        List<String> out = new ArrayList<>();
        if (symbols == null) return out;
        for (String s : symbols.split(",")) {
            String t = s.trim();
            if (!t.isEmpty()) out.add(t);
        }
        return out;
    }

    public Cluster getCluster() { return cluster; }
    public Fix getFix() { return fix; }
    public MarketData getMarketData() { return marketData; }
    public Raft getRaft() { return raft; }
    public CpuPinning getCpuPinning() { return cpuPinning; }
    public Rings getRings() { return rings; }

    public static final class Cluster {
        private String peers = "";
        public String getPeers() { return peers; }
        public void setPeers(String peers) { this.peers = peers; }
    }

    public static final class Fix {
        private int port = 9100;
        private int rateLimitMsgsPerSec = 5000;
        private String senderCompId = "EXCHANGE";
        private boolean enabled = true;
        public int getPort() { return port; }
        public void setPort(int port) { this.port = port; }
        public int getRateLimitMsgsPerSec() { return rateLimitMsgsPerSec; }
        public void setRateLimitMsgsPerSec(int rateLimitMsgsPerSec) { this.rateLimitMsgsPerSec = rateLimitMsgsPerSec; }
        public String getSenderCompId() { return senderCompId; }
        public void setSenderCompId(String senderCompId) { this.senderCompId = senderCompId; }
        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
    }

    public static final class MarketData {
        private int port = 9200;
        public int getPort() { return port; }
        public void setPort(int port) { this.port = port; }
    }

    public static final class Raft {
        private int port = 9001;
        private int heartbeatIntervalMs = 50;
        private int electionTimeoutMinMs = 150;
        private int electionTimeoutMaxMs = 300;
        private boolean enabled = false;
        public int getPort() { return port; }
        public void setPort(int port) { this.port = port; }
        public int getHeartbeatIntervalMs() { return heartbeatIntervalMs; }
        public void setHeartbeatIntervalMs(int heartbeatIntervalMs) { this.heartbeatIntervalMs = heartbeatIntervalMs; }
        public int getElectionTimeoutMinMs() { return electionTimeoutMinMs; }
        public void setElectionTimeoutMinMs(int electionTimeoutMinMs) { this.electionTimeoutMinMs = electionTimeoutMinMs; }
        public int getElectionTimeoutMaxMs() { return electionTimeoutMaxMs; }
        public void setElectionTimeoutMaxMs(int electionTimeoutMaxMs) { this.electionTimeoutMaxMs = electionTimeoutMaxMs; }
        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
    }

    public static final class CpuPinning {
        private boolean enabled = false;
        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
    }

    /** Ring + event log sizes. Defaults sized for the 120s @ 10k ord/s load test. */
    public static final class Rings {
        private int slotSize = 256;
        private int slotCount = 65536;          // 16 MiB per ring
        private int eventLogSlots = 4 * 1024 * 1024;  // 1 GiB log: ~4M events at 256B
        public int getSlotSize() { return slotSize; }
        public void setSlotSize(int slotSize) { this.slotSize = slotSize; }
        public int getSlotCount() { return slotCount; }
        public void setSlotCount(int slotCount) { this.slotCount = slotCount; }
        public int getEventLogSlots() { return eventLogSlots; }
        public void setEventLogSlots(int eventLogSlots) { this.eventLogSlots = eventLogSlots; }
    }
}
