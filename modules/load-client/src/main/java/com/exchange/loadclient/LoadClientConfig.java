package com.exchange.loadclient;

import java.util.List;

/**
 * Immutable, parsed CLI configuration for the load client. Built by
 * {@link Main#parse(String[])}.
 */
public final class LoadClientConfig {

    private final List<String> targets;
    private final int rate;
    private final int durationSec;
    private final List<String> symbols;
    private final int sessions;
    private final double p99CeilingMs;
    private final long account;
    private final long priceBaseFp;
    private final int warmUpSec;

    public LoadClientConfig(List<String> targets,
                            int rate,
                            int durationSec,
                            List<String> symbols,
                            int sessions,
                            double p99CeilingMs,
                            long account,
                            long priceBaseFp,
                            int warmUpSec) {
        this.targets = List.copyOf(targets);
        this.rate = rate;
        this.durationSec = durationSec;
        this.symbols = List.copyOf(symbols);
        this.sessions = sessions;
        this.p99CeilingMs = p99CeilingMs;
        this.account = account;
        this.priceBaseFp = priceBaseFp;
        this.warmUpSec = warmUpSec;
    }

    public List<String> targets() { return targets; }
    public int rate() { return rate; }
    public int durationSec() { return durationSec; }
    public List<String> symbols() { return symbols; }
    public int sessions() { return sessions; }
    public double p99CeilingMs() { return p99CeilingMs; }
    public long account() { return account; }
    public long priceBaseFp() { return priceBaseFp; }
    public int warmUpSec() { return warmUpSec; }

    @Override
    public String toString() {
        return "LoadClientConfig{"
                + "targets=" + targets
                + ", rate=" + rate
                + ", duration=" + durationSec + "s"
                + ", symbols=" + symbols
                + ", sessions=" + sessions
                + ", p99CeilingMs=" + p99CeilingMs
                + ", account=" + account
                + ", priceBaseFp=" + priceBaseFp
                + ", warmUpSec=" + warmUpSec
                + '}';
    }
}
