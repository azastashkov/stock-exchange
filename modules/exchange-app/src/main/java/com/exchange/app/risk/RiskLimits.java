package com.exchange.app.risk;

/**
 * Risk-check thresholds. Immutable after construction; configured via
 * {@link Builder} from Spring config or directly in tests.
 */
public final class RiskLimits {

    /** Maximum |open notional| per account, in fixed-point price * qty. */
    private final long maxOpenNotionalPerAccount;
    /** Maximum |position qty| per (symbol, account). */
    private final long maxQtyPerSymbolPerAccount;
    /** Allowed deviation from last trade price for limit orders, e.g. 0.5 = 50%. */
    private final double fatFingerBandPct;

    private RiskLimits(Builder b) {
        this.maxOpenNotionalPerAccount = b.maxOpenNotionalPerAccount;
        this.maxQtyPerSymbolPerAccount = b.maxQtyPerSymbolPerAccount;
        this.fatFingerBandPct = b.fatFingerBandPct;
    }

    public long maxOpenNotionalPerAccount() { return maxOpenNotionalPerAccount; }
    public long maxQtyPerSymbolPerAccount() { return maxQtyPerSymbolPerAccount; }
    public double fatFingerBandPct() { return fatFingerBandPct; }

    public static Builder builder() {
        return new Builder();
    }

    public static RiskLimits permissive() {
        // Defaults that allow load tests through.
        return builder()
                .maxOpenNotionalPerAccount(1_000_000_000_000L)
                .maxQtyPerSymbolPerAccount(1_000_000L)
                .fatFingerBandPct(0.5)
                .build();
    }

    public static final class Builder {
        private long maxOpenNotionalPerAccount = 1_000_000_000_000L;
        private long maxQtyPerSymbolPerAccount = 1_000_000L;
        private double fatFingerBandPct = 0.5;

        public Builder maxOpenNotionalPerAccount(long v) { this.maxOpenNotionalPerAccount = v; return this; }
        public Builder maxQtyPerSymbolPerAccount(long v) { this.maxQtyPerSymbolPerAccount = v; return this; }
        public Builder fatFingerBandPct(double v) { this.fatFingerBandPct = v; return this; }
        public RiskLimits build() { return new RiskLimits(this); }
    }
}
