package com.exchange.domain;

/**
 * Order side. Codes are stable wire values; do not change once published.
 */
public enum Side {
    BUY((byte) 1),
    SELL((byte) 2);

    public static final byte CODE_BUY = 1;
    public static final byte CODE_SELL = 2;

    private final byte code;

    Side(byte code) {
        this.code = code;
    }

    public byte code() {
        return code;
    }

    public static Side fromCode(byte code) {
        return switch (code) {
            case CODE_BUY -> BUY;
            case CODE_SELL -> SELL;
            default -> throw new IllegalArgumentException("Unknown Side code: " + code);
        };
    }

    public static byte opposite(byte code) {
        if (code == CODE_BUY) return CODE_SELL;
        if (code == CODE_SELL) return CODE_BUY;
        throw new IllegalArgumentException("Unknown Side code: " + code);
    }
}
