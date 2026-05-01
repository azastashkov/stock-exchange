package com.exchange.domain;

/**
 * Order type. Codes are stable wire values.
 */
public enum OrdType {
    LIMIT((byte) 1),
    MARKET((byte) 2);

    public static final byte CODE_LIMIT = 1;
    public static final byte CODE_MARKET = 2;

    private final byte code;

    OrdType(byte code) {
        this.code = code;
    }

    public byte code() {
        return code;
    }

    public static OrdType fromCode(byte code) {
        return switch (code) {
            case CODE_LIMIT -> LIMIT;
            case CODE_MARKET -> MARKET;
            default -> throw new IllegalArgumentException("Unknown OrdType code: " + code);
        };
    }
}
