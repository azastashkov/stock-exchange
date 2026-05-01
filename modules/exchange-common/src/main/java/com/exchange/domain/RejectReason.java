package com.exchange.domain;

/**
 * Reject reason codes carried in {@code ORDER_REJECTED} events.
 * <p>
 * Stable wire values; never reuse a code.
 */
public final class RejectReason {

    public static final byte UNKNOWN_SYMBOL = 1;
    public static final byte INVALID_QTY = 2;
    public static final byte INVALID_PRICE = 3;
    public static final byte DUPLICATE_CLORDID = 4;
    public static final byte UNKNOWN_ORDER = 5;
    public static final byte MARKET_NOT_OPEN = 6;
    public static final byte CROSSED_SELF = 7;
    public static final byte RISK_LIMIT = 8;

    private RejectReason() {
        // utility class
    }

    public static String description(byte code) {
        return switch (code) {
            case UNKNOWN_SYMBOL -> "UNKNOWN_SYMBOL";
            case INVALID_QTY -> "INVALID_QTY";
            case INVALID_PRICE -> "INVALID_PRICE";
            case DUPLICATE_CLORDID -> "DUPLICATE_CLORDID";
            case UNKNOWN_ORDER -> "UNKNOWN_ORDER";
            case MARKET_NOT_OPEN -> "MARKET_NOT_OPEN";
            case CROSSED_SELF -> "CROSSED_SELF";
            case RISK_LIMIT -> "RISK_LIMIT";
            default -> "UNKNOWN(" + (code & 0xFF) + ")";
        };
    }
}
