package com.exchange.domain;

/**
 * Time-in-force. Only DAY is wired in v1; IOC is reserved for a later phase.
 */
public enum TimeInForce {
    DAY((byte) 1),
    IOC((byte) 3);

    public static final byte CODE_DAY = 1;
    public static final byte CODE_IOC = 3;

    private final byte code;

    TimeInForce(byte code) {
        this.code = code;
    }

    public byte code() {
        return code;
    }

    public static TimeInForce fromCode(byte code) {
        return switch (code) {
            case CODE_DAY -> DAY;
            case CODE_IOC -> IOC;
            default -> throw new IllegalArgumentException("Unknown TimeInForce code: " + code);
        };
    }
}
