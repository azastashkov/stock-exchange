package com.exchange.commands;

/**
 * Inbound binary command type discriminators. Stored as the first byte of
 * each ring slot payload.
 * <p>
 * Codes are stable wire values; never reuse a code.
 */
public final class CommandType {

    public static final byte NEW_ORDER = 1;
    public static final byte CANCEL = 2;
    public static final byte CANCEL_REPLACE = 3;

    private CommandType() {
        // utility class
    }
}
