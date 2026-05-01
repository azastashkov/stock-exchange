package com.exchange.commands;

/**
 * Stable wire codes for {@link ExecutionReportCommand#execType}. Never reuse
 * a value once assigned.
 */
public final class ExecType {

    public static final byte NEW = 0;
    public static final byte PARTIAL_FILL = 1;
    public static final byte FILL = 2;
    public static final byte CANCELED = 3;
    public static final byte REJECTED = 4;
    public static final byte REPLACED = 5;

    private ExecType() {
        // utility class
    }
}
