package com.exchange.store;

/**
 * Short-coded event type constants written into the canonical event log.
 * <p>
 * Codes are stable across releases; once assigned they never change. New
 * event kinds must be appended with a fresh, never-before-used short value.
 */
public final class EventType {

    public static final short ORDER_ACCEPTED = 1;
    public static final short TRADE = 2;
    public static final short ORDER_CANCELLED = 3;
    public static final short ORDER_REJECTED = 4;
    public static final short RISK_REJECTED = 5;
    public static final short BOOK_OPEN = 10;
    public static final short BOOK_CLOSE = 11;

    private EventType() {
        // utility class
    }
}
