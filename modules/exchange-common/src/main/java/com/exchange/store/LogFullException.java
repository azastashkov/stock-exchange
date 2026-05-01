package com.exchange.store;

/**
 * Thrown when the canonical event log has no free slot for an append.
 * <p>
 * The log is sized at construction time and never grows. A producer that
 * sees this is expected to halt or escalate; in production we run with logs
 * sized for many hours of expected throughput.
 */
public class LogFullException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public LogFullException(String message) {
        super(message);
    }
}
