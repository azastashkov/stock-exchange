package com.exchange.domain;

/**
 * Logical status an order may transition through.
 */
public enum OrderStatus {
    NEW,
    PARTIALLY_FILLED,
    FILLED,
    CANCELLED,
    REJECTED,
    REPLACED
}
