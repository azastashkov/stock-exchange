package com.exchange.engine;

import com.exchange.domain.ClOrdId;

/**
 * Mutable, poolable order. Holds everything the matching engine needs to
 * keep an order alive on the book. Reset to defaults via {@link #reset()}
 * before reuse.
 * <p>
 * The {@code clOrdIdBytes} buffer is allocated once in the constructor and
 * mutated in-place to avoid allocation on the hot path.
 */
public final class Order {

    public long orderId;
    public int symbolId;
    public byte side;
    public byte ordType;
    public byte tif;
    public long account;
    public long senderId;
    public final byte[] clOrdIdBytes = new byte[ClOrdId.LENGTH];
    public long qty;
    public long remainingQty;
    public long price;
    public long enteredTsNs;
    public long clientTsNs;
    /** Back-pointer set by the book on insert; cleared on remove. */
    public OrderEntry entry;

    public Order() {
        // default-constructed; populated via setters or pool checkout
    }

    /** Populate from an external request. Used by tests and gateway wiring. */
    public void set(long orderId,
                    int symbolId,
                    byte side,
                    byte ordType,
                    byte tif,
                    long account,
                    long senderId,
                    byte[] clOrdId16,
                    long qty,
                    long price,
                    long enteredTsNs,
                    long clientTsNs) {
        this.orderId = orderId;
        this.symbolId = symbolId;
        this.side = side;
        this.ordType = ordType;
        this.tif = tif;
        this.account = account;
        this.senderId = senderId;
        for (int i = 0; i < ClOrdId.LENGTH; i++) {
            this.clOrdIdBytes[i] = clOrdId16[i];
        }
        this.qty = qty;
        this.remainingQty = qty;
        this.price = price;
        this.enteredTsNs = enteredTsNs;
        this.clientTsNs = clientTsNs;
        this.entry = null;
    }

    public void reset() {
        orderId = 0;
        symbolId = 0;
        side = 0;
        ordType = 0;
        tif = 0;
        account = 0;
        senderId = 0;
        for (int i = 0; i < clOrdIdBytes.length; i++) {
            clOrdIdBytes[i] = 0;
        }
        qty = 0;
        remainingQty = 0;
        price = 0;
        enteredTsNs = 0;
        clientTsNs = 0;
        entry = null;
    }
}
