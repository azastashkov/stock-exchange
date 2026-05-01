package com.exchange.app.md;

import java.nio.ByteBuffer;

/**
 * Wire format for a market-data trade tick.
 * <pre>
 *   0  symbolId    int(4)
 *   4  makerSide   byte(1)
 *   5  reserved    byte(3)
 *   8  qty         long(8)
 *  16  price       long(8)
 *  24  tradeTsNs   long(8)
 * </pre>
 * Total 32 bytes; transmitted as a 4-byte length-prefixed frame to TCP
 * subscribers.
 */
public final class TradeMdMessage {

    public static final int SIZE = 32;

    private TradeMdMessage() {
        // utility class
    }

    public static void encode(ByteBuffer out, int offset,
                              int symbolId,
                              byte makerSide,
                              long qty,
                              long price,
                              long tradeTsNs) {
        out.putInt(offset, symbolId);
        out.put(offset + 4, makerSide);
        out.put(offset + 5, (byte) 0);
        out.put(offset + 6, (byte) 0);
        out.put(offset + 7, (byte) 0);
        out.putLong(offset + 8, qty);
        out.putLong(offset + 16, price);
        out.putLong(offset + 24, tradeTsNs);
    }
}
