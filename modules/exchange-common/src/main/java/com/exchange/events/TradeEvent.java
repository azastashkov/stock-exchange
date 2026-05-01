package com.exchange.events;

import java.nio.ByteBuffer;

/**
 * Codec for the {@code TRADE} event payload (64 bytes, little-endian).
 * <pre>
 *   0  symbolId       int(4)
 *   4  _pad           int(4)
 *   8  makerOrderId   long(8)
 *  16  takerOrderId   long(8)
 *  24  qty            long(8)
 *  32  price          long(8)  // fixed-point
 *  40  makerSide      byte(1)
 *  41  _pad           byte(7)
 *  48  tradeTsNs      long(8)
 *  56  clientTsNs     long(8)  // echoed from taker for RT latency calc
 * </pre>
 */
public final class TradeEvent {

    public static final int SIZE = 64;

    private TradeEvent() {
        // utility class
    }

    public static void encode(ByteBuffer out, int offset,
                              int symbolId,
                              long makerOrderId,
                              long takerOrderId,
                              long qty,
                              long price,
                              byte makerSide,
                              long tradeTsNs,
                              long clientTsNs) {
        out.putInt(offset, symbolId);
        out.putInt(offset + 4, 0);
        out.putLong(offset + 8, makerOrderId);
        out.putLong(offset + 16, takerOrderId);
        out.putLong(offset + 24, qty);
        out.putLong(offset + 32, price);
        out.put(offset + 40, makerSide);
        for (int i = 41; i < 48; i++) {
            out.put(offset + i, (byte) 0);
        }
        out.putLong(offset + 48, tradeTsNs);
        out.putLong(offset + 56, clientTsNs);
    }

    public static void decode(ByteBuffer in, int offset, MutableTrade into) {
        into.symbolId = in.getInt(offset);
        into.makerOrderId = in.getLong(offset + 8);
        into.takerOrderId = in.getLong(offset + 16);
        into.qty = in.getLong(offset + 24);
        into.price = in.getLong(offset + 32);
        into.makerSide = in.get(offset + 40);
        into.tradeTsNs = in.getLong(offset + 48);
        into.clientTsNs = in.getLong(offset + 56);
    }

    /** Mutable, reusable holder populated by {@link #decode}. */
    public static final class MutableTrade {
        public int symbolId;
        public long makerOrderId;
        public long takerOrderId;
        public long qty;
        public long price;
        public byte makerSide;
        public long tradeTsNs;
        public long clientTsNs;

        public void reset() {
            symbolId = 0;
            makerOrderId = 0;
            takerOrderId = 0;
            qty = 0;
            price = 0;
            makerSide = 0;
            tradeTsNs = 0;
            clientTsNs = 0;
        }
    }
}
