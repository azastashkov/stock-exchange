package com.exchange.events;

import com.exchange.domain.ClOrdId;

import java.nio.ByteBuffer;

/**
 * Codec for the {@code ORDER_ACCEPTED} event payload (80 bytes, little-endian).
 * <pre>
 *   0  orderId       long(8)
 *   8  symbolId      int(4)
 *  12  side          byte(1)
 *  13  ordType       byte(1)
 *  14  tif           byte(1)
 *  15  _pad          byte(1)
 *  16  qty           long(8)
 *  24  price         long(8)   // fixed-point, 0 for market
 *  32  account       long(8)
 *  40  senderId      long(8)
 *  48  clOrdId       char[16]
 *  64  clientTsNs    long(8)
 *  72  orderTsNs     long(8)
 * </pre>
 * All methods are allocation-free (the writer is expected to pass in a
 * pre-allocated direct {@link ByteBuffer}).
 */
public final class OrderAcceptedEvent {

    public static final int SIZE = 80;

    private OrderAcceptedEvent() {
        // utility class
    }

    public static void encode(ByteBuffer out, int offset,
                              long orderId,
                              int symbolId,
                              byte side,
                              byte ordType,
                              byte tif,
                              long qty,
                              long price,
                              long account,
                              long senderId,
                              byte[] clOrdId16,
                              long clientTsNs,
                              long orderTsNs) {
        out.putLong(offset, orderId);
        out.putInt(offset + 8, symbolId);
        out.put(offset + 12, side);
        out.put(offset + 13, ordType);
        out.put(offset + 14, tif);
        out.put(offset + 15, (byte) 0);
        out.putLong(offset + 16, qty);
        out.putLong(offset + 24, price);
        out.putLong(offset + 32, account);
        out.putLong(offset + 40, senderId);
        for (int i = 0; i < ClOrdId.LENGTH; i++) {
            out.put(offset + 48 + i, clOrdId16[i]);
        }
        out.putLong(offset + 64, clientTsNs);
        out.putLong(offset + 72, orderTsNs);
    }

    public static void decode(ByteBuffer in, int offset, MutableOrderAccepted into) {
        into.orderId = in.getLong(offset);
        into.symbolId = in.getInt(offset + 8);
        into.side = in.get(offset + 12);
        into.ordType = in.get(offset + 13);
        into.tif = in.get(offset + 14);
        into.qty = in.getLong(offset + 16);
        into.price = in.getLong(offset + 24);
        into.account = in.getLong(offset + 32);
        into.senderId = in.getLong(offset + 40);
        for (int i = 0; i < ClOrdId.LENGTH; i++) {
            into.clOrdId[i] = in.get(offset + 48 + i);
        }
        into.clientTsNs = in.getLong(offset + 64);
        into.orderTsNs = in.getLong(offset + 72);
    }

    /** Mutable, reusable holder populated by {@link #decode}. */
    public static final class MutableOrderAccepted {
        public long orderId;
        public int symbolId;
        public byte side;
        public byte ordType;
        public byte tif;
        public long qty;
        public long price;
        public long account;
        public long senderId;
        public final byte[] clOrdId = new byte[ClOrdId.LENGTH];
        public long clientTsNs;
        public long orderTsNs;

        public void reset() {
            orderId = 0;
            symbolId = 0;
            side = 0;
            ordType = 0;
            tif = 0;
            qty = 0;
            price = 0;
            account = 0;
            senderId = 0;
            for (int i = 0; i < clOrdId.length; i++) clOrdId[i] = 0;
            clientTsNs = 0;
            orderTsNs = 0;
        }
    }
}
