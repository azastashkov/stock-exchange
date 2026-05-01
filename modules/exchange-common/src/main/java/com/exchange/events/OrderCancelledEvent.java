package com.exchange.events;

import com.exchange.domain.ClOrdId;

import java.nio.ByteBuffer;

/**
 * Codec for the {@code ORDER_CANCELLED} event payload (80 bytes, little-endian).
 * <pre>
 *   0  orderId        long(8)
 *   8  symbolId       int(4)
 *  12  _pad           int(4)
 *  16  remainingQty   long(8)
 *  24  senderId       long(8)
 *  32  clOrdId        char[16]
 *  48  origClOrdId    char[16]
 *  64  account        long(8)
 *  72  clientTsNs     long(8)
 * </pre>
 */
public final class OrderCancelledEvent {

    public static final int SIZE = 80;

    private OrderCancelledEvent() {
        // utility class
    }

    public static void encode(ByteBuffer out, int offset,
                              long orderId,
                              int symbolId,
                              long remainingQty,
                              long senderId,
                              byte[] clOrdId16,
                              byte[] origClOrdId16,
                              long account,
                              long clientTsNs) {
        out.putLong(offset, orderId);
        out.putInt(offset + 8, symbolId);
        out.putInt(offset + 12, 0);
        out.putLong(offset + 16, remainingQty);
        out.putLong(offset + 24, senderId);
        for (int i = 0; i < ClOrdId.LENGTH; i++) {
            out.put(offset + 32 + i, clOrdId16[i]);
        }
        for (int i = 0; i < ClOrdId.LENGTH; i++) {
            out.put(offset + 48 + i, origClOrdId16[i]);
        }
        out.putLong(offset + 64, account);
        out.putLong(offset + 72, clientTsNs);
    }

    public static void decode(ByteBuffer in, int offset, MutableOrderCancelled into) {
        into.orderId = in.getLong(offset);
        into.symbolId = in.getInt(offset + 8);
        into.remainingQty = in.getLong(offset + 16);
        into.senderId = in.getLong(offset + 24);
        for (int i = 0; i < ClOrdId.LENGTH; i++) {
            into.clOrdId[i] = in.get(offset + 32 + i);
        }
        for (int i = 0; i < ClOrdId.LENGTH; i++) {
            into.origClOrdId[i] = in.get(offset + 48 + i);
        }
        into.account = in.getLong(offset + 64);
        into.clientTsNs = in.getLong(offset + 72);
    }

    /** Mutable, reusable holder populated by {@link #decode}. */
    public static final class MutableOrderCancelled {
        public long orderId;
        public int symbolId;
        public long remainingQty;
        public long senderId;
        public final byte[] clOrdId = new byte[ClOrdId.LENGTH];
        public final byte[] origClOrdId = new byte[ClOrdId.LENGTH];
        public long account;
        public long clientTsNs;

        public void reset() {
            orderId = 0;
            symbolId = 0;
            remainingQty = 0;
            senderId = 0;
            for (int i = 0; i < clOrdId.length; i++) {
                clOrdId[i] = 0;
                origClOrdId[i] = 0;
            }
            account = 0;
            clientTsNs = 0;
        }
    }
}
