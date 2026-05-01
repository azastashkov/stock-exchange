package com.exchange.events;

import com.exchange.domain.ClOrdId;

import java.nio.ByteBuffer;

/**
 * Codec for the {@code ORDER_REJECTED} event payload (48 bytes, little-endian).
 * <pre>
 *   0  symbolId      int(4)
 *   4  reasonCode    byte(1)
 *   5  _pad          byte(3)
 *   8  senderId      long(8)
 *  16  clOrdId       char[16]
 *  32  account       long(8)
 *  40  clientTsNs    long(8)
 * </pre>
 */
public final class OrderRejectedEvent {

    public static final int SIZE = 48;

    private OrderRejectedEvent() {
        // utility class
    }

    public static void encode(ByteBuffer out, int offset,
                              int symbolId,
                              byte reasonCode,
                              long senderId,
                              byte[] clOrdId16,
                              long account,
                              long clientTsNs) {
        out.putInt(offset, symbolId);
        out.put(offset + 4, reasonCode);
        out.put(offset + 5, (byte) 0);
        out.put(offset + 6, (byte) 0);
        out.put(offset + 7, (byte) 0);
        out.putLong(offset + 8, senderId);
        for (int i = 0; i < ClOrdId.LENGTH; i++) {
            out.put(offset + 16 + i, clOrdId16[i]);
        }
        out.putLong(offset + 32, account);
        out.putLong(offset + 40, clientTsNs);
    }

    public static void decode(ByteBuffer in, int offset, MutableOrderRejected into) {
        into.symbolId = in.getInt(offset);
        into.reasonCode = in.get(offset + 4);
        into.senderId = in.getLong(offset + 8);
        for (int i = 0; i < ClOrdId.LENGTH; i++) {
            into.clOrdId[i] = in.get(offset + 16 + i);
        }
        into.account = in.getLong(offset + 32);
        into.clientTsNs = in.getLong(offset + 40);
    }

    /** Mutable, reusable holder populated by {@link #decode}. */
    public static final class MutableOrderRejected {
        public int symbolId;
        public byte reasonCode;
        public long senderId;
        public final byte[] clOrdId = new byte[ClOrdId.LENGTH];
        public long account;
        public long clientTsNs;

        public void reset() {
            symbolId = 0;
            reasonCode = 0;
            senderId = 0;
            for (int i = 0; i < clOrdId.length; i++) clOrdId[i] = 0;
            account = 0;
            clientTsNs = 0;
        }
    }
}
