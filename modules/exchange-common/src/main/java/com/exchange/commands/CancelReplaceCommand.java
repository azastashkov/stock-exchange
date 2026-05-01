package com.exchange.commands;

import com.exchange.domain.ClOrdId;

import java.nio.ByteBuffer;

/**
 * Codec for a {@code CANCEL_REPLACE} command (94 bytes, little-endian).
 * <pre>
 *   0  type         byte(1) = 3
 *   1  side         byte(1)
 *   2  reserved     byte(2)
 *   4  symbolId     int(4)
 *   8  qty          long(8)
 *  16  price        long(8)
 *  24  account      long(8)
 *  32  senderId     long(8)
 *  40  sessionId    long(8)
 *  48  clOrdId      char[16]
 *  64  origClOrdId  char[16]
 *  80  clientTsNs   long(8)
 *  88  reserved     byte[6]
 * </pre>
 */
public final class CancelReplaceCommand {

    public static final int SIZE = 94;

    private CancelReplaceCommand() {
        // utility class
    }

    public static void encode(ByteBuffer out, int offset,
                              byte side,
                              int symbolId,
                              long qty,
                              long price,
                              long account,
                              long senderId,
                              long sessionId,
                              byte[] clOrdId16,
                              byte[] origClOrdId16,
                              long clientTsNs) {
        out.put(offset, CommandType.CANCEL_REPLACE);
        out.put(offset + 1, side);
        out.put(offset + 2, (byte) 0);
        out.put(offset + 3, (byte) 0);
        out.putInt(offset + 4, symbolId);
        out.putLong(offset + 8, qty);
        out.putLong(offset + 16, price);
        out.putLong(offset + 24, account);
        out.putLong(offset + 32, senderId);
        out.putLong(offset + 40, sessionId);
        for (int i = 0; i < ClOrdId.LENGTH; i++) {
            out.put(offset + 48 + i, clOrdId16[i]);
        }
        for (int i = 0; i < ClOrdId.LENGTH; i++) {
            out.put(offset + 64 + i, origClOrdId16[i]);
        }
        out.putLong(offset + 80, clientTsNs);
        for (int i = 88; i < SIZE; i++) {
            out.put(offset + i, (byte) 0);
        }
    }

    public static void decode(ByteBuffer in, int offset, MutableCancelReplace into) {
        into.side = in.get(offset + 1);
        into.symbolId = in.getInt(offset + 4);
        into.qty = in.getLong(offset + 8);
        into.price = in.getLong(offset + 16);
        into.account = in.getLong(offset + 24);
        into.senderId = in.getLong(offset + 32);
        into.sessionId = in.getLong(offset + 40);
        for (int i = 0; i < ClOrdId.LENGTH; i++) {
            into.clOrdId[i] = in.get(offset + 48 + i);
        }
        for (int i = 0; i < ClOrdId.LENGTH; i++) {
            into.origClOrdId[i] = in.get(offset + 64 + i);
        }
        into.clientTsNs = in.getLong(offset + 80);
    }

    /** Mutable, reusable holder populated by {@link #decode}. */
    public static final class MutableCancelReplace {
        public byte side;
        public int symbolId;
        public long qty;
        public long price;
        public long account;
        public long senderId;
        public long sessionId;
        public final byte[] clOrdId = new byte[ClOrdId.LENGTH];
        public final byte[] origClOrdId = new byte[ClOrdId.LENGTH];
        public long clientTsNs;

        public void reset() {
            side = 0;
            symbolId = 0;
            qty = 0;
            price = 0;
            account = 0;
            senderId = 0;
            sessionId = 0;
            for (int i = 0; i < clOrdId.length; i++) {
                clOrdId[i] = 0;
                origClOrdId[i] = 0;
            }
            clientTsNs = 0;
        }
    }
}
