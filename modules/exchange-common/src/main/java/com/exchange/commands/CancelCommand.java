package com.exchange.commands;

import com.exchange.domain.ClOrdId;

import java.nio.ByteBuffer;

/**
 * Codec for a {@code CANCEL} command (78 bytes, little-endian).
 * <pre>
 *   0  type         byte(1) = 2
 *   1  reserved     byte(3)
 *   4  symbolId     int(4)
 *   8  account      long(8)
 *  16  senderId     long(8)
 *  24  sessionId    long(8)
 *  32  clOrdId      char[16]
 *  48  origClOrdId  char[16]
 *  64  clientTsNs   long(8)
 *  72  reserved     byte[6]
 * </pre>
 */
public final class CancelCommand {

    public static final int SIZE = 78;

    private CancelCommand() {
        // utility class
    }

    public static void encode(ByteBuffer out, int offset,
                              int symbolId,
                              long account,
                              long senderId,
                              long sessionId,
                              byte[] clOrdId16,
                              byte[] origClOrdId16,
                              long clientTsNs) {
        out.put(offset, CommandType.CANCEL);
        out.put(offset + 1, (byte) 0);
        out.put(offset + 2, (byte) 0);
        out.put(offset + 3, (byte) 0);
        out.putInt(offset + 4, symbolId);
        out.putLong(offset + 8, account);
        out.putLong(offset + 16, senderId);
        out.putLong(offset + 24, sessionId);
        for (int i = 0; i < ClOrdId.LENGTH; i++) {
            out.put(offset + 32 + i, clOrdId16[i]);
        }
        for (int i = 0; i < ClOrdId.LENGTH; i++) {
            out.put(offset + 48 + i, origClOrdId16[i]);
        }
        out.putLong(offset + 64, clientTsNs);
        for (int i = 72; i < SIZE; i++) {
            out.put(offset + i, (byte) 0);
        }
    }

    public static void decode(ByteBuffer in, int offset, MutableCancel into) {
        into.symbolId = in.getInt(offset + 4);
        into.account = in.getLong(offset + 8);
        into.senderId = in.getLong(offset + 16);
        into.sessionId = in.getLong(offset + 24);
        for (int i = 0; i < ClOrdId.LENGTH; i++) {
            into.clOrdId[i] = in.get(offset + 32 + i);
        }
        for (int i = 0; i < ClOrdId.LENGTH; i++) {
            into.origClOrdId[i] = in.get(offset + 48 + i);
        }
        into.clientTsNs = in.getLong(offset + 64);
    }

    /** Mutable, reusable holder populated by {@link #decode}. */
    public static final class MutableCancel {
        public int symbolId;
        public long account;
        public long senderId;
        public long sessionId;
        public final byte[] clOrdId = new byte[ClOrdId.LENGTH];
        public final byte[] origClOrdId = new byte[ClOrdId.LENGTH];
        public long clientTsNs;

        public void reset() {
            symbolId = 0;
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
