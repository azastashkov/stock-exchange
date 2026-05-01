package com.exchange.commands;

import com.exchange.domain.ClOrdId;

import java.nio.ByteBuffer;

/**
 * Codec for an {@code ExecutionReportCommand} (200 bytes, little-endian).
 * <pre>
 *    0  sessionId      long(8)
 *    8  senderId       long(8)
 *   16  orderId        long(8)
 *   24  symbolId       int(4)
 *   28  side           byte(1)
 *   29  execType       byte(1)
 *   30  ordStatus      byte(1)
 *   31  rejectReason   byte(1)
 *   32  qty            long(8)
 *   40  filledQty      long(8)
 *   48  remainingQty   long(8)
 *   56  price          long(8)
 *   64  lastQty        long(8)
 *   72  lastPx         long(8)
 *   80  account        long(8)
 *   88  clientTsNs     long(8)
 *   96  reportTsNs     long(8)
 *  104  clOrdId        char[16]
 *  120  origClOrdId    char[16]
 *  136  text           char[64]
 * </pre>
 * Total 200 bytes; recommended ring slot size is 256 to leave room for the
 * 4-byte length prefix and a tail margin.
 */
public final class ExecutionReportCommand {

    public static final int SIZE = 200;
    public static final int TEXT_LENGTH = 64;

    private ExecutionReportCommand() {
        // utility class
    }

    public static void encode(ByteBuffer out, int offset,
                              long sessionId,
                              long senderId,
                              long orderId,
                              int symbolId,
                              byte side,
                              byte execType,
                              byte ordStatus,
                              byte rejectReason,
                              long qty,
                              long filledQty,
                              long remainingQty,
                              long price,
                              long lastQty,
                              long lastPx,
                              long account,
                              long clientTsNs,
                              long reportTsNs,
                              byte[] clOrdId16,
                              byte[] origClOrdId16,
                              byte[] textAscii) {
        out.putLong(offset, sessionId);
        out.putLong(offset + 8, senderId);
        out.putLong(offset + 16, orderId);
        out.putInt(offset + 24, symbolId);
        out.put(offset + 28, side);
        out.put(offset + 29, execType);
        out.put(offset + 30, ordStatus);
        out.put(offset + 31, rejectReason);
        out.putLong(offset + 32, qty);
        out.putLong(offset + 40, filledQty);
        out.putLong(offset + 48, remainingQty);
        out.putLong(offset + 56, price);
        out.putLong(offset + 64, lastQty);
        out.putLong(offset + 72, lastPx);
        out.putLong(offset + 80, account);
        out.putLong(offset + 88, clientTsNs);
        out.putLong(offset + 96, reportTsNs);
        for (int i = 0; i < ClOrdId.LENGTH; i++) {
            out.put(offset + 104 + i, clOrdId16[i]);
        }
        for (int i = 0; i < ClOrdId.LENGTH; i++) {
            out.put(offset + 120 + i, origClOrdId16[i]);
        }
        // Text: copy up to TEXT_LENGTH bytes; zero-pad the rest.
        int textBase = offset + 136;
        if (textAscii == null) {
            for (int i = 0; i < TEXT_LENGTH; i++) {
                out.put(textBase + i, (byte) 0);
            }
        } else {
            int copyLen = Math.min(textAscii.length, TEXT_LENGTH);
            for (int i = 0; i < copyLen; i++) {
                out.put(textBase + i, textAscii[i]);
            }
            for (int i = copyLen; i < TEXT_LENGTH; i++) {
                out.put(textBase + i, (byte) 0);
            }
        }
    }

    public static void decode(ByteBuffer in, int offset, MutableExecutionReport into) {
        into.sessionId = in.getLong(offset);
        into.senderId = in.getLong(offset + 8);
        into.orderId = in.getLong(offset + 16);
        into.symbolId = in.getInt(offset + 24);
        into.side = in.get(offset + 28);
        into.execType = in.get(offset + 29);
        into.ordStatus = in.get(offset + 30);
        into.rejectReason = in.get(offset + 31);
        into.qty = in.getLong(offset + 32);
        into.filledQty = in.getLong(offset + 40);
        into.remainingQty = in.getLong(offset + 48);
        into.price = in.getLong(offset + 56);
        into.lastQty = in.getLong(offset + 64);
        into.lastPx = in.getLong(offset + 72);
        into.account = in.getLong(offset + 80);
        into.clientTsNs = in.getLong(offset + 88);
        into.reportTsNs = in.getLong(offset + 96);
        for (int i = 0; i < ClOrdId.LENGTH; i++) {
            into.clOrdId[i] = in.get(offset + 104 + i);
        }
        for (int i = 0; i < ClOrdId.LENGTH; i++) {
            into.origClOrdId[i] = in.get(offset + 120 + i);
        }
        for (int i = 0; i < TEXT_LENGTH; i++) {
            into.text[i] = in.get(offset + 136 + i);
        }
    }

    /** Mutable, reusable holder populated by {@link #decode}. */
    public static final class MutableExecutionReport {
        public long sessionId;
        public long senderId;
        public long orderId;
        public int symbolId;
        public byte side;
        public byte execType;
        public byte ordStatus;
        public byte rejectReason;
        public long qty;
        public long filledQty;
        public long remainingQty;
        public long price;
        public long lastQty;
        public long lastPx;
        public long account;
        public long clientTsNs;
        public long reportTsNs;
        public final byte[] clOrdId = new byte[ClOrdId.LENGTH];
        public final byte[] origClOrdId = new byte[ClOrdId.LENGTH];
        public final byte[] text = new byte[TEXT_LENGTH];

        public void reset() {
            sessionId = 0;
            senderId = 0;
            orderId = 0;
            symbolId = 0;
            side = 0;
            execType = 0;
            ordStatus = 0;
            rejectReason = 0;
            qty = 0;
            filledQty = 0;
            remainingQty = 0;
            price = 0;
            lastQty = 0;
            lastPx = 0;
            account = 0;
            clientTsNs = 0;
            reportTsNs = 0;
            for (int i = 0; i < clOrdId.length; i++) {
                clOrdId[i] = 0;
                origClOrdId[i] = 0;
            }
            for (int i = 0; i < text.length; i++) text[i] = 0;
        }

        /** Trim trailing zeros from the text field and decode as ASCII. */
        public String textAsString() {
            int len = 0;
            for (; len < text.length; len++) {
                if (text[len] == 0) break;
            }
            if (len == 0) return "";
            return new String(text, 0, len, java.nio.charset.StandardCharsets.US_ASCII);
        }
    }
}
