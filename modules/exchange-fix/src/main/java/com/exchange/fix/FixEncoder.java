package com.exchange.fix;

import com.exchange.commands.ExecType;
import com.exchange.commands.ExecutionReportCommand;
import com.exchange.domain.OrderStatus;
import com.exchange.domain.Side;

import java.nio.charset.StandardCharsets;

/**
 * Allocation-free encoders for the subset of FIX 4.4 messages emitted by the
 * gateway. Each helper writes the entire framed message into the caller-
 * provided destination array starting at {@code destOff} and returns the
 * number of bytes written.
 * <p>
 * The standard layout is:
 * <pre>
 *   8=FIX.4.4|9=BBB|35=X|49=SENDER|56=TARGET|34=N|52=YYYYMMDD-HH:MM:SS.sss|...|10=NNN|
 * </pre>
 * where {@code BBB} is the BodyLength (count of bytes from after {@code 9=BBB|}
 * up to but not including the {@code 10=}) and the trailing checksum is the
 * mod-256 byte sum of all preceding bytes formatted as 3 ASCII digits.
 */
public final class FixEncoder {

    private static final byte[] BEGIN_STRING_PREFIX = "8=FIX.4.4".getBytes(StandardCharsets.US_ASCII);
    private static final byte SOH = FixCodec.SOH;

    private FixEncoder() {
        // utility class
    }

    public static int writeLogonResponse(byte[] dest, int destOff,
                                         byte[] sender, byte[] target,
                                         int outSeqNum, int heartBtInt,
                                         long sendingTimeMs) {
        Cursor c = startMessage(dest, destOff, FixMsgType.LOGON, sender, target, outSeqNum, sendingTimeMs);
        appendIntField(c, FixField.ENCRYPT_METHOD, 0);
        appendIntField(c, FixField.HEART_BT_INT, heartBtInt);
        return finalizeMessage(c, destOff);
    }

    public static int writeHeartbeat(byte[] dest, int destOff,
                                     byte[] sender, byte[] target,
                                     int outSeqNum, byte[] testReqIdOrNull,
                                     long sendingTimeMs) {
        Cursor c = startMessage(dest, destOff, FixMsgType.HEARTBEAT, sender, target, outSeqNum, sendingTimeMs);
        if (testReqIdOrNull != null && testReqIdOrNull.length > 0) {
            appendBytesField(c, FixField.TEST_REQ_ID, testReqIdOrNull, 0, testReqIdOrNull.length);
        }
        return finalizeMessage(c, destOff);
    }

    public static int writeTestRequest(byte[] dest, int destOff,
                                       byte[] sender, byte[] target,
                                       int outSeqNum, byte[] testReqId,
                                       long sendingTimeMs) {
        Cursor c = startMessage(dest, destOff, FixMsgType.TEST_REQUEST, sender, target, outSeqNum, sendingTimeMs);
        appendBytesField(c, FixField.TEST_REQ_ID, testReqId, 0, testReqId.length);
        return finalizeMessage(c, destOff);
    }

    public static int writeLogout(byte[] dest, int destOff,
                                  byte[] sender, byte[] target,
                                  int outSeqNum, String text,
                                  long sendingTimeMs) {
        Cursor c = startMessage(dest, destOff, FixMsgType.LOGOUT, sender, target, outSeqNum, sendingTimeMs);
        if (text != null && !text.isEmpty()) {
            appendStringField(c, FixField.TEXT, text);
        }
        return finalizeMessage(c, destOff);
    }

    public static int writeReject(byte[] dest, int destOff,
                                  byte[] sender, byte[] target,
                                  int outSeqNum,
                                  int refSeqNum, int reasonCode, String text,
                                  long sendingTimeMs) {
        Cursor c = startMessage(dest, destOff, FixMsgType.REJECT, sender, target, outSeqNum, sendingTimeMs);
        appendIntField(c, FixField.REF_SEQ_NUM, refSeqNum);
        appendIntField(c, FixField.SESSION_REJECT_REASON, reasonCode);
        if (text != null && !text.isEmpty()) {
            appendStringField(c, FixField.TEXT, text);
        }
        return finalizeMessage(c, destOff);
    }

    public static int writeBusinessMessageReject(byte[] dest, int destOff,
                                                 byte[] sender, byte[] target,
                                                 int outSeqNum,
                                                 int refSeqNum, String refMsgType,
                                                 int reasonCode, String text,
                                                 long sendingTimeMs) {
        Cursor c = startMessage(dest, destOff, FixMsgType.BUSINESS_MESSAGE_REJECT, sender, target, outSeqNum, sendingTimeMs);
        appendIntField(c, FixField.REF_SEQ_NUM, refSeqNum);
        if (refMsgType != null && !refMsgType.isEmpty()) {
            appendStringField(c, FixField.REF_MSG_TYPE, refMsgType);
        }
        appendIntField(c, FixField.BUSINESS_REJECT_REASON, reasonCode);
        if (text != null && !text.isEmpty()) {
            appendStringField(c, FixField.TEXT, text);
        }
        return finalizeMessage(c, destOff);
    }

    public static int writeResendRequest(byte[] dest, int destOff,
                                         byte[] sender, byte[] target,
                                         int outSeqNum,
                                         int beginSeqNo, int endSeqNo,
                                         long sendingTimeMs) {
        Cursor c = startMessage(dest, destOff, FixMsgType.RESEND_REQUEST, sender, target, outSeqNum, sendingTimeMs);
        appendIntField(c, FixField.BEGIN_SEQ_NO, beginSeqNo);
        appendIntField(c, FixField.END_SEQ_NO, endSeqNo);
        return finalizeMessage(c, destOff);
    }

    public static int writeSequenceReset(byte[] dest, int destOff,
                                         byte[] sender, byte[] target,
                                         int outSeqNum,
                                         int newSeqNo, boolean gapFill,
                                         long sendingTimeMs) {
        Cursor c = startMessage(dest, destOff, FixMsgType.SEQUENCE_RESET, sender, target, outSeqNum, sendingTimeMs);
        if (gapFill) {
            appendCharField(c, FixField.GAP_FILL_FLAG, (byte) 'Y');
        }
        appendIntField(c, FixField.NEW_SEQ_NO, newSeqNo);
        return finalizeMessage(c, destOff);
    }

    /**
     * Encode an ExecutionReport (35=8) message from the binary {@code er}
     * command. {@code symbolName} is the resolved ASCII bytes for tag 55;
     * the encoder does not look it up.
     */
    public static int writeExecutionReport(byte[] dest, int destOff,
                                           byte[] sender, byte[] target,
                                           int outSeqNum,
                                           ExecutionReportCommand.MutableExecutionReport er,
                                           byte[] symbolName,
                                           long sendingTimeMs) {
        Cursor c = startMessage(dest, destOff, FixMsgType.EXECUTION_REPORT, sender, target, outSeqNum, sendingTimeMs);
        // OrderId — required, even if unset use a placeholder
        long orderId = er.orderId != 0L ? er.orderId : outSeqNum;
        appendLongField(c, FixField.ORDER_ID, orderId);
        // ExecId — non-empty per spec
        appendLongField(c, FixField.EXEC_ID, er.reportTsNs == 0L ? outSeqNum : er.reportTsNs);
        // ExecType
        appendCharField(c, FixField.EXEC_TYPE, mapExecType(er.execType));
        // OrdStatus
        appendCharField(c, FixField.ORD_STATUS, mapOrdStatus(er.ordStatus));
        // ClOrdId — strip trailing nulls
        appendClOrdIdField(c, FixField.CL_ORD_ID, er.clOrdId);
        if (anyNonZero(er.origClOrdId)) {
            appendClOrdIdField(c, FixField.ORIG_CL_ORD_ID, er.origClOrdId);
        }
        appendBytesField(c, FixField.SYMBOL, symbolName, 0, symbolName.length);
        if (er.side != 0) {
            appendCharField(c, FixField.SIDE, mapSide(er.side));
        }
        if (er.qty > 0) {
            appendLongField(c, FixField.ORDER_QTY, er.qty);
        }
        appendLongField(c, FixField.LEAVES_QTY, Math.max(0L, er.remainingQty));
        appendLongField(c, FixField.CUM_QTY, Math.max(0L, er.filledQty));
        appendDecimalPriceField(c, FixField.AVG_PX, er.filledQty > 0 ? er.lastPx : 0L);
        if (er.lastQty > 0) {
            appendLongField(c, FixField.LAST_QTY, er.lastQty);
            appendDecimalPriceField(c, FixField.LAST_PX, er.lastPx);
        }
        if (er.price != 0L) {
            appendDecimalPriceField(c, FixField.PRICE, er.price);
        }
        if (er.account != 0L) {
            appendLongField(c, FixField.ACCOUNT, er.account);
        }
        if (er.execType == ExecType.REJECTED) {
            appendIntField(c, FixField.ORD_REJ_REASON, er.rejectReason & 0xFF);
            int textLen = trimmedLen(er.text);
            if (textLen > 0) {
                appendBytesField(c, FixField.TEXT, er.text, 0, textLen);
            }
        }
        return finalizeMessage(c, destOff);
    }

    // ---------- internals ----------

    /** Mutable cursor used by the write helpers. */
    static final class Cursor {
        byte[] dest;
        int pos;
        int bodyLengthValuePos; // offset of the first BodyLength digit
        int bodyLengthValueLen; // 6 placeholder digits
        int bodyStartPos;       // first body byte (after 9=N|)
    }

    private static final ThreadLocal<Cursor> CURSOR_TL = ThreadLocal.withInitial(Cursor::new);

    private static Cursor startMessage(byte[] dest, int destOff,
                                       String msgType,
                                       byte[] sender, byte[] target,
                                       int outSeqNum,
                                       long sendingTimeMs) {
        Cursor c = CURSOR_TL.get();
        c.dest = dest;
        c.pos = destOff;
        // BeginString (already includes trailing SOH)
        System.arraycopy(BEGIN_STRING_PREFIX, 0, dest, c.pos, BEGIN_STRING_PREFIX.length);
        c.pos += BEGIN_STRING_PREFIX.length;
        // BodyLength placeholder: 9=000000|
        dest[c.pos++] = '9';
        dest[c.pos++] = '=';
        c.bodyLengthValuePos = c.pos;
        c.bodyLengthValueLen = 6;
        for (int i = 0; i < 6; i++) {
            dest[c.pos++] = '0';
        }
        dest[c.pos++] = SOH;
        c.bodyStartPos = c.pos;

        // 35=msgType
        dest[c.pos++] = '3';
        dest[c.pos++] = '5';
        dest[c.pos++] = '=';
        for (int i = 0; i < msgType.length(); i++) {
            dest[c.pos++] = (byte) msgType.charAt(i);
        }
        dest[c.pos++] = SOH;
        // 49=sender
        appendBytesField(c, FixField.SENDER_COMP_ID, sender, 0, sender.length);
        // 56=target
        appendBytesField(c, FixField.TARGET_COMP_ID, target, 0, target.length);
        // 34=outSeqNum
        appendIntField(c, FixField.MSG_SEQ_NUM, outSeqNum);
        // 52=sendingTime in UTC YYYYMMDD-HH:MM:SS.sss
        appendTagPrefix(c, FixField.SENDING_TIME);
        c.pos += writeUtcTimestamp(dest, c.pos, sendingTimeMs);
        dest[c.pos++] = SOH;
        return c;
    }

    private static int finalizeMessage(Cursor c, int destOff) {
        // Compute body length: bytes from bodyStartPos to current pos (exclusive).
        int bodyLength = c.pos - c.bodyStartPos;
        writeFixedDigits(c.dest, c.bodyLengthValuePos, c.bodyLengthValueLen, bodyLength);

        // Append checksum: 10=NNN|
        int sum = FixCodec.checksum(c.dest, destOff, c.pos);
        c.dest[c.pos++] = '1';
        c.dest[c.pos++] = '0';
        c.dest[c.pos++] = '=';
        c.dest[c.pos++] = (byte) ('0' + (sum / 100) % 10);
        c.dest[c.pos++] = (byte) ('0' + (sum / 10) % 10);
        c.dest[c.pos++] = (byte) ('0' + sum % 10);
        c.dest[c.pos++] = SOH;
        return c.pos - destOff;
    }

    private static void appendTagPrefix(Cursor c, int tag) {
        c.pos += writeIntAscii(c.dest, c.pos, tag);
        c.dest[c.pos++] = '=';
    }

    static void appendIntField(Cursor c, int tag, int value) {
        appendTagPrefix(c, tag);
        c.pos += writeIntAscii(c.dest, c.pos, value);
        c.dest[c.pos++] = SOH;
    }

    static void appendLongField(Cursor c, int tag, long value) {
        appendTagPrefix(c, tag);
        c.pos += writeLongAscii(c.dest, c.pos, value);
        c.dest[c.pos++] = SOH;
    }

    static void appendCharField(Cursor c, int tag, byte value) {
        appendTagPrefix(c, tag);
        c.dest[c.pos++] = value;
        c.dest[c.pos++] = SOH;
    }

    static void appendStringField(Cursor c, int tag, String value) {
        appendTagPrefix(c, tag);
        for (int i = 0; i < value.length(); i++) {
            c.dest[c.pos++] = (byte) value.charAt(i);
        }
        c.dest[c.pos++] = SOH;
    }

    static void appendBytesField(Cursor c, int tag, byte[] value, int off, int len) {
        appendTagPrefix(c, tag);
        System.arraycopy(value, off, c.dest, c.pos, len);
        c.pos += len;
        c.dest[c.pos++] = SOH;
    }

    static void appendClOrdIdField(Cursor c, int tag, byte[] clOrdId16) {
        int len = trimmedLen(clOrdId16);
        if (len == 0) {
            // emit a single '0' so we don't violate FIX field length constraints
            appendCharField(c, tag, (byte) '0');
            return;
        }
        appendBytesField(c, tag, clOrdId16, 0, len);
    }

    static void appendDecimalPriceField(Cursor c, int tag, long fixedPointTimes10000) {
        appendTagPrefix(c, tag);
        c.pos += writeDecimal4(c.dest, c.pos, fixedPointTimes10000);
        c.dest[c.pos++] = SOH;
    }

    /** Write {@code v} as ASCII decimal at {@code dest[off..]}; returns chars written. */
    static int writeIntAscii(byte[] dest, int off, int v) {
        if (v == 0) {
            dest[off] = '0';
            return 1;
        }
        int written = 0;
        boolean neg = false;
        if (v < 0) {
            dest[off++] = '-';
            written = 1;
            // careful with Integer.MIN_VALUE; pure decimal works because we only invert below
            v = -v;
            if (v < 0) {
                // INT_MIN
                return written + writeLongAscii(dest, off, Math.abs((long) Integer.MIN_VALUE));
            }
        }
        int start = off;
        while (v > 0) {
            dest[off++] = (byte) ('0' + v % 10);
            v /= 10;
        }
        reverse(dest, start, off - 1);
        return written + (off - start);
    }

    static int writeLongAscii(byte[] dest, int off, long v) {
        if (v == 0) {
            dest[off] = '0';
            return 1;
        }
        int written = 0;
        if (v < 0) {
            dest[off++] = '-';
            written = 1;
            // handle Long.MIN_VALUE specially since -Long.MIN_VALUE overflows
            if (v == Long.MIN_VALUE) {
                String s = Long.toString(v);
                // s already includes '-' so re-emit from index 1
                for (int i = 1; i < s.length(); i++) {
                    dest[off++] = (byte) s.charAt(i);
                }
                return written + (s.length() - 1);
            }
            v = -v;
        }
        int start = off;
        while (v > 0) {
            dest[off++] = (byte) ('0' + (int) (v % 10));
            v /= 10;
        }
        reverse(dest, start, off - 1);
        return written + (off - start);
    }

    /** Write a 4-decimal-place fixed-point representation of {@code fp} (value*10000). */
    static int writeDecimal4(byte[] dest, int off, long fp) {
        if (fp == 0L) {
            dest[off] = '0';
            return 1;
        }
        boolean neg = fp < 0L;
        if (neg) fp = -fp;
        long whole = fp / 10000L;
        long frac = fp % 10000L;
        int start = off;
        if (neg) {
            dest[off++] = '-';
        }
        off += writeLongAscii(dest, off, whole);
        if (frac != 0L) {
            dest[off++] = '.';
            // emit frac with up to 4 zero-padded digits, trimming trailing zeros
            int digits = 4;
            int divisor = 1000;
            int trailingTrim = 0;
            byte[] tmp = new byte[4];
            int idx = 0;
            int remainingDigits = digits;
            while (remainingDigits-- > 0) {
                int d = (int) ((frac / divisor) % 10);
                tmp[idx++] = (byte) ('0' + d);
                divisor /= 10;
            }
            // trim trailing zeros in tmp
            int last = idx - 1;
            while (last > 0 && tmp[last] == '0') {
                last--;
            }
            for (int i = 0; i <= last; i++) {
                dest[off++] = tmp[i];
            }
        }
        return off - start;
    }

    private static void reverse(byte[] dest, int from, int to) {
        while (from < to) {
            byte tmp = dest[from];
            dest[from] = dest[to];
            dest[to] = tmp;
            from++;
            to--;
        }
    }

    /** Write {@code value} into {@code dest[off..off+width)} as zero-padded decimal. */
    static void writeFixedDigits(byte[] dest, int off, int width, int value) {
        if (value < 0) {
            throw new IllegalArgumentException("value must be non-negative");
        }
        for (int i = width - 1; i >= 0; i--) {
            dest[off + i] = (byte) ('0' + (value % 10));
            value /= 10;
        }
        if (value > 0) {
            // value did not fit in width digits — caller's bug
            throw new IllegalStateException("value exceeded fixed width");
        }
    }

    /** Write a UTC sending-time as YYYYMMDD-HH:MM:SS.sss ; returns 21 bytes. */
    static int writeUtcTimestamp(byte[] dest, int off, long epochMillis) {
        long days = Math.floorDiv(epochMillis, 86_400_000L);
        long ms = Math.floorMod(epochMillis, 86_400_000L);
        int[] ymd = civilFromDays(days);
        int year = ymd[0], month = ymd[1], day = ymd[2];
        int hour = (int) (ms / 3_600_000L);
        ms -= hour * 3_600_000L;
        int min = (int) (ms / 60_000L);
        ms -= min * 60_000L;
        int sec = (int) (ms / 1000L);
        int millis = (int) (ms - sec * 1000L);

        writeFixedDigits(dest, off + 0, 4, year);
        writeFixedDigits(dest, off + 4, 2, month);
        writeFixedDigits(dest, off + 6, 2, day);
        dest[off + 8] = '-';
        writeFixedDigits(dest, off + 9, 2, hour);
        dest[off + 11] = ':';
        writeFixedDigits(dest, off + 12, 2, min);
        dest[off + 14] = ':';
        writeFixedDigits(dest, off + 15, 2, sec);
        dest[off + 17] = '.';
        writeFixedDigits(dest, off + 18, 3, millis);
        return 21;
    }

    /** Convert a count of days since 1970-01-01 into [year, month, day]. Howard Hinnant's algorithm. */
    static int[] civilFromDays(long z) {
        z += 719468L;
        long era = (z >= 0 ? z : z - 146096L) / 146097L;
        long doe = z - era * 146097L;
        long yoe = (doe - doe / 1460L + doe / 36524L - doe / 146096L) / 365L;
        long y = yoe + era * 400L;
        long doy = doe - (365L * yoe + yoe / 4L - yoe / 100L);
        long mp = (5L * doy + 2L) / 153L;
        int d = (int) (doy - (153L * mp + 2L) / 5L + 1L);
        int m = (int) (mp + (mp < 10 ? 3 : -9));
        int yy = (int) (y + (m <= 2 ? 1 : 0));
        return new int[]{yy, m, d};
    }

    private static int trimmedLen(byte[] arr) {
        int n = arr.length;
        while (n > 0 && arr[n - 1] == 0) {
            n--;
        }
        return n;
    }

    private static boolean anyNonZero(byte[] arr) {
        for (int i = 0; i < arr.length; i++) if (arr[i] != 0) return true;
        return false;
    }

    private static byte mapExecType(byte execType) {
        return switch (execType) {
            case ExecType.NEW -> '0';
            case ExecType.PARTIAL_FILL -> 'F'; // Trade (FIX 4.4) — partial uses Trade with leaves > 0
            case ExecType.FILL -> 'F';
            case ExecType.CANCELED -> '4';
            case ExecType.REJECTED -> '8';
            case ExecType.REPLACED -> '5';
            default -> '0';
        };
    }

    private static byte mapOrdStatus(byte ordStatus) {
        // ordStatus values come from OrderStatus.ordinal() in ExecutionReportCommand.
        if (ordStatus == OrderStatus.NEW.ordinal()) return '0';
        if (ordStatus == OrderStatus.PARTIALLY_FILLED.ordinal()) return '1';
        if (ordStatus == OrderStatus.FILLED.ordinal()) return '2';
        if (ordStatus == OrderStatus.CANCELLED.ordinal()) return '4';
        if (ordStatus == OrderStatus.REJECTED.ordinal()) return '8';
        if (ordStatus == OrderStatus.REPLACED.ordinal()) return '5';
        return '0';
    }

    private static byte mapSide(byte side) {
        if (side == Side.CODE_BUY) return '1';
        if (side == Side.CODE_SELL) return '2';
        return '0';
    }
}
