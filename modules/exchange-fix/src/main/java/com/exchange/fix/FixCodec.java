package com.exchange.fix;

/**
 * Allocation-free FIX 4.4 frame decoder. Operates on a caller-supplied byte
 * buffer slice and populates a reusable {@link FixMessage}.
 * <p>
 * The encoding helpers all live on {@link FixEncoder} to keep this class
 * small and focused.
 */
public final class FixCodec {

    public static final byte SOH = 0x01;
    public static final byte EQUALS = '=';

    /** Reason code reported when the wire bytes are syntactically invalid. */
    public static final int REJECT_REASON_OTHER = 99;
    public static final int REJECT_REASON_INVALID_TAG = 0;
    public static final int REJECT_REASON_VALUE_OUT_OF_RANGE = 5;
    public static final int REJECT_REASON_BAD_CHECKSUM = 6;

    /** "FIX.4.4" as bytes — used for BeginString validation. */
    private static final byte[] BEGIN_STRING_VALUE = {'F', 'I', 'X', '.', '4', '.', '4'};

    private FixCodec() {
        // utility class
    }

    /**
     * Attempt to decode one FIX message starting at {@code offset} within
     * {@code buffer}. {@code limit} is exclusive.
     * <p>
     * Returns:
     * <ul>
     *   <li>{@code 0} if more bytes are needed to complete the frame;</li>
     *   <li>{@code n > 0} the number of bytes consumed (length of the frame).</li>
     * </ul>
     * Throws {@link FixDecodeException} if the bytes are syntactically
     * malformed (bad checksum, garbled BodyLength, missing SOH, etc).
     */
    public static int decode(byte[] buffer, int offset, int limit, FixMessage into) {
        int available = limit - offset;
        if (available < 14) {
            // need at least "8=FIX.4.4|9=N|10=NNN|" — far too short to fit anything
            return 0;
        }

        // Tag 8: BeginString — must be the first field
        if (buffer[offset] != '8' || buffer[offset + 1] != '=') {
            throw new FixDecodeException(REJECT_REASON_OTHER, "Frame does not start with BeginString");
        }
        int beginValStart = offset + 2;
        int firstSoh = indexOfSoh(buffer, beginValStart, limit);
        if (firstSoh < 0) {
            return 0; // wait for more
        }
        int beginValLen = firstSoh - beginValStart;
        if (beginValLen != BEGIN_STRING_VALUE.length) {
            throw new FixDecodeException(REJECT_REASON_VALUE_OUT_OF_RANGE,
                    "BeginString must be FIX.4.4");
        }
        for (int i = 0; i < beginValLen; i++) {
            if (buffer[beginValStart + i] != BEGIN_STRING_VALUE[i]) {
                throw new FixDecodeException(REJECT_REASON_VALUE_OUT_OF_RANGE,
                        "BeginString must be FIX.4.4");
            }
        }

        // Tag 9: BodyLength
        int bodyLenTagStart = firstSoh + 1;
        if (bodyLenTagStart + 2 > limit) return 0;
        if (buffer[bodyLenTagStart] != '9' || buffer[bodyLenTagStart + 1] != '=') {
            throw new FixDecodeException(REJECT_REASON_OTHER, "BodyLength tag missing");
        }
        int bodyLenValStart = bodyLenTagStart + 2;
        int secondSoh = indexOfSoh(buffer, bodyLenValStart, limit);
        if (secondSoh < 0) {
            return 0;
        }
        int bodyLength = FixMessage.parseIntAscii(buffer, bodyLenValStart, secondSoh - bodyLenValStart);
        if (bodyLength <= 0 || bodyLength > 1 << 22) {
            throw new FixDecodeException(REJECT_REASON_VALUE_OUT_OF_RANGE,
                    "BodyLength out of range: " + bodyLength);
        }
        // The body starts AFTER the SOH following 9=N
        int bodyStart = secondSoh + 1;
        // Total frame length = bodyStart - offset + bodyLength + 7 (the trailing "10=NNN|" is 7 chars)
        int frameLength = (bodyStart - offset) + bodyLength + 7;
        if (limit - offset < frameLength) {
            return 0; // wait
        }
        int checksumTagStart = bodyStart + bodyLength;
        if (buffer[checksumTagStart] != '1'
                || buffer[checksumTagStart + 1] != '0'
                || buffer[checksumTagStart + 2] != '=') {
            throw new FixDecodeException(REJECT_REASON_OTHER, "CheckSum tag missing where expected");
        }
        if (buffer[checksumTagStart + 6] != SOH) {
            throw new FixDecodeException(REJECT_REASON_OTHER, "CheckSum not terminated by SOH");
        }
        // Validate checksum
        int sum = 0;
        for (int i = offset; i < checksumTagStart; i++) {
            sum += (buffer[i] & 0xFF);
        }
        sum &= 0xFF;
        int declared = (buffer[checksumTagStart + 3] - '0') * 100
                + (buffer[checksumTagStart + 4] - '0') * 10
                + (buffer[checksumTagStart + 5] - '0');
        if (sum != declared) {
            throw new FixDecodeException(REJECT_REASON_BAD_CHECKSUM,
                    "Checksum mismatch: computed " + sum + " declared " + declared);
        }

        // Populate the FixMessage
        into.clear();
        into.setRaw(buffer, offset, frameLength);

        // BeginString and BodyLength fields recorded relative to into.buffer (rooted at 0)
        into.fieldOffsets.put(FixField.BEGIN_STRING, 2);
        into.fieldLengths.put(FixField.BEGIN_STRING, beginValLen);
        into.fieldOffsets.put(FixField.BODY_LENGTH, (bodyLenValStart - offset));
        into.fieldLengths.put(FixField.BODY_LENGTH, secondSoh - bodyLenValStart);

        // Walk body
        int p = bodyStart - offset; // offset within the copied buffer
        int bodyEnd = checksumTagStart - offset;
        while (p < bodyEnd) {
            int eq = -1;
            for (int j = p; j < bodyEnd; j++) {
                if (into.buffer[j] == EQUALS) { eq = j; break; }
            }
            if (eq < 0) {
                throw new FixDecodeException(REJECT_REASON_OTHER, "Field has no '=' at offset " + p);
            }
            int tag = FixMessage.parseIntAscii(into.buffer, p, eq - p);
            int valStart = eq + 1;
            int sohAt = -1;
            for (int j = valStart; j < bodyEnd; j++) {
                if (into.buffer[j] == SOH) { sohAt = j; break; }
            }
            if (sohAt < 0) {
                throw new FixDecodeException(REJECT_REASON_OTHER, "Field has no SOH terminator at " + p);
            }
            into.fieldOffsets.put(tag, valStart);
            into.fieldLengths.put(tag, sohAt - valStart);
            p = sohAt + 1;
        }

        // Record the checksum field as well
        int checksumValStart = (checksumTagStart - offset) + 3;
        into.fieldOffsets.put(FixField.CHECKSUM, checksumValStart);
        into.fieldLengths.put(FixField.CHECKSUM, 3);

        return frameLength;
    }

    private static int indexOfSoh(byte[] buf, int from, int to) {
        for (int i = from; i < to; i++) {
            if (buf[i] == SOH) return i;
        }
        return -1;
    }

    /** Compute the FIX checksum (modulo-256 byte sum) over [from, to). */
    public static int checksum(byte[] buf, int from, int to) {
        int sum = 0;
        for (int i = from; i < to; i++) {
            sum += (buf[i] & 0xFF);
        }
        return sum & 0xFF;
    }
}
