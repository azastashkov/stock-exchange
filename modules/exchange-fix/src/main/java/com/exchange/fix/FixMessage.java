package com.exchange.fix;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

import java.nio.charset.StandardCharsets;

/**
 * Mutable, allocation-friendly representation of a single FIX message.
 * <p>
 * Internally points at a backing byte array that holds the raw wire bytes
 * (always rooted at offset 0) plus parallel maps from tag → byte offset of
 * the value within {@link #buffer} and tag → length of the value.
 * <p>
 * Reused across decode calls via {@link #clear()}; the caller owns
 * lifecycle. Not thread-safe.
 */
public final class FixMessage {

    public static final int DEFAULT_CAPACITY = 4096;

    /** Backing bytes; always populated from offset 0. */
    public byte[] buffer;
    /** Bytes of {@link #buffer} that are populated. */
    public int length;

    /** tag → offset of value in {@link #buffer}. */
    public final Int2IntOpenHashMap fieldOffsets;
    /** tag → length of value in {@link #buffer}. */
    public final Int2IntOpenHashMap fieldLengths;

    public FixMessage() {
        this(DEFAULT_CAPACITY);
    }

    public FixMessage(int capacity) {
        this.buffer = new byte[capacity];
        this.length = 0;
        this.fieldOffsets = new Int2IntOpenHashMap(64);
        this.fieldOffsets.defaultReturnValue(-1);
        this.fieldLengths = new Int2IntOpenHashMap(64);
        this.fieldLengths.defaultReturnValue(-1);
    }

    /** Reset state for reuse; preserves the backing buffer. */
    public void clear() {
        length = 0;
        fieldOffsets.clear();
        fieldLengths.clear();
    }

    /**
     * Replace contents with {@code length} bytes copied from {@code data}
     * starting at {@code offset}. The bytes are copied as-is; the caller is
     * responsible for indexing tags via {@code fieldOffsets} after a decode.
     */
    public void setRaw(byte[] data, int offset, int length) {
        ensureCapacity(length);
        System.arraycopy(data, offset, this.buffer, 0, length);
        this.length = length;
    }

    /** Ensure the backing buffer can hold at least {@code n} bytes. */
    public void ensureCapacity(int n) {
        if (buffer.length < n) {
            byte[] grown = new byte[Math.max(n, buffer.length * 2)];
            System.arraycopy(buffer, 0, grown, 0, length);
            buffer = grown;
        }
    }

    public boolean has(int tag) {
        return fieldOffsets.get(tag) >= 0;
    }

    /**
     * Decode the value at {@code tag} as a non-negative decimal int.
     * Throws if the tag is missing or non-numeric.
     */
    public int getInt(int tag) {
        int off = fieldOffsets.get(tag);
        int len = fieldLengths.get(tag);
        if (off < 0) {
            throw new FixDecodeException(1, "Missing required tag " + tag);
        }
        return parseIntAscii(buffer, off, len);
    }

    /**
     * Decode the value at {@code tag} as a long. Supports values prefixed
     * with '-' for negative numbers.
     */
    public long getLong(int tag) {
        int off = fieldOffsets.get(tag);
        int len = fieldLengths.get(tag);
        if (off < 0) {
            throw new FixDecodeException(1, "Missing required tag " + tag);
        }
        return parseLongAscii(buffer, off, len);
    }

    /** Decode the value at {@code tag} as a String (allocates). */
    public String getString(int tag) {
        int off = fieldOffsets.get(tag);
        int len = fieldLengths.get(tag);
        if (off < 0) {
            throw new FixDecodeException(1, "Missing required tag " + tag);
        }
        return new String(buffer, off, len, StandardCharsets.US_ASCII);
    }

    /** Decode the value at {@code tag} as a single ASCII char. */
    public byte getChar(int tag) {
        int off = fieldOffsets.get(tag);
        int len = fieldLengths.get(tag);
        if (off < 0) {
            throw new FixDecodeException(1, "Missing required tag " + tag);
        }
        if (len != 1) {
            throw new FixDecodeException(5, "Tag " + tag + " is not a single char");
        }
        return buffer[off];
    }

    /**
     * Copy the value bytes for {@code tag} into {@code dest} starting at
     * {@code destOff}. The destination is zero-filled in any region beyond
     * the value's length up to {@code dest.length - destOff}.
     */
    public void getBytes(int tag, byte[] dest, int destOff) {
        int off = fieldOffsets.get(tag);
        int len = fieldLengths.get(tag);
        if (off < 0) {
            throw new FixDecodeException(1, "Missing required tag " + tag);
        }
        int copy = Math.min(len, dest.length - destOff);
        System.arraycopy(buffer, off, dest, destOff, copy);
        for (int i = destOff + copy; i < dest.length; i++) {
            dest[i] = 0;
        }
    }

    /** Convenience for tag 35; allocates. */
    public String msgType() {
        return getString(FixField.MSG_TYPE);
    }

    /** Length of the value at {@code tag}, or -1 if absent. */
    public int valueLength(int tag) {
        return fieldLengths.get(tag);
    }

    /** Offset of the value at {@code tag} within {@link #buffer}, or -1. */
    public int valueOffset(int tag) {
        return fieldOffsets.get(tag);
    }

    static int parseIntAscii(byte[] buf, int off, int len) {
        if (len <= 0) {
            throw new FixDecodeException(5, "Empty integer field");
        }
        int i = 0;
        boolean neg = false;
        if (buf[off] == '-') {
            neg = true;
            i = 1;
        } else if (buf[off] == '+') {
            i = 1;
        }
        int v = 0;
        for (; i < len; i++) {
            byte b = buf[off + i];
            if (b < '0' || b > '9') {
                throw new FixDecodeException(5, "Non-numeric character in integer at offset " + (off + i));
            }
            v = v * 10 + (b - '0');
        }
        return neg ? -v : v;
    }

    static long parseLongAscii(byte[] buf, int off, int len) {
        if (len <= 0) {
            throw new FixDecodeException(5, "Empty long field");
        }
        int i = 0;
        boolean neg = false;
        if (buf[off] == '-') {
            neg = true;
            i = 1;
        } else if (buf[off] == '+') {
            i = 1;
        }
        long v = 0L;
        for (; i < len; i++) {
            byte b = buf[off + i];
            if (b < '0' || b > '9') {
                throw new FixDecodeException(5, "Non-numeric character in long at offset " + (off + i));
            }
            v = v * 10 + (b - '0');
        }
        return neg ? -v : v;
    }
}
