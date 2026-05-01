package com.exchange.domain;

import java.nio.ByteBuffer;

/**
 * Helpers for the wire encoding of a 16-byte fixed-width client order id.
 * Strings are ASCII (≤16 chars) zero-padded on the right. The 16-byte
 * representation is the canonical form everywhere except the public API
 * boundary (FIX gateway).
 */
public final class ClOrdId {

    public static final int LENGTH = 16;

    private ClOrdId() {
        // utility class
    }

    /**
     * Write {@code s} into {@code out} at {@code offset} as 16 ASCII bytes,
     * zero-padded. Throws if longer than 16 chars or contains non-ASCII.
     * Does not mutate position/limit of {@code out}.
     */
    public static void writeAscii(ByteBuffer out, int offset, String s) {
        if (s == null) {
            for (int i = 0; i < LENGTH; i++) {
                out.put(offset + i, (byte) 0);
            }
            return;
        }
        int n = s.length();
        if (n > LENGTH) {
            throw new IllegalArgumentException("ClOrdId longer than " + LENGTH + ": " + s);
        }
        for (int i = 0; i < n; i++) {
            char c = s.charAt(i);
            if (c == 0 || c > 0x7F) {
                throw new IllegalArgumentException("ClOrdId must be ASCII without NULs");
            }
            out.put(offset + i, (byte) c);
        }
        for (int i = n; i < LENGTH; i++) {
            out.put(offset + i, (byte) 0);
        }
    }

    /**
     * Read 16 ASCII bytes from {@code in} at {@code offset}, stripping
     * trailing 0x00 padding. Allocates exactly one String. Caller paths
     * that need allocation-free decoding should use the byte[] form.
     */
    public static String readAscii(ByteBuffer in, int offset) {
        int len = 0;
        for (; len < LENGTH; len++) {
            byte b = in.get(offset + len);
            if (b == 0) break;
        }
        if (len == 0) return "";
        byte[] tmp = new byte[len];
        for (int i = 0; i < len; i++) {
            tmp[i] = in.get(offset + i);
        }
        return new String(tmp, java.nio.charset.StandardCharsets.US_ASCII);
    }

    /**
     * Encode {@code s} into a caller-provided 16-byte array.
     */
    public static void writeAscii(byte[] out, String s) {
        if (out.length != LENGTH) {
            throw new IllegalArgumentException("Destination must be 16 bytes");
        }
        if (s == null) {
            for (int i = 0; i < LENGTH; i++) out[i] = 0;
            return;
        }
        int n = s.length();
        if (n > LENGTH) {
            throw new IllegalArgumentException("ClOrdId longer than " + LENGTH + ": " + s);
        }
        for (int i = 0; i < n; i++) {
            char c = s.charAt(i);
            if (c == 0 || c > 0x7F) {
                throw new IllegalArgumentException("ClOrdId must be ASCII without NULs");
            }
            out[i] = (byte) c;
        }
        for (int i = n; i < LENGTH; i++) out[i] = 0;
    }

    /** Read into a caller-provided destination buffer (also 16 bytes). */
    public static void readBytes(ByteBuffer in, int offset, byte[] dst) {
        if (dst.length != LENGTH) {
            throw new IllegalArgumentException("Destination must be 16 bytes");
        }
        for (int i = 0; i < LENGTH; i++) {
            dst[i] = in.get(offset + i);
        }
    }

    /**
     * Allocation-free 64-bit hash of a 16-byte ClOrdId. Used as a key
     * component for duplicate-detection maps without holding a String.
     * Mixes with FNV-style folding so all 16 bytes contribute.
     */
    public static long hash64(byte[] clOrdId16) {
        if (clOrdId16.length != LENGTH) {
            throw new IllegalArgumentException("ClOrdId must be 16 bytes");
        }
        long h = 0xcbf29ce484222325L;
        for (int i = 0; i < LENGTH; i++) {
            h ^= clOrdId16[i] & 0xFFL;
            h *= 0x100000001b3L;
        }
        return h;
    }

    public static boolean equals(byte[] a, byte[] b) {
        if (a.length != LENGTH || b.length != LENGTH) return false;
        for (int i = 0; i < LENGTH; i++) {
            if (a[i] != b[i]) return false;
        }
        return true;
    }
}
