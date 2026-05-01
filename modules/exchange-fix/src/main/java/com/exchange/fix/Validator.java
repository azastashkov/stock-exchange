package com.exchange.fix;

import com.exchange.domain.OrdType;
import com.exchange.domain.RejectReason;
import com.exchange.domain.Side;
import com.exchange.domain.Symbols;
import com.exchange.domain.TimeInForce;

import java.nio.charset.StandardCharsets;

/**
 * Inbound FIX validation for NEW/CANCEL/CANCEL_REPLACE messages. Does not
 * mutate the message; returns a {@link RejectReason} byte (0 if valid).
 * <p>
 * Allocation discipline: a thread-local 16-byte scratch is used to copy out
 * Symbol values for the {@link Symbols#idOf(String)} lookup. This costs one
 * String per Symbol resolution; acceptable for the gateway hot path which
 * already pays for FIX parsing.
 */
public final class Validator {

    /** Special: validation succeeded. */
    public static final byte OK = 0;

    private final Symbols symbols;
    private final byte[] symScratch = new byte[32];

    public Validator(Symbols symbols) {
        this.symbols = symbols;
    }

    /** Validate a NewOrderSingle (35=D). Returns {@link #OK} or a {@link RejectReason}. */
    public byte validateNewOrder(FixMessage m) {
        if (!m.has(FixField.CL_ORD_ID)) return RejectReason.UNKNOWN_ORDER;
        if (!m.has(FixField.SYMBOL)) return RejectReason.UNKNOWN_SYMBOL;
        if (!m.has(FixField.SIDE)) return RejectReason.INVALID_QTY;
        if (!m.has(FixField.ORDER_QTY)) return RejectReason.INVALID_QTY;
        if (!m.has(FixField.ORD_TYPE)) return RejectReason.INVALID_PRICE;

        // Symbol whitelist
        int symId = resolveSymbol(m);
        if (symId == 0) return RejectReason.UNKNOWN_SYMBOL;

        // Side
        byte side = m.getChar(FixField.SIDE);
        if (side != '1' && side != '2') return RejectReason.INVALID_QTY;

        // OrdType
        byte ord = m.getChar(FixField.ORD_TYPE);
        if (ord != '1' && ord != '2') return RejectReason.INVALID_PRICE;

        long qty = m.getLong(FixField.ORDER_QTY);
        if (qty <= 0) return RejectReason.INVALID_QTY;

        if (ord == '2') {
            // LIMIT — price required and > 0
            if (!m.has(FixField.PRICE)) return RejectReason.INVALID_PRICE;
            long priceFp = readPriceFixedPoint(m);
            if (priceFp <= 0L) return RejectReason.INVALID_PRICE;
        } else {
            // MARKET — price absent or 0
            if (m.has(FixField.PRICE)) {
                long priceFp = readPriceFixedPoint(m);
                if (priceFp != 0L) return RejectReason.INVALID_PRICE;
            }
        }

        if (m.has(FixField.TIME_IN_FORCE)) {
            byte tif = m.getChar(FixField.TIME_IN_FORCE);
            if (tif != '0' && tif != '3') return RejectReason.INVALID_QTY;
        }

        return OK;
    }

    /** Validate an OrderCancelRequest (35=F). */
    public byte validateCancel(FixMessage m) {
        if (!m.has(FixField.CL_ORD_ID)) return RejectReason.UNKNOWN_ORDER;
        if (!m.has(FixField.ORIG_CL_ORD_ID)) return RejectReason.UNKNOWN_ORDER;
        if (!m.has(FixField.SYMBOL)) return RejectReason.UNKNOWN_SYMBOL;
        int symId = resolveSymbol(m);
        if (symId == 0) return RejectReason.UNKNOWN_SYMBOL;
        return OK;
    }

    /** Validate an OrderCancelReplaceRequest (35=G). */
    public byte validateCancelReplace(FixMessage m) {
        if (!m.has(FixField.CL_ORD_ID)) return RejectReason.UNKNOWN_ORDER;
        if (!m.has(FixField.ORIG_CL_ORD_ID)) return RejectReason.UNKNOWN_ORDER;
        if (!m.has(FixField.SYMBOL)) return RejectReason.UNKNOWN_SYMBOL;
        if (!m.has(FixField.SIDE)) return RejectReason.INVALID_QTY;
        if (!m.has(FixField.ORDER_QTY)) return RejectReason.INVALID_QTY;
        int symId = resolveSymbol(m);
        if (symId == 0) return RejectReason.UNKNOWN_SYMBOL;
        long qty = m.getLong(FixField.ORDER_QTY);
        if (qty <= 0) return RejectReason.INVALID_QTY;
        if (m.has(FixField.PRICE)) {
            long priceFp = readPriceFixedPoint(m);
            if (priceFp < 0L) return RejectReason.INVALID_PRICE;
        }
        return OK;
    }

    /**
     * Look up tag 55 (Symbol) in {@code symbols}. Returns the symbol id, or 0
     * if unknown / out-of-range.
     */
    public int resolveSymbol(FixMessage m) {
        int len = m.valueLength(FixField.SYMBOL);
        if (len <= 0 || len > symScratch.length) return 0;
        m.getBytes(FixField.SYMBOL, symScratch, 0);
        // The getBytes() API zero-fills the rest of dest; but since len <= scratch.length
        // we control the slice we hand to Symbols.
        String name = new String(symScratch, 0, len, StandardCharsets.US_ASCII);
        try {
            return symbols.idOf(name);
        } catch (IllegalArgumentException e) {
            return 0;
        }
    }

    /**
     * Parse tag 44 (Price) into a long fixed-point (×10000) representation.
     * Accepts decimal forms with up to 4 fractional digits.
     */
    public long readPriceFixedPoint(FixMessage m) {
        int off = m.valueOffset(FixField.PRICE);
        int len = m.valueLength(FixField.PRICE);
        if (off < 0 || len <= 0) return 0L;
        return parseDecimalToFixedPoint4(m.buffer, off, len);
    }

    /**
     * Parse a decimal number with up to 4 fractional digits into a long
     * scaled by 10000. "12.5" → 125000. Any extra digits beyond the 4th
     * are truncated.
     */
    public static long parseDecimalToFixedPoint4(byte[] buf, int off, int len) {
        boolean neg = false;
        int i = 0;
        if (len > 0 && buf[off] == '-') {
            neg = true;
            i = 1;
        } else if (len > 0 && buf[off] == '+') {
            i = 1;
        }
        long whole = 0L;
        int dot = -1;
        for (; i < len; i++) {
            byte b = buf[off + i];
            if (b == '.') {
                dot = i;
                i++;
                break;
            }
            if (b < '0' || b > '9') {
                throw new FixDecodeException(5, "Non-decimal in price");
            }
            whole = whole * 10L + (b - '0');
        }
        long frac = 0L;
        int fracDigits = 0;
        if (dot >= 0) {
            for (; i < len && fracDigits < 4; i++) {
                byte b = buf[off + i];
                if (b < '0' || b > '9') {
                    throw new FixDecodeException(5, "Non-decimal in price fraction");
                }
                frac = frac * 10L + (b - '0');
                fracDigits++;
            }
        }
        for (int k = fracDigits; k < 4; k++) {
            frac *= 10L;
        }
        long v = whole * 10000L + frac;
        return neg ? -v : v;
    }
}
