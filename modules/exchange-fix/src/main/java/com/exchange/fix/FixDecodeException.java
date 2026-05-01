package com.exchange.fix;

/**
 * Thrown by {@link FixCodec#decode(byte[], int, int, FixMessage)} when it
 * encounters a malformed FIX message (bad checksum, truncated header,
 * non-numeric BodyLength, etc).
 * <p>
 * Carries a session-reject reason code suitable for use as tag 373.
 */
public final class FixDecodeException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final int reasonCode;

    public FixDecodeException(int reasonCode, String message) {
        super(message);
        this.reasonCode = reasonCode;
    }

    public int reasonCode() {
        return reasonCode;
    }
}
