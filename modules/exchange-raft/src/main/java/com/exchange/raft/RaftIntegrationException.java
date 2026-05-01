package com.exchange.raft;

/**
 * Fatal error in the Raft subsystem. Thrown when an invariant is violated
 * (a slot installation lands at a wrong index, the persistent state is
 * corrupt, etc.). Callers should treat this as non-recoverable.
 */
public final class RaftIntegrationException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public RaftIntegrationException(String msg) {
        super(msg);
    }

    public RaftIntegrationException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
