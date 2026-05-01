package com.exchange.raft;

/**
 * Host/port pair for a Raft peer's RPC endpoint.
 */
public record NetAddress(String host, int port) {

    public NetAddress {
        if (host == null || host.isEmpty()) {
            throw new IllegalArgumentException("host must be non-empty");
        }
        if (port <= 0 || port > 65535) {
            throw new IllegalArgumentException("port out of range: " + port);
        }
    }

    @Override
    public String toString() {
        return host + ":" + port;
    }
}
