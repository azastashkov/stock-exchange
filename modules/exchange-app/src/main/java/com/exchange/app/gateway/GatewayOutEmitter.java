package com.exchange.app.gateway;

import com.exchange.ipc.RingProducer;

import java.nio.ByteBuffer;

/**
 * Thin wrapper around the gateway-out {@link RingProducer} that serializes
 * concurrent writers (the Reporter and the various reject-emit paths in
 * OrderManager/Risk).
 * <p>
 * The base ring is single-producer; we add a {@code synchronized} block
 * here so that multiple loops can publish ExecutionReports without racing
 * on the producer's local sequence counter.
 */
public final class GatewayOutEmitter {

    private final RingProducer producer;

    public GatewayOutEmitter(RingProducer producer) {
        this.producer = producer;
    }

    /**
     * Spin until the slot is offered. Mirrors the pattern used by services
     * directly on the ring; centralising it here gives one well-known
     * contention point.
     */
    public void offerSpin(ByteBuffer payload) {
        synchronized (this) {
            while (!producer.offer(payload)) {
                Thread.onSpinWait();
            }
        }
    }
}
