package com.exchange.app.gateway;

import com.exchange.ipc.RingConsumer;
import com.exchange.ipc.RingProducer;

/**
 * Placeholder for the FIX gateway (Phase 4). Owns the inbound and gateway-out
 * rings; for now just exposes their producer/consumer ends so tests and the
 * load harness can drive the system without the FIX wire layer.
 */
public final class GatewayStub {

    private final RingProducer inboundProducer;
    private final RingConsumer gatewayOutConsumer;

    public GatewayStub(RingProducer inboundProducer, RingConsumer gatewayOutConsumer) {
        this.inboundProducer = inboundProducer;
        this.gatewayOutConsumer = gatewayOutConsumer;
    }

    /** Producer onto the inbound ring (gateway → OM). */
    public RingProducer inboundProducer() {
        return inboundProducer;
    }

    /** Consumer from the gateway-out ring (Reporter/Risk/OM → gateway). */
    public RingConsumer gatewayOutConsumer() {
        return gatewayOutConsumer;
    }
}
