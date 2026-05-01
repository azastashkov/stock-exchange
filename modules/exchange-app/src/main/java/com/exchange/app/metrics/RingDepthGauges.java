package com.exchange.app.metrics;

import com.exchange.ipc.MmapRing;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;

/**
 * Registers a {@code exchange.ring.depth} gauge per named ring.
 * <p>
 * Depth is computed as {@code producerCursor - consumerCursor} via the
 * acquire-load accessors on {@link MmapRing}; reads are cheap and bounded.
 */
public final class RingDepthGauges {

    private RingDepthGauges() {
        // utility class
    }

    /** Register a gauge tagged with {@code name} reading the depth of {@code ring}. */
    public static void register(MeterRegistry mr, String name, MmapRing ring) {
        if (mr == null || ring == null) return;
        Gauge.builder("exchange.ring.depth", ring,
                        r -> (double) (r.producerCursor() - r.consumerCursor()))
                .tags(java.util.List.of(Tag.of("ring", name)))
                .register(mr);
    }
}
