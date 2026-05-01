package com.exchange.ipc;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

class MmapRingTest {

    @Test
    void offer_then_poll_round_trip(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("ring.dat");
        try (MmapRing ring = MmapRing.create(file, 64, 8)) {
            RingProducer p = ring.producer();
            RingConsumer c = ring.consumer();

            byte[] payload = "abc".getBytes();
            assertThat(p.offer(payload, 0, payload.length)).isTrue();

            MutableSlice slice = new MutableSlice();
            assertThat(c.poll(slice)).isTrue();
            assertThat(slice.length).isEqualTo(3);
            byte[] got = new byte[slice.length];
            slice.buffer.get(got);
            assertThat(got).isEqualTo(payload);

            assertThat(c.poll(slice)).isFalse();
        }
    }

    @Test
    void full_returns_false(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("ring-full.dat");
        try (MmapRing ring = MmapRing.create(file, 32, 4)) {
            RingProducer p = ring.producer();
            byte[] payload = new byte[8];
            for (int i = 0; i < 4; i++) {
                payload[0] = (byte) i;
                assertThat(p.offer(payload, 0, payload.length)).isTrue();
            }
            assertThat(p.offer(payload, 0, payload.length)).isFalse();
        }
    }

    @Test
    void wraparound(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("ring-wrap.dat");
        try (MmapRing ring = MmapRing.create(file, 16, 4)) {
            RingProducer p = ring.producer();
            RingConsumer c = ring.consumer();
            MutableSlice slice = new MutableSlice();

            // Pattern: offer 2, poll 1, offer 2, poll 1, offer 2, poll all.
            // 6 offers total, ring depth 4, with 4 polls in between.
            int produced = 0;
            int consumed = 0;
            for (int round = 0; round < 3; round++) {
                for (int j = 0; j < 2; j++) {
                    byte[] payload = new byte[]{(byte) produced};
                    assertThat(p.offer(payload, 0, 1)).as("offer %d", produced).isTrue();
                    produced++;
                }
                if (round < 2) {
                    assertThat(c.poll(slice)).isTrue();
                    assertThat(slice.length).isEqualTo(1);
                    assertThat(slice.buffer.get()).isEqualTo((byte) consumed);
                    consumed++;
                }
            }
            // Drain remaining 4
            while (consumed < 6) {
                assertThat(c.poll(slice)).isTrue();
                assertThat(slice.length).isEqualTo(1);
                assertThat(slice.buffer.get()).isEqualTo((byte) consumed);
                consumed++;
            }
            assertThat(c.poll(slice)).isFalse();
            assertThat(consumed).isEqualTo(6);
        }
    }

    @Test
    void offer_via_byte_buffer(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("ring-bb.dat");
        try (MmapRing ring = MmapRing.create(file, 32, 4)) {
            RingProducer p = ring.producer();
            RingConsumer c = ring.consumer();

            ByteBuffer payload = ByteBuffer.wrap("hello".getBytes());
            int posBefore = payload.position();
            assertThat(p.offer(payload)).isTrue();
            assertThat(payload.position()).isEqualTo(posBefore);

            MutableSlice slice = new MutableSlice();
            assertThat(c.poll(slice)).isTrue();
            byte[] got = new byte[slice.length];
            slice.buffer.get(got);
            assertThat(new String(got)).isEqualTo("hello");
        }
    }

    @Test
    void concurrent_producer_consumer(@TempDir Path tmp) throws Exception {
        Path file = tmp.resolve("ring-cc.dat");
        int total = 10_000;
        try (MmapRing ring = MmapRing.create(file, 16, 1024)) {
            RingProducer p = ring.producer();
            RingConsumer c = ring.consumer();

            AtomicLong consumed = new AtomicLong(0);

            Thread consumer = new Thread(() -> {
                MutableSlice slice = new MutableSlice();
                long expected = 0;
                while (expected < total) {
                    if (c.poll(slice)) {
                        long got = slice.buffer.getLong();
                        if (got != expected) {
                            throw new AssertionError("expected " + expected + " got " + got);
                        }
                        expected++;
                        consumed.set(expected);
                    } else {
                        Thread.onSpinWait();
                    }
                }
            }, "consumer");
            consumer.setDaemon(true);
            consumer.start();

            ByteBuffer buf = ByteBuffer.allocate(8).order(java.nio.ByteOrder.LITTLE_ENDIAN);
            for (long i = 0; i < total; i++) {
                buf.clear();
                buf.putLong(i);
                buf.flip();
                while (!p.offer(buf)) {
                    Thread.onSpinWait();
                }
            }

            consumer.join(10_000);
            assertThat(consumed.get()).isEqualTo((long) total);
        }
    }
}
