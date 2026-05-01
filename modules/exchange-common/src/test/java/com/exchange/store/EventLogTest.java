package com.exchange.store;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EventLogTest {

    private static byte[] payload(String s) {
        return s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    @Test
    void creates_and_reopens_log(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("evt.log");

        try (EventLog log = EventLog.open(file, 32)) {
            EventLogWriter writer = log.writer();
            for (int i = 0; i < 5; i++) {
                byte[] p = payload("event-" + i);
                long seq = writer.append(EventType.ORDER_ACCEPTED, 7L, 1000L + i, p, 0, p.length);
                assertThat(seq).isEqualTo(i + 1L);
            }
            assertThat(log.lastSequence()).isEqualTo(5L);
        }

        try (EventLog reopened = EventLog.open(file)) {
            assertThat(reopened.lastSequence()).isEqualTo(5L);
            assertThat(reopened.slotCount()).isEqualTo(32);
            assertThat(reopened.slotSize()).isEqualTo(256);

            EventLogReader reader = reopened.reader("replay");
            EventView view = new EventView();
            for (int i = 0; i < 5; i++) {
                assertThat(reader.poll(view)).isTrue();
                assertThat(view.sequence()).isEqualTo(i + 1L);
                assertThat(view.term()).isEqualTo(7L);
                assertThat(view.type()).isEqualTo(EventType.ORDER_ACCEPTED);
                byte[] got = new byte[view.payloadLength()];
                view.payloadBuffer().get(got);
                assertThat(new String(got, java.nio.charset.StandardCharsets.UTF_8))
                        .isEqualTo("event-" + i);
            }
            assertThat(reader.poll(view)).isFalse();
        }
    }

    @Test
    void single_writer_multiple_readers(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("multi.log");
        try (EventLog log = EventLog.open(file, 16)) {
            EventLogWriter w = log.writer();
            for (int i = 0; i < 4; i++) {
                byte[] p = payload("e" + i);
                w.append(EventType.TRADE, 1L, 100L + i, p, 0, p.length);
            }
            EventLogReader r1 = log.reader("alpha");
            EventLogReader r2 = log.reader("beta");

            EventView v1 = new EventView();
            EventView v2 = new EventView();
            for (int i = 0; i < 4; i++) {
                assertThat(r1.poll(v1)).isTrue();
                assertThat(r2.poll(v2)).isTrue();
                assertThat(v1.sequence()).isEqualTo(i + 1L);
                assertThat(v2.sequence()).isEqualTo(i + 1L);
                byte[] b1 = new byte[v1.payloadLength()];
                v1.payloadBuffer().get(b1);
                byte[] b2 = new byte[v2.payloadLength()];
                v2.payloadBuffer().get(b2);
                assertThat(b1).isEqualTo(b2);
            }
            assertThat(r1.poll(v1)).isFalse();
            assertThat(r2.poll(v2)).isFalse();
        }
    }

    @Test
    void reader_persists_cursor(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("cursor.log");
        try (EventLog log = EventLog.open(file, 16)) {
            EventLogWriter w = log.writer();
            for (int i = 0; i < 6; i++) {
                byte[] p = payload("x" + i);
                w.append(EventType.ORDER_ACCEPTED, 0L, i, p, 0, p.length);
            }

            EventLogReader r = log.reader("worker");
            EventView v = new EventView();
            for (int i = 0; i < 3; i++) {
                assertThat(r.poll(v)).isTrue();
                assertThat(v.sequence()).isEqualTo(i + 1L);
            }
            r.close();
        }

        try (EventLog reopened = EventLog.open(file)) {
            EventLogReader r = reopened.reader("worker");
            assertThat(r.cursor()).isEqualTo(2L);
            EventView v = new EventView();
            assertThat(r.poll(v)).isTrue();
            assertThat(v.sequence()).isEqualTo(4L);
            byte[] got = new byte[v.payloadLength()];
            v.payloadBuffer().get(got);
            assertThat(new String(got, java.nio.charset.StandardCharsets.UTF_8)).isEqualTo("x3");
        }
    }

    @Test
    void writer_throws_on_full(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("full.log");
        try (EventLog log = EventLog.open(file, 4)) {
            EventLogWriter w = log.writer();
            for (int i = 0; i < 4; i++) {
                byte[] p = payload("p" + i);
                w.append(EventType.ORDER_ACCEPTED, 0L, i, p, 0, p.length);
            }
            byte[] last = payload("nope");
            assertThatThrownBy(() ->
                    w.append(EventType.ORDER_ACCEPTED, 0L, 99L, last, 0, last.length))
                    .isInstanceOf(LogFullException.class);
        }
    }

    @Test
    void payload_round_trip(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("rt.log");
        try (EventLog log = EventLog.open(file, 8)) {
            EventLogWriter w = log.writer();
            byte[] expected = new byte[EventLog.MAX_PAYLOAD];
            for (int i = 0; i < expected.length; i++) {
                expected[i] = (byte) (i ^ 0x5A);
            }
            long seq = w.append(EventType.TRADE, 42L, 12345L, expected, 0, expected.length);
            assertThat(seq).isEqualTo(1L);

            EventLogReader r = log.reader("rt");
            EventView v = new EventView();
            assertThat(r.poll(v)).isTrue();
            assertThat(v.sequence()).isEqualTo(1L);
            assertThat(v.term()).isEqualTo(42L);
            assertThat(v.timestampNanos()).isEqualTo(12345L);
            assertThat(v.type()).isEqualTo(EventType.TRADE);
            assertThat(v.payloadLength()).isEqualTo(expected.length);
            byte[] got = new byte[v.payloadLength()];
            v.payloadBuffer().get(got);
            assertThat(got).isEqualTo(expected);
        }
    }

    @Test
    void payload_round_trip_via_byte_buffer(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("rt2.log");
        try (EventLog log = EventLog.open(file, 8)) {
            EventLogWriter w = log.writer();
            byte[] expected = "hello-world-12345".getBytes(java.nio.charset.StandardCharsets.UTF_8);
            ByteBuffer bb = ByteBuffer.wrap(expected);
            int posBefore = bb.position();
            w.append(EventType.BOOK_OPEN, 0L, 0L, bb);
            // append must not consume the caller's buffer position
            assertThat(bb.position()).isEqualTo(posBefore);

            EventLogReader r = log.reader("rt2");
            EventView v = new EventView();
            assertThat(r.poll(v)).isTrue();
            byte[] got = new byte[v.payloadLength()];
            v.payloadBuffer().get(got);
            assertThat(got).isEqualTo(expected);
        }
    }

    @Test
    void concurrent_writer_reader(@TempDir Path tmp) throws Exception {
        Path file = tmp.resolve("concurrent.log");
        int total = 1000;
        try (EventLog log = EventLog.open(file, total + 8)) {
            EventLogWriter w = log.writer();
            EventLogReader r = log.reader("c");

            AtomicLong observed = new AtomicLong(0);

            Thread reader = new Thread(() -> {
                EventView v = new EventView();
                long seen = 0;
                while (seen < total) {
                    if (r.poll(v)) {
                        long expectedSeq = seen + 1L;
                        if (v.sequence() != expectedSeq) {
                            throw new AssertionError("expected " + expectedSeq + " got " + v.sequence());
                        }
                        if (v.payloadLength() != 8) {
                            throw new AssertionError("bad payload length " + v.payloadLength());
                        }
                        long encoded = v.payloadBuffer().getLong();
                        if (encoded != v.sequence()) {
                            throw new AssertionError("payload mismatch for seq " + v.sequence());
                        }
                        seen++;
                        observed.set(seen);
                    } else {
                        Thread.onSpinWait();
                    }
                }
            }, "reader");
            reader.setDaemon(true);
            reader.start();

            ByteBuffer buf = ByteBuffer.allocate(8).order(java.nio.ByteOrder.LITTLE_ENDIAN);
            for (int i = 0; i < total; i++) {
                buf.clear();
                buf.putLong(i + 1L);
                buf.flip();
                w.append(EventType.TRADE, 0L, i, buf);
            }

            reader.join(5_000);
            assertThat(observed.get()).isEqualTo((long) total);
        }
    }
}
