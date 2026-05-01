package com.exchange.store;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EventLogRaftAdditionsTest {

    private static byte[] payload(String s) {
        return s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    @Test
    void readSlotRaw_returns_full_slot_bytes(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("raw.log");
        try (EventLog log = EventLog.open(file, 8)) {
            EventLogWriter w = log.writer();
            byte[] p = payload("hello");
            w.append(EventType.ORDER_ACCEPTED, 7L, 1234L, p, 0, p.length);

            byte[] raw = log.readSlotRaw(0);
            assertThat(raw.length).isEqualTo(EventLog.SLOT_SIZE_BYTES);
            // status @ offset 0 is COMMITTED (=2)
            assertThat(raw[0]).isEqualTo((byte) EventLog.STATUS_COMMITTED);
            // length @ offset 4 = 5
            assertThat(raw[4]).isEqualTo((byte) p.length);
            // term @ offset 16 (LE)
            long term = bytesToLongLE(raw, 16);
            assertThat(term).isEqualTo(7L);
        }
    }

    @Test
    void readSlotTermAt_returns_minus_one_for_empty(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("term.log");
        try (EventLog log = EventLog.open(file, 4)) {
            assertThat(log.readSlotTermAt(0)).isEqualTo(-1L);
            EventLogWriter w = log.writer();
            byte[] p = payload("x");
            w.append(EventType.TRADE, 99L, 1L, p, 0, p.length);
            assertThat(log.readSlotTermAt(0)).isEqualTo(99L);
            assertThat(log.readSlotTermAt(1)).isEqualTo(-1L);
            assertThat(log.readSlotTermAt(99L)).isEqualTo(-1L);
        }
    }

    @Test
    void installSlot_copies_bytes_and_advances_writer(@TempDir Path tmp) throws IOException {
        Path leaderFile = tmp.resolve("leader.log");
        Path followerFile = tmp.resolve("follower.log");
        try (EventLog leader = EventLog.open(leaderFile, 8);
             EventLog follower = EventLog.open(followerFile, 8)) {
            EventLogWriter lw = leader.writer();
            for (int i = 0; i < 3; i++) {
                byte[] p = payload("e" + i);
                lw.append(EventType.TRADE, 5L, 100L + i, p, 0, p.length);
            }
            // Replicate raw slots into follower.
            for (int i = 0; i < 3; i++) {
                byte[] raw = leader.readSlotRaw(i);
                follower.installSlot(i, raw);
                // Follower slot must be WRITING until commit.
                assertThat(follower.slotStatus(i)).isEqualTo(EventLog.STATUS_WRITING);
            }
            assertThat(follower.lastSlotIndex()).isEqualTo(3L);
            assertThat(follower.lastSequence()).isEqualTo(3L);
            // Commit and verify reader can poll all.
            for (int i = 0; i < 3; i++) {
                follower.commit(i);
            }
            EventLogReader r = follower.reader("c");
            EventView v = new EventView();
            for (int i = 0; i < 3; i++) {
                assertThat(r.poll(v)).isTrue();
                byte[] got = new byte[v.payloadLength()];
                v.payloadBuffer().get(got);
                assertThat(new String(got, java.nio.charset.StandardCharsets.UTF_8)).isEqualTo("e" + i);
                assertThat(v.term()).isEqualTo(5L);
            }
            assertThat(r.poll(v)).isFalse();
        }
    }

    @Test
    void installSlot_rejects_wrong_size(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("bad.log");
        try (EventLog log = EventLog.open(file, 4)) {
            assertThatThrownBy(() -> log.installSlot(0, new byte[10]))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> log.installSlot(99L, new byte[EventLog.SLOT_SIZE_BYTES]))
                    .isInstanceOf(IndexOutOfBoundsException.class);
        }
    }

    @Test
    void truncateAfter_zeros_tail_and_resets_pointers(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("trunc.log");
        try (EventLog log = EventLog.open(file, 8)) {
            EventLogWriter w = log.writer();
            for (int i = 0; i < 5; i++) {
                byte[] p = payload("a" + i);
                w.append(EventType.TRADE, 1L, i, p, 0, p.length);
            }
            assertThat(log.lastSlotIndex()).isEqualTo(5L);

            log.truncateAfter(2L);

            assertThat(log.lastSlotIndex()).isEqualTo(3L);
            // Sequence at slot 2 should be the remaining last sequence (=3).
            assertThat(log.lastSequence()).isEqualTo(3L);
            // Slots 3, 4 should be EMPTY now.
            assertThat(log.slotStatus(3L)).isEqualTo(EventLog.STATUS_EMPTY);
            assertThat(log.slotStatus(4L)).isEqualTo(EventLog.STATUS_EMPTY);
            // Slots 0..2 still committed.
            assertThat(log.slotStatus(0L)).isEqualTo(EventLog.STATUS_COMMITTED);
            assertThat(log.slotStatus(2L)).isEqualTo(EventLog.STATUS_COMMITTED);
        }
    }

    @Test
    void truncateAfter_neg_one_clears_log(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("trunc2.log");
        try (EventLog log = EventLog.open(file, 4)) {
            EventLogWriter w = log.writer();
            byte[] p = payload("p0");
            w.append(EventType.TRADE, 1L, 0L, p, 0, p.length);
            log.truncateAfter(-1L);
            assertThat(log.lastSlotIndex()).isEqualTo(0L);
            assertThat(log.lastSequence()).isEqualTo(0L);
            assertThat(log.slotStatus(0L)).isEqualTo(EventLog.STATUS_EMPTY);
        }
    }

    @Test
    void appendUncommitted_holds_back_status_until_commit(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("uc.log");
        try (EventLog log = EventLog.open(file, 4)) {
            EventLogWriter w = log.writer();
            byte[] p = payload("x");
            java.nio.ByteBuffer bb = java.nio.ByteBuffer.wrap(p);
            long idx = w.appendUncommitted(EventType.ORDER_ACCEPTED, 11L, 22L, bb);
            assertThat(idx).isEqualTo(0L);
            assertThat(log.slotStatus(0L)).isEqualTo(EventLog.STATUS_WRITING);

            // Reader must NOT see uncommitted slots.
            EventLogReader r = log.reader("ro");
            EventView v = new EventView();
            assertThat(r.poll(v)).isFalse();

            log.commit(0L);
            assertThat(r.poll(v)).isTrue();
            assertThat(v.term()).isEqualTo(11L);
            assertThat(v.timestampNanos()).isEqualTo(22L);
        }
    }

    @Test
    void commit_throws_on_empty_slot(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("c.log");
        try (EventLog log = EventLog.open(file, 4)) {
            assertThatThrownBy(() -> log.commit(0L))
                    .isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    void commit_is_idempotent(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("c2.log");
        try (EventLog log = EventLog.open(file, 4)) {
            EventLogWriter w = log.writer();
            byte[] p = payload("x");
            w.append(EventType.TRADE, 0L, 0L, p, 0, p.length);
            log.commit(0L); // already committed; should be no-op
            assertThat(log.slotStatus(0L)).isEqualTo(EventLog.STATUS_COMMITTED);
        }
    }

    private static long bytesToLongLE(byte[] b, int off) {
        long v = 0L;
        for (int i = 7; i >= 0; i--) {
            v = (v << 8) | (b[off + i] & 0xFFL);
        }
        return v;
    }
}
