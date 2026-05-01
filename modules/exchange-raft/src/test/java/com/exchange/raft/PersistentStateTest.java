package com.exchange.raft;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PersistentStateTest {

    @Test
    void initial_state_has_zero_term_and_no_vote(@TempDir Path tmp) {
        Path file = tmp.resolve("state.bin");
        try (PersistentState ps = PersistentState.open(file)) {
            assertThat(ps.currentTerm()).isEqualTo(0L);
            assertThat(ps.votedFor()).isEqualTo(PersistentState.VOTED_FOR_NONE);
            // -1 sentinel = "no committed/applied entries yet".
            assertThat(ps.commitIndex()).isEqualTo(-1L);
            assertThat(ps.lastApplied()).isEqualTo(-1L);
        }
    }

    @Test
    void survives_close_and_reopen(@TempDir Path tmp) {
        Path file = tmp.resolve("state.bin");
        try (PersistentState ps = PersistentState.open(file)) {
            ps.setTermAndVote(5L, 3);
            ps.setCommitIndex(42L);
            ps.setLastApplied(40L);
        }
        try (PersistentState ps = PersistentState.open(file)) {
            assertThat(ps.currentTerm()).isEqualTo(5L);
            assertThat(ps.votedFor()).isEqualTo(3);
            assertThat(ps.commitIndex()).isEqualTo(42L);
            assertThat(ps.lastApplied()).isEqualTo(40L);
        }
    }

    @Test
    void independent_setters_persist(@TempDir Path tmp) {
        Path file = tmp.resolve("state.bin");
        try (PersistentState ps = PersistentState.open(file)) {
            ps.setCurrentTerm(7L);
            assertThat(ps.currentTerm()).isEqualTo(7L);
            ps.setVotedFor(2);
            assertThat(ps.votedFor()).isEqualTo(2);
            ps.setVotedFor(PersistentState.VOTED_FOR_NONE);
            assertThat(ps.votedFor()).isEqualTo(-1);
        }
    }

    @Test
    void rejects_negative_term(@TempDir Path tmp) {
        try (PersistentState ps = PersistentState.open(tmp.resolve("s.bin"))) {
            assertThatThrownBy(() -> ps.setCurrentTerm(-1L))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> ps.setTermAndVote(-2L, 0))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> ps.setCommitIndex(-2L))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> ps.setLastApplied(-2L))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void closed_state_throws(@TempDir Path tmp) {
        PersistentState ps = PersistentState.open(tmp.resolve("s.bin"));
        ps.close();
        assertThatThrownBy(ps::currentTerm).isInstanceOf(IllegalStateException.class);
    }
}
