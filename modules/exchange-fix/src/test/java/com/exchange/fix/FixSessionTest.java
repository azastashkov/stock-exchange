package com.exchange.fix;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class FixSessionTest {

    @Test
    void persistAndReloadRoundTrip(@TempDir Path tmp) throws IOException {
        Path seqFile = tmp.resolve("CLIENT01.seq");
        FixSession a = new FixSession("CLIENT01", "EXCHANGE", null, null, seqFile);
        a.inSeqNumExpected = 42;
        a.outSeqNum = 17;
        a.persist();

        assertThat(Files.exists(seqFile)).isTrue();

        FixSession b = new FixSession("CLIENT01", "EXCHANGE", null, null, seqFile);
        b.load();
        assertThat(b.inSeqNumExpected).isEqualTo(42);
        assertThat(b.outSeqNum).isEqualTo(17);
    }

    @Test
    void firstLoadInitialisesToOne(@TempDir Path tmp) throws IOException {
        Path seqFile = tmp.resolve("FRESH.seq");
        FixSession s = new FixSession("FRESH", "EXCHANGE", null, null, seqFile);
        s.load();
        assertThat(s.inSeqNumExpected).isEqualTo(1);
        assertThat(s.outSeqNum).isEqualTo(1);
    }

    @Test
    void senderCompIdHashIsStable() {
        long h1 = FixSession.senderCompIdHash("CLIENT01");
        long h2 = FixSession.senderCompIdHash("CLIENT01");
        long h3 = FixSession.senderCompIdHash("CLIENT02");
        assertThat(h1).isEqualTo(h2);
        assertThat(h1).isNotEqualTo(h3);
    }

    @Test
    void incInSeqAndNextOutSeqWorkAsAdvertised(@TempDir Path tmp) {
        FixSession s = new FixSession("X", "Y", null, null, tmp.resolve("X.seq"));
        s.inSeqNumExpected = 5;
        s.outSeqNum = 3;
        int got = s.nextOutSeq();
        assertThat(got).isEqualTo(3);
        assertThat(s.outSeqNum).isEqualTo(4);
        s.incInSeq();
        assertThat(s.inSeqNumExpected).isEqualTo(6);
    }
}
