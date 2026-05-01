package com.exchange.raft;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.assertj.core.api.Assertions.assertThat;

class RpcCodecTest {

    @Test
    void request_vote_round_trip() {
        RaftRpc.RequestVote rv = new RaftRpc.RequestVote(7L, 3, 42L, 5L);
        ByteBuffer encoded = RpcCodec.encode(rv);
        ByteBuffer body = stripFrame(encoded);
        RaftRpc.Message m = RpcCodec.decode(body);
        assertThat(m).isInstanceOf(RaftRpc.RequestVote.class);
        RaftRpc.RequestVote out = (RaftRpc.RequestVote) m;
        assertThat(out.term()).isEqualTo(7L);
        assertThat(out.candidateId()).isEqualTo(3);
        assertThat(out.lastLogIndex()).isEqualTo(42L);
        assertThat(out.lastLogTerm()).isEqualTo(5L);
    }

    @Test
    void request_vote_resp_round_trip() {
        RaftRpc.RequestVoteResp r = new RaftRpc.RequestVoteResp(11L, 4, true);
        ByteBuffer body = stripFrame(RpcCodec.encode(r));
        RaftRpc.Message m = RpcCodec.decode(body);
        assertThat(m).isInstanceOf(RaftRpc.RequestVoteResp.class);
        RaftRpc.RequestVoteResp out = (RaftRpc.RequestVoteResp) m;
        assertThat(out.term()).isEqualTo(11L);
        assertThat(out.voterId()).isEqualTo(4);
        assertThat(out.voteGranted()).isTrue();
    }

    @Test
    void append_entries_round_trip_with_entries() {
        byte[] e1 = new byte[256];
        byte[] e2 = new byte[256];
        for (int i = 0; i < 256; i++) {
            e1[i] = (byte) i;
            e2[i] = (byte) (255 - i);
        }
        RaftRpc.AppendEntries ae = new RaftRpc.AppendEntries(
                9L, 1, 10L, 8L, new byte[][] { e1, e2 }, 12L);
        ByteBuffer body = stripFrame(RpcCodec.encode(ae));
        RaftRpc.Message m = RpcCodec.decode(body);
        assertThat(m).isInstanceOf(RaftRpc.AppendEntries.class);
        RaftRpc.AppendEntries out = (RaftRpc.AppendEntries) m;
        assertThat(out.term()).isEqualTo(9L);
        assertThat(out.leaderId()).isEqualTo(1);
        assertThat(out.prevLogIndex()).isEqualTo(10L);
        assertThat(out.prevLogTerm()).isEqualTo(8L);
        assertThat(out.leaderCommit()).isEqualTo(12L);
        assertThat(out.entries().length).isEqualTo(2);
        assertThat(out.entries()[0]).isEqualTo(e1);
        assertThat(out.entries()[1]).isEqualTo(e2);
    }

    @Test
    void append_entries_round_trip_empty() {
        RaftRpc.AppendEntries ae = new RaftRpc.AppendEntries(
                1L, 0, -1L, 0L, new byte[0][], 0L);
        ByteBuffer body = stripFrame(RpcCodec.encode(ae));
        RaftRpc.AppendEntries out = (RaftRpc.AppendEntries) RpcCodec.decode(body);
        assertThat(out.entries().length).isEqualTo(0);
        assertThat(out.prevLogIndex()).isEqualTo(-1L);
    }

    @Test
    void append_entries_resp_round_trip() {
        RaftRpc.AppendEntriesResp r = new RaftRpc.AppendEntriesResp(3L, 5, false, -1L, 7L);
        ByteBuffer body = stripFrame(RpcCodec.encode(r));
        RaftRpc.AppendEntriesResp out = (RaftRpc.AppendEntriesResp) RpcCodec.decode(body);
        assertThat(out.term()).isEqualTo(3L);
        assertThat(out.followerId()).isEqualTo(5);
        assertThat(out.success()).isFalse();
        assertThat(out.matchIndex()).isEqualTo(-1L);
        assertThat(out.conflictIndex()).isEqualTo(7L);
    }

    @Test
    void install_snapshot_round_trip() {
        byte[] data = new byte[] { 1, 2, 3, 4 };
        RaftRpc.InstallSnapshot is = new RaftRpc.InstallSnapshot(2L, 0, 100L, 1L, 0L, data, true);
        ByteBuffer body = stripFrame(RpcCodec.encode(is));
        RaftRpc.InstallSnapshot out = (RaftRpc.InstallSnapshot) RpcCodec.decode(body);
        assertThat(out.term()).isEqualTo(2L);
        assertThat(out.lastIncludedIndex()).isEqualTo(100L);
        assertThat(out.data()).isEqualTo(data);
        assertThat(out.done()).isTrue();
    }

    /** Strip the 4-byte length prefix to leave [type+body] for {@link RpcCodec#decode}. */
    private static ByteBuffer stripFrame(ByteBuffer framed) {
        ByteBuffer dup = framed.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        int len = dup.getInt();
        ByteBuffer body = ByteBuffer.allocate(len).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < len; i++) {
            body.put(dup.get());
        }
        body.flip();
        return body;
    }
}
