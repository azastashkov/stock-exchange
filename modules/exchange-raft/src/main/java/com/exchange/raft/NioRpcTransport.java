package com.exchange.raft;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * NIO-based TCP transport. Single selector serves one server socket plus an
 * outbound connection per peer. One thread runs the selector; inbound bytes
 * are decoded and the resulting {@link RaftRpc.Message} is enqueued onto a
 * concurrent queue that the Raft loop thread drains via {@link #drain(int)}.
 * <p>
 * Wire framing: every message is prefixed with a 4-byte LE length (the size
 * of the {@code [type+body]} bytes that follow). See {@link RpcCodec}.
 */
public final class NioRpcTransport implements RpcTransport {

    private static final Logger LOG = LoggerFactory.getLogger(NioRpcTransport.class);

    private static final int MAX_FRAME_BYTES = 16 * 1024 * 1024; // 16 MB cap
    private static final int OUTBOUND_QUEUE_CAP = 4096;

    private final int nodeId;
    private final PeerConfig peers;
    private Selector selector;
    private ServerSocketChannel serverChannel;
    private final Map<Integer, PeerConn> peerConns = new HashMap<>();
    private final ConcurrentLinkedQueue<RaftRpc.Message> inbound = new ConcurrentLinkedQueue<>();
    private volatile Listener listener;
    private volatile boolean running = false;
    private Thread thread;

    public NioRpcTransport(PeerConfig peers) {
        this.nodeId = peers.nodeId();
        this.peers = peers;
    }

    @Override
    public void setListener(Listener listener) {
        this.listener = listener;
    }

    @Override
    public synchronized void start() {
        if (running) return;
        try {
            selector = Selector.open();
            serverChannel = ServerSocketChannel.open();
            serverChannel.configureBlocking(false);
            serverChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            NetAddress self = peers.selfAddress();
            serverChannel.bind(new InetSocketAddress(self.host(), self.port()));
            serverChannel.register(selector, SelectionKey.OP_ACCEPT);
            // Initialise per-peer outbound holders eagerly so we can connect
            // lazily on first send.
            for (int peerId : peers.peerIds()) {
                peerConns.put(peerId, new PeerConn(peerId));
            }
            running = true;
            thread = new Thread(this::loop, "raft-net-" + nodeId);
            thread.setDaemon(true);
            thread.start();
            LOG.info("Raft NIO transport for node {} listening on {}", nodeId, self);
        } catch (IOException e) {
            try { closeQuietly(); } catch (Throwable ignored) { /* best-effort */ }
            throw new RaftIntegrationException("Failed to start NIO transport on " + peers.selfAddress(), e);
        }
    }

    @Override
    public boolean send(int peerId, RaftRpc.Message msg) {
        if (!running) return false;
        PeerConn pc = peerConns.get(peerId);
        if (pc == null) return false;
        if (pc.outbound.size() > OUTBOUND_QUEUE_CAP) {
            return false;
        }
        ByteBuffer encoded = RpcCodec.encode(msg);
        pc.outbound.add(encoded);
        Selector s = selector;
        if (s != null) s.wakeup();
        return true;
    }

    @Override
    public synchronized void close() {
        running = false;
        Selector s = selector;
        if (s != null) s.wakeup();
        Thread t = thread;
        if (t != null && t != Thread.currentThread()) {
            try { t.join(500); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
        }
        closeQuietly();
    }

    /** Draining hook: invoked from the Raft loop tick. */
    public int drain(int max) {
        if (listener == null) return 0;
        int n = 0;
        while (n < max) {
            RaftRpc.Message m = inbound.poll();
            if (m == null) break;
            try {
                listener.onMessage(m);
            } catch (Throwable t) {
                LOG.error("listener error on node {}", nodeId, t);
            }
            n++;
        }
        return n;
    }

    // -----------------------------------------------------------------
    // Selector loop
    // -----------------------------------------------------------------

    private void loop() {
        try {
            while (running) {
                ensureConnections();
                try {
                    selector.select(50L);
                } catch (IOException e) {
                    LOG.warn("selector select failed: {}", e.toString());
                    break;
                }
                Set<SelectionKey> keys = selector.selectedKeys();
                for (Iterator<SelectionKey> it = keys.iterator(); it.hasNext(); ) {
                    SelectionKey key = it.next();
                    it.remove();
                    if (!key.isValid()) continue;
                    try {
                        if (key.isAcceptable()) {
                            doAccept();
                        } else if (key.isConnectable()) {
                            doConnect(key);
                        } else {
                            if (key.isReadable()) {
                                doRead(key);
                            }
                            if (key.isValid() && key.isWritable()) {
                                doWrite(key);
                            }
                        }
                    } catch (Throwable t) {
                        LOG.warn("io error on key, dropping: {}", t.toString());
                        Object att = key.attachment();
                        if (att instanceof PeerConn pc) {
                            pc.dropChannel();
                        } else {
                            // inbound channel: just close
                            try { key.channel().close(); } catch (IOException ignored) { /* best-effort */ }
                        }
                    }
                }
                // Flush any pending writes after handling reads
                for (PeerConn pc : peerConns.values()) {
                    if (pc.channel != null && !pc.outbound.isEmpty()) {
                        try {
                            pc.flush();
                        } catch (IOException e) {
                            pc.dropChannel();
                        }
                    }
                }
            }
        } finally {
            closeQuietly();
        }
    }

    private void ensureConnections() {
        for (PeerConn pc : peerConns.values()) {
            if (pc.channel == null && pc.outbound.size() > 0 && System.nanoTime() > pc.nextRetryNanos) {
                NetAddress addr = peers.addressOf(pc.peerId);
                if (addr == null) continue;
                try {
                    SocketChannel ch = SocketChannel.open();
                    ch.configureBlocking(false);
                    ch.setOption(StandardSocketOptions.TCP_NODELAY, true);
                    boolean connected = ch.connect(new InetSocketAddress(addr.host(), addr.port()));
                    pc.channel = ch;
                    pc.attachKey(ch.register(selector,
                            connected ? (SelectionKey.OP_READ | SelectionKey.OP_WRITE) : SelectionKey.OP_CONNECT,
                            pc));
                } catch (IOException e) {
                    pc.nextRetryNanos = System.nanoTime() + 200_000_000L; // 200ms backoff
                }
            }
        }
    }

    private void doAccept() throws IOException {
        SocketChannel client = serverChannel.accept();
        if (client == null) return;
        client.configureBlocking(false);
        client.setOption(StandardSocketOptions.TCP_NODELAY, true);
        InboundConn ic = new InboundConn(client);
        client.register(selector, SelectionKey.OP_READ, ic);
    }

    private void doConnect(SelectionKey key) throws IOException {
        PeerConn pc = (PeerConn) key.attachment();
        SocketChannel ch = (SocketChannel) key.channel();
        if (ch.isConnectionPending()) {
            try {
                if (ch.finishConnect()) {
                    key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
                    LOG.debug("node {} connected to peer {}", nodeId, pc.peerId);
                }
            } catch (IOException e) {
                pc.dropChannel();
            }
        }
    }

    private void doRead(SelectionKey key) throws IOException {
        Object att = key.attachment();
        SocketChannel ch = (SocketChannel) key.channel();
        ByteBuffer rb;
        if (att instanceof PeerConn pc) {
            rb = pc.readBuf;
        } else if (att instanceof InboundConn ic) {
            rb = ic.readBuf;
        } else {
            ch.close();
            return;
        }
        int n = ch.read(rb);
        if (n < 0) {
            try { ch.close(); } catch (IOException ignored) { /* best-effort */ }
            if (att instanceof PeerConn pc) pc.dropChannel();
            return;
        }
        if (n == 0) return;
        // Try to parse as many frames as possible.
        rb.flip();
        while (rb.remaining() >= RpcCodec.LENGTH_PREFIX_BYTES) {
            int markPos = rb.position();
            int frameLen = rb.getInt();
            if (frameLen < 1 || frameLen > MAX_FRAME_BYTES) {
                LOG.warn("bad frame length {} on node {}, closing channel", frameLen, nodeId);
                ch.close();
                if (att instanceof PeerConn pc) pc.dropChannel();
                return;
            }
            if (rb.remaining() < frameLen) {
                rb.position(markPos); // rewind; need more bytes
                break;
            }
            // Slice off this frame and decode.
            int saveLimit = rb.limit();
            rb.limit(rb.position() + frameLen);
            ByteBuffer slice = rb.slice().order(ByteOrder.LITTLE_ENDIAN);
            rb.position(rb.limit());
            rb.limit(saveLimit);
            RaftRpc.Message m = RpcCodec.decode(slice);
            if (m != null) {
                inbound.add(m);
            }
        }
        rb.compact();
    }

    private void doWrite(SelectionKey key) throws IOException {
        Object att = key.attachment();
        if (att instanceof PeerConn pc) {
            pc.flush();
            if (pc.outbound.isEmpty()) {
                key.interestOps(SelectionKey.OP_READ);
            }
        }
    }

    private void closeQuietly() {
        for (PeerConn pc : peerConns.values()) pc.dropChannel();
        if (serverChannel != null) {
            try { serverChannel.close(); } catch (IOException ignored) { /* best-effort */ }
            serverChannel = null;
        }
        if (selector != null) {
            try { selector.close(); } catch (IOException ignored) { /* best-effort */ }
            selector = null;
        }
    }

    // -----------------------------------------------------------------
    // Per-peer state
    // -----------------------------------------------------------------

    private final class PeerConn {
        final int peerId;
        SocketChannel channel;
        SelectionKey key;
        final ByteBuffer readBuf = ByteBuffer.allocate(64 * 1024).order(ByteOrder.LITTLE_ENDIAN);
        final ConcurrentLinkedQueue<ByteBuffer> outbound = new ConcurrentLinkedQueue<>();
        long nextRetryNanos = 0L;

        PeerConn(int peerId) { this.peerId = peerId; }

        void attachKey(SelectionKey k) {
            this.key = k;
        }

        void flush() throws IOException {
            if (channel == null) return;
            ByteBuffer head;
            while ((head = outbound.peek()) != null) {
                channel.write(head);
                if (head.hasRemaining()) {
                    if (key != null && key.isValid()) {
                        key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
                    }
                    return;
                }
                outbound.poll();
            }
        }

        void dropChannel() {
            if (channel != null) {
                try { channel.close(); } catch (IOException ignored) { /* best-effort */ }
                channel = null;
            }
            if (key != null) {
                key.cancel();
                key = null;
            }
            // Drop pending writes; the Raft layer retries via its own
            // resend logic (heartbeats, AppendEntries on next tick).
            outbound.clear();
            readBuf.clear();
            nextRetryNanos = System.nanoTime() + 200_000_000L;
        }
    }

    private final class InboundConn {
        final SocketChannel channel;
        final ByteBuffer readBuf = ByteBuffer.allocate(64 * 1024).order(ByteOrder.LITTLE_ENDIAN);
        InboundConn(SocketChannel ch) { this.channel = ch; }
    }
}
