package com.exchange.app.md;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * NIO TCP server that accepts market-data subscribers and lets the
 * MarketDataPublisherService push frames to them.
 * <p>
 * Stub-grade: each frame is {@code [length:int32 LE][payload]}, written
 * synchronously to all currently-connected sockets. The accept loop runs
 * on its own daemon thread.
 */
public final class MdServer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(MdServer.class);

    private final int requestedPort;
    private ServerSocketChannel ssc;
    private Selector selector;
    private final CopyOnWriteArrayList<SocketChannel> subscribers = new CopyOnWriteArrayList<>();
    private Thread acceptThread;
    private volatile boolean running = false;
    private int boundPort;

    public MdServer(int port) {
        this.requestedPort = port;
    }

    /** Open the server socket; returns the bound port (useful when port==0). */
    public synchronized int start() throws IOException {
        if (running) return boundPort;
        ssc = ServerSocketChannel.open();
        ssc.configureBlocking(false);
        ssc.bind(new InetSocketAddress(requestedPort));
        boundPort = ((java.net.InetSocketAddress) ssc.getLocalAddress()).getPort();
        selector = Selector.open();
        ssc.register(selector, SelectionKey.OP_ACCEPT);
        running = true;
        acceptThread = new Thread(this::acceptLoop, "md-accept");
        acceptThread.setDaemon(true);
        acceptThread.start();
        LOG.info("MdServer listening on port {}", boundPort);
        return boundPort;
    }

    public int subscriberCount() {
        return subscribers.size();
    }

    public int port() {
        return boundPort;
    }

    /** Synchronously broadcast {@code payload} (length-prefixed) to all subscribers. */
    public void broadcast(ByteBuffer payload) {
        if (subscribers.isEmpty()) return;
        int len = payload.remaining();
        ByteBuffer header = ByteBuffer.allocate(4).order(java.nio.ByteOrder.LITTLE_ENDIAN);
        header.putInt(len);
        header.flip();
        for (SocketChannel ch : subscribers) {
            try {
                writeFully(ch, header.duplicate());
                writeFully(ch, payload.duplicate());
            } catch (IOException e) {
                LOG.debug("MD subscriber write failed: {}", e.toString());
                subscribers.remove(ch);
                try { ch.close(); } catch (IOException ignored) { /* best-effort */ }
            }
        }
    }

    private static void writeFully(SocketChannel ch, ByteBuffer b) throws IOException {
        while (b.hasRemaining()) {
            int n = ch.write(b);
            if (n < 0) throw new ClosedChannelException();
        }
    }

    private void acceptLoop() {
        try {
            while (running) {
                int n = selector.select(100L);
                if (n == 0) continue;
                Iterator<SelectionKey> it = selector.selectedKeys().iterator();
                while (it.hasNext()) {
                    SelectionKey key = it.next();
                    it.remove();
                    if (key.isAcceptable()) {
                        SocketChannel client = ssc.accept();
                        if (client != null) {
                            client.configureBlocking(true);
                            client.socket().setTcpNoDelay(true);
                            subscribers.add(client);
                            LOG.info("MD subscriber connected: {}", client.getRemoteAddress());
                        }
                    }
                }
            }
        } catch (IOException e) {
            if (running) {
                LOG.warn("MdServer accept loop terminated: {}", e.toString());
            }
        }
    }

    @Override
    public synchronized void close() {
        running = false;
        if (selector != null) selector.wakeup();
        for (SocketChannel ch : subscribers) {
            try { ch.close(); } catch (IOException ignored) { /* best-effort */ }
        }
        subscribers.clear();
        try { if (ssc != null) ssc.close(); } catch (IOException ignored) { /* best-effort */ }
        try { if (selector != null) selector.close(); } catch (IOException ignored) { /* best-effort */ }
        if (acceptThread != null) {
            try {
                acceptThread.join(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
