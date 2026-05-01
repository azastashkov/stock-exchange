package com.exchange.loadclient;

import com.exchange.fix.FixField;
import com.exchange.fix.FixMessage;
import com.exchange.fix.FixMsgType;
import com.exchange.time.Clocks;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Owns a single FIX session: one {@link FixClient}, one
 * {@link OrderGenerator}, one thread.
 * <p>
 * The thread alternates: drain inbound bytes (which fires
 * {@link FixClient.Listener#onMessage(FixMessage)} for ExecutionReports) and
 * drain a tiny token-fed outbound queue (driven by the central scheduler).
 * <p>
 * On a "NOT LEADER" reject the runner reconnects to a new target picked by
 * {@link LeaderTracker} and retries the rejected order with the same
 * ClOrdID.
 */
public final class SessionRunner implements Runnable, FixClient.Listener {

    private static final Logger LOG = LoggerFactory.getLogger(SessionRunner.class);

    /** Queue commands. */
    public static final byte CMD_NEW_ORDER = 1;
    public static final byte CMD_CANCEL = 2;

    /** Ratio of orders followed by a cancel — about 10%. */
    public static final double CANCEL_RATIO = 0.10;
    /** Minimum age before a cancel can fire, in ns. */
    public static final long CANCEL_MIN_AGE_NS = 50L * 1_000_000L;

    private final int sessionIdx;
    private final LoadClientConfig cfg;
    private final LeaderTracker tracker;
    private final LatencyRecorder latency;
    private final FixClient client;
    private final OrderGenerator generator;
    private final Long2LongOpenHashMap clOrdToSentNs;
    private final AtomicBoolean stop;
    private final Thread thread;
    private final Random rng = new Random();

    /** A tiny inbound command queue: when the scheduler hands us a token, we tick this. */
    private final ConcurrentLinkedQueue<Byte> tokens = new ConcurrentLinkedQueue<>();

    private final OrderGenerator.NewOrderSpec specScratch = new OrderGenerator.NewOrderSpec();
    private final FixClient.CancelSpec cancelScratch = new FixClient.CancelSpec();

    /** Orders sent total / cancels sent total / latency observations recorded. */
    private long ordersSent = 0L;
    private long cancelsSent = 0L;

    /** Currently-targeted host:port. Round-robin assigned at construction. */
    private String currentTarget;

    /** Earliest nanos at which we may attempt another reconnect (back-off after failure). */
    private long nextReconnectAtNs = 0L;
    /** Current backoff duration after consecutive failures. */
    private long reconnectBackoffNs = 200_000_000L;

    public SessionRunner(int sessionIdx,
                         LoadClientConfig cfg,
                         LeaderTracker tracker,
                         LatencyRecorder latency,
                         AtomicBoolean stop) {
        this.sessionIdx = sessionIdx;
        this.cfg = cfg;
        this.tracker = tracker;
        this.latency = latency;
        this.stop = stop;
        this.clOrdToSentNs = new Long2LongOpenHashMap(4096);
        this.clOrdToSentNs.defaultReturnValue(-1L);
        String sender = String.format("LOAD%02d", sessionIdx);
        this.client = new FixClient(sender, "EXCHANGE", this);
        String[] symArr = cfg.symbols().toArray(new String[0]);
        this.generator = new OrderGenerator(symArr, cfg.priceBaseFp());
        // Stagger seq across sessions so ClOrdIDs never collide between sessions.
        this.generator.resetSeq(0L);
        this.currentTarget = cfg.targets().get(sessionIdx % cfg.targets().size());
        this.thread = new Thread(this, "load-session-" + sessionIdx);
        this.thread.setDaemon(true);
    }

    public int sessionIdx() { return sessionIdx; }

    public void start() {
        thread.start();
    }

    public void awaitTermination(long timeoutMs) {
        try {
            thread.join(timeoutMs);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    public long ordersSent() { return ordersSent; }
    public long cancelsSent() { return cancelsSent; }

    /** Called by the central scheduler — non-blocking. Each token = one outbound command. */
    public void offerToken() {
        if (stop.get()) return;
        // Cap the queue so a stuck session doesn't accumulate tokens forever.
        if (tokens.size() > 4096) return;
        // Decide if this token will produce a cancel or new order. Bias 10%
        // toward cancel _but only_ if we have a cancellable resting order.
        boolean tryCancel = (rng.nextDouble() < CANCEL_RATIO);
        tokens.offer(tryCancel ? CMD_CANCEL : CMD_NEW_ORDER);
    }

    @Override
    public void run() {
        // Initial connect + logon.
        if (!ensureConnected()) {
            LOG.warn("[{}] could not connect to any target on startup", client.senderCompId());
        }
        long lastProgressLogNs = Clocks.nanoTime();
        while (!stop.get()) {
            // 1) drain inbound (fires onMessage for ERs)
            try {
                client.onPoll();
            } catch (RuntimeException e) {
                LOG.warn("[{}] poll error: {}", client.senderCompId(), e.toString());
            }
            // 2) drain a few tokens
            int worked = 0;
            while (worked < 32 && !stop.get()) {
                Byte t = tokens.poll();
                if (t == null) break;
                if (!client.isConnected()) {
                    if (!ensureConnected()) {
                        // give up this token; try again later
                        break;
                    }
                }
                if (t == CMD_CANCEL) {
                    if (!sendOneCancel()) {
                        // fall through to a new order on the same token
                        sendOneNewOrder();
                    }
                } else {
                    sendOneNewOrder();
                }
                worked++;
            }
            // 3) reconnect if disconnected
            if (!client.isConnected()) {
                ensureConnected();
            }
            // 4) idle sleep so we don't burn cpu if nothing is happening
            if (worked == 0) {
                try { Thread.sleep(1L); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            }
            // periodic progress log
            long now = Clocks.nanoTime();
            if (now - lastProgressLogNs > 10L * 1_000_000_000L) {
                LOG.debug("[{}] sent {} new {} cancels target {}",
                        client.senderCompId(), ordersSent, cancelsSent, currentTarget);
                lastProgressLogNs = now;
            }
        }
        // Drain a few more inbound, send Logout, close.
        long drainDeadline = System.currentTimeMillis() + 1000L;
        while (System.currentTimeMillis() < drainDeadline) {
            int events = client.onPoll();
            if (events == 0) break;
        }
        client.close();
    }

    private boolean ensureConnected() {
        if (client.isConnected()) return true;
        long now = Clocks.nanoTime();
        if (now < nextReconnectAtNs) return false;

        // One pass through the targets, with no inner sleep — the outer
        // backoff schedule keeps us from spamming when nothing is reachable.
        int attempts = cfg.targets().size();
        String trying = currentTarget;
        for (int i = 0; i < attempts && !stop.get(); i++) {
            if (client.connectAndLogon(trying)) {
                currentTarget = trying;
                reconnectBackoffNs = 200_000_000L; // reset on success
                nextReconnectAtNs = 0L;
                return true;
            }
            trying = tracker.nextTarget(trying);
        }
        // All targets failed this cycle. Schedule next attempt with exponential backoff up to 5s.
        nextReconnectAtNs = Clocks.nanoTime() + reconnectBackoffNs;
        reconnectBackoffNs = Math.min(reconnectBackoffNs * 2L, 5_000_000_000L);
        return false;
    }

    private void sendOneNewOrder() {
        generator.next(specScratch, sessionIdx, cfg.account());
        long now = Clocks.nanoTime();
        long key = clOrdHash(specScratch.clOrdId);
        clOrdToSentNs.put(key, now);
        if (!client.sendNewOrder(specScratch)) {
            // remove the stamp; we never sent
            clOrdToSentNs.remove(key);
            return;
        }
        ordersSent++;
    }

    private boolean sendOneCancel() {
        OrderGenerator.RestingOrder r = generator.pollCancellable(CANCEL_MIN_AGE_NS);
        if (r == null) return false;
        // Build cancel: cancel-id is "C<sessionIdx>-<seq>", origClOrdId is the original.
        long now = Clocks.nanoTime();
        formatCancelClOrdId(cancelScratch.clOrdId, sessionIdx, r.clOrdSeq);
        OrderGenerator.formatClOrdId(cancelScratch.origClOrdId, sessionIdx, r.clOrdSeq);
        cancelScratch.symbol = r.symbol;
        cancelScratch.side = r.side;
        cancelScratch.account = r.account;
        long key = clOrdHash(cancelScratch.clOrdId);
        clOrdToSentNs.put(key, now);
        if (!client.sendCancel(cancelScratch)) {
            clOrdToSentNs.remove(key);
            return false;
        }
        cancelsSent++;
        return true;
    }

    private static void formatCancelClOrdId(byte[] dest, int sessionIdx, long seq) {
        if (dest.length != 16) throw new IllegalArgumentException("dest must be 16 bytes");
        int p = 0;
        dest[p++] = 'C';
        if (sessionIdx >= 100) {
            dest[p++] = (byte) ('0' + (sessionIdx / 100) % 10);
            dest[p++] = (byte) ('0' + (sessionIdx / 10) % 10);
            dest[p++] = (byte) ('0' + sessionIdx % 10);
        } else if (sessionIdx >= 10) {
            dest[p++] = (byte) ('0' + (sessionIdx / 10) % 10);
            dest[p++] = (byte) ('0' + sessionIdx % 10);
        } else {
            dest[p++] = (byte) ('0' + Math.max(0, sessionIdx));
        }
        dest[p++] = '-';
        long v = seq;
        if (v <= 0L) {
            dest[p++] = '0';
        } else {
            byte[] tmp = new byte[20];
            int idx = 0;
            while (v > 0L && idx < tmp.length) {
                tmp[idx++] = (byte) ('0' + (int) (v % 10L));
                v /= 10L;
            }
            int writable = Math.min(idx, 16 - p);
            for (int i = 0; i < writable; i++) {
                dest[p + i] = tmp[idx - 1 - i];
            }
            p += writable;
        }
        for (int i = p; i < 16; i++) dest[i] = 0;
    }

    /** FNV-style 64-bit hash of a 16-byte ClOrdID. Mirrors the encoder convention. */
    static long clOrdHash(byte[] clOrdId16) {
        long h = 0xcbf29ce484222325L;
        for (int i = 0; i < 16; i++) {
            h ^= clOrdId16[i] & 0xFFL;
            h *= 0x100000001b3L;
        }
        return h;
    }

    @Override
    public void onMessage(FixMessage msg) {
        String mt;
        try { mt = msg.msgType(); } catch (RuntimeException re) { return; }
        if (FixMsgType.EXECUTION_REPORT.equals(mt)) {
            handleExecutionReport(msg);
        } else if (FixMsgType.BUSINESS_MESSAGE_REJECT.equals(mt)) {
            handleBusinessReject(msg);
        }
    }

    private void handleExecutionReport(FixMessage msg) {
        // Look up the ClOrdID and compute round-trip.
        if (!msg.has(FixField.CL_ORD_ID)) return;
        byte[] cl = new byte[16];
        // copy at most 16 bytes (right-pad with zero)
        int valLen = msg.valueLength(FixField.CL_ORD_ID);
        int valOff = msg.valueOffset(FixField.CL_ORD_ID);
        int copy = Math.min(valLen, 16);
        for (int i = 0; i < copy; i++) cl[i] = msg.buffer[valOff + i];
        for (int i = copy; i < 16; i++) cl[i] = 0;
        long key = clOrdHash(cl);
        long sentNs = clOrdToSentNs.remove(key);
        long now = Clocks.nanoTime();
        if (sentNs > 0L) {
            latency.recordRoundtripNanos(now - sentNs);
        }
        // Detect NOT LEADER (ExecType = 8 = REJECTED) and redirect.
        boolean rejected = false;
        if (msg.has(FixField.EXEC_TYPE)) {
            byte execType = msg.getChar(FixField.EXEC_TYPE);
            rejected = (execType == '8');
        }
        if (rejected && msg.has(FixField.TEXT)) {
            String text = msg.getString(FixField.TEXT);
            if (text.contains("NOT LEADER") || text.contains("use node")) {
                tracker.onNotLeader(currentTarget, text);
                redirectToLeader();
            }
        } else if (!rejected) {
            tracker.onLeaderAccepted(currentTarget);
        }
    }

    private void handleBusinessReject(FixMessage msg) {
        if (msg.has(FixField.TEXT)) {
            String text = msg.getString(FixField.TEXT);
            if (text.contains("NOT LEADER") || text.contains("use node")) {
                tracker.onNotLeader(currentTarget, text);
                redirectToLeader();
            }
        }
    }

    private void redirectToLeader() {
        String next = tracker.nextTarget(currentTarget);
        if (next == null || next.equals(currentTarget)) {
            return;
        }
        LOG.info("[{}] redirecting from {} to {} (leader hint)",
                client.senderCompId(), currentTarget, next);
        currentTarget = next;
        client.disconnect();
        // SessionRunner main loop will reconnect on next iteration.
    }
}
