package com.exchange.loadclient;

import com.exchange.fix.FixCodec;
import com.exchange.fix.FixDecodeException;
import com.exchange.fix.FixField;
import com.exchange.fix.FixMessage;
import com.exchange.fix.FixMsgType;
import com.exchange.time.Clocks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;

/**
 * Single-threaded FIX 4.4 client used by the load harness. Owns one TCP
 * socket and uses blocking IO with a short read timeout so the same thread
 * can periodically push outbound bytes (heartbeats, new orders, cancels).
 * <p>
 * Threading: every method on this class is invoked from one
 * {@link SessionRunner} thread. There is no internal locking.
 * <p>
 * Encoding: messages are built in a re-used StringBuilder + byte[] buffer.
 * The load client tolerates per-message allocation; the gateway does not,
 * but we are not measuring the client's allocation profile.
 */
public final class FixClient {

    private static final Logger LOG = LoggerFactory.getLogger(FixClient.class);

    /** FIX SOH separator. */
    public static final char SOH = '';

    /** Heartbeat seconds — matches gateway's default. */
    public static final int HEARTBEAT_SEC = 30;

    /** Read timeout in ms — short so we can also do outbound work on the same thread. */
    public static final int READ_TIMEOUT_MS = 100;

    public interface Listener {
        /**
         * Called for every inbound application-level FIX message
         * (ExecutionReport, BusinessMessageReject, etc). The {@link FixMessage}
         * is mutable scratch — do not retain.
         */
        void onMessage(FixMessage msg);
    }

    private final String senderCompId;
    private final String targetCompId;
    private final Listener listener;

    private Socket socket;
    private InputStream in;
    private OutputStream out;

    // Reusable scratch
    private final byte[] readBuf = new byte[64 * 1024];
    private int readBufFill = 0;
    private final FixMessage scratch = new FixMessage();
    private final StringBuilder sb = new StringBuilder(256);

    // Sequence numbers
    private int outSeqNum = 1;

    // Heartbeat scheduling
    private long lastOutboundNs = 0L;

    // Connection liveness
    private volatile boolean connected = false;

    // Current target host:port (for diagnostics)
    private String currentTarget;

    /** Most recent connect-error string, used to suppress repeat WARN noise. */
    private String lastConnectError;
    private long lastConnectErrorAtMs;

    public FixClient(String senderCompId, String targetCompId, Listener listener) {
        this.senderCompId = senderCompId;
        this.targetCompId = targetCompId;
        this.listener = listener;
    }

    public String senderCompId() { return senderCompId; }
    public String currentTarget() { return currentTarget; }
    public boolean isConnected() { return connected; }

    /**
     * Open a TCP connection to {@code target} (host:port) and send a Logon
     * with {@code ResetSeqNumFlag=Y}. Blocks until the Logon response is
     * received or the timeout (5s) expires.
     */
    public boolean connectAndLogon(String target) {
        disconnect();
        this.currentTarget = target;
        int colon = target.lastIndexOf(':');
        if (colon < 0) {
            LOG.warn("[{}] invalid target: {}", senderCompId, target);
            return false;
        }
        String host = target.substring(0, colon);
        int port;
        try {
            port = Integer.parseInt(target.substring(colon + 1));
        } catch (NumberFormatException nfe) {
            LOG.warn("[{}] invalid port: {}", senderCompId, target);
            return false;
        }
        try {
            Socket s = new Socket();
            s.connect(new InetSocketAddress(host, port), 5000);
            s.setTcpNoDelay(true);
            s.setSoTimeout(READ_TIMEOUT_MS);
            s.setKeepAlive(true);
            this.socket = s;
            this.in = s.getInputStream();
            this.out = s.getOutputStream();
            this.outSeqNum = 1;
            this.readBufFill = 0;
            this.lastOutboundNs = Clocks.nanoTime();
            sendLogon();
            this.connected = true;
            // Wait briefly for Logon-response so SessionRunner doesn't immediately fire orders into a half-open connection.
            long deadline = System.currentTimeMillis() + 5000L;
            while (System.currentTimeMillis() < deadline) {
                int events = onPoll();
                if (events > 0 && lastWasLogon) {
                    return true;
                }
            }
            return false;
        } catch (IOException ioe) {
            String msg = ioe.toString();
            long now = System.currentTimeMillis();
            if (!msg.equals(lastConnectError) || now - lastConnectErrorAtMs > 30_000L) {
                LOG.warn("[{}] connect to {} failed: {}", senderCompId, target, msg);
                lastConnectError = msg;
                lastConnectErrorAtMs = now;
            }
            this.connected = false;
            return false;
        }
    }

    private boolean lastWasLogon = false;

    /** Forcibly close and reset state. Safe to call multiple times. */
    public void disconnect() {
        connected = false;
        if (socket != null) {
            try { socket.close(); } catch (IOException ignored) { /* best-effort */ }
        }
        socket = null;
        in = null;
        out = null;
        readBufFill = 0;
        lastWasLogon = false;
    }

    /**
     * Drain any inbound bytes, decode framed FIX messages, and dispatch via
     * the listener. Returns the number of messages dispatched.
     */
    public int onPoll() {
        if (!connected || in == null) return 0;
        // Try to read as much as we can in one go.
        try {
            int n = in.read(readBuf, readBufFill, readBuf.length - readBufFill);
            if (n < 0) {
                LOG.info("[{}] peer closed connection on {}", senderCompId, currentTarget);
                connected = false;
                return 0;
            }
            if (n > 0) {
                readBufFill += n;
            }
        } catch (SocketTimeoutException ste) {
            // expected — caller will retry
        } catch (IOException ioe) {
            LOG.warn("[{}] read error on {}: {}", senderCompId, currentTarget, ioe.toString());
            connected = false;
            return 0;
        }
        // Decode as many full frames as the buffer holds.
        int dispatched = 0;
        while (readBufFill > 0) {
            int consumed;
            try {
                consumed = FixCodec.decode(readBuf, 0, readBufFill, scratch);
            } catch (FixDecodeException fde) {
                LOG.warn("[{}] decode error: {} — closing", senderCompId, fde.getMessage());
                connected = false;
                return dispatched;
            }
            if (consumed == 0) break; // need more bytes
            shift(consumed);
            // Inspect msg type. Heartbeats / Logon-response / Logout we handle internally.
            String mt;
            try {
                mt = scratch.msgType();
            } catch (RuntimeException re) {
                continue;
            }
            switch (mt) {
                case FixMsgType.LOGON -> {
                    lastWasLogon = true;
                    LOG.debug("[{}] logon ack from {}", senderCompId, currentTarget);
                }
                case FixMsgType.HEARTBEAT, FixMsgType.TEST_REQUEST -> {
                    if (FixMsgType.TEST_REQUEST.equals(mt)) {
                        // Reply with Heartbeat carrying TestReqID
                        String trId = null;
                        if (scratch.has(FixField.TEST_REQ_ID)) {
                            trId = scratch.getString(FixField.TEST_REQ_ID);
                        }
                        try { sendHeartbeat(trId); } catch (IOException e) {
                            LOG.warn("[{}] failed heartbeat reply: {}", senderCompId, e.toString());
                            connected = false;
                            return dispatched;
                        }
                    }
                }
                case FixMsgType.LOGOUT -> {
                    LOG.info("[{}] logout from {}: text='{}'",
                            senderCompId, currentTarget,
                            scratch.has(FixField.TEXT) ? scratch.getString(FixField.TEXT) : "");
                    connected = false;
                }
                default -> {
                    // Application-level: ExecutionReport, BusinessMessageReject, Reject, etc.
                    if (listener != null) {
                        listener.onMessage(scratch);
                    }
                    dispatched++;
                }
            }
        }
        // If it's been a while since we sent anything, send a heartbeat.
        long now = Clocks.nanoTime();
        if (now - lastOutboundNs > HEARTBEAT_SEC * 1_000_000_000L / 2L) {
            try { sendHeartbeat(null); } catch (IOException ignored) { /* will detect on next read */ }
        }
        return dispatched;
    }

    private void shift(int consumed) {
        if (consumed >= readBufFill) {
            readBufFill = 0;
            return;
        }
        System.arraycopy(readBuf, consumed, readBuf, 0, readBufFill - consumed);
        readBufFill -= consumed;
    }

    /**
     * Encode and send a NewOrderSingle. The caller has already populated
     * {@code spec.clOrdId} (16 ASCII bytes, zero-padded). Returns {@code true}
     * on a clean write.
     */
    public boolean sendNewOrder(OrderGenerator.NewOrderSpec spec) {
        if (!connected || out == null) return false;
        sb.setLength(0);
        appendField(sb, "35", "D");
        appendField(sb, "49", senderCompId);
        appendField(sb, "56", targetCompId);
        appendField(sb, "34", Integer.toString(outSeqNum));
        appendField(sb, "52", utcTimestamp());
        appendClOrdField(sb, spec.clOrdId);
        appendField(sb, "55", spec.symbol);
        sb.append("54=").append((char) spec.side).append(SOH);
        appendField(sb, "60", utcTimestamp());
        appendField(sb, "38", Long.toString(spec.qty));
        appendField(sb, "40", "2"); // OrdType = Limit
        appendField(sb, "44", formatPrice(spec.priceFp));
        appendField(sb, "1", Long.toString(spec.account));
        appendField(sb, "59", "0"); // TIF = Day
        return sendFramed();
    }

    /** Encode and send an OrderCancelRequest. */
    public boolean sendCancel(CancelSpec spec) {
        if (!connected || out == null) return false;
        sb.setLength(0);
        appendField(sb, "35", "F");
        appendField(sb, "49", senderCompId);
        appendField(sb, "56", targetCompId);
        appendField(sb, "34", Integer.toString(outSeqNum));
        appendField(sb, "52", utcTimestamp());
        appendClOrdField(sb, spec.clOrdId);
        appendClOrdField(sb, "41", spec.origClOrdId);
        appendField(sb, "55", spec.symbol);
        sb.append("54=").append((char) spec.side).append(SOH);
        appendField(sb, "1", Long.toString(spec.account));
        appendField(sb, "60", utcTimestamp());
        return sendFramed();
    }

    /** Send Logout with no text. */
    public void sendLogout() {
        if (!connected || out == null) return;
        sb.setLength(0);
        appendField(sb, "35", "5");
        appendField(sb, "49", senderCompId);
        appendField(sb, "56", targetCompId);
        appendField(sb, "34", Integer.toString(outSeqNum));
        appendField(sb, "52", utcTimestamp());
        try { sendFramedThrowing(); } catch (IOException ignored) { /* best-effort */ }
    }

    /** Send Logon (35=A) with ResetSeqNumFlag=Y. */
    private void sendLogon() throws IOException {
        sb.setLength(0);
        appendField(sb, "35", "A");
        appendField(sb, "49", senderCompId);
        appendField(sb, "56", targetCompId);
        appendField(sb, "34", Integer.toString(outSeqNum));
        appendField(sb, "52", utcTimestamp());
        appendField(sb, "98", "0");
        appendField(sb, "108", Integer.toString(HEARTBEAT_SEC));
        appendField(sb, "141", "Y");
        sendFramedThrowing();
    }

    /** Send a Heartbeat. {@code testReqId} is null on unsolicited beats. */
    private void sendHeartbeat(String testReqId) throws IOException {
        if (!connected || out == null) return;
        sb.setLength(0);
        appendField(sb, "35", "0");
        appendField(sb, "49", senderCompId);
        appendField(sb, "56", targetCompId);
        appendField(sb, "34", Integer.toString(outSeqNum));
        appendField(sb, "52", utcTimestamp());
        if (testReqId != null && !testReqId.isEmpty()) {
            appendField(sb, "112", testReqId);
        }
        sendFramedThrowing();
    }

    /** Close: send Logout then close socket. */
    public void close() {
        try {
            sendLogout();
        } catch (RuntimeException ignored) {
            // ignore
        }
        disconnect();
    }

    // ---------- framing ----------

    private boolean sendFramed() {
        try {
            sendFramedThrowing();
            return true;
        } catch (IOException e) {
            LOG.warn("[{}] write error: {}", senderCompId, e.toString());
            connected = false;
            return false;
        }
    }

    private void sendFramedThrowing() throws IOException {
        // body = current sb contents
        int bodyLen = sb.length();
        StringBuilder full = new StringBuilder(bodyLen + 32);
        full.append("8=FIX.4.4").append(SOH).append("9=").append(bodyLen).append(SOH).append(sb);
        int sum = 0;
        for (int i = 0; i < full.length(); i++) sum += full.charAt(i);
        sum &= 0xFF;
        full.append("10=");
        if (sum < 100) full.append('0');
        if (sum < 10) full.append('0');
        full.append(sum).append(SOH);
        byte[] bytes = full.toString().getBytes(StandardCharsets.US_ASCII);
        out.write(bytes);
        out.flush();
        outSeqNum++;
        lastOutboundNs = Clocks.nanoTime();
    }

    private static void appendField(StringBuilder sb, String tag, String value) {
        sb.append(tag).append('=').append(value).append(SOH);
    }

    private static void appendClOrdField(StringBuilder sb, byte[] clOrdId16) {
        sb.append("11=");
        appendTrimmed(sb, clOrdId16);
        sb.append(SOH);
    }

    private static void appendClOrdField(StringBuilder sb, String tag, byte[] clOrdId16) {
        sb.append(tag).append('=');
        appendTrimmed(sb, clOrdId16);
        sb.append(SOH);
    }

    private static void appendTrimmed(StringBuilder sb, byte[] clOrdId16) {
        int len = clOrdId16.length;
        while (len > 0 && clOrdId16[len - 1] == 0) len--;
        for (int i = 0; i < len; i++) {
            sb.append((char) (clOrdId16[i] & 0xFF));
        }
    }

    /** Format a fixed-point times-10000 price as decimal text. Allocates. */
    static String formatPrice(long fp) {
        if (fp == 0L) return "0";
        long abs = Math.abs(fp);
        long whole = abs / 10000L;
        long frac = abs % 10000L;
        StringBuilder out = new StringBuilder(16);
        if (fp < 0L) out.append('-');
        out.append(whole);
        if (frac != 0L) {
            out.append('.');
            // emit 4 decimal digits, then strip trailing zeros
            int divisor = 1000;
            int len4 = 4;
            byte[] tmp = new byte[4];
            int idx = 0;
            int rem = len4;
            while (rem-- > 0) {
                int d = (int) ((frac / divisor) % 10);
                tmp[idx++] = (byte) ('0' + d);
                divisor /= 10;
            }
            int last = idx - 1;
            while (last > 0 && tmp[last] == '0') last--;
            for (int i = 0; i <= last; i++) out.append((char) tmp[i]);
        }
        return out.toString();
    }

    /** UTC timestamp formatted as YYYYMMDD-HH:MM:SS.sss. Allocates one String. */
    static String utcTimestamp() {
        java.time.LocalDateTime now = java.time.LocalDateTime.now(java.time.ZoneOffset.UTC);
        return String.format("%04d%02d%02d-%02d:%02d:%02d.%03d",
                now.getYear(), now.getMonthValue(), now.getDayOfMonth(),
                now.getHour(), now.getMinute(), now.getSecond(),
                now.getNano() / 1_000_000);
    }

    /** Spec for an OrderCancelRequest. */
    public static final class CancelSpec {
        public final byte[] clOrdId = new byte[16];
        public final byte[] origClOrdId = new byte[16];
        public String symbol;
        public byte side;
        public long account;
    }
}
