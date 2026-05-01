package com.exchange.fix.test;

import com.exchange.fix.FixCodec;
import com.exchange.fix.FixDecodeException;
import com.exchange.fix.FixMessage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * Tiny synchronous FIX client used by gateway tests and (eventually) the
 * load harness. Wraps a {@link Socket} with a single-threaded send/receive
 * API.
 */
public final class TestFixClient implements AutoCloseable {

    /** FIX field separator (SOH = 0x01). */
    public static final char SOH = '';
    private static final String SOH_STR = Character.toString(SOH);

    private final Socket socket;
    private final InputStream in;
    private final OutputStream out;
    private final byte[] readBuf = new byte[16384];
    private int readBufFill = 0;

    public final byte[] senderCompId;
    public final byte[] targetCompId;
    public final String senderCompIdStr;
    public final String targetCompIdStr;

    public int outSeqNum = 1;

    public TestFixClient(String host, int port,
                         String senderCompId,
                         String targetCompId) throws IOException {
        this.socket = new Socket(host, port);
        this.socket.setTcpNoDelay(true);
        this.in = socket.getInputStream();
        this.out = socket.getOutputStream();
        this.senderCompIdStr = senderCompId;
        this.targetCompIdStr = targetCompId;
        this.senderCompId = senderCompId.getBytes(StandardCharsets.US_ASCII);
        this.targetCompId = targetCompId.getBytes(StandardCharsets.US_ASCII);
    }

    public void sendLogon(int heartBtInt, boolean resetSeq) throws IOException {
        StringBuilder body = new StringBuilder();
        body.append("35=A").append(SOH);
        body.append("49=").append(senderCompIdStr).append(SOH);
        body.append("56=").append(targetCompIdStr).append(SOH);
        body.append("34=").append(outSeqNum).append(SOH);
        body.append("52=").append(timestamp()).append(SOH);
        body.append("98=0").append(SOH);
        body.append("108=").append(heartBtInt).append(SOH);
        if (resetSeq) body.append("141=Y").append(SOH);
        sendFramed(body.toString());
        outSeqNum++;
    }

    public void sendHeartbeat() throws IOException {
        StringBuilder body = new StringBuilder();
        body.append("35=0").append(SOH);
        body.append("49=").append(senderCompIdStr).append(SOH);
        body.append("56=").append(targetCompIdStr).append(SOH);
        body.append("34=").append(outSeqNum).append(SOH);
        body.append("52=").append(timestamp()).append(SOH);
        sendFramed(body.toString());
        outSeqNum++;
    }

    public void sendLogout(String reason) throws IOException {
        StringBuilder body = new StringBuilder();
        body.append("35=5").append(SOH);
        body.append("49=").append(senderCompIdStr).append(SOH);
        body.append("56=").append(targetCompIdStr).append(SOH);
        body.append("34=").append(outSeqNum).append(SOH);
        body.append("52=").append(timestamp()).append(SOH);
        if (reason != null) body.append("58=").append(reason).append(SOH);
        sendFramed(body.toString());
        outSeqNum++;
    }

    public void sendNewOrderSingle(String clOrdId, String symbol, char side,
                                   long qty, long priceFp, char ordType,
                                   long account) throws IOException {
        StringBuilder body = new StringBuilder();
        body.append("35=D").append(SOH);
        body.append("49=").append(senderCompIdStr).append(SOH);
        body.append("56=").append(targetCompIdStr).append(SOH);
        body.append("34=").append(outSeqNum).append(SOH);
        body.append("52=").append(timestamp()).append(SOH);
        body.append("11=").append(clOrdId).append(SOH);
        body.append("55=").append(symbol).append(SOH);
        body.append("54=").append(side).append(SOH);
        body.append("60=").append(timestamp()).append(SOH);
        body.append("38=").append(qty).append(SOH);
        body.append("40=").append(ordType).append(SOH);
        if (priceFp > 0) {
            body.append("44=").append(formatPrice(priceFp)).append(SOH);
        }
        body.append("1=").append(account).append(SOH);
        body.append("59=0").append(SOH);
        sendFramed(body.toString());
        outSeqNum++;
    }

    public void sendCancel(String clOrdId, String origClOrdId, String symbol,
                           char side, long account) throws IOException {
        StringBuilder body = new StringBuilder();
        body.append("35=F").append(SOH);
        body.append("49=").append(senderCompIdStr).append(SOH);
        body.append("56=").append(targetCompIdStr).append(SOH);
        body.append("34=").append(outSeqNum).append(SOH);
        body.append("52=").append(timestamp()).append(SOH);
        body.append("11=").append(clOrdId).append(SOH);
        body.append("41=").append(origClOrdId).append(SOH);
        body.append("55=").append(symbol).append(SOH);
        body.append("54=").append(side).append(SOH);
        body.append("1=").append(account).append(SOH);
        body.append("60=").append(timestamp()).append(SOH);
        sendFramed(body.toString());
        outSeqNum++;
    }

    private void sendFramed(String body) throws IOException {
        int bodyLen = body.length();
        String header = "8=FIX.4.4" + SOH + "9=" + bodyLen + SOH;
        String full = header + body;
        int sum = 0;
        for (int i = 0; i < full.length(); i++) sum += full.charAt(i);
        sum &= 0xFF;
        String trailer = String.format("10=%03d", sum) + SOH;
        String wire = full + trailer;
        byte[] bytes = wire.getBytes(StandardCharsets.US_ASCII);
        out.write(bytes);
        out.flush();
    }

    /** Block until a complete FIX message arrives or timeout. */
    public FixMessage receive(long timeoutMs) throws IOException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        socket.setSoTimeout(50);
        FixMessage msg = new FixMessage();
        while (System.currentTimeMillis() < deadline) {
            int consumed = tryDecode(msg);
            if (consumed > 0) {
                shift(consumed);
                return msg;
            }
            try {
                int n = in.read(readBuf, readBufFill, readBuf.length - readBufFill);
                if (n < 0) {
                    return null;
                }
                if (n > 0) {
                    readBufFill += n;
                }
            } catch (java.net.SocketTimeoutException te) {
                // continue waiting
            }
        }
        return null;
    }

    private int tryDecode(FixMessage msg) {
        try {
            return FixCodec.decode(readBuf, 0, readBufFill, msg);
        } catch (FixDecodeException e) {
            return -1;
        }
    }

    private void shift(int n) {
        if (n > readBufFill) n = readBufFill;
        System.arraycopy(readBuf, n, readBuf, 0, readBufFill - n);
        readBufFill -= n;
    }

    private static String formatPrice(long fixedPointTimes10000) {
        long whole = fixedPointTimes10000 / 10000L;
        long frac = fixedPointTimes10000 % 10000L;
        if (frac == 0) {
            return Long.toString(whole);
        }
        StringBuilder sb = new StringBuilder();
        sb.append(whole);
        sb.append('.');
        String f = Long.toString(frac);
        for (int i = f.length(); i < 4; i++) sb.append('0');
        sb.append(f);
        while (sb.charAt(sb.length() - 1) == '0') sb.setLength(sb.length() - 1);
        return sb.toString();
    }

    private static String timestamp() {
        java.time.LocalDateTime now = java.time.LocalDateTime.now(java.time.ZoneOffset.UTC);
        return String.format("%04d%02d%02d-%02d:%02d:%02d.%03d",
                now.getYear(), now.getMonthValue(), now.getDayOfMonth(),
                now.getHour(), now.getMinute(), now.getSecond(),
                now.getNano() / 1_000_000);
    }

    public boolean isConnected() {
        return socket.isConnected() && !socket.isClosed();
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }
}
