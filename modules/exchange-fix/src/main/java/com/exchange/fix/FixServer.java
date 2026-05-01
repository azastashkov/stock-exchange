package com.exchange.fix;

import com.exchange.commands.CancelCommand;
import com.exchange.commands.CancelReplaceCommand;
import com.exchange.commands.CommandCodec;
import com.exchange.commands.ExecutionReportCommand;
import com.exchange.commands.NewOrderCommand;
import com.exchange.domain.ClOrdId;
import com.exchange.domain.OrdType;
import com.exchange.domain.RejectReason;
import com.exchange.domain.Side;
import com.exchange.domain.Symbols;
import com.exchange.domain.TimeInForce;
import com.exchange.ipc.MutableSlice;
import com.exchange.ipc.RingConsumer;
import com.exchange.ipc.RingProducer;
import com.exchange.loop.ApplicationLoop;
import com.exchange.time.Clocks;
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
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Iterator;

/**
 * Hand-rolled FIX 4.4 gateway. Listens on a TCP port, accepts FIX sessions,
 * and bridges them with the exchange's binary command rings.
 * <p>
 * Two threads drive the gateway:
 * <ul>
 *   <li>{@code gateway-io} — single NIO selector loop. Owns the
 *       {@link Selector}, all SocketChannels, and the per-session read &amp;
 *       write buffers.</li>
 *   <li>{@code gateway-out} — reads execution reports off the gateway-out
 *       ring and stages encoded bytes onto each session's write buffer
 *       under {@link FixSession#lock}. The io loop drains those bytes to
 *       the socket on its next iteration.</li>
 * </ul>
 * Between the two threads, the {@link FixSession#writeBuffer} and
 * {@link FixSession#lock} are the only shared mutable state. The lock is
 * fast and contended only for the brief moment of an outbound stage; idle
 * paths take it in the io loop only when there is something to drain.
 */
public final class FixServer {

    private static final Logger LOG = LoggerFactory.getLogger(FixServer.class);

    /** Per-thread scratch buffer used by the outbound encoder. */
    private static final ThreadLocal<byte[]> ENCODE_SCRATCH = ThreadLocal.withInitial(() -> new byte[4096]);

    private final int port;
    private final byte[] serverSenderCompId;
    private final String serverSenderCompIdStr;
    private final FixSessionRegistry registry;
    private final Symbols symbols;
    private final Validator validator;
    private final RingProducer inboundProducer;
    private final RingConsumer gatewayOutConsumer;
    private final RateLimiterFactory rateLimiterFactory;
    private final Path fixSeqDir;

    private ServerSocketChannel serverChannel;
    private Selector selector;

    private final IoLoop ioLoop;
    private final OutLoop outLoop;
    private volatile boolean started = false;

    /** Counter snapshots, useful for tests. */
    private long backpressureRejected = 0L;
    private long inboundOrdersAccepted = 0L;

    public FixServer(int port,
                     byte[] serverSenderCompId,
                     FixSessionRegistry registry,
                     Symbols symbols,
                     Validator validator,
                     RingProducer inboundProducer,
                     RingConsumer gatewayOutConsumer,
                     RateLimiterFactory rateLimiterFactory,
                     Path fixSeqDir) {
        this.port = port;
        this.serverSenderCompId = serverSenderCompId.clone();
        this.serverSenderCompIdStr = new String(this.serverSenderCompId, StandardCharsets.US_ASCII);
        this.registry = registry;
        this.symbols = symbols;
        this.validator = validator;
        this.inboundProducer = inboundProducer;
        this.gatewayOutConsumer = gatewayOutConsumer;
        this.rateLimiterFactory = rateLimiterFactory;
        this.fixSeqDir = fixSeqDir;
        this.ioLoop = new IoLoop();
        this.outLoop = new OutLoop();
    }

    public synchronized void start() throws IOException {
        if (started) return;
        selector = Selector.open();
        serverChannel = ServerSocketChannel.open();
        serverChannel.configureBlocking(false);
        serverChannel.socket().setReuseAddress(true);
        serverChannel.bind(new InetSocketAddress(port));
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);
        started = true;
        ioLoop.start();
        outLoop.start();
    }

    public synchronized void stop() {
        if (!started) return;
        started = false;
        ioLoop.stop();
        outLoop.stop();
        FixSession[] sessions = registry.snapshot();
        for (FixSession s : sessions) {
            try { s.channel.close(); } catch (IOException ignored) { /* best-effort */ }
        }
        if (serverChannel != null) {
            try { serverChannel.close(); } catch (IOException ignored) { /* best-effort */ }
        }
        if (selector != null) {
            try { selector.close(); } catch (IOException ignored) { /* best-effort */ }
        }
    }

    public int boundPort() {
        if (serverChannel == null) return 0;
        try {
            return ((InetSocketAddress) serverChannel.getLocalAddress()).getPort();
        } catch (IOException e) {
            return 0;
        }
    }

    /** Test/inspection: how many backpressure rejects we have emitted. */
    public long backpressureRejected() { return backpressureRejected; }

    /** Test/inspection: number of inbound orders forwarded to the ring. */
    public long inboundOrdersAccepted() { return inboundOrdersAccepted; }

    public ApplicationLoop ioLoop() { return ioLoop; }
    public ApplicationLoop outLoop() { return outLoop; }

    // ======================================================================
    // gateway-io loop
    // ======================================================================

    final class IoLoop extends ApplicationLoop {

        private final FixMessage scratchMsg = new FixMessage();
        private final ByteBuffer encodeBb;
        private final byte[] clOrdScratch = new byte[ClOrdId.LENGTH];
        private final byte[] origClOrdScratch = new byte[ClOrdId.LENGTH];

        IoLoop() {
            super("gateway-io", false);
            this.encodeBb = ByteBuffer.allocateDirect(CommandCodec.MAX_COMMAND_SIZE);
            this.encodeBb.order(java.nio.ByteOrder.LITTLE_ENDIAN);
        }

        @Override
        protected boolean pollOnce() {
            boolean did = false;
            try {
                int selected = selector.selectNow();
                if (selected > 0) {
                    Iterator<SelectionKey> it = selector.selectedKeys().iterator();
                    while (it.hasNext()) {
                        SelectionKey key = it.next();
                        it.remove();
                        if (!key.isValid()) continue;
                        if (key.isAcceptable()) {
                            handleAccept();
                            did = true;
                        }
                        if (key.isValid() && key.isReadable()) {
                            handleRead(key, key.attachment());
                            did = true;
                        }
                        if (key.isValid() && key.isWritable()) {
                            Object att = key.attachment();
                            if (att instanceof FixSession s) {
                                drainWriteBuffer(key, s);
                                did = true;
                            }
                        }
                    }
                }
                // Drain any sessions whose write buffers were filled by outLoop while we were blocked
                FixSession[] all = registry.snapshot();
                long now = Clocks.nanoTime();
                for (FixSession s : all) {
                    if (drainWriteBufferIfPending(s)) {
                        did = true;
                    }
                    checkLiveness(s, now);
                }
            } catch (IOException e) {
                LOG.warn("gateway-io selector error: {}", e.toString());
            }
            return did;
        }

        private void handleAccept() throws IOException {
            SocketChannel ch = serverChannel.accept();
            if (ch == null) return;
            ch.configureBlocking(false);
            ch.socket().setTcpNoDelay(true);
            // Sender unknown until Logon. Build a placeholder session — once logon arrives we
            // populate the rest. We don't register a session yet.
            ch.register(selector, SelectionKey.OP_READ, new PendingSession(ch));
        }

        private void handleRead(SelectionKey key, Object attachment) throws IOException {
            if (attachment instanceof PendingSession ps) {
                // Read into a temp buffer; we expect a Logon message first.
                int n = ps.channel.read(ps.readBuffer);
                if (n < 0) {
                    closePendingSession(key, ps);
                    return;
                }
                if (n == 0) return;
                // Attempt to decode a Logon
                attemptLogonDecode(key, ps);
                return;
            }
            FixSession session = (FixSession) attachment;
            int n;
            try {
                n = session.channel.read(session.readBuffer);
            } catch (IOException e) {
                closeSession(key, session, "read error: " + e.getMessage());
                return;
            }
            if (n < 0) {
                closeSession(key, session, "peer closed");
                return;
            }
            if (n == 0) return;
            session.lastInboundNanos = Clocks.nanoTime();
            decodeLoop(key, session);
        }

        private void attemptLogonDecode(SelectionKey key, PendingSession ps) {
            ByteBuffer rb = ps.readBuffer;
            rb.flip();
            byte[] backing = ps.scratch;
            int avail = rb.remaining();
            if (avail > backing.length) {
                ps.scratch = new byte[Math.max(backing.length * 2, avail)];
                backing = ps.scratch;
            }
            for (int i = 0; i < avail; i++) {
                backing[i] = rb.get(i);
            }
            int consumed;
            try {
                consumed = FixCodec.decode(backing, 0, avail, scratchMsg);
            } catch (FixDecodeException ex) {
                // Bad bytes from someone who hasn't logged in — close the connection
                closePendingSession(key, ps);
                return;
            }
            if (consumed == 0) {
                rb.compact();
                return;
            }
            // Confirm it is a Logon (35=A); else reject
            String mt = scratchMsg.msgType();
            if (!FixMsgType.LOGON.equals(mt)) {
                closePendingSession(key, ps);
                return;
            }
            String senderCompId = scratchMsg.getString(FixField.SENDER_COMP_ID);
            if (senderCompId.isEmpty()) {
                closePendingSession(key, ps);
                return;
            }
            // Check no duplicate
            FixSession existing = registry.bySenderCompId(senderCompId);
            if (existing != null && existing.loggedOn) {
                closePendingSession(key, ps);
                return;
            }

            // Build the FixSession
            int heartBtInt = scratchMsg.has(FixField.HEART_BT_INT)
                    ? scratchMsg.getInt(FixField.HEART_BT_INT)
                    : 30;
            RateLimiter rl = rateLimiterFactory == null ? null : rateLimiterFactory.create(senderCompId);
            Path seqFile = fixSeqDir.resolve(safeFileName(senderCompId) + ".seq");
            FixSession s = new FixSession(senderCompId, serverSenderCompIdStr, ps.channel, rl, seqFile);
            s.heartBtInt = heartBtInt;
            try {
                s.load();
            } catch (IOException e) {
                LOG.warn("Failed to load seq file {}: {}", seqFile, e.toString());
            }
            // ResetSeqNumFlag=Y → reset both sides
            boolean resetRequested = scratchMsg.has(FixField.RESET_SEQ_NUM_FLAG)
                    && scratchMsg.getChar(FixField.RESET_SEQ_NUM_FLAG) == 'Y';
            int incomingSeq = scratchMsg.getInt(FixField.MSG_SEQ_NUM);
            if (resetRequested) {
                s.inSeqNumExpected = FixSession.INITIAL_SEQ_NUM;
                s.outSeqNum = FixSession.INITIAL_SEQ_NUM;
            }
            // Validate seq num
            if (!resetRequested && incomingSeq < s.inSeqNumExpected) {
                // Sequence too low without PossDup — protocol error, close
                LOG.warn("Logon seq too low for {}: got {} expected {}", senderCompId, incomingSeq, s.inSeqNumExpected);
                closePendingSession(key, ps);
                return;
            }
            // Re-attach as fully-fledged session
            key.attach(s);
            registry.register(s);
            // Copy any leftover bytes into session.readBuffer
            int leftover = avail - consumed;
            if (leftover > 0) {
                s.readBuffer.put(backing, consumed, leftover);
            }
            s.loggedOn = true;
            // Consume the logon seq num
            s.inSeqNumExpected = incomingSeq + 1;
            persist(s);
            // Send Logon response
            sendLogonResponse(s);
            // If the client's seq num was higher than expected, request a resend
            if (!resetRequested && incomingSeq > s.inSeqNumExpected - 1 + 0) {
                // Note: at this point s.inSeqNumExpected has been advanced by 1 already.
                // The condition for "client ahead of us" is: incomingSeq > prevExpected.
                // But we've already accepted this Logon, so any gap was on the future side.
                // We don't issue a ResendRequest here for the v1 admit-only behaviour.
            }
            // Continue decoding any leftover bytes (could be next message)
            decodeLoop(key, s);
        }

        private void closePendingSession(SelectionKey key, PendingSession ps) {
            try { ps.channel.close(); } catch (IOException ignored) { /* best-effort */ }
            key.cancel();
        }

        private void decodeLoop(SelectionKey key, FixSession session) {
            ByteBuffer rb = session.readBuffer;
            rb.flip();
            // Copy the bytes to a heap byte[] for the codec
            byte[] backing = ENCODE_SCRATCH.get();
            int avail = rb.remaining();
            if (avail > backing.length) {
                backing = new byte[Math.max(backing.length * 2, avail)];
            }
            for (int i = 0; i < avail; i++) {
                backing[i] = rb.get(i);
            }
            int total = 0;
            while (total < avail) {
                int consumed;
                try {
                    consumed = FixCodec.decode(backing, total, avail, scratchMsg);
                } catch (FixDecodeException ex) {
                    sendReject(session, 0, ex.reasonCode(), "Malformed FIX: " + ex.getMessage());
                    closeSession(key, session, "Malformed FIX");
                    return;
                }
                if (consumed == 0) break;
                handleMessage(key, session, consumed);
                total += consumed;
                if (!session.loggedOn || session.closing) break;
            }
            // Compact unread bytes back into readBuffer
            int leftover = avail - total;
            rb.clear();
            if (leftover > 0) {
                rb.put(backing, total, leftover);
            }
        }

        private void handleMessage(SelectionKey key, FixSession session, int frameLen) {
            String msgType = scratchMsg.msgType();
            int seq;
            try {
                seq = scratchMsg.getInt(FixField.MSG_SEQ_NUM);
            } catch (FixDecodeException e) {
                sendReject(session, 0, e.reasonCode(), e.getMessage());
                return;
            }

            // Sequence handling — applies to all admin/business messages except SequenceReset GapFill,
            // which is admin-only.
            boolean possDup = scratchMsg.has(FixField.POSS_DUP_FLAG)
                    && scratchMsg.getChar(FixField.POSS_DUP_FLAG) == 'Y';
            if (seq < session.inSeqNumExpected) {
                if (!possDup) {
                    LOG.warn("Seq below expected for {}: got {} expected {}",
                            session.senderCompIdAsClient, seq, session.inSeqNumExpected);
                    sendLogout(session, "MsgSeqNum below expected");
                    closeSession(key, session, "seq too low");
                    return;
                } else {
                    // ignore duplicate
                    return;
                }
            } else if (seq > session.inSeqNumExpected) {
                // Gap: ask for resend
                int begin = session.inSeqNumExpected;
                sendResendRequest(session, begin, 0);
                return;
            }

            // Rate limit
            if (!session.checkAndConsumeRate()) {
                sendBusinessReject(session, seq, msgType, 8, "rate limit exceeded");
                session.incInSeq();
                persist(session);
                return;
            }

            switch (msgType) {
                case FixMsgType.HEARTBEAT -> {
                    session.testRequestSent = false;
                    session.pendingTestReqId = null;
                    session.incInSeq();
                    persist(session);
                }
                case FixMsgType.TEST_REQUEST -> handleTestRequest(session);
                case FixMsgType.RESEND_REQUEST -> handleResendRequest(session);
                case FixMsgType.SEQUENCE_RESET -> handleSequenceReset(session);
                case FixMsgType.LOGOUT -> {
                    sendLogout(session, "ack");
                    session.incInSeq();
                    persist(session);
                    // Drain pending writes (the ack we just staged) before closing.
                    drainWriteBufferIfPending(session);
                    closeSession(key, session, "logout");
                }
                case FixMsgType.NEW_ORDER_SINGLE -> handleNewOrderSingle(session);
                case FixMsgType.ORDER_CANCEL_REQUEST -> handleCancel(session);
                case FixMsgType.ORDER_CANCEL_REPLACE_REQUEST -> handleCancelReplace(session);
                case FixMsgType.LOGON -> {
                    // Already logged on — protocol violation
                    sendLogout(session, "duplicate Logon");
                    closeSession(key, session, "duplicate Logon");
                }
                default -> {
                    sendBusinessReject(session, seq, msgType, 3, "unsupported MsgType");
                    session.incInSeq();
                    persist(session);
                }
            }
        }

        private void handleTestRequest(FixSession session) {
            // Reply with a Heartbeat carrying TestReqID
            int len = scratchMsg.valueLength(FixField.TEST_REQ_ID);
            byte[] testReqId = null;
            if (len > 0) {
                testReqId = new byte[len];
                scratchMsg.getBytes(FixField.TEST_REQ_ID, testReqId, 0);
            }
            sendHeartbeatWithTestReqId(session, testReqId);
            session.incInSeq();
            persist(session);
        }

        private void handleResendRequest(FixSession session) {
            // For v1 we admit gaps via SequenceReset-GapFill from our current seq num.
            int begin;
            try {
                begin = scratchMsg.getInt(FixField.BEGIN_SEQ_NO);
                // EndSeqNo is parsed but unused (admit-gap policy).
                scratchMsg.getInt(FixField.END_SEQ_NO);
            } catch (FixDecodeException e) {
                sendReject(session, scratchMsg.getInt(FixField.MSG_SEQ_NUM), e.reasonCode(), e.getMessage());
                return;
            }
            // We send SequenceReset-GapFill with NewSeqNo = our current outSeqNum,
            // re-using the requested begin as the seq num of the gap-fill message.
            int newSeqNo = session.outSeqNum;
            int origOut = session.outSeqNum;
            session.outSeqNum = begin; // craft the gap-fill at the requested begin
            byte[] enc = ENCODE_SCRATCH.get();
            int n = FixEncoder.writeSequenceReset(enc, 0,
                    session.senderCompIdBytes, session.targetCompIdBytes,
                    session.outSeqNum, newSeqNo, true,
                    System.currentTimeMillis());
            session.outSeqNum = origOut; // restore: gap-fill doesn't consume our seq
            stage(session, enc, 0, n);
            session.incInSeq();
            persist(session);
        }

        private void handleSequenceReset(FixSession session) {
            boolean gapFill = scratchMsg.has(FixField.GAP_FILL_FLAG)
                    && scratchMsg.getChar(FixField.GAP_FILL_FLAG) == 'Y';
            int newSeqNo;
            try {
                newSeqNo = scratchMsg.getInt(FixField.NEW_SEQ_NO);
            } catch (FixDecodeException e) {
                sendReject(session, 0, e.reasonCode(), e.getMessage());
                return;
            }
            if (newSeqNo > session.inSeqNumExpected) {
                session.inSeqNumExpected = newSeqNo;
            }
            if (gapFill) {
                // Just advance silently
            }
            persist(session);
        }

        private void handleNewOrderSingle(FixSession session) {
            byte reason = validator.validateNewOrder(scratchMsg);
            int incomingSeq = scratchMsg.getInt(FixField.MSG_SEQ_NUM);
            if (reason != Validator.OK) {
                sendBusinessReject(session, incomingSeq, FixMsgType.NEW_ORDER_SINGLE, reason & 0xFF,
                        RejectReason.description(reason));
                session.incInSeq();
                persist(session);
                return;
            }
            int symbolId = validator.resolveSymbol(scratchMsg);
            byte side = scratchMsg.getChar(FixField.SIDE) == '1' ? Side.CODE_BUY : Side.CODE_SELL;
            byte ordType = scratchMsg.getChar(FixField.ORD_TYPE) == '1' ? OrdType.CODE_MARKET : OrdType.CODE_LIMIT;
            byte tif = TimeInForce.CODE_DAY;
            if (scratchMsg.has(FixField.TIME_IN_FORCE)) {
                byte t = scratchMsg.getChar(FixField.TIME_IN_FORCE);
                if (t == '3') tif = TimeInForce.CODE_IOC;
            }
            long qty = scratchMsg.getLong(FixField.ORDER_QTY);
            long price = scratchMsg.has(FixField.PRICE) ? validator.readPriceFixedPoint(scratchMsg) : 0L;
            long account = scratchMsg.has(FixField.ACCOUNT) ? scratchMsg.getLong(FixField.ACCOUNT) : 0L;
            int clLen = scratchMsg.valueLength(FixField.CL_ORD_ID);
            if (clLen > ClOrdId.LENGTH) {
                sendBusinessReject(session, incomingSeq, FixMsgType.NEW_ORDER_SINGLE,
                        RejectReason.UNKNOWN_ORDER & 0xFF, "ClOrdID too long");
                session.incInSeq();
                persist(session);
                return;
            }
            scratchMsg.getBytes(FixField.CL_ORD_ID, clOrdScratch, 0);
            long clientTs = Clocks.nanoTime();
            long senderId = session.sessionId;
            long sessionId = session.sessionId;
            ByteBuffer out = scratchEncodeBb();
            NewOrderCommand.encode(out, 0,
                    side, ordType, tif,
                    symbolId, qty, price, account,
                    senderId, sessionId, clOrdScratch, clientTs);
            out.position(0).limit(NewOrderCommand.SIZE);
            if (!inboundProducer.offer(out)) {
                backpressureRejected++;
                sendBusinessReject(session, incomingSeq, FixMsgType.NEW_ORDER_SINGLE,
                        8, "system busy");
                session.incInSeq();
                persist(session);
                return;
            }
            inboundOrdersAccepted++;
            session.incInSeq();
            persist(session);
        }

        private void handleCancel(FixSession session) {
            byte reason = validator.validateCancel(scratchMsg);
            int incomingSeq = scratchMsg.getInt(FixField.MSG_SEQ_NUM);
            if (reason != Validator.OK) {
                sendBusinessReject(session, incomingSeq, FixMsgType.ORDER_CANCEL_REQUEST,
                        reason & 0xFF, RejectReason.description(reason));
                session.incInSeq();
                persist(session);
                return;
            }
            int symbolId = validator.resolveSymbol(scratchMsg);
            long account = scratchMsg.has(FixField.ACCOUNT) ? scratchMsg.getLong(FixField.ACCOUNT) : 0L;
            scratchMsg.getBytes(FixField.CL_ORD_ID, clOrdScratch, 0);
            scratchMsg.getBytes(FixField.ORIG_CL_ORD_ID, origClOrdScratch, 0);
            long clientTs = Clocks.nanoTime();
            ByteBuffer out = scratchEncodeBb();
            CancelCommand.encode(out, 0,
                    symbolId, account,
                    session.sessionId, session.sessionId,
                    clOrdScratch, origClOrdScratch, clientTs);
            out.position(0).limit(CancelCommand.SIZE);
            if (!inboundProducer.offer(out)) {
                backpressureRejected++;
                sendBusinessReject(session, incomingSeq, FixMsgType.ORDER_CANCEL_REQUEST, 8, "system busy");
                session.incInSeq();
                persist(session);
                return;
            }
            session.incInSeq();
            persist(session);
        }

        private void handleCancelReplace(FixSession session) {
            byte reason = validator.validateCancelReplace(scratchMsg);
            int incomingSeq = scratchMsg.getInt(FixField.MSG_SEQ_NUM);
            if (reason != Validator.OK) {
                sendBusinessReject(session, incomingSeq, FixMsgType.ORDER_CANCEL_REPLACE_REQUEST,
                        reason & 0xFF, RejectReason.description(reason));
                session.incInSeq();
                persist(session);
                return;
            }
            int symbolId = validator.resolveSymbol(scratchMsg);
            byte side = scratchMsg.getChar(FixField.SIDE) == '1' ? Side.CODE_BUY : Side.CODE_SELL;
            long qty = scratchMsg.getLong(FixField.ORDER_QTY);
            long price = scratchMsg.has(FixField.PRICE) ? validator.readPriceFixedPoint(scratchMsg) : 0L;
            long account = scratchMsg.has(FixField.ACCOUNT) ? scratchMsg.getLong(FixField.ACCOUNT) : 0L;
            scratchMsg.getBytes(FixField.CL_ORD_ID, clOrdScratch, 0);
            scratchMsg.getBytes(FixField.ORIG_CL_ORD_ID, origClOrdScratch, 0);
            long clientTs = Clocks.nanoTime();
            ByteBuffer out = scratchEncodeBb();
            CancelReplaceCommand.encode(out, 0,
                    side, symbolId, qty, price, account,
                    session.sessionId, session.sessionId,
                    clOrdScratch, origClOrdScratch, clientTs);
            out.position(0).limit(CancelReplaceCommand.SIZE);
            if (!inboundProducer.offer(out)) {
                backpressureRejected++;
                sendBusinessReject(session, incomingSeq, FixMsgType.ORDER_CANCEL_REPLACE_REQUEST,
                        8, "system busy");
                session.incInSeq();
                persist(session);
                return;
            }
            session.incInSeq();
            persist(session);
        }

        private ByteBuffer scratchEncodeBb() {
            encodeBb.clear();
            return encodeBb;
        }

        private void checkLiveness(FixSession session, long nowNanos) {
            if (!session.loggedOn || session.closing) return;
            long hbNanos = (long) session.heartBtInt * 1_000_000_000L;
            if (hbNanos == 0) return;
            // Send Heartbeat if no outbound for hbNanos
            if (nowNanos - session.lastOutboundNanos > hbNanos) {
                sendHeartbeat(session);
            }
            // Detect stale inbound
            long sinceIn = nowNanos - session.lastInboundNanos;
            if (sinceIn > (long) (hbNanos * 2.5)) {
                sendLogout(session, "no heartbeat received");
                // Close on the io loop's next iteration; we look up keys via channel
                try {
                    SelectionKey k = session.channel.keyFor(selector);
                    if (k != null) closeSession(k, session, "stale");
                    else session.channel.close();
                } catch (IOException ignored) { /* best-effort */ }
            } else if (sinceIn > (long) (hbNanos * 1.5) && !session.testRequestSent) {
                sendTestRequest(session);
            }
        }

        private void sendLogonResponse(FixSession session) {
            byte[] enc = ENCODE_SCRATCH.get();
            int n = FixEncoder.writeLogonResponse(enc, 0,
                    session.senderCompIdBytes,
                    session.targetCompIdBytes,
                    session.outSeqNum, session.heartBtInt,
                    System.currentTimeMillis());
            session.outSeqNum++;
            stage(session, enc, 0, n);
            persist(session);
        }

        private void sendHeartbeat(FixSession session) {
            byte[] enc = ENCODE_SCRATCH.get();
            int n = FixEncoder.writeHeartbeat(enc, 0,
                    session.senderCompIdBytes,
                    session.targetCompIdBytes,
                    session.outSeqNum, null,
                    System.currentTimeMillis());
            session.outSeqNum++;
            stage(session, enc, 0, n);
            persist(session);
        }

        private void sendHeartbeatWithTestReqId(FixSession session, byte[] testReqId) {
            byte[] enc = ENCODE_SCRATCH.get();
            int n = FixEncoder.writeHeartbeat(enc, 0,
                    session.senderCompIdBytes,
                    session.targetCompIdBytes,
                    session.outSeqNum, testReqId,
                    System.currentTimeMillis());
            session.outSeqNum++;
            stage(session, enc, 0, n);
            persist(session);
        }

        private void sendTestRequest(FixSession session) {
            byte[] enc = ENCODE_SCRATCH.get();
            byte[] reqId = ("TR" + session.outSeqNum).getBytes(StandardCharsets.US_ASCII);
            int n = FixEncoder.writeTestRequest(enc, 0,
                    session.senderCompIdBytes,
                    session.targetCompIdBytes,
                    session.outSeqNum, reqId,
                    System.currentTimeMillis());
            session.outSeqNum++;
            session.testRequestSent = true;
            session.pendingTestReqId = reqId;
            stage(session, enc, 0, n);
            persist(session);
        }

        private void sendLogout(FixSession session, String text) {
            byte[] enc = ENCODE_SCRATCH.get();
            int n = FixEncoder.writeLogout(enc, 0,
                    session.senderCompIdBytes,
                    session.targetCompIdBytes,
                    session.outSeqNum, text,
                    System.currentTimeMillis());
            session.outSeqNum++;
            stage(session, enc, 0, n);
            persist(session);
        }

        private void sendReject(FixSession session, int refSeq, int reasonCode, String text) {
            byte[] enc = ENCODE_SCRATCH.get();
            int n = FixEncoder.writeReject(enc, 0,
                    session.senderCompIdBytes,
                    session.targetCompIdBytes,
                    session.outSeqNum, refSeq, reasonCode, text,
                    System.currentTimeMillis());
            session.outSeqNum++;
            stage(session, enc, 0, n);
            persist(session);
        }

        private void sendBusinessReject(FixSession session, int refSeq, String refMsgType,
                                        int reasonCode, String text) {
            byte[] enc = ENCODE_SCRATCH.get();
            int n = FixEncoder.writeBusinessMessageReject(enc, 0,
                    session.senderCompIdBytes,
                    session.targetCompIdBytes,
                    session.outSeqNum, refSeq, refMsgType, reasonCode, text,
                    System.currentTimeMillis());
            session.outSeqNum++;
            stage(session, enc, 0, n);
            persist(session);
        }

        private void sendResendRequest(FixSession session, int begin, int end) {
            byte[] enc = ENCODE_SCRATCH.get();
            int n = FixEncoder.writeResendRequest(enc, 0,
                    session.senderCompIdBytes,
                    session.targetCompIdBytes,
                    session.outSeqNum, begin, end,
                    System.currentTimeMillis());
            session.outSeqNum++;
            stage(session, enc, 0, n);
            persist(session);
        }

        private void closeSession(SelectionKey key, FixSession session, String reason) {
            if (session.closing) return;
            session.closing = true;
            // Force-flush seq nums on close so reconnects pick up where we left off.
            try { session.persist(); } catch (IOException e) { LOG.warn("final persist for {} failed: {}", session.senderCompIdAsClient, e.toString()); }
            registry.remove(session.senderCompIdAsClient);
            try { session.channel.close(); } catch (IOException ignored) { /* best-effort */ }
            if (key != null) key.cancel();
            LOG.info("Closed FIX session {}: {}", session.senderCompIdAsClient, reason);
        }
    }

    // ======================================================================
    // gateway-out loop
    // ======================================================================

    final class OutLoop extends ApplicationLoop {

        private final MutableSlice slice = new MutableSlice();
        private final ExecutionReportCommand.MutableExecutionReport mutEr =
                new ExecutionReportCommand.MutableExecutionReport();
        private final it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap<byte[]> symBytesCache =
                new it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap<>(64);
        private final byte[] unknownSymbolBytes = "UNKNOWN".getBytes(StandardCharsets.US_ASCII);

        OutLoop() {
            super("gateway-out", false);
        }

        private byte[] symBytesFor(int symbolId) {
            byte[] cached = symBytesCache.get(symbolId);
            if (cached != null) return cached;
            String name = symbols.contains(symbolId) ? symbols.nameOf(symbolId) : null;
            if (name == null) return unknownSymbolBytes;
            byte[] b = name.getBytes(StandardCharsets.US_ASCII);
            symBytesCache.put(symbolId, b);
            return b;
        }

        @Override
        protected boolean pollOnce() {
            if (!gatewayOutConsumer.poll(slice)) return false;
            ByteBuffer buf = slice.buffer;
            int basePos = buf.position();
            ExecutionReportCommand.decode(buf, basePos, mutEr);
            FixSession session = registry.bySessionId(mutEr.sessionId);
            if (session == null || !session.loggedOn || session.closing) {
                // No live session — drop. Reporter has already emitted; client is gone.
                return true;
            }
            byte[] enc = ENCODE_SCRATCH.get();
            byte[] symBytes = symBytesFor(mutEr.symbolId);
            int seqNum;
            int n;
            // Acquire next outbound seq num and encode under the session lock so
            // the io loop sees a consistent state.
            synchronized (session.lock) {
                seqNum = session.outSeqNum++;
                n = FixEncoder.writeExecutionReport(enc, 0,
                        session.senderCompIdBytes,
                        session.targetCompIdBytes,
                        seqNum, mutEr, symBytes,
                        System.currentTimeMillis());
                appendToWriteBuffer(session, enc, 0, n);
            }
            try { session.persistThrottled(); } catch (IOException e) { LOG.warn("persist failed: {}", e.toString()); }
            // The IoLoop runs selectNow() in a busy loop, so no wakeup needed.
            return true;
        }
    }

    // ======================================================================
    // Outbound buffer plumbing
    // ======================================================================

    /** Append {@code len} bytes from {@code src} to {@code session.writeBuffer} (under the session lock). */
    static void appendToWriteBuffer(FixSession session, byte[] src, int off, int len) {
        // session.lock must be held by caller
        ByteBuffer wb = session.writeBuffer;
        if (wb.remaining() < len) {
            // Compact and try again. If still insufficient, grow is not supported (direct buf);
            // best-effort: log and drop.
            wb.flip();
            wb.compact();
            if (wb.remaining() < len) {
                LOG.warn("Write buffer overflow on {} (drop {} bytes)", session.senderCompIdAsClient, len);
                return;
            }
        }
        wb.put(src, off, len);
        session.lastOutboundNanos = Clocks.nanoTime();
    }

    /** Stage from the io loop side: same path, but acquires the lock. */
    void stage(FixSession session, byte[] src, int off, int len) {
        synchronized (session.lock) {
            appendToWriteBuffer(session, src, off, len);
        }
        // After staging, request OP_WRITE so the next selectNow drains it
        SelectionKey k = session.channel.keyFor(selector);
        if (k != null && k.isValid()) {
            k.interestOps(k.interestOps() | SelectionKey.OP_WRITE);
        }
    }

    private boolean drainWriteBufferIfPending(FixSession session) {
        boolean did;
        synchronized (session.lock) {
            ByteBuffer wb = session.writeBuffer;
            wb.flip();
            int n = wb.remaining();
            if (n == 0) {
                wb.compact();
                return false;
            }
            try {
                int written = session.channel.write(wb);
                did = written > 0;
            } catch (ClosedChannelException ce) {
                wb.compact();
                return false;
            } catch (IOException e) {
                LOG.warn("write to {} failed: {}", session.senderCompIdAsClient, e.toString());
                wb.compact();
                return false;
            }
            wb.compact();
            // If unsent data remains, ensure OP_WRITE is set
            if (wb.position() > 0) {
                SelectionKey k = session.channel.keyFor(selector);
                if (k != null && k.isValid()) {
                    k.interestOps(k.interestOps() | SelectionKey.OP_WRITE);
                }
            } else {
                SelectionKey k = session.channel.keyFor(selector);
                if (k != null && k.isValid() && (k.interestOps() & SelectionKey.OP_WRITE) != 0) {
                    k.interestOps(k.interestOps() & ~SelectionKey.OP_WRITE);
                }
            }
        }
        return did;
    }

    private void drainWriteBuffer(SelectionKey key, FixSession session) {
        drainWriteBufferIfPending(session);
    }

    private void persist(FixSession session) {
        try {
            session.persistThrottled();
        } catch (IOException e) {
            LOG.warn("persist seq for {} failed: {}", session.senderCompIdAsClient, e.toString());
        }
    }

    /** Pre-logon attachment: a SocketChannel and a small read buffer. */
    static final class PendingSession {
        final SocketChannel channel;
        final ByteBuffer readBuffer = ByteBuffer.allocate(8192);
        byte[] scratch = new byte[8192];

        PendingSession(SocketChannel channel) {
            this.channel = channel;
        }
    }

    private static String safeFileName(String s) {
        StringBuilder sb = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (Character.isLetterOrDigit(c) || c == '_' || c == '-' || c == '.') {
                sb.append(c);
            } else {
                sb.append('_');
            }
        }
        return sb.toString();
    }
}
