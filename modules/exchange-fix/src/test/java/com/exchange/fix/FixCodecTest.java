package com.exchange.fix;

import com.exchange.commands.ExecType;
import com.exchange.commands.ExecutionReportCommand;
import com.exchange.domain.ClOrdId;
import com.exchange.domain.OrderStatus;
import com.exchange.domain.Side;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FixCodecTest {

    private static final byte[] SENDER = "EXCHANGE".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] TARGET = "CLIENT01".getBytes(StandardCharsets.US_ASCII);
    private static final long TS_MS = 1730000000000L; // some fixed instant

    @Test
    void roundTripLogonResponse() {
        byte[] dest = new byte[4096];
        int n = FixEncoder.writeLogonResponse(dest, 0, SENDER, TARGET, 1, 30, TS_MS);
        FixMessage msg = new FixMessage();
        int consumed = FixCodec.decode(dest, 0, n, msg);
        assertThat(consumed).isEqualTo(n);
        assertThat(msg.msgType()).isEqualTo("A");
        assertThat(msg.getString(FixField.SENDER_COMP_ID)).isEqualTo("EXCHANGE");
        assertThat(msg.getString(FixField.TARGET_COMP_ID)).isEqualTo("CLIENT01");
        assertThat(msg.getInt(FixField.MSG_SEQ_NUM)).isEqualTo(1);
        assertThat(msg.getInt(FixField.HEART_BT_INT)).isEqualTo(30);
    }

    @Test
    void roundTripHeartbeat() {
        byte[] dest = new byte[4096];
        int n = FixEncoder.writeHeartbeat(dest, 0, SENDER, TARGET, 7, "TR1".getBytes(StandardCharsets.US_ASCII), TS_MS);
        FixMessage msg = new FixMessage();
        FixCodec.decode(dest, 0, n, msg);
        assertThat(msg.msgType()).isEqualTo("0");
        assertThat(msg.getInt(FixField.MSG_SEQ_NUM)).isEqualTo(7);
        assertThat(msg.getString(FixField.TEST_REQ_ID)).isEqualTo("TR1");
    }

    @Test
    void roundTripReject() {
        byte[] dest = new byte[4096];
        int n = FixEncoder.writeReject(dest, 0, SENDER, TARGET, 4, 99, 5, "Bad msg", TS_MS);
        FixMessage msg = new FixMessage();
        FixCodec.decode(dest, 0, n, msg);
        assertThat(msg.msgType()).isEqualTo("3");
        assertThat(msg.getInt(FixField.REF_SEQ_NUM)).isEqualTo(99);
        assertThat(msg.getInt(FixField.SESSION_REJECT_REASON)).isEqualTo(5);
        assertThat(msg.getString(FixField.TEXT)).isEqualTo("Bad msg");
    }

    @Test
    void roundTripBusinessReject() {
        byte[] dest = new byte[4096];
        int n = FixEncoder.writeBusinessMessageReject(dest, 0, SENDER, TARGET, 5, 7, "D", 4, "Symbol not found", TS_MS);
        FixMessage msg = new FixMessage();
        FixCodec.decode(dest, 0, n, msg);
        assertThat(msg.msgType()).isEqualTo("j");
        assertThat(msg.getInt(FixField.REF_SEQ_NUM)).isEqualTo(7);
        assertThat(msg.getString(FixField.REF_MSG_TYPE)).isEqualTo("D");
        assertThat(msg.getInt(FixField.BUSINESS_REJECT_REASON)).isEqualTo(4);
    }

    @Test
    void roundTripExecutionReport() {
        byte[] dest = new byte[4096];
        ExecutionReportCommand.MutableExecutionReport er = new ExecutionReportCommand.MutableExecutionReport();
        er.sessionId = 1L;
        er.senderId = 2L;
        er.orderId = 12345L;
        er.symbolId = 1;
        er.side = Side.CODE_BUY;
        er.execType = ExecType.NEW;
        er.ordStatus = (byte) OrderStatus.NEW.ordinal();
        er.qty = 100L;
        er.filledQty = 0L;
        er.remainingQty = 100L;
        er.price = 1500000L; // 150.0000 (price is fixed-point ×10000)
        er.account = 1001L;
        ClOrdId.writeAscii(er.clOrdId, "CL1");

        int n = FixEncoder.writeExecutionReport(dest, 0,
                SENDER, TARGET, 10, er, "AAPL".getBytes(StandardCharsets.US_ASCII), TS_MS);
        FixMessage msg = new FixMessage();
        FixCodec.decode(dest, 0, n, msg);
        assertThat(msg.msgType()).isEqualTo("8");
        assertThat(msg.getString(FixField.CL_ORD_ID)).isEqualTo("CL1");
        assertThat(msg.getString(FixField.SYMBOL)).isEqualTo("AAPL");
        assertThat(msg.getChar(FixField.SIDE)).isEqualTo((byte) '1');
        assertThat(msg.getChar(FixField.EXEC_TYPE)).isEqualTo((byte) '0');
        assertThat(msg.getChar(FixField.ORD_STATUS)).isEqualTo((byte) '0');
        assertThat(msg.getLong(FixField.ORDER_QTY)).isEqualTo(100L);
        assertThat(msg.getString(FixField.PRICE)).isEqualTo("150");
        assertThat(msg.getLong(FixField.LEAVES_QTY)).isEqualTo(100L);
        assertThat(msg.getLong(FixField.CUM_QTY)).isEqualTo(0L);
        assertThat(msg.getLong(FixField.ACCOUNT)).isEqualTo(1001L);
    }

    @Test
    void roundTripNewOrderSingle() {
        // Build a NewOrderSingle on the wire and decode it
        StringBuilder sb = new StringBuilder();
        sb.append("8=FIX.4.49=0"); // body length placeholder; replaced below
        // We manually compute: but easier to use the encoder's manual SOH-separated form
        // Hand-build the body with known field set
        String body =
                "35=D" +
                "49=CLIENT01" +
                "56=EXCHANGE" +
                "34=2" +
                "52=20251101-12:00:00.000" +
                "11=ORD123" +
                "55=AAPL" +
                "54=1" +
                "60=20251101-12:00:00.000" +
                "38=100" +
                "40=2" +
                "44=150.50" +
                "59=0";
        int bodyLen = body.length();
        String header = "8=FIX.4.49=" + bodyLen + "";
        String full = header + body;
        // Compute checksum
        int sum = 0;
        for (int i = 0; i < full.length(); i++) {
            sum += full.charAt(i);
        }
        sum &= 0xFF;
        String trailer = String.format("10=%03d", sum);
        byte[] wire = (full + trailer).getBytes(StandardCharsets.US_ASCII);

        FixMessage msg = new FixMessage();
        int consumed = FixCodec.decode(wire, 0, wire.length, msg);
        assertThat(consumed).isEqualTo(wire.length);
        assertThat(msg.msgType()).isEqualTo("D");
        assertThat(msg.getString(FixField.CL_ORD_ID)).isEqualTo("ORD123");
        assertThat(msg.getString(FixField.SYMBOL)).isEqualTo("AAPL");
        assertThat(msg.getChar(FixField.SIDE)).isEqualTo((byte) '1');
        assertThat(msg.getLong(FixField.ORDER_QTY)).isEqualTo(100L);
        assertThat(msg.getChar(FixField.ORD_TYPE)).isEqualTo((byte) '2');
        assertThat(Validator.parseDecimalToFixedPoint4(msg.buffer, msg.valueOffset(FixField.PRICE),
                msg.valueLength(FixField.PRICE))).isEqualTo(1505000L);
    }

    @Test
    void incompleteMessageReturnsZero() {
        // Truncate a known-good message at various points
        byte[] dest = new byte[4096];
        int n = FixEncoder.writeHeartbeat(dest, 0, SENDER, TARGET, 3, null, TS_MS);
        FixMessage msg = new FixMessage();
        for (int trunc = 1; trunc < n; trunc++) {
            int got = FixCodec.decode(dest, 0, trunc, msg);
            assertThat(got).as("truncated at " + trunc).isEqualTo(0);
        }
        assertThat(FixCodec.decode(dest, 0, n, msg)).isEqualTo(n);
    }

    @Test
    void malformedChecksumRejected() {
        byte[] dest = new byte[4096];
        int n = FixEncoder.writeHeartbeat(dest, 0, SENDER, TARGET, 3, null, TS_MS);
        // Corrupt the checksum
        dest[n - 4] = '0';
        dest[n - 3] = '0';
        dest[n - 2] = '0';
        FixMessage msg = new FixMessage();
        assertThrows(FixDecodeException.class, () -> FixCodec.decode(dest, 0, n, msg));
    }

    @Test
    void multipleFramesInOneBuffer() {
        byte[] dest = new byte[4096];
        int n1 = FixEncoder.writeHeartbeat(dest, 0, SENDER, TARGET, 1, null, TS_MS);
        int n2 = FixEncoder.writeHeartbeat(dest, n1, SENDER, TARGET, 2, null, TS_MS);
        FixMessage msg = new FixMessage();
        int c1 = FixCodec.decode(dest, 0, n1 + n2, msg);
        assertThat(c1).isEqualTo(n1);
        assertThat(msg.getInt(FixField.MSG_SEQ_NUM)).isEqualTo(1);
        int c2 = FixCodec.decode(dest, n1, n1 + n2, msg);
        assertThat(c2).isEqualTo(n2);
        assertThat(msg.getInt(FixField.MSG_SEQ_NUM)).isEqualTo(2);
    }
}
