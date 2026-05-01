package com.exchange.app.commands;

import com.exchange.commands.CancelCommand;
import com.exchange.commands.CancelReplaceCommand;
import com.exchange.commands.CommandCodec;
import com.exchange.commands.CommandType;
import com.exchange.commands.ExecType;
import com.exchange.commands.ExecutionReportCommand;
import com.exchange.commands.NewOrderCommand;
import com.exchange.domain.ClOrdId;
import com.exchange.domain.OrdType;
import com.exchange.domain.Side;
import com.exchange.domain.TimeInForce;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.assertj.core.api.Assertions.assertThat;

class CommandCodecTest {

    @Test
    void newOrderRoundTrip() {
        ByteBuffer buf = ByteBuffer.allocate(NewOrderCommand.SIZE).order(ByteOrder.LITTLE_ENDIAN);
        byte[] cl = clOrdId("ABC123");
        NewOrderCommand.encode(buf, 0,
                Side.CODE_BUY,
                OrdType.CODE_LIMIT,
                TimeInForce.CODE_DAY,
                42,
                100L,
                1500000L,
                9000L,
                7L,
                3L,
                cl,
                12345L);
        assertThat(CommandCodec.peekType(buf, 0)).isEqualTo(CommandType.NEW_ORDER);

        NewOrderCommand.MutableNewOrder out = new NewOrderCommand.MutableNewOrder();
        NewOrderCommand.decode(buf, 0, out);
        assertThat(out.side).isEqualTo(Side.CODE_BUY);
        assertThat(out.ordType).isEqualTo(OrdType.CODE_LIMIT);
        assertThat(out.tif).isEqualTo(TimeInForce.CODE_DAY);
        assertThat(out.symbolId).isEqualTo(42);
        assertThat(out.qty).isEqualTo(100L);
        assertThat(out.price).isEqualTo(1500000L);
        assertThat(out.account).isEqualTo(9000L);
        assertThat(out.senderId).isEqualTo(7L);
        assertThat(out.sessionId).isEqualTo(3L);
        assertThat(out.clientTsNs).isEqualTo(12345L);
        assertThat(out.clOrdId).isEqualTo(cl);
    }

    @Test
    void cancelRoundTrip() {
        ByteBuffer buf = ByteBuffer.allocate(CancelCommand.SIZE).order(ByteOrder.LITTLE_ENDIAN);
        byte[] cl = clOrdId("CXL1");
        byte[] orig = clOrdId("ABC123");
        CancelCommand.encode(buf, 0,
                42,
                9000L,
                7L,
                3L,
                cl,
                orig,
                88L);
        assertThat(CommandCodec.peekType(buf, 0)).isEqualTo(CommandType.CANCEL);

        CancelCommand.MutableCancel out = new CancelCommand.MutableCancel();
        CancelCommand.decode(buf, 0, out);
        assertThat(out.symbolId).isEqualTo(42);
        assertThat(out.account).isEqualTo(9000L);
        assertThat(out.senderId).isEqualTo(7L);
        assertThat(out.sessionId).isEqualTo(3L);
        assertThat(out.clOrdId).isEqualTo(cl);
        assertThat(out.origClOrdId).isEqualTo(orig);
        assertThat(out.clientTsNs).isEqualTo(88L);
    }

    @Test
    void cancelReplaceRoundTrip() {
        ByteBuffer buf = ByteBuffer.allocate(CancelReplaceCommand.SIZE).order(ByteOrder.LITTLE_ENDIAN);
        byte[] cl = clOrdId("R2");
        byte[] orig = clOrdId("R1");
        CancelReplaceCommand.encode(buf, 0,
                Side.CODE_SELL,
                42,
                100L,
                1500000L,
                9000L,
                7L,
                3L,
                cl,
                orig,
                123L);
        assertThat(CommandCodec.peekType(buf, 0)).isEqualTo(CommandType.CANCEL_REPLACE);

        CancelReplaceCommand.MutableCancelReplace out = new CancelReplaceCommand.MutableCancelReplace();
        CancelReplaceCommand.decode(buf, 0, out);
        assertThat(out.side).isEqualTo(Side.CODE_SELL);
        assertThat(out.symbolId).isEqualTo(42);
        assertThat(out.qty).isEqualTo(100L);
        assertThat(out.price).isEqualTo(1500000L);
        assertThat(out.account).isEqualTo(9000L);
        assertThat(out.senderId).isEqualTo(7L);
        assertThat(out.sessionId).isEqualTo(3L);
        assertThat(out.clOrdId).isEqualTo(cl);
        assertThat(out.origClOrdId).isEqualTo(orig);
        assertThat(out.clientTsNs).isEqualTo(123L);
    }

    @Test
    void executionReportRoundTrip() {
        ByteBuffer buf = ByteBuffer.allocate(ExecutionReportCommand.SIZE).order(ByteOrder.LITTLE_ENDIAN);
        byte[] cl = clOrdId("ER1");
        byte[] orig = clOrdId("ER0");
        byte[] text = "FILLED".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
        ExecutionReportCommand.encode(buf, 0,
                /*sessionId*/ 3L,
                /*senderId*/ 7L,
                /*orderId*/ 9L,
                /*symbolId*/ 42,
                Side.CODE_BUY,
                ExecType.PARTIAL_FILL,
                /*ordStatus*/ (byte) 1,
                /*rejectReason*/ (byte) 0,
                /*qty*/ 100L,
                /*filledQty*/ 30L,
                /*remainingQty*/ 70L,
                /*price*/ 1500000L,
                /*lastQty*/ 30L,
                /*lastPx*/ 1500000L,
                /*account*/ 9000L,
                /*clientTsNs*/ 100L,
                /*reportTsNs*/ 200L,
                cl,
                orig,
                text);

        ExecutionReportCommand.MutableExecutionReport out = new ExecutionReportCommand.MutableExecutionReport();
        ExecutionReportCommand.decode(buf, 0, out);
        assertThat(out.sessionId).isEqualTo(3L);
        assertThat(out.senderId).isEqualTo(7L);
        assertThat(out.orderId).isEqualTo(9L);
        assertThat(out.symbolId).isEqualTo(42);
        assertThat(out.side).isEqualTo(Side.CODE_BUY);
        assertThat(out.execType).isEqualTo(ExecType.PARTIAL_FILL);
        assertThat(out.ordStatus).isEqualTo((byte) 1);
        assertThat(out.rejectReason).isEqualTo((byte) 0);
        assertThat(out.qty).isEqualTo(100L);
        assertThat(out.filledQty).isEqualTo(30L);
        assertThat(out.remainingQty).isEqualTo(70L);
        assertThat(out.price).isEqualTo(1500000L);
        assertThat(out.lastQty).isEqualTo(30L);
        assertThat(out.lastPx).isEqualTo(1500000L);
        assertThat(out.account).isEqualTo(9000L);
        assertThat(out.clientTsNs).isEqualTo(100L);
        assertThat(out.reportTsNs).isEqualTo(200L);
        assertThat(out.clOrdId).isEqualTo(cl);
        assertThat(out.origClOrdId).isEqualTo(orig);
        assertThat(out.textAsString()).isEqualTo("FILLED");
    }

    @Test
    void executionReportTextTruncatesToCapacity() {
        ByteBuffer buf = ByteBuffer.allocate(ExecutionReportCommand.SIZE).order(ByteOrder.LITTLE_ENDIAN);
        byte[] longText = new byte[ExecutionReportCommand.TEXT_LENGTH + 16];
        for (int i = 0; i < longText.length; i++) longText[i] = (byte) 'X';
        byte[] cl = clOrdId("ER1");
        byte[] orig = new byte[ClOrdId.LENGTH];
        ExecutionReportCommand.encode(buf, 0,
                3L, 7L, 9L, 42,
                Side.CODE_BUY, ExecType.NEW, (byte) 0, (byte) 0,
                100L, 0L, 100L, 1500000L,
                0L, 0L, 9000L,
                0L, 0L,
                cl, orig, longText);
        ExecutionReportCommand.MutableExecutionReport out = new ExecutionReportCommand.MutableExecutionReport();
        ExecutionReportCommand.decode(buf, 0, out);
        // All text bytes should be 'X' (no null terminators), so textAsString
        // should return the entire 64-byte run.
        assertThat(out.textAsString().length()).isEqualTo(ExecutionReportCommand.TEXT_LENGTH);
    }

    private static byte[] clOrdId(String s) {
        byte[] b = new byte[ClOrdId.LENGTH];
        ClOrdId.writeAscii(b, s);
        return b;
    }
}
