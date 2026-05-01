package com.exchange.fix;

import com.exchange.domain.RejectReason;
import com.exchange.domain.Symbols;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class ValidatorTest {

    private Symbols symbols;
    private Validator v;

    @BeforeEach
    void setup() {
        symbols = Symbols.register(new String[]{"AAPL", "MSFT"});
        v = new Validator(symbols);
    }

    @Test
    void acceptsValidLimitNew() {
        FixMessage m = buildNew("AAPL", "1", "100", "2", "150.50", "0");
        assertThat(v.validateNewOrder(m)).isEqualTo(Validator.OK);
    }

    @Test
    void acceptsValidMarketNew() {
        FixMessage m = buildNew("AAPL", "1", "100", "1", null, "0");
        assertThat(v.validateNewOrder(m)).isEqualTo(Validator.OK);
    }

    @Test
    void rejectsUnknownSymbol() {
        FixMessage m = buildNew("ZZZZ", "1", "100", "2", "150.0", "0");
        assertThat(v.validateNewOrder(m)).isEqualTo(RejectReason.UNKNOWN_SYMBOL);
    }

    @Test
    void rejectsInvalidQty() {
        FixMessage m = buildNew("AAPL", "1", "0", "2", "150.0", "0");
        assertThat(v.validateNewOrder(m)).isEqualTo(RejectReason.INVALID_QTY);
    }

    @Test
    void rejectsInvalidLimitPrice() {
        FixMessage m = buildNew("AAPL", "1", "100", "2", "0", "0");
        assertThat(v.validateNewOrder(m)).isEqualTo(RejectReason.INVALID_PRICE);
    }

    @Test
    void rejectsMarketWithNonZeroPrice() {
        FixMessage m = buildNew("AAPL", "1", "100", "1", "10.0", "0");
        assertThat(v.validateNewOrder(m)).isEqualTo(RejectReason.INVALID_PRICE);
    }

    @Test
    void rejectsMissingSymbol() {
        FixMessage m = buildNew(null, "1", "100", "2", "150.0", "0");
        assertThat(v.validateNewOrder(m)).isEqualTo(RejectReason.UNKNOWN_SYMBOL);
    }

    @Test
    void parseDecimalToFixedPoint4() {
        byte[] b = "12.5".getBytes(StandardCharsets.US_ASCII);
        assertThat(Validator.parseDecimalToFixedPoint4(b, 0, b.length)).isEqualTo(125000L);
        b = "100".getBytes(StandardCharsets.US_ASCII);
        assertThat(Validator.parseDecimalToFixedPoint4(b, 0, b.length)).isEqualTo(1000000L);
        b = "0".getBytes(StandardCharsets.US_ASCII);
        assertThat(Validator.parseDecimalToFixedPoint4(b, 0, b.length)).isEqualTo(0L);
        b = "1.2345".getBytes(StandardCharsets.US_ASCII);
        assertThat(Validator.parseDecimalToFixedPoint4(b, 0, b.length)).isEqualTo(12345L);
    }

    private static FixMessage buildNew(String symbol, String side, String qty,
                                       String ordType, String price, String tif) {
        StringBuilder body = new StringBuilder();
        body.append("35=D");
        body.append("49=CLIENT01");
        body.append("56=EXCHANGE");
        body.append("34=2");
        body.append("52=20251101-12:00:00.000");
        body.append("11=ORD123");
        if (symbol != null) body.append("55=").append(symbol).append("");
        body.append("54=").append(side).append("");
        body.append("60=20251101-12:00:00.000");
        body.append("38=").append(qty).append("");
        body.append("40=").append(ordType).append("");
        if (price != null) body.append("44=").append(price).append("");
        if (tif != null) body.append("59=").append(tif).append("");
        int bodyLen = body.length();
        String header = "8=FIX.4.49=" + bodyLen + "";
        String full = header + body.toString();
        int sum = 0;
        for (int i = 0; i < full.length(); i++) sum += full.charAt(i);
        sum &= 0xFF;
        String trailer = String.format("10=%03d", sum);
        byte[] wire = (full + trailer).getBytes(StandardCharsets.US_ASCII);
        FixMessage m = new FixMessage();
        FixCodec.decode(wire, 0, wire.length, m);
        return m;
    }
}
