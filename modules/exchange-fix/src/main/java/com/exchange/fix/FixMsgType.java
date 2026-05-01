package com.exchange.fix;

/**
 * FIX 4.4 message type discriminators (tag 35) used by this gateway.
 */
public final class FixMsgType {

    public static final String HEARTBEAT = "0";
    public static final String TEST_REQUEST = "1";
    public static final String RESEND_REQUEST = "2";
    public static final String REJECT = "3";
    public static final String SEQUENCE_RESET = "4";
    public static final String LOGOUT = "5";
    public static final String LOGON = "A";
    public static final String NEW_ORDER_SINGLE = "D";
    public static final String ORDER_CANCEL_REQUEST = "F";
    public static final String ORDER_CANCEL_REPLACE_REQUEST = "G";
    public static final String EXECUTION_REPORT = "8";
    public static final String BUSINESS_MESSAGE_REJECT = "j";

    private FixMsgType() {
        // utility class
    }
}
