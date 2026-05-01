package com.exchange.fix;

/**
 * FIX 4.4 tag numbers used by this gateway. Constants only; do not extend.
 */
public final class FixField {

    public static final int BEGIN_STRING = 8;          // "FIX.4.4"
    public static final int BODY_LENGTH = 9;
    public static final int MSG_TYPE = 35;
    public static final int SENDER_COMP_ID = 49;
    public static final int TARGET_COMP_ID = 56;
    public static final int MSG_SEQ_NUM = 34;
    public static final int SENDING_TIME = 52;
    public static final int ENCRYPT_METHOD = 98;
    public static final int HEART_BT_INT = 108;
    public static final int RESET_SEQ_NUM_FLAG = 141;
    public static final int TEST_REQ_ID = 112;
    public static final int GAP_FILL_FLAG = 123;
    public static final int NEW_SEQ_NO = 36;
    public static final int POSS_DUP_FLAG = 43;
    public static final int POSS_RESEND = 97;
    public static final int ORIG_SENDING_TIME = 122;
    public static final int CHECKSUM = 10;
    public static final int TEXT = 58;
    public static final int REF_SEQ_NUM = 45;
    public static final int REF_TAG_ID = 371;
    public static final int SESSION_REJECT_REASON = 373;
    public static final int BUSINESS_REJECT_REASON = 380;
    public static final int REF_MSG_TYPE = 372;
    public static final int BEGIN_SEQ_NO = 7;
    public static final int END_SEQ_NO = 16;

    // Order tags
    public static final int CL_ORD_ID = 11;
    public static final int ORIG_CL_ORD_ID = 41;
    public static final int ORDER_ID = 37;
    public static final int EXEC_ID = 17;
    public static final int ORDER_QTY = 38;
    public static final int ORD_TYPE = 40;
    public static final int PRICE = 44;
    public static final int SIDE = 54;
    public static final int SYMBOL = 55;
    public static final int TIME_IN_FORCE = 59;
    public static final int TRANSACT_TIME = 60;
    public static final int ACCOUNT = 1;
    public static final int EXEC_TYPE = 150;
    public static final int ORD_STATUS = 39;
    public static final int LAST_QTY = 32;
    public static final int LAST_PX = 31;
    public static final int CUM_QTY = 14;
    public static final int LEAVES_QTY = 151;
    public static final int AVG_PX = 6;
    public static final int ORD_REJ_REASON = 103;

    private FixField() {
        // utility class
    }
}
