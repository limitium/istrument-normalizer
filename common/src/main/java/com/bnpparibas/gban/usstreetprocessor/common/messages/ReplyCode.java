package com.bnpparibas.gban.usstreetprocessor.common.messages;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public enum ReplyCode {
    OK("100", Constants.ACK_REASON),
    CAN_NOT_PARSE("001", "Message can not be parsed"),
    NEGATIVE_QTY("002", "Qty < 0"),
    NEGATIVE_PRICE("003", "Price < 0"),
    INVALID_SIDE("004", "Side is invalid"),
    INVALID_SRC_ACTION("003", "Source Action Type is invalid"),
    MISSING_ISIN("006", "ISIN is empty"),
    MISSING_TRANSACTION_ID("007", "Transaction id is empty"),
    MISSING_CLEARING_ACCOUNT("008", "Clearing Account is empty"),
    MISSING_BOOK("009", "Book is empty"),
    MISSING_EXEC_TIME("010", "Execution Timestamp is empty"),
    MISSING_MARKET_NAME("011", "Market name is empty"),
    UNKNOWN_MARKET_NAME("012", "Unknown Market name"),
    MISSING_CAPACITY_CODE("013", "Capacity Code is empty"),
    DUPLICATE_EXECUTION("014", "Duplicate Execution"),
    CAN_NOT_VALIDATE("015", "Can Not Validate"),
    MISSING_CLIENT_CRDS_CODE("016", "Crds is empty");

    public final String code;
    public final String reason;
    public final boolean isAck;
    static Map<String, ReplyCode> codeToReply = new HashMap<>();

    static {
        Arrays.stream(ReplyCode.values()).forEach(replyCode -> codeToReply.put(replyCode.code, replyCode));
    }

    public static ReplyCode getBy(String code) {
        return codeToReply.get(code);
    }

    ReplyCode(String code, String reason) {
        this.code = code;
        this.reason = reason;
        this.isAck = Constants.ACK_REASON.equals(reason);
    }

    private static class Constants {
        public static final String ACK_REASON = "OK";
    }
}
