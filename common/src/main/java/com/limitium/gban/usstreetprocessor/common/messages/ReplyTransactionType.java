package com.limitium.gban.usstreetprocessor.common.messages;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public enum ReplyTransactionType {
    NEW(0),
    CANCEL(2);

    public final int code;
    static Map<Integer, ReplyTransactionType> codeToReply = new HashMap<>();

    static {
        Arrays.stream(ReplyTransactionType.values()).forEach(replyCode -> codeToReply.put(replyCode.code, replyCode));
    }

    public static ReplyTransactionType getBy(int code) {
        return codeToReply.get(code);
    }

    ReplyTransactionType(int code) {
        this.code = code;
    }
}
