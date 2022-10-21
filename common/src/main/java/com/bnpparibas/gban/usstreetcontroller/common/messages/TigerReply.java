package com.bnpparibas.gban.usstreetcontroller.common.messages;

import java.time.LocalDateTime;

public class TigerReply {
    public long allocationId;
    public int version;
    public LocalDateTime ackTimestamp;
    public ReplyCode replyCode;
    public  ReplyTransactionType replyTransactionType;

    public TigerReply() {}

    public TigerReply(long allocationId, int allocationVersion, LocalDateTime ackTimestamp, ReplyCode replyCode, ReplyTransactionType replyTransactionType) {
        this.allocationId = allocationId;
        this.version = allocationVersion;
        this.ackTimestamp = ackTimestamp;
        this.replyCode = replyCode;
        this.replyTransactionType = replyTransactionType;
    }
}
