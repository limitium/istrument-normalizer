package com.bnpparibas.gban.usstreetprocessor.common.messages;

import java.time.LocalDateTime;

public class TigerReply {
    public long allocationId;
    public int version;
    public LocalDateTime ackTimestamp;
    public ReplyCode replyCode;

    public TigerReply() {
    }

    public TigerReply(long allocationId, int version, LocalDateTime ackTimestamp, ReplyCode replyCode) {
        this.allocationId = allocationId;
        this.version = version;
        this.ackTimestamp = ackTimestamp;
        this.replyCode = replyCode;
    }
}
