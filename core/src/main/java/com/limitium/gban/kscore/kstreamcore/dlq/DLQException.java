package com.limitium.gban.kscore.kstreamcore.dlq;

public class DLQException extends RuntimeException {
    public String key;
    public String subKey;

    public DLQException(String message, String key, String subKey) {
        super(message);
        this.key = key;
        this.subKey = subKey;
    }

    public DLQException(String message, Throwable cause, String key, String subKey) {
        super(message, cause);
        this.key = key;
        this.subKey = subKey;
    }

    public String getKey() {
        return key;
    }

    public String getSubKey() {
        return subKey;
    }
}
