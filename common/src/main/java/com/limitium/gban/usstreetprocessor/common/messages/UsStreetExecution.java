package com.limitium.gban.usstreetprocessor.common.messages;

public class UsStreetExecution {
    public long executionId;
    public String securityId;

    public double qty;
    public double price;
    public String portfolioCode;

    public long instrumentId;
    public long bookId;
    public String state;

    @Override
    public String toString() {
        return "UsStreetExecution{"
                + "executionId="
                + executionId
                + ", securityId='"
                + securityId
                + '\''
                + ", qty="
                + qty
                + ", price="
                + price
                + ", instrumentId="
                + instrumentId
                + ", bookId="
                + bookId
                + ", portfolioCode="
                + portfolioCode
                + ", state="
                + state
                + '}';
    }
}
