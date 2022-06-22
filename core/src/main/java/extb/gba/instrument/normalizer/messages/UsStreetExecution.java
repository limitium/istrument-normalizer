package extb.gba.instrument.normalizer.messages;

import extb.gba.instrument.normalizer.model.Instrumental;

//todo: migrate generic message container with binary payload
public class UsStreetExecution implements Instrumental {
    public long executionId;
    public String securityId;

    public double qty;
    public double price;

    public long instrumentId;
    public long bookId;

    @Override
    public String getSecurityId() {
        return securityId;
    }
    @Override
    public void setInstrumentId(long instrumentId) {
        this.instrumentId = instrumentId;
    }

    @Override
    public String toString() {
        return "UsStreetExecution{" +
                "executionId=" + executionId +
                ", securityId='" + securityId + '\'' +
                ", qty=" + qty +
                ", price=" + price +
                ", instrumentId=" + instrumentId +
                ", bookId=" + bookId +
                '}';
    }
}
