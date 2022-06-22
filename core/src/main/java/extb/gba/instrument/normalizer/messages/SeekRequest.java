package extb.gba.instrument.normalizer.messages;

public class SeekRequest {
    public String securityId;

    public SeekRequest() {
    }

    public SeekRequest(String securityId) {
        this.securityId = securityId;
    }
}
