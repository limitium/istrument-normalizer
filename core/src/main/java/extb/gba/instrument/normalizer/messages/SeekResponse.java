package extb.gba.instrument.normalizer.messages;

public class SeekResponse {
    public Status status;
    public String reason;

    public String ric;

    public enum Status {
        SUCCESS,
    }
}
