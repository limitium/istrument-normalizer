package extb.gba.instrument.normalizer.external;

import extb.gba.instrument.normalizer.messages.SeekResponse;
import extb.gba.instrument.normalizer.messages.SeekRequest;
import org.springframework.stereotype.Component;


@Component
public class InstrumentSeeker {
    public SeekResponse lookupInstrument(SeekRequest seekRequest) {
        SeekResponse seekResponse = new SeekResponse();
        seekResponse.ric = seekRequest.securityId;
        seekResponse.status = SeekResponse.Status.SUCCESS;
        return seekResponse;
    }
}
