package extb.gba.instrument.normalizer.external;

import extb.gba.instrument.normalizer.messages.InstrumentDefinition;
import extb.gba.instrument.normalizer.messages.UpsertInstrument;
import extb.gba.instrument.normalizer.model.Instrumental;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Component
public class InstrumentKeeper {

    public static final List<String> SECURITIES = new ArrayList<>(Arrays.asList("IBM", "NVDA", "MSFT"));

    /**
     * Must be thread safe!!!
     *
     * @param instrumental
     */
    public void enrichInstrument(Instrumental instrumental) {
        long instrumentId = lookUpInstrumentId(instrumental.getSecurityId());
        if (instrumentId != 0) {
            instrumental.setInstrumentId(instrumentId);
        }
    }

    /**
     * Should return 0 for on demand loaded, probably cache them to save ordering
     * @param securityId
     * @return
     */
    public long lookUpInstrumentId(String securityId) {
        return SECURITIES.indexOf(securityId) + 1;
    }


    public InstrumentDefinition upsert(UpsertInstrument instrument) {
        InstrumentDefinition instrumentDefinition = new InstrumentDefinition();
        if (!SECURITIES.contains(instrument.ric)) {
            SECURITIES.add(instrument.ric);
        }
        instrumentDefinition.instrumentId = lookUpInstrumentId(instrument.ric);
        return instrumentDefinition;
    }
}
