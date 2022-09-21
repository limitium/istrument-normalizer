package com.bnpparibas.gban.instrumentnormalizer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class InstrumentKeeper {

    public static final List<String> SECURITIES = new ArrayList<>(Arrays.asList("IBM", "NVDA", "MSFT"));

    public InstrumentKeeper(String host, int port) {

    }

    /**
     * Should return 0 for notfound
     *
     * @param securityId
     * @return public instrument id or 0 if not found
     */
    public long lookupInstrumentId(String securityId) {
        return SECURITIES.indexOf(securityId) + 1;
    }

}
