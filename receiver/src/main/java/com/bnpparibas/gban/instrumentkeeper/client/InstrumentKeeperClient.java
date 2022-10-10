package com.bnpparibas.gban.instrumentkeeper.client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class InstrumentKeeperClient {

    public static final List<String> SECURITIES = new ArrayList<>(Arrays.asList("IBM", "NVDA", "MSFT"));

    public InstrumentKeeperClient(String host, int port) {

    }

    /**
     * Should return 0 for notfound
     *
     * @param securityId
     * @return public instrument id or 0 if not found
     */
    public long lookupIdBy(String securityId, String qwe, String asd) {
        return SECURITIES.indexOf(securityId) + 1;
    }

}
