package com.limitium.gban.usstreetprocessor.common.external;

import org.springframework.stereotype.Component;

@Component
public class InstrumentKeeper {
    public Equity getById(long id) {
        return new Equity();
    }

    public static class Equity {
        public long id;
        public String isin;
    }
}
