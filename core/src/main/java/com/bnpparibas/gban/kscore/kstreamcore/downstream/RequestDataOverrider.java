package com.bnpparibas.gban.kscore.kstreamcore.downstream;

public interface RequestDataOverrider<RequestData> {
    RequestData override(RequestData toOverride, RequestData override);
}
