package com.limitium.gban.kscore.kstreamcore.downstream;

public interface RequestDataOverrider<RequestData> {
    RequestData override(RequestData original, RequestData override);
}
