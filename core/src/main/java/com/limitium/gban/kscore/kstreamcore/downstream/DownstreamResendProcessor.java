package com.limitium.gban.kscore.kstreamcore.downstream;

import com.limitium.gban.kscore.kstreamcore.Downstream;
import com.limitium.gban.kscore.kstreamcore.processor.ExtendedProcessor;
import com.limitium.gban.kscore.kstreamcore.processor.ExtendedProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

public class DownstreamResendProcessor implements ExtendedProcessor<Long, String, Object, Object> {
    public static String RESEND_MODEL = "RESEND";

    final String downstreamName;
    private Downstream<Object, Object, Object> downstream;

    public DownstreamResendProcessor(String downstreamName) {
        this.downstreamName = downstreamName;
    }

    @Override
    public void init(ExtendedProcessorContext<Long, String, Object, Object> context) {
        downstream = context.getDownstream(downstreamName);
    }

    @Override
    public void process(Record<Long, String> record) {
        if (RESEND_MODEL.equals(record.value())) {
            downstream.resendRequest(record.key());
        } else {
            downstream.retryRequest(record.key());
        }
    }
}
