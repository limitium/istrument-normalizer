package com.limitium.gban.kscore.kstreamcore.downstream;

import com.limitium.gban.kscore.kstreamcore.Downstream;
import com.limitium.gban.kscore.kstreamcore.processor.ExtendedProcessorContext;
import com.limitium.gban.kscore.kstreamcore.processor.ExtendedProcessor;
import org.apache.kafka.streams.processor.api.Record;

public class DownstreamResendProcessor implements ExtendedProcessor<Long, Object, Object, Object> {
    final String downstreamName;
    private Downstream<Object, Object, Object> downstream;

    public DownstreamResendProcessor(String downstreamName) {
        this.downstreamName = downstreamName;
    }

    @Override
    public void init(ExtendedProcessorContext<Long, Object, Object, Object> context) {
        downstream = context.getDownstream(downstreamName);
    }

    @Override
    public void process(Record<Long, Object> record) {
        downstream.resend(record.key());
    }
}
