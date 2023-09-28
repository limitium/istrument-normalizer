package com.limitium.gban.kscore.kstreamcore.downstream;

import com.limitium.gban.kscore.kstreamcore.Downstream;
import com.limitium.gban.kscore.kstreamcore.processor.ExtendedProcessor;
import com.limitium.gban.kscore.kstreamcore.processor.ExtendedProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

public class DownstreamOverrideProcessor<RequestData> implements ExtendedProcessor<Long, RequestData, Object, Object> {
    final String downstreamName;
    private Downstream<Object, Object, Object> downstream;

    public DownstreamOverrideProcessor(String downstreamName) {
        this.downstreamName = downstreamName;
    }

    @Override
    public void init(ExtendedProcessorContext<Long, RequestData, Object, Object> context) {
        downstream = context.getDownstream(downstreamName);
    }

    @Override
    public void process(Record<Long, RequestData> record) {
        downstream.updateOverride(record.key(), record.value());
    }
}
