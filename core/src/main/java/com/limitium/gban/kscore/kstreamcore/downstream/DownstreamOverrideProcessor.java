package com.limitium.gban.kscore.kstreamcore.downstream;

import com.limitium.gban.kscore.kstreamcore.Downstream;
import com.limitium.gban.kscore.kstreamcore.KSProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

public class DownstreamOverrideProcessor<RequestData> extends KSProcessor<Long, RequestData, Object, Object> {
    final String downstreamName;
    private Downstream<Object, Object, Object> downstream;

    public DownstreamOverrideProcessor(String downstreamName) {
        this.downstreamName = downstreamName;
    }

    @Override
    public void init(ProcessorContext<Object, Object> context) {
        super.init(context);
        downstream = getDownstream(downstreamName);
    }

    @Override
    public void process(Record<Long, RequestData> record) {
        downstream.updateOverride(record.key(), record.value());
    }
}
