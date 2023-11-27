package com.limitium.gban.kscore.kstreamcore.downstream;

import com.limitium.gban.kscore.kstreamcore.Downstream;
import com.limitium.gban.kscore.kstreamcore.processor.ExtendedProcessor;
import com.limitium.gban.kscore.kstreamcore.processor.ExtendedProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

public class DownstreamTerminateProcessor implements ExtendedProcessor<Long, Long, Object, Object> {
    final String downstreamName;
    private Downstream<Object, Object, Object> downstream;

    public DownstreamTerminateProcessor(String downstreamName) {
        this.downstreamName = downstreamName;
    }

    @Override
    public void init(ExtendedProcessorContext<Long, Long, Object, Object> context) {
        downstream = context.getDownstream(downstreamName);
    }

    @Override
    public void process(Record<Long, Long> record) {
        downstream.terminateRequest(record.key(), record.value());
    }
}
