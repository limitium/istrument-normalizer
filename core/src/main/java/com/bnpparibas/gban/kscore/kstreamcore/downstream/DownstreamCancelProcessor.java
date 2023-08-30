package com.bnpparibas.gban.kscore.kstreamcore.downstream;

import com.bnpparibas.gban.kscore.kstreamcore.Downstream;
import com.bnpparibas.gban.kscore.kstreamcore.KSProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

public class DownstreamCancelProcessor extends KSProcessor<Long, Long, Object, Object> {
    final String downstreamName;
    private Downstream<Object, Object, Object> downstream;

    public DownstreamCancelProcessor(String downstreamName) {
        this.downstreamName = downstreamName;
    }

    @Override
    public void init(ProcessorContext<Object, Object> context) {
        super.init(context);
        downstream = getDownstream(downstreamName);
    }

    @Override
    public void process(Record<Long, Long> record) {
        downstream.cancel(record.key(), record.value());
    }
}
