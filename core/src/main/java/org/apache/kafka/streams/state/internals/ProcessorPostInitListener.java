package org.apache.kafka.streams.state.internals;

import org.apache.kafka.streams.processor.api.ProcessorContext;

/**
 * Additional listener, which is used by {@link IndexedMeteredKeyValueStore} and {@link WrappedMeteredKeyValueStore} to perform post {@link org.apache.kafka.streams.processor.api.Processor#init(ProcessorContext)}
 * initialization per {@link org.apache.kafka.streams.processor.internals.Task} level.
 *
 * @param <PC> process context type
 */
@SuppressWarnings("rawtypes")
public interface ProcessorPostInitListener<PC extends ProcessorContext> {
    void onPostInit(PC processorContext);
}
