package com.limitium.gban.kscore.kstreamcore.processor;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.processor.*;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;

import java.io.File;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

public class ProcessorContextComposer<KOut, VOut> implements ProcessorContext<KOut, VOut> {
    protected final ProcessorContext<KOut, VOut> context;

    public ProcessorContextComposer(ProcessorContext<KOut, VOut> context) {
        this.context = context;
    }

    @Override
    public <K extends KOut, V extends VOut> void forward(Record<K, V> record) {
        context.forward(record);
    }

    @Override
    public <K extends KOut, V extends VOut> void forward(Record<K, V> record, String childName) {
        context.forward(record, childName);
    }

    @Override
    public String applicationId() {
        return context.applicationId();
    }

    @Override
    public TaskId taskId() {
        return context.taskId();
    }

    @Override
    public Optional<RecordMetadata> recordMetadata() {
        return context.recordMetadata();
    }

    @Override
    public Serde<?> keySerde() {
        return context.keySerde();
    }

    @Override
    public Serde<?> valueSerde() {
        return context.valueSerde();
    }

    @Override
    public File stateDir() {
        return context.stateDir();
    }

    @Override
    public StreamsMetrics metrics() {
        return context.metrics();
    }

    @Override
    public <S extends StateStore> S getStateStore(String name) {
        return context.getStateStore(name);
    }

    @Override
    public Cancellable schedule(Duration interval, PunctuationType type, Punctuator callback) {
        return context.schedule(interval, type, callback);
    }

    @Override
    public void commit() {
        context.commit();
    }

    @Override
    public Map<String, Object> appConfigs() {
        return context.appConfigs();
    }

    @Override
    public Map<String, Object> appConfigsWithPrefix(String prefix) {
        return context.appConfigsWithPrefix(prefix);
    }

    @Override
    public long currentSystemTimeMs() {
        return context.currentSystemTimeMs();
    }

    @Override
    public long currentStreamTimeMs() {
        return context.currentStreamTimeMs();
    }
}
