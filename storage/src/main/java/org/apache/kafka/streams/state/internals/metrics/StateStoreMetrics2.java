package org.apache.kafka.streams.state.internals.metrics;

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;

import java.util.Map;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.*;

public class StateStoreMetrics2 {
    private static final String AVG_DESCRIPTION_PREFIX = "The average ";
    private static final String MAX_DESCRIPTION_PREFIX = "The maximum ";
    private static final String LATENCY_DESCRIPTION = "latency of ";
    private static final String AVG_LATENCY_DESCRIPTION_PREFIX = AVG_DESCRIPTION_PREFIX + LATENCY_DESCRIPTION;
    private static final String MAX_LATENCY_DESCRIPTION_PREFIX = MAX_DESCRIPTION_PREFIX + LATENCY_DESCRIPTION;

    private static final String REBUILD_INDEX = "rebuild-indexes";
    private static final String REBUILD_INDEX_DESCRIPTION = "rebuild secondary indexes";
    private static final String REBUILD_INDEX_RATE_DESCRIPTION = RATE_DESCRIPTION_PREFIX + REBUILD_INDEX_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;
    private static final String REBUILD_INDEX_AVG_LATENCY_DESCRIPTION = AVG_LATENCY_DESCRIPTION_PREFIX + REBUILD_INDEX_DESCRIPTION;
    private static final String REBUILD_INDEX_MAX_LATENCY_DESCRIPTION = MAX_LATENCY_DESCRIPTION_PREFIX + REBUILD_INDEX_DESCRIPTION;

    private static final String LOOKUP_INDEX = "lookup-index";
    private static final String LOOKUP_INDEX_DESCRIPTION = "lookup secondary index";
    private static final String LOOKUP_INDEX_RATE_DESCRIPTION = RATE_DESCRIPTION_PREFIX + LOOKUP_INDEX_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;
    private static final String LOOKUP_INDEX_AVG_LATENCY_DESCRIPTION = AVG_LATENCY_DESCRIPTION_PREFIX + LOOKUP_INDEX_DESCRIPTION;
    private static final String LOOKUP_INDEX_MAX_LATENCY_DESCRIPTION = MAX_LATENCY_DESCRIPTION_PREFIX + LOOKUP_INDEX_DESCRIPTION;

    private static final String UPDATE_INDEX = "update-index";
    private static final String UPDATE_INDEX_DESCRIPTION = "update secondary index";
    private static final String UPDATE_INDEX_RATE_DESCRIPTION = RATE_DESCRIPTION_PREFIX + UPDATE_INDEX_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;
    private static final String UPDATE_INDEX_AVG_LATENCY_DESCRIPTION = AVG_LATENCY_DESCRIPTION_PREFIX + UPDATE_INDEX_DESCRIPTION;
    private static final String UPDATE_INDEX_MAX_LATENCY_DESCRIPTION = MAX_LATENCY_DESCRIPTION_PREFIX + UPDATE_INDEX_DESCRIPTION;


    private static final String REMOVE_INDEX = "remove-index";
    private static final String REMOVE_INDEX_DESCRIPTION = "remove secondary index";
    private static final String REMOVE_INDEX_RATE_DESCRIPTION = RATE_DESCRIPTION_PREFIX + REMOVE_INDEX_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;
    private static final String REMOVE_INDEX_AVG_LATENCY_DESCRIPTION = AVG_LATENCY_DESCRIPTION_PREFIX + REMOVE_INDEX_DESCRIPTION;
    private static final String REMOVE_INDEX_MAX_LATENCY_DESCRIPTION = MAX_LATENCY_DESCRIPTION_PREFIX + REMOVE_INDEX_DESCRIPTION;


    public static Sensor restoreSensor(final String taskId,
                                       final String storeType,
                                       final String storeName,
                                       final StreamsMetricsImpl streamsMetrics) {
        return throughputAndLatencySensor(
                taskId, storeType,
                storeName,
                REBUILD_INDEX,
                REBUILD_INDEX_RATE_DESCRIPTION,
                REBUILD_INDEX_AVG_LATENCY_DESCRIPTION,
                REBUILD_INDEX_MAX_LATENCY_DESCRIPTION,
                Sensor.RecordingLevel.DEBUG,
                streamsMetrics
        );
    }

    public static Sensor lookupIndexSensor(final String taskId,
                                           final String storeType,
                                           final String storeName,
                                           final StreamsMetricsImpl streamsMetrics) {
        return throughputAndLatencySensor(
                taskId, storeType,
                storeName,
                LOOKUP_INDEX,
                LOOKUP_INDEX_RATE_DESCRIPTION,
                LOOKUP_INDEX_AVG_LATENCY_DESCRIPTION,
                LOOKUP_INDEX_MAX_LATENCY_DESCRIPTION,
                Sensor.RecordingLevel.DEBUG,
                streamsMetrics
        );
    }

    public static Sensor updateIndexSensor(final String taskId,
                                           final String storeType,
                                           final String storeName,
                                           final StreamsMetricsImpl streamsMetrics) {
        return throughputAndLatencySensor(
                taskId, storeType,
                storeName,
                UPDATE_INDEX,
                UPDATE_INDEX_RATE_DESCRIPTION,
                UPDATE_INDEX_AVG_LATENCY_DESCRIPTION,
                UPDATE_INDEX_MAX_LATENCY_DESCRIPTION,
                Sensor.RecordingLevel.DEBUG,
                streamsMetrics
        );
    }

    public static Sensor removeIndexSensor(final String taskId,
                                           final String storeType,
                                           final String storeName,
                                           final StreamsMetricsImpl streamsMetrics) {
        return throughputAndLatencySensor(
                taskId, storeType,
                storeName,
                REMOVE_INDEX,
                REMOVE_INDEX_RATE_DESCRIPTION,
                REMOVE_INDEX_AVG_LATENCY_DESCRIPTION,
                REMOVE_INDEX_MAX_LATENCY_DESCRIPTION,
                Sensor.RecordingLevel.DEBUG,
                streamsMetrics
        );
    }

    private static Sensor throughputAndLatencySensor(final String taskId,
                                                     final String storeType,
                                                     final String storeName,
                                                     final String metricName,
                                                     final String descriptionOfRate,
                                                     final String descriptionOfAvg,
                                                     final String descriptionOfMax,
                                                     final Sensor.RecordingLevel recordingLevel,
                                                     final StreamsMetricsImpl streamsMetrics) {
        final Sensor sensor;
        final String latencyMetricName = metricName + LATENCY_SUFFIX;
        final Map<String, String> tagMap = streamsMetrics.storeLevelTagMap(taskId, storeType, storeName);
        sensor = streamsMetrics.storeLevelSensor(taskId, storeName, metricName, recordingLevel);
        addInvocationRateToSensor(sensor, STATE_STORE_LEVEL_GROUP, tagMap, metricName, descriptionOfRate);
        addAvgAndMaxToSensor(
                sensor,
                STATE_STORE_LEVEL_GROUP,
                tagMap,
                latencyMetricName,
                descriptionOfAvg,
                descriptionOfMax
        );
        return sensor;
    }
}
