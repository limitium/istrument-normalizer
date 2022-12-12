/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.state;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.internals.RecordBatchingStateRestoreCallback;
import org.apache.kafka.streams.state.internals.IndexedKeyValueStoreBuilder;
import org.apache.kafka.streams.state.internals.IndexedMeteredKeyValueStore;

import java.util.Collection;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.apache.kafka.common.annotation.InterfaceStability.*;

/**
 * A key-value store that supports put/get/delete, range queries and uniq index lookup.
 *
 * @param <K> The key type
 * @param <V> The value type
 */
public interface IndexedKeyValueStore<K, V> extends KeyValueStore<K, V> {
    /**
     * Get the value from the index by the {@code indexKey}
     *
     * @param indexName index name which was used in {@link IndexedKeyValueStoreBuilder#addUniqIndex(String, Function)}
     * @param indexKey  index key which was generated by {@link IndexedKeyValueStoreBuilder#addUniqIndex(String, Function)} function
     * @return value from original {@link KeyValueStore}
     */
    V getUnique(String indexName, String indexKey);

    /**
     * Get the stream of values from the index by the {@code indexKey}
     *
     * @param indexName index name which was used in {@link IndexedKeyValueStoreBuilder#addNonUniqIndex(String, Function)}
     * @param indexKey  index key which was generated by {@link IndexedKeyValueStoreBuilder#addNonUniqIndex(String, Function)} function
     * @return value from original {@link KeyValueStore}
     */
    Stream<V> getNonUnique(String indexName, String indexKey);

    /**
     * Since there isn't a way to trigger rebuild indexes on restore from snapshot and from changelog, rebuild must be called manually in {@link org.apache.kafka.streams.processor.api.Processor#init(ProcessorContext)}
     * Single point {@link StateRestoreListener#onRestoreEnd(TopicPartition, String, long)} is used globally as a part of {@link org.apache.kafka.streams.KafkaStreams} settings for all stores
     * {@link RecordBatchingStateRestoreCallback#restoreBatch(Collection)} is occupied by {@link org.apache.kafka.streams.state.internals.RocksDBStore#init(StateStoreContext, StateStore)} with {@link IndexedMeteredKeyValueStore#name()} and can't have multiple instances
     */
    @Evolving
    void rebuildIndexes();
}