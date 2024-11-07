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

import jakarta.annotation.Nonnull;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.internals.WrappedValue;

/**
 * A key-value store that supports put/get/delete, range queries and uniq index lookup.
 *
 * @param <K> The key type
 * @param <V> The value type
 * @param <W> The wrapper type
 */
public interface WrappedKeyValueStore<K, V, W> extends KeyValueStore<K, WrappedValue<W, V>> {

    /**
     * Get the wrapper corresponding to this key.
     *
     * @param key The key to fetch
     * @return The value or null if no value is found.
     * @throws NullPointerException       If null is used for key.
     * @throws InvalidStateStoreException if the store is not initialized
     */
    W getWrapper(K key);

    /**
     * Get the value corresponding to this key.
     *
     * @param key The key to fetch
     * @return The value or null if no value is found.
     * @throws NullPointerException       If null is used for key.
     * @throws InvalidStateStoreException if the store is not initialized
     */
    V getValue(K key);


    /**
     * Update the value associated with this key.
     *
     * @param key   The key to associate the value to
     * @param value The value to update, it can be {@code null};
     *              if the serialized bytes are also {@code null} it is interpreted as deletes
     * @throws NullPointerException If {@code null} is used for key.
     */
    void putValue(K key,@Nonnull V value);
}