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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Composite serde to serializer/deserializer wrapper header and bounded value
 *
 * @param <W> wrapper type
 * @param <V> value type
 */
public class WrapperValueSerde<W, V> implements Serde<WrapperValue<W, V>> {

    private final WrapperAndValueSerializer<W, V> wrapperAndValueSerializer;
    private final WrapperAndValueDeserializer<W, V> wrapperAndValueDeserializer;

    public static <W, V> WrapperValueSerde<W, V> create(final Serde<W> wrapperSerde, final Serde<V> valueSerde) {
        return new WrapperValueSerde<>(wrapperSerde, valueSerde);
    }

    public WrapperValueSerde(final Serde<W> wrapperSerde, final Serde<V> valueSerde) {
        wrapperAndValueSerializer = new WrapperAndValueSerializer<>(requireNonNull(wrapperSerde, "wrapperSerde was null").serializer(), requireNonNull(valueSerde, "valueSerde was null").serializer());
        wrapperAndValueDeserializer = new WrapperAndValueDeserializer<>(requireNonNull(wrapperSerde, "wrapperSerde was null").deserializer(), requireNonNull(valueSerde, "valueSerde was null").deserializer());
    }

    @Override
    public Serializer<WrapperValue<W, V>> serializer() {
        return wrapperAndValueSerializer;
    }

    @Override
    public Deserializer<WrapperValue<W, V>> deserializer() {
        return wrapperAndValueDeserializer;
    }

    public static class WrapperAndValueSerializer<W, V> implements Serializer<WrapperValue<W, V>> {
        public final Serializer<W> wrapperSerializer;
        public final Serializer<V> valueSerializer;

        WrapperAndValueSerializer(final Serializer<W> wrapperSerializer, final Serializer<V> valueSerializer) {
            Objects.requireNonNull(wrapperSerializer);
            Objects.requireNonNull(valueSerializer);
            this.wrapperSerializer = wrapperSerializer;
            this.valueSerializer = valueSerializer;
        }


        @Override
        public void configure(final Map<String, ?> configs,
                              final boolean isKey) {
            wrapperSerializer.configure(configs, isKey);
            valueSerializer.configure(configs, isKey);
        }

        @Override
        public byte[] serialize(final String topic,
                                final WrapperValue<W, V> data) {
            if (data == null) {
                return null;
            }
            return serialize(topic, data.wrapper(), data.value());
        }

        public byte[] serialize(final String topic,
                                final W wrapper,
                                final V value) {

            final byte[] rawValue = valueSerializer.serialize(topic, value);

            // Since we can't control the result of the internal serializer, we make sure that the result
            // is not null as well.
            // Serializing non-null values to null can be useful when working with Optional-like values
            // where the Optional.empty case is serialized to null.
            // See the discussion here: https://github.com/apache/kafka/pull/7679
            if (rawValue == null) {
                return null;
            }

            final byte[] rawWrapper = wrapperSerializer.serialize(topic, wrapper);
            return ByteBuffer
                    .allocate(4 + rawWrapper.length + rawValue.length)
                    .putInt(rawWrapper.length)
                    .put(rawWrapper)
                    .put(rawValue)
                    .array();
        }

        @Override
        public void close() {
            wrapperSerializer.close();
            valueSerializer.close();
        }
    }

    public static class WrapperAndValueDeserializer<W, V> implements Deserializer<WrapperValue<W, V>> {
        private final Deserializer<W> wrapperDeserializer;
        public final Deserializer<V> valueDeserializer;

        WrapperAndValueDeserializer(final Deserializer<W> wrapperDeserializer, final Deserializer<V> valueDeserializer) {
            Objects.requireNonNull(wrapperDeserializer);
            Objects.requireNonNull(valueDeserializer);
            this.wrapperDeserializer = wrapperDeserializer;
            this.valueDeserializer = valueDeserializer;
        }

        @Override
        public void configure(final Map<String, ?> configs,
                              final boolean isKey) {
            wrapperDeserializer.configure(configs, isKey);
            valueDeserializer.configure(configs, isKey);
        }

        @Override
        public WrapperValue<W, V> deserialize(final String topic,
                                              final byte[] wrapperAndValue) {
            if (wrapperAndValue == null) {
                return null;
            }
            ByteBuffer byteBuffer = ByteBuffer.wrap(wrapperAndValue);

            int wrapperSize = byteBuffer.getInt();
            byte[] rawWrapper = new byte[wrapperSize];
            byteBuffer.get(rawWrapper, 0, wrapperSize);

            int valueSize = wrapperAndValue.length - wrapperSize - 4;
            byte[] rawValue = new byte[valueSize];
            byteBuffer.get(rawValue, 0, valueSize);

            final W wrapper = wrapperDeserializer.deserialize(topic, rawWrapper);
            final V value = valueDeserializer.deserialize(topic, rawValue);
            return new WrapperValue<>(wrapper, value);
        }

        @Override
        public void close() {
            wrapperDeserializer.close();
            valueDeserializer.close();
        }
    }
}