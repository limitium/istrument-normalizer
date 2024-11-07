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
package org.apache.kafka.streams.state.wrapped;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.state.internals.WrappedValue;
import org.apache.kafka.streams.state.internals.WrapperValueSerde;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class WrapperValueSerdeTest {
    @Test
    void shouldSerAndDeser() {
        WrapperValueSerde<byte[], byte[]> wrapperValueSerde = new WrapperValueSerde<>(Serdes.ByteArray(), Serdes.ByteArray());
        byte[] b1 = {1, 2};
        byte[] b2 = {3, 4};

        byte[] serialize = wrapperValueSerde.serializer().serialize(null, new WrappedValue<>(b1, b2));
        assertArrayEquals(new byte[]{0, 0, 0, 2, 1, 2, 3, 4}, serialize);

        WrappedValue<byte[], byte[]> deserialize = wrapperValueSerde.deserializer().deserialize(null, serialize);
        assertArrayEquals(b1, deserialize.wrapper());
        assertArrayEquals(b2, deserialize.value());
    }
}