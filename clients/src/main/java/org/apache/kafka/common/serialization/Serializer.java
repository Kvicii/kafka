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
package org.apache.kafka.common.serialization;

import org.apache.kafka.common.header.Headers;

import java.io.Closeable;
import java.util.Map;

/**
 * An interface for converting objects to bytes.
 * <p>
 * A class that implements this interface is expected to have a constructor with no parameter.
 * <p>
 * Implement {@link org.apache.kafka.common.ClusterResourceListener} to receive cluster metadata once it's available. Please see the class documentation for ClusterResourceListener for more information.
 * <p>
 * Kafka Producer 序列化器
 * <p>
 * 将对象转换为byte数组的接口
 * 该接口的实现类需要提供无参构造器
 *
 * @param <T> Type to be serialized from.
 *            从哪个类型转换
 */
public interface Serializer<T> extends Closeable {

    /**
     * Configure this class.
     * <p>
     * 类的配置信息
     *
     * @param configs configs in key/value pairs
     * @param isKey   whether is for key or value
     *                key的序列化还是value的序列化
     */
    default void configure(Map<String, ?> configs, boolean isKey) {
        // intentionally left blank
    }

    /**
     * Convert {@code data} into a byte array.
     * <p>
     * 将对象转换为字节数组
     *
     * @param topic topic associated with data
     *              Topic名称
     * @param data  typed data
     *              需要转换的对象
     * @return serialized bytes. 序列化的字节数组
     */
    byte[] serialize(String topic, T data);

    /**
     * Convert {@code data} into a byte array.
     *
     * @param topic   topic associated with data
     * @param headers headers associated with the record
     * @param data    typed data
     * @return serialized bytes
     */
    default byte[] serialize(String topic, Headers headers, T data) {
        return serialize(topic, data);
    }

    /**
     * Close this serializer.
     * <p>
     * 关闭序列化器
     * 该方法需要提供幂等性 因为可能调用多次
     * <p>
     * This method must be idempotent as it may be called multiple times.
     */
    @Override
    default void close() {
        // intentionally left blank
    }
}
