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
package org.apache.kafka.clients.producer;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Configurable;

import java.io.Closeable;

/**
 * Partitioner Interface
 * Kafka Producer分区器 自定义分区策略时需要实现的接口
 * <p>
 * 1.如果KafkaRecord提供了分区号 则使用record提供的分区号
 * 2.如果KafkaRecord没有提供分区号 则使用key的序列化后的值的hash值对分区数量取模
 * 3.如果KafkaRecord没有提供分区号 也没有提供key 则使用轮询的方式分配分区号
 * ----a.会首先在可用的分区中分配分区号
 * ----b.如果没有可用的分区 则在该主题所有分区中分配分区号
 */
public interface Partitioner extends Configurable, Closeable {

    /**
     * Compute the partition for the given record.
     *
     * @param topic      The topic name
     * @param key        The key to partition on (or null if no key)
     * @param keyBytes   The serialized key to partition on( or null if no key)
     * @param value      The value to partition on or null
     * @param valueBytes The serialized value to partition on or null
     * @param cluster    The current cluster metadata
     */
    int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster);

    /**
     * This is called when partitioner is closed.
     */
    void close();

    /**
     * Notifies the partitioner a new batch is about to be created. When using the sticky partitioner,
     * this method can change the chosen sticky partition for the new batch.
     *
     * @param topic         The topic name
     * @param cluster       The current cluster metadata
     * @param prevPartition The partition previously selected for the record that triggered a new batch
     */
    default void onNewBatch(String topic, Cluster cluster, int prevPartition) {
    }
}
