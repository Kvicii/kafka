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

import org.apache.kafka.clients.ClientDnsLookup;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SecurityConfig;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Range.between;
import static org.apache.kafka.common.config.ConfigDef.ValidString.in;

/**
 * Configuration for the Kafka Producer. Documentation for these configurations can be found in the <a
 * href="http://kafka.apache.org/documentation.html#producerconfigs">Kafka documentation</a>
 */
public class ProducerConfig extends AbstractConfig {

    /*
     * NOTE: DO NOT CHANGE EITHER CONFIG STRINGS OR THEIR JAVA VARIABLE NAMES AS THESE ARE PART OF THE PUBLIC API AND
     * CHANGE WILL BREAK USER CODE.
     */

    private static final ConfigDef CONFIG;

    /**
     * <code>bootstrap.servers</code>
     */
    public static final String BOOTSTRAP_SERVERS_CONFIG = CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

    /**
     * <code>client.dns.lookup</code>
     */
    public static final String CLIENT_DNS_LOOKUP_CONFIG = CommonClientConfigs.CLIENT_DNS_LOOKUP_CONFIG;

    /**
     * <code>metadata.max.age.ms</code>
     */
    public static final String METADATA_MAX_AGE_CONFIG = CommonClientConfigs.METADATA_MAX_AGE_CONFIG;
    private static final String METADATA_MAX_AGE_DOC = CommonClientConfigs.METADATA_MAX_AGE_DOC;

    /**
     * <code>metadata.max.idle.ms</code>
     */
    public static final String METADATA_MAX_IDLE_CONFIG = "metadata.max.idle.ms";
    private static final String METADATA_MAX_IDLE_DOC =
            "Controls how long the producer will cache metadata for a topic that's idle. If the elapsed " +
                    "time since a topic was last produced to exceeds the metadata idle duration, then the topic's " +
                    "metadata is forgotten and the next access to it will force a metadata fetch request.";

    /**
     * <code>batch.size</code>
     * <p>
     * 当多个消息发送到同一个分区的时候 生产者尝试将多个记录作为一个批来处理 批处理提高了客户端和服务器的处理效率
     * 该配置项以字节为单位控制默认批的大小 所有的批 <= 该值
     * <p>
     * 发送给broker的请求将包含多个批次 每个分区一个 并包含可发送的数据
     * 如果该值设置的比较小 会限制吞吐量(设置为0会完全禁用批处理)
     * 如果设置的很大 消息发送时延增加 浪费内存 因为Kafka会永远分配这么大的内存来参与到消息的批整合中
     */
    public static final String BATCH_SIZE_CONFIG = "batch.size";
    private static final String BATCH_SIZE_DOC = "The producer will attempt to batch records together into fewer requests whenever multiple records are being sent"
            + " to the same partition. This helps performance on both the client and the server. This configuration controls the "
            + "default batch size in bytes. "
            + "<p>"
            + "No attempt will be made to batch records larger than this size. "
            + "<p>"
            + "Requests sent to brokers will contain multiple batches, one for each partition with data available to be sent. "
            + "<p>"
            + "A small batch size will make batching less common and may reduce throughput (a batch size of zero will disable "
            + "batching entirely). A very large batch size may use memory a bit more wastefully as we will always allocate a "
            + "buffer of the specified batch size in anticipation of additional records.";

    /**
     * <code>acks</code>
     * <p>
     * acks=0:
     * 表示生产者不会等待broker对消息的确认 只要将消息放到缓冲区 就认为消息已经发送完成
     * 该情形不能保证broker是否真的收到了消息 retries配置也不会生效因为客户端不需要知道消息是否发送成功
     * 发送的消息的返回的消息偏移量永远是-1
     * <p>
     * acks=1:
     * 表示消息只需要写到主分区即可 然后就响应客户端 而不等待副本分区的确认
     * 在该情形下 如果主分区收到消息确认之后就宕机了 而副本分区还没来得 及同步该消息 则该消息丢失
     * <p>
     * acks=all(acks=-1):
     * Leader分区会等待所有的ISR副本分区确认记录
     * 该处理保证了只要有一个ISR副本分区冗余 消息就不会丢失 这是Kafka最强的可靠性保证
     */
    public static final String ACKS_CONFIG = "acks";
    private static final String ACKS_DOC = "The number of acknowledgments the producer requires the leader to have received before considering a request complete. This controls the "
            + " durability of records that are sent. The following settings are allowed: "
            + " <ul>"
            + " <li><code>acks=0</code> If set to zero then the producer will not wait for any acknowledgment from the"
            + " server at all. The record will be immediately added to the socket buffer and considered sent. No guarantee can be"
            + " made that the server has received the record in this case, and the <code>retries</code> configuration will not"
            + " take effect (as the client won't generally know of any failures). The offset given back for each record will"
            + " always be set to <code>-1</code>."
            + " <li><code>acks=1</code> This will mean the leader will write the record to its local log but will respond"
            + " without awaiting full acknowledgement from all followers. In this case should the leader fail immediately after"
            + " acknowledging the record but before the followers have replicated it then the record will be lost."
            + " <li><code>acks=all</code> This means the leader will wait for the full set of in-sync replicas to"
            + " acknowledge the record. This guarantees that the record will not be lost as long as at least one in-sync replica"
            + " remains alive. This is the strongest available guarantee. This is equivalent to the acks=-1 setting."
            + "</ul>";

    /**
     * <code>linger.ms</code>
     * <p>
     * 生产者在发送请求传输间隔会对需要发送的消息进行累积 然后作为一个批次发送 一般情况是消息的发送的速度比消息累积的速度慢
     * 有时客户端需要减少请求的次数 即使是在发送负载不大的情况下
     * 该配置设置了一个延迟 生产者不会立即将消息发送到broker 而是等待这么一段时间以累积消息
     * 然后将这段时间之内的消息作为一个批次发送该设置是批处理的另一个上限:一旦批消息达到了batch.size 指定的值 消息批会立即发送
     * 如果积累的消息字节数达不到batch.size的值 可以设置该毫秒值 等待这么长时间之后 也会发送消息批 该属性默认值是0(没有延迟)
     */
    public static final String LINGER_MS_CONFIG = "linger.ms";
    private static final String LINGER_MS_DOC = "The producer groups together any records that arrive in between request transmissions into a single batched request. "
            + "Normally this occurs only under load when records arrive faster than they can be sent out. However in some circumstances the client may want to "
            + "reduce the number of requests even under moderate load. This setting accomplishes this by adding a small amount "
            + "of artificial delay&mdash;that is, rather than immediately sending out a record the producer will wait for up to "
            + "the given delay to allow other records to be sent so that the sends can be batched together. This can be thought "
            + "of as analogous to Nagle's algorithm in TCP. This setting gives the upper bound on the delay for batching: once "
            + "we get <code>" + BATCH_SIZE_CONFIG + "</code> worth of records for a partition it will be sent immediately regardless of this "
            + "setting, however if we have fewer than this many bytes accumulated for this partition we will 'linger' for the "
            + "specified time waiting for more records to show up. This setting defaults to 0 (i.e. no delay). Setting <code>" + LINGER_MS_CONFIG + "=5</code>, "
            + "for example, would have the effect of reducing the number of requests sent but would add up to 5ms of latency to records sent in the absence of load.";

    /**
     * <code>request.timeout.ms</code>
     * <p>
     * 客户端等待请求响应的最大时长 如果服务端响应超时 则会重发请求
     * 除非达到重试次数 该设置应该比replica.lag.time.max.ms(broker configuration)要大 以免在服务器延迟时间内重发消息
     */
    public static final String REQUEST_TIMEOUT_MS_CONFIG = CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG;
    private static final String REQUEST_TIMEOUT_MS_DOC = CommonClientConfigs.REQUEST_TIMEOUT_MS_DOC
            + " This should be larger than <code>replica.lag.time.max.ms</code> (a broker configuration)"
            + " to reduce the possibility of message duplication due to unnecessary producer retries.";

    /**
     * <code>delivery.timeout.ms</code>
     */
    public static final String DELIVERY_TIMEOUT_MS_CONFIG = "delivery.timeout.ms";
    private static final String DELIVERY_TIMEOUT_MS_DOC = "An upper bound on the time to report success or failure "
            + "after a call to <code>send()</code> returns. This limits the total time that a record will be delayed "
            + "prior to sending, the time to await acknowledgement from the broker (if expected), and the time allowed "
            + "for retriable send failures. The producer may report failure to send a record earlier than this config if "
            + "either an unrecoverable error is encountered, the retries have been exhausted (deprecated), "
            + "or the record is added to a batch which reached an earlier delivery expiration deadline. "
            + "The value of this config should be greater than or equal to the sum of <code>" + REQUEST_TIMEOUT_MS_CONFIG + "</code> "
            + "and <code>" + LINGER_MS_CONFIG + "</code>.";

    /**
     * <code>client.id</code>
     * <p>
     * 生产者发送请求的时候传递给broker的id字符串 用于在broker的请求日志中追踪什么应用发送了什么消息 一般该id是跟业务有关的字符串
     */
    public static final String CLIENT_ID_CONFIG = CommonClientConfigs.CLIENT_ID_CONFIG;

    /**
     * <code>send.buffer.bytes</code>
     * <p>
     * TCP发送数据的时候使用的缓冲区(SO_SNDBUF)大小 如果设置为0 则使用操作系统默认的
     */
    public static final String SEND_BUFFER_CONFIG = CommonClientConfigs.SEND_BUFFER_CONFIG;

    /**
     * <code>receive.buffer.bytes</code>
     * <p>
     * TCP接收缓存(SO_RCVBUF) 如果设置为-1 则使用操作系统默认的值
     */
    public static final String RECEIVE_BUFFER_CONFIG = CommonClientConfigs.RECEIVE_BUFFER_CONFIG;

    /**
     * <code>max.request.size</code>
     * <p>
     * 单个请求的最大字节数 该设置会限制单个请求中消息批的消息个数 以免单个请求发送太多的数据
     * 服务器有自己的限制批大小的设置 与该配置可能不一样
     */
    public static final String MAX_REQUEST_SIZE_CONFIG = "max.request.size";
    private static final String MAX_REQUEST_SIZE_DOC =
            "The maximum size of a request in bytes. This setting will limit the number of record " +
                    "batches the producer will send in a single request to avoid sending huge requests. " +
                    "This is also effectively a cap on the maximum uncompressed record batch size. Note that the server " +
                    "has its own cap on the record batch size (after compression if compression is enabled) which may be different from this.";

    /**
     * <code>reconnect.backoff.ms</code>
     * <p>
     * 尝试重连指定主机的基础等待时间 避免了到该主机的密集重连
     * 该退避时间应用于该客户端到broker的所有连接
     */
    public static final String RECONNECT_BACKOFF_MS_CONFIG = CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG;

    /**
     * <code>reconnect.backoff.max.ms</code>
     * <p>
     * 对于每个连续的连接失败 每台主机的退避将成倍增加 直至达到此最大值
     * 在计算退避增量之后 添加20％的随机抖动以避免连接风暴
     */
    public static final String RECONNECT_BACKOFF_MAX_MS_CONFIG = CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG;

    /**
     * <code>max.block.ms</code>
     * <p>
     * 控制KafkaProducer.send()和KafkaProducer.partitionsFor()阻塞的时长 当缓存满了或元数据不可用的时候 这些方法阻塞
     * 在用户提供的序列化器和分区器的阻塞时间不计入
     */
    public static final String MAX_BLOCK_MS_CONFIG = "max.block.ms";
    private static final String MAX_BLOCK_MS_DOC = "The configuration controls how long the <code>KafkaProducer</code>'s <code>send()</code>, <code>partitionsFor()</code>, "
            + "<code>initTransactions()</code>, <code>sendOffsetsToTransaction()</code>, <code>commitTransaction()</code> "
            + "and <code>abortTransaction()</code> methods will block. "
            + "For <code>send()</code> this timeout bounds the total time waiting for both metadata fetch and buffer allocation "
            + "(blocking in the user-supplied serializers or partitioner is not counted against this timeout). "
            + "For <code>partitionsFor()</code> this timeout bounds the time spent waiting for metadata if it is unavailable. "
            + "The transaction-related methods always block, but may timeout if "
            + "the transaction coordinator could not be discovered or did not respond within the timeout.";

    /**
     * <code>buffer.memory</code>
     * <p>
     * 生产者可以用来缓存等待发送到服务器的消息的总内存字节
     * 如果消息从producer发送到缓冲区的速度超过了将消息从缓冲区发送到broker的速度 则生产者将阻塞max.block.ms的时间 此后它将引发异常 此设置应大致对应于生产者将使用的总内存
     * 但并非生产者使用的所有内存都用于缓冲 一些额外的内存将用于压缩(如果启用了压缩)以及维护运行中的请求
     */
    public static final String BUFFER_MEMORY_CONFIG = "buffer.memory";
    private static final String BUFFER_MEMORY_DOC = "The total bytes of memory the producer can use to buffer records waiting to be sent to the server. If records are "
            + "sent faster than they can be delivered to the server the producer will block for <code>" + MAX_BLOCK_MS_CONFIG + "</code> after which it will throw an exception."
            + "<p>"
            + "This setting should correspond roughly to the total memory the producer will use, but is not a hard bound since "
            + "not all memory the producer uses is used for buffering. Some additional memory will be used for compression (if "
            + "compression is enabled) as well as for maintaining in-flight requests.";

    /**
     * <code>retry.backoff.ms</code>
     * <p>
     * 在向一个指定的主题分区重发消息的时候 重试之间的等待时间
     */
    public static final String RETRY_BACKOFF_MS_CONFIG = CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG;

    /**
     * <code>compression.type</code>
     * <p>
     * 生产者发送的所有数据的压缩方式 默认是none 也就是不压缩
     * 支持的值: none | gzip | snappy | lz4
     * 压缩是对于整个批来讲的 所以批处理的效率也会影响到压缩的比例
     */
    public static final String COMPRESSION_TYPE_CONFIG = "compression.type";
    private static final String COMPRESSION_TYPE_DOC = "The compression type for all data generated by the producer. The default is none (i.e. no compression). Valid "
            + " values are <code>none</code>, <code>gzip</code>, <code>snappy</code>, <code>lz4</code>, or <code>zstd</code>. "
            + "Compression is of full batches of data, so the efficacy of batching will also impact the compression ratio (more batching means better compression).";

    /**
     * <code>metrics.sample.window.ms</code>
     */
    public static final String METRICS_SAMPLE_WINDOW_MS_CONFIG = CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG;

    /**
     * <code>metrics.num.samples</code>
     */
    public static final String METRICS_NUM_SAMPLES_CONFIG = CommonClientConfigs.METRICS_NUM_SAMPLES_CONFIG;

    /**
     * <code>metrics.recording.level</code>
     */
    public static final String METRICS_RECORDING_LEVEL_CONFIG = CommonClientConfigs.METRICS_RECORDING_LEVEL_CONFIG;

    /**
     * <code>metric.reporters</code>
     */
    public static final String METRIC_REPORTER_CLASSES_CONFIG = CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG;

    /**
     * <code>max.in.flight.requests.per.connection</code>
     * <p>
     * 单个连接上未确认请求的最大数量 达到这个数量 客户端阻塞
     * 如果该值大于1 且存在失败的请求 在重试的时候消息顺序不能保证
     */
    public static final String MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION = "max.in.flight.requests.per.connection";
    private static final String MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_DOC = "The maximum number of unacknowledged requests the client will send on a single connection before blocking."
            + " Note that if this setting is set to be greater than 1 and there are failed sends, there is a risk of"
            + " message re-ordering due to retries (i.e., if retries are enabled).";

    /**
     * <code>retries</code>
     * <p>
     * retries重试次数
     * 当消息发送出现错误的时候 系统会重发消息
     * 跟客户端收到错误时重发一样 如果设置了重试 还想保证消息的有序性
     * 需要设置 MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION=1
     *
     * @deprecated since 2.7
     */
    @Deprecated
    public static final String RETRIES_CONFIG = CommonClientConfigs.RETRIES_CONFIG;
    private static final String RETRIES_DOC = "(Deprecated) Setting a value greater than zero will cause the client to resend any record whose send fails with a potentially transient error."
            + " Note that this retry is no different than if the client resent the record upon receiving the error."
            + " Allowing retries without setting <code>" + MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION + "</code> to 1 will potentially change the"
            + " ordering of records because if two batches are sent to a single partition, and the first fails and is retried but the second"
            + " succeeds, then the records in the second batch may appear first. Note additionally that produce requests will be"
            + " failed before the number of retries has been exhausted if the timeout configured by"
            + " <code>" + DELIVERY_TIMEOUT_MS_CONFIG + "</code> expires first before successful acknowledgement. Users should generally"
            + " prefer to leave this config unset and instead use <code>" + DELIVERY_TIMEOUT_MS_CONFIG + "</code> to control"
            + " retry behavior.";

    /**
     * <code>key.serializer</code>
     */
    public static final String KEY_SERIALIZER_CLASS_CONFIG = "key.serializer";
    public static final String KEY_SERIALIZER_CLASS_DOC = "Serializer class for key that implements the <code>org.apache.kafka.common.serialization.Serializer</code> interface.";

    /**
     * <code>value.serializer</code>
     */
    public static final String VALUE_SERIALIZER_CLASS_CONFIG = "value.serializer";
    public static final String VALUE_SERIALIZER_CLASS_DOC = "Serializer class for value that implements the <code>org.apache.kafka.common.serialization.Serializer</code> interface.";

    /**
     * <code>socket.connection.setup.timeout.ms</code>
     */
    public static final String SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG;

    /**
     * <code>socket.connection.setup.timeout.max.ms</code>
     */
    public static final String SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG;

    /**
     * <code>connections.max.idle.ms</code>
     * <p>
     * 当连接空闲时间达到这个值 就关闭连接
     */
    public static final String CONNECTIONS_MAX_IDLE_MS_CONFIG = CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG;

    /**
     * <code>partitioner.class</code>
     * <p>
     * 实现了接口org.apache.kafka.clients.producer.Partitioner 的分区器实现类
     */
    public static final String PARTITIONER_CLASS_CONFIG = "partitioner.class";
    private static final String PARTITIONER_CLASS_DOC = "Partitioner class that implements the <code>org.apache.kafka.clients.producer.Partitioner</code> interface.";

    /**
     * <code>interceptor.classes</code>
     * <p>
     * 在生产者接收到该消息 向Kafka集群传输之前 由序列化器处理之前 可以通过拦截器对消息进行处理
     * 要求拦截器类必须实现 {@link ProducerInterceptor} 接口 默认没有拦截器
     */
    public static final String INTERCEPTOR_CLASSES_CONFIG = "interceptor.classes";
    public static final String INTERCEPTOR_CLASSES_DOC = "A list of classes to use as interceptors. "
            + "Implementing the <code>org.apache.kafka.clients.producer.ProducerInterceptor</code> interface allows you to intercept (and possibly mutate) the records "
            + "received by the producer before they are published to the Kafka cluster. By default, there are no interceptors.";

    /**
     * <code>enable.idempotence</code>
     */
    public static final String ENABLE_IDEMPOTENCE_CONFIG = "enable.idempotence";
    public static final String ENABLE_IDEMPOTENCE_DOC = "When set to 'true', the producer will ensure that exactly one copy of each message is written in the stream. If 'false', producer "
            + "retries due to broker failures, etc., may write duplicates of the retried message in the stream. "
            + "Note that enabling idempotence requires <code>" + MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION + "</code> to be less than or equal to 5, "
            + "<code>" + RETRIES_CONFIG + "</code> (deprecated) to be greater than 0 and <code>" + ACKS_CONFIG + "</code> must be 'all'. If these values "
            + "are not explicitly set by the user, suitable values will be chosen. If incompatible values are set, "
            + "a <code>ConfigException</code> will be thrown.";

    /**
     * <code> transaction.timeout.ms </code>
     */
    public static final String TRANSACTION_TIMEOUT_CONFIG = "transaction.timeout.ms";
    public static final String TRANSACTION_TIMEOUT_DOC = "The maximum amount of time in ms that the transaction coordinator will wait for a transaction status update from the producer before proactively aborting the ongoing transaction." +
            "If this value is larger than the transaction.max.timeout.ms setting in the broker, the request will fail with a <code>InvalidTxnTimeoutException</code> error.";

    /**
     * <code> transactional.id </code>
     */
    public static final String TRANSACTIONAL_ID_CONFIG = "transactional.id";
    public static final String TRANSACTIONAL_ID_DOC = "The TransactionalId to use for transactional delivery. This enables reliability semantics which span multiple producer sessions since it allows the client to guarantee that transactions using the same TransactionalId have been completed prior to starting any new transactions. If no TransactionalId is provided, then the producer is limited to idempotent delivery. " +
            "If a TransactionalId is configured, <code>enable.idempotence</code> is implied. " +
            "By default the TransactionId is not configured, which means transactions cannot be used. " +
            "Note that, by default, transactions require a cluster of at least three brokers which is the recommended setting for production; for development you can change this, by adjusting broker setting <code>transaction.state.log.replication.factor</code>.";

    /**
     * <code>security.providers</code>
     */
    public static final String SECURITY_PROVIDERS_CONFIG = SecurityConfig.SECURITY_PROVIDERS_CONFIG;
    private static final String SECURITY_PROVIDERS_DOC = SecurityConfig.SECURITY_PROVIDERS_DOC;

    /**
     * <code>internal.auto.downgrade.txn.commit</code>
     * Whether or not the producer should automatically downgrade the transactional commit request when the new group metadata
     * feature is not supported by the broker.
     * <p>
     * The purpose of this flag is to make Kafka Streams being capable of working with old brokers when applying this new API.
     * Non Kafka Streams users who are building their own EOS applications should be careful playing around
     * with config as there is a risk of violating EOS semantics when turning on this flag.
     *
     * <p>
     * Note: this is an internal configuration and could be changed in the future in a backward incompatible way
     */
    static final String AUTO_DOWNGRADE_TXN_COMMIT = "internal.auto.downgrade.txn.commit";

    private static final AtomicInteger PRODUCER_CLIENT_ID_SEQUENCE = new AtomicInteger(1);

    static {
        CONFIG = new ConfigDef().define(BOOTSTRAP_SERVERS_CONFIG, Type.LIST, Collections.emptyList(), new ConfigDef.NonNullValidator(), Importance.HIGH, CommonClientConfigs.BOOTSTRAP_SERVERS_DOC)
                .define(CLIENT_DNS_LOOKUP_CONFIG,
                        Type.STRING,
                        ClientDnsLookup.USE_ALL_DNS_IPS.toString(),
                        in(ClientDnsLookup.DEFAULT.toString(),
                                ClientDnsLookup.USE_ALL_DNS_IPS.toString(),
                                ClientDnsLookup.RESOLVE_CANONICAL_BOOTSTRAP_SERVERS_ONLY.toString()),
                        Importance.MEDIUM,
                        CommonClientConfigs.CLIENT_DNS_LOOKUP_DOC)
                .define(BUFFER_MEMORY_CONFIG, Type.LONG, 32 * 1024 * 1024L, atLeast(0L), Importance.HIGH, BUFFER_MEMORY_DOC)
                .define(RETRIES_CONFIG, Type.INT, Integer.MAX_VALUE, between(0, Integer.MAX_VALUE), Importance.HIGH, RETRIES_DOC)
                .define(ACKS_CONFIG,
                        Type.STRING,
                        "1",
                        in("all", "-1", "0", "1"),
                        Importance.HIGH,
                        ACKS_DOC)
                .define(COMPRESSION_TYPE_CONFIG, Type.STRING, "none", Importance.HIGH, COMPRESSION_TYPE_DOC)
                .define(BATCH_SIZE_CONFIG, Type.INT, 16384, atLeast(0), Importance.MEDIUM, BATCH_SIZE_DOC)
                .define(LINGER_MS_CONFIG, Type.LONG, 0, atLeast(0), Importance.MEDIUM, LINGER_MS_DOC)
                .define(DELIVERY_TIMEOUT_MS_CONFIG, Type.INT, 120 * 1000, atLeast(0), Importance.MEDIUM, DELIVERY_TIMEOUT_MS_DOC)
                .define(CLIENT_ID_CONFIG, Type.STRING, "", Importance.MEDIUM, CommonClientConfigs.CLIENT_ID_DOC)
                .define(SEND_BUFFER_CONFIG, Type.INT, 128 * 1024, atLeast(CommonClientConfigs.SEND_BUFFER_LOWER_BOUND), Importance.MEDIUM, CommonClientConfigs.SEND_BUFFER_DOC)
                .define(RECEIVE_BUFFER_CONFIG, Type.INT, 32 * 1024, atLeast(CommonClientConfigs.RECEIVE_BUFFER_LOWER_BOUND), Importance.MEDIUM, CommonClientConfigs.RECEIVE_BUFFER_DOC)
                .define(MAX_REQUEST_SIZE_CONFIG,
                        Type.INT,
                        1024 * 1024,
                        atLeast(0),
                        Importance.MEDIUM,
                        MAX_REQUEST_SIZE_DOC)
                .define(RECONNECT_BACKOFF_MS_CONFIG, Type.LONG, 50L, atLeast(0L), Importance.LOW, CommonClientConfigs.RECONNECT_BACKOFF_MS_DOC)
                .define(RECONNECT_BACKOFF_MAX_MS_CONFIG, Type.LONG, 1000L, atLeast(0L), Importance.LOW, CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_DOC)
                .define(RETRY_BACKOFF_MS_CONFIG, Type.LONG, 100L, atLeast(0L), Importance.LOW, CommonClientConfigs.RETRY_BACKOFF_MS_DOC)
                .define(MAX_BLOCK_MS_CONFIG,
                        Type.LONG,
                        60 * 1000,
                        atLeast(0),
                        Importance.MEDIUM,
                        MAX_BLOCK_MS_DOC)
                .define(REQUEST_TIMEOUT_MS_CONFIG,
                        Type.INT,
                        30 * 1000,
                        atLeast(0),
                        Importance.MEDIUM,
                        REQUEST_TIMEOUT_MS_DOC)
                .define(METADATA_MAX_AGE_CONFIG, Type.LONG, 5 * 60 * 1000, atLeast(0), Importance.LOW, METADATA_MAX_AGE_DOC)
                .define(METADATA_MAX_IDLE_CONFIG,
                        Type.LONG,
                        5 * 60 * 1000,
                        atLeast(5000),
                        Importance.LOW,
                        METADATA_MAX_IDLE_DOC)
                .define(METRICS_SAMPLE_WINDOW_MS_CONFIG,
                        Type.LONG,
                        30000,
                        atLeast(0),
                        Importance.LOW,
                        CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_DOC)
                .define(METRICS_NUM_SAMPLES_CONFIG, Type.INT, 2, atLeast(1), Importance.LOW, CommonClientConfigs.METRICS_NUM_SAMPLES_DOC)
                .define(METRICS_RECORDING_LEVEL_CONFIG,
                        Type.STRING,
                        Sensor.RecordingLevel.INFO.toString(),
                        in(Sensor.RecordingLevel.INFO.toString(), Sensor.RecordingLevel.DEBUG.toString(), Sensor.RecordingLevel.TRACE.toString()),
                        Importance.LOW,
                        CommonClientConfigs.METRICS_RECORDING_LEVEL_DOC)
                .define(METRIC_REPORTER_CLASSES_CONFIG,
                        Type.LIST,
                        Collections.emptyList(),
                        new ConfigDef.NonNullValidator(),
                        Importance.LOW,
                        CommonClientConfigs.METRIC_REPORTER_CLASSES_DOC)
                .define(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
                        Type.INT,
                        5,
                        atLeast(1),
                        Importance.LOW,
                        MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_DOC)
                .define(KEY_SERIALIZER_CLASS_CONFIG,
                        Type.CLASS,
                        Importance.HIGH,
                        KEY_SERIALIZER_CLASS_DOC)
                .define(VALUE_SERIALIZER_CLASS_CONFIG,
                        Type.CLASS,
                        Importance.HIGH,
                        VALUE_SERIALIZER_CLASS_DOC)
                .define(SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG,
                        Type.LONG,
                        CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MS,
                        Importance.MEDIUM,
                        CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_DOC)
                .define(SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG,
                        Type.LONG,
                        CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS,
                        Importance.MEDIUM,
                        CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_DOC)
                /* default is set to be a bit lower than the server default (10 min), to avoid both client and server closing connection at same time */
                .define(CONNECTIONS_MAX_IDLE_MS_CONFIG,
                        Type.LONG,
                        9 * 60 * 1000,
                        Importance.MEDIUM,
                        CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_DOC)
                .define(PARTITIONER_CLASS_CONFIG,
                        Type.CLASS,
                        DefaultPartitioner.class,
                        Importance.MEDIUM, PARTITIONER_CLASS_DOC)
                .define(INTERCEPTOR_CLASSES_CONFIG,
                        Type.LIST,
                        Collections.emptyList(),
                        new ConfigDef.NonNullValidator(),
                        Importance.LOW,
                        INTERCEPTOR_CLASSES_DOC)
                .define(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                        Type.STRING,
                        CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL,
                        Importance.MEDIUM,
                        CommonClientConfigs.SECURITY_PROTOCOL_DOC)
                .define(SECURITY_PROVIDERS_CONFIG,
                        Type.STRING,
                        null,
                        Importance.LOW,
                        SECURITY_PROVIDERS_DOC)
                .withClientSslSupport()
                .withClientSaslSupport()
                .define(ENABLE_IDEMPOTENCE_CONFIG,
                        Type.BOOLEAN,
                        false,
                        Importance.LOW,
                        ENABLE_IDEMPOTENCE_DOC)
                .define(TRANSACTION_TIMEOUT_CONFIG,
                        Type.INT,
                        60000,
                        Importance.LOW,
                        TRANSACTION_TIMEOUT_DOC)
                .define(TRANSACTIONAL_ID_CONFIG,
                        Type.STRING,
                        null,
                        new ConfigDef.NonEmptyString(),
                        Importance.LOW,
                        TRANSACTIONAL_ID_DOC)
                .defineInternal(AUTO_DOWNGRADE_TXN_COMMIT,
                        Type.BOOLEAN,
                        false,
                        Importance.LOW);
    }

    @Override
    protected Map<String, Object> postProcessParsedConfig(final Map<String, Object> parsedValues) {
        CommonClientConfigs.warnIfDeprecatedDnsLookupValue(this);
        CommonClientConfigs.warnIfDeprecatedRetriesValue(this);
        Map<String, Object> refinedConfigs = CommonClientConfigs.postProcessReconnectBackoffConfigs(this, parsedValues);
        maybeOverrideEnableIdempotence(refinedConfigs);
        maybeOverrideClientId(refinedConfigs);
        maybeOverrideAcksAndRetries(refinedConfigs);
        return refinedConfigs;
    }

    private void maybeOverrideClientId(final Map<String, Object> configs) {
        String refinedClientId;
        boolean userConfiguredClientId = this.originals().containsKey(CLIENT_ID_CONFIG);
        if (userConfiguredClientId) {
            refinedClientId = this.getString(CLIENT_ID_CONFIG);
        } else {
            String transactionalId = this.getString(TRANSACTIONAL_ID_CONFIG);
            refinedClientId = "producer-" + (transactionalId != null ? transactionalId : PRODUCER_CLIENT_ID_SEQUENCE.getAndIncrement());
        }
        configs.put(CLIENT_ID_CONFIG, refinedClientId);
    }

    private void maybeOverrideEnableIdempotence(final Map<String, Object> configs) {
        boolean userConfiguredIdempotence = this.originals().containsKey(ENABLE_IDEMPOTENCE_CONFIG);
        boolean userConfiguredTransactions = this.originals().containsKey(TRANSACTIONAL_ID_CONFIG);

        if (userConfiguredTransactions && !userConfiguredIdempotence) {
            configs.put(ENABLE_IDEMPOTENCE_CONFIG, true);
        }
    }

    private void maybeOverrideAcksAndRetries(final Map<String, Object> configs) {
        final String acksStr = parseAcks(this.getString(ACKS_CONFIG));
        configs.put(ACKS_CONFIG, acksStr);
        // For idempotence producers, values for `RETRIES_CONFIG` and `ACKS_CONFIG` might need to be overridden.
        if (idempotenceEnabled()) {
            boolean userConfiguredRetries = this.originals().containsKey(RETRIES_CONFIG);
            if (this.getInt(RETRIES_CONFIG) == 0) {
                throw new ConfigException("Must set " + ProducerConfig.RETRIES_CONFIG + " to non-zero when using the idempotent producer.");
            }
            configs.put(RETRIES_CONFIG, userConfiguredRetries ? this.getInt(RETRIES_CONFIG) : Integer.MAX_VALUE);

            boolean userConfiguredAcks = this.originals().containsKey(ACKS_CONFIG);
            final short acks = Short.valueOf(acksStr);
            if (userConfiguredAcks && acks != (short) -1) {
                throw new ConfigException("Must set " + ACKS_CONFIG + " to all in order to use the idempotent " +
                        "producer. Otherwise we cannot guarantee idempotence.");
            }
            configs.put(ACKS_CONFIG, "-1");
        }
    }

    private static String parseAcks(String acksString) {
        try {
            return acksString.trim().equalsIgnoreCase("all") ? "-1" : Short.parseShort(acksString.trim()) + "";
        } catch (NumberFormatException e) {
            throw new ConfigException("Invalid configuration value for 'acks': " + acksString);
        }
    }

    /**
     * @deprecated Since 2.7.0. This will be removed in a future major release.
     */
    @Deprecated
    public static Map<String, Object> addSerializerToConfig(Map<String, Object> configs,
                                                            Serializer<?> keySerializer, Serializer<?> valueSerializer) {
        return appendSerializerToConfig(configs, keySerializer, valueSerializer);
    }

    static Map<String, Object> appendSerializerToConfig(Map<String, Object> configs,
                                                        Serializer<?> keySerializer,
                                                        Serializer<?> valueSerializer) {
        Map<String, Object> newConfigs = new HashMap<>(configs);
        if (keySerializer != null)
            newConfigs.put(KEY_SERIALIZER_CLASS_CONFIG, keySerializer.getClass());
        if (valueSerializer != null)
            newConfigs.put(VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer.getClass());
        return newConfigs;
    }

    /**
     * @deprecated Since 2.7.0. This will be removed in a future major release.
     */
    @Deprecated
    public static Properties addSerializerToConfig(Properties properties,
                                                   Serializer<?> keySerializer,
                                                   Serializer<?> valueSerializer) {
        Properties newProperties = new Properties();
        newProperties.putAll(properties);
        if (keySerializer != null)
            newProperties.put(KEY_SERIALIZER_CLASS_CONFIG, keySerializer.getClass().getName());
        if (valueSerializer != null)
            newProperties.put(VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer.getClass().getName());
        return newProperties;
    }

    public ProducerConfig(Properties props) {
        super(CONFIG, props);
    }

    public ProducerConfig(Map<String, Object> props) {
        super(CONFIG, props);
    }

    boolean idempotenceEnabled() {
        boolean userConfiguredIdempotence = this.originals().containsKey(ENABLE_IDEMPOTENCE_CONFIG);
        boolean userConfiguredTransactions = this.originals().containsKey(TRANSACTIONAL_ID_CONFIG);
        boolean idempotenceEnabled = userConfiguredIdempotence && this.getBoolean(ENABLE_IDEMPOTENCE_CONFIG);

        if (!idempotenceEnabled && userConfiguredIdempotence && userConfiguredTransactions)
            throw new ConfigException("Cannot set a " + ProducerConfig.TRANSACTIONAL_ID_CONFIG + " without also enabling idempotence.");
        return userConfiguredTransactions || idempotenceEnabled;
    }

    ProducerConfig(Map<?, ?> props, boolean doLog) {
        super(CONFIG, props, doLog);
    }

    public static Set<String> configNames() {
        return CONFIG.names();
    }

    public static ConfigDef configDef() {
        return new ConfigDef(CONFIG);
    }

    public static void main(String[] args) {
        System.out.println(CONFIG.toHtml());
    }

}
