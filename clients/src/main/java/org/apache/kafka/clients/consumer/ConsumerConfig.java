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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.clients.ClientDnsLookup;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.SecurityConfig;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.ValidString.in;

/**
 * The consumer configuration keys
 */
public class ConsumerConfig extends AbstractConfig {
    private static final ConfigDef CONFIG;

    /*
     * NOTE: DO NOT CHANGE EITHER CONFIG STRINGS OR THEIR JAVA VARIABLE NAMES AS
     * THESE ARE PART OF THE PUBLIC API AND CHANGE WILL BREAK USER CODE.
     */

    /**
     * <code>group.id</code>
     * <p>
     * 用于唯一标志当前消费者所属的消费组的字符串
     * 如果消费者使用组管理功能如subscribe(topic)或使用基于Kafka的偏移量管理策略 该项必须设置
     */
    public static final String GROUP_ID_CONFIG = CommonClientConfigs.GROUP_ID_CONFIG;
    private static final String GROUP_ID_DOC = CommonClientConfigs.GROUP_ID_DOC;

    /**
     * <code>group.instance.id</code>
     */
    public static final String GROUP_INSTANCE_ID_CONFIG = CommonClientConfigs.GROUP_INSTANCE_ID_CONFIG;
    private static final String GROUP_INSTANCE_ID_DOC = CommonClientConfigs.GROUP_INSTANCE_ID_DOC;

    /**
     * <code>max.poll.records</code>
     * <p>
     * 一次调用poll()方法返回的记录最大数量
     */
    public static final String MAX_POLL_RECORDS_CONFIG = "max.poll.records";
    private static final String MAX_POLL_RECORDS_DOC = "The maximum number of records returned in a single call to poll()."
        + " Note, that <code>" + MAX_POLL_RECORDS_CONFIG + "</code> does not impact the underlying fetching behavior."
        + " The consumer will cache the records from each fetch request and returns them incrementally from each poll.";

    /**
     * <code>max.poll.interval.ms</code>
     * <p>
     * 使用消费组的时候调用poll()方法的时间间隔 该条目指定了消费者调用poll()方法的最大时间间隔
     * 如果在此时间内消费者没有调用poll()方法 则broker认为消费者失败 触发再平衡 将分区分配给消费组中其他消费者
     */
    public static final String MAX_POLL_INTERVAL_MS_CONFIG = CommonClientConfigs.MAX_POLL_INTERVAL_MS_CONFIG;
    private static final String MAX_POLL_INTERVAL_MS_DOC = CommonClientConfigs.MAX_POLL_INTERVAL_MS_DOC;
    /**
     * <code>session.timeout.ms</code>
     * <p>
     * 当使用Kafka的消费组的时候 消费者周期性地向broker发送心跳表明自己的存在
     * 如果经过该超时时间还没有收到消费者的心跳 则broker将消费者从消费组移除 并启动重平衡
     * 该值必须在broker配置group.min.session.timeout.ms 和group.max.session.timeout.ms 之间
     */
    public static final String SESSION_TIMEOUT_MS_CONFIG = CommonClientConfigs.SESSION_TIMEOUT_MS_CONFIG;
    private static final String SESSION_TIMEOUT_MS_DOC = CommonClientConfigs.SESSION_TIMEOUT_MS_DOC;

    /**
     * <code>heartbeat.interval.ms</code>
     * <p>
     * 当使用消费组的时候 该条目指定消费者向消费者协调器发送心跳的时间间隔
     * 心跳是为了确保消费者会话的活跃状态 同时在消费者加入或离开消费组的时候方便进行重平衡
     * 该条目的值必须小于session.timeout.ms 也不应该高于session.timeout.ms 的1/3
     * 可以将其调整得更小 以控制正常重新平衡的预期时间
     */
    public static final String HEARTBEAT_INTERVAL_MS_CONFIG = CommonClientConfigs.HEARTBEAT_INTERVAL_MS_CONFIG;
    private static final String HEARTBEAT_INTERVAL_MS_DOC = CommonClientConfigs.HEARTBEAT_INTERVAL_MS_DOC;

    /**
     * <code>bootstrap.servers</code>
     * <p>
     * 向Kafka集群建立初始连接用到的host/port列表
     * 客户端会使用这里列出的所有服务器进行集群其他服务器的发现 而不管是否指定了哪个服务器用作引导
     * 这个列表仅影响用来发现集群所有服务器的初始主机
     * 字符串形式: host1:port1,host2:port2,...
     * 由于这组服务器仅用于建立初始链接 然后发现集群中的所有服务器 因此没有必要将集群中的所有地址写在这里
     */
    public static final String BOOTSTRAP_SERVERS_CONFIG = CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

    /**
     * <code>client.dns.lookup</code>
     */
    public static final String CLIENT_DNS_LOOKUP_CONFIG = CommonClientConfigs.CLIENT_DNS_LOOKUP_CONFIG;

    /**
     * <code>enable.auto.commit</code>
     * <p>
     * 如果设置为true 消费者会自动周期性地向服务器提交偏移量
     */
    public static final String ENABLE_AUTO_COMMIT_CONFIG = "enable.auto.commit";
    private static final String ENABLE_AUTO_COMMIT_DOC = "If true the consumer's offset will be periodically committed in the background.";

    /**
     * <code>auto.commit.interval.ms</code>
     * <p>
     * 如果设置了enable.auto.commit 的值为true 则该值定义了消费者偏移量向Kafka提交的频率
     */
    public static final String AUTO_COMMIT_INTERVAL_MS_CONFIG = "auto.commit.interval.ms";
    private static final String AUTO_COMMIT_INTERVAL_MS_DOC = "The frequency in milliseconds that the consumer offsets are auto-committed to Kafka if <code>enable.auto.commit</code> is set to <code>true</code>.";

    /**
     * <code>partition.assignment.strategy</code>
     * <p>
     * 当使用消费组的时候 分区分配策略的类名
     */
    public static final String PARTITION_ASSIGNMENT_STRATEGY_CONFIG = "partition.assignment.strategy";
    private static final String PARTITION_ASSIGNMENT_STRATEGY_DOC = "A list of class names or class types, " +
        "ordered by preference, of supported partition assignment strategies that the client will use to distribute " +
        "partition ownership amongst consumer instances when group management is used. Available options are:" +
        "<ul>" +
        "<li><code>org.apache.kafka.clients.consumer.RangeAssignor</code>: The default assignor, which works on a per-topic basis.</li>" +
        "<li><code>org.apache.kafka.clients.consumer.RoundRobinAssignor</code>: Assigns partitions to consumers in a round-robin fashion.</li>" +
        "<li><code>org.apache.kafka.clients.consumer.StickyAssignor</code>: Guarantees an assignment that is " +
        "maximally balanced while preserving as many existing partition assignments as possible.</li>" +
        "<li><code>org.apache.kafka.clients.consumer.CooperativeStickyAssignor</code>: Follows the same StickyAssignor " +
        "logic, but allows for cooperative rebalancing.</li>" +
        "</ul>" +
        "<p>Implementing the <code>org.apache.kafka.clients.consumer.ConsumerPartitionAssignor</code> " +
        "interface allows you to plug in a custom assignment strategy.";

    /**
     * <code>auto.offset.reset</code>
     * <p>
     * 当Kafka中没有初始偏移量或当前偏移量在服务器中不存在(如数据被删除了) 此时有以下可选项
     * earliest: 自动重置偏移量到最早的偏移量
     * latest: 自动重置偏移量为最新的偏移量
     * none: 如果消费组原来的(previous)偏移量不存在 则向消费者抛异常
     * anything: 向消费者抛异常
     */
    public static final String AUTO_OFFSET_RESET_CONFIG = "auto.offset.reset";
    public static final String AUTO_OFFSET_RESET_DOC = "What to do when there is no initial offset in Kafka or if the current offset does not exist any more on the server (e.g. because that data has been deleted): <ul><li>earliest: automatically reset the offset to the earliest offset<li>latest: automatically reset the offset to the latest offset</li><li>none: throw exception to the consumer if no previous offset is found for the consumer's group</li><li>anything else: throw exception to the consumer.</li></ul>";

    /**
     * <code>fetch.min.bytes</code>
     * <p>
     * 服务器对每个拉取消息的请求返回的数据量最小值 如果数据量达不到这个值 请求等待 以让更多的数据累积
     * 达到这个值之后响应请求
     * 默认设置是1个字节 表示只要有一个字节的数据 就立即响应请求 或者在没有数据的时候请求超时
     * 将该值设置为大一点儿的数字 会让服务器等待稍微长一点儿的时间以累积数据 如此则可以提高服务器的吞吐量 代价是额外的延迟时间
     */
    public static final String FETCH_MIN_BYTES_CONFIG = "fetch.min.bytes";
    private static final String FETCH_MIN_BYTES_DOC = "The minimum amount of data the server should return for a fetch request. If insufficient data is available the request will wait for that much data to accumulate before answering the request. The default setting of 1 byte means that fetch requests are answered as soon as a single byte of data is available or the fetch request times out waiting for data to arrive. Setting this to something greater than 1 will cause the server to wait for larger amounts of data to accumulate which can improve server throughput a bit at the cost of some additional latency.";

    /**
     * <code>fetch.max.bytes</code>
     * <p>
     * 服务器给单个拉取请求返回的最大数据量 消费者批量拉取消息 如果第一个非空消息批次的值比该值大 消息批也会返回 以让消费者可以接着进行
     * 即该配置并不是绝对的最大值 broker可以接收的消息批最大值通过 message.max.bytes(broker配置) 或max.message.bytes(主题配置)来指定
     * 需要注意的是 消费者一般会并发拉取请求
     */
    public static final String FETCH_MAX_BYTES_CONFIG = "fetch.max.bytes";
    private static final String FETCH_MAX_BYTES_DOC = "The maximum amount of data the server should return for a fetch request. " +
            "Records are fetched in batches by the consumer, and if the first record batch in the first non-empty partition of the fetch is larger than " +
            "this value, the record batch will still be returned to ensure that the consumer can make progress. As such, this is not a absolute maximum. " +
            "The maximum record batch size accepted by the broker is defined via <code>message.max.bytes</code> (broker config) or " +
            "<code>max.message.bytes</code> (topic config). Note that the consumer performs multiple fetches in parallel.";
    public static final int DEFAULT_FETCH_MAX_BYTES = 50 * 1024 * 1024;

    /**
     * <code>fetch.max.wait.ms</code>
     * <p>
     * 如果服务器端的数据量达不到fetch.min.bytes 的话 服务器端不能立即响应请求
     * 该时间用于配置服务器端阻塞请求的最大时长
     */
    public static final String FETCH_MAX_WAIT_MS_CONFIG = "fetch.max.wait.ms";
    private static final String FETCH_MAX_WAIT_MS_DOC = "The maximum amount of time the server will block before answering the fetch request if there isn't sufficient data to immediately satisfy the requirement given by fetch.min.bytes.";

    /**
     * <code>metadata.max.age.ms</code>
     */
    public static final String METADATA_MAX_AGE_CONFIG = CommonClientConfigs.METADATA_MAX_AGE_CONFIG;

    /**
     * <code>max.partition.fetch.bytes</code>
     * <p>
     * 对每个分区 服务器返回的最大数量 消费者按批次拉取数据 如果非空分区的第一个记录大于这个值 批处理依然可以返回
     * 以保证消费者可以进行下去 broker接收批的大小由message.max.bytes(broker参数)或max.message.bytes(主题参数)指定
     * fetch.max.bytes 用于限制消费者单次请求的数据量
     */
    public static final String MAX_PARTITION_FETCH_BYTES_CONFIG = "max.partition.fetch.bytes";
    private static final String MAX_PARTITION_FETCH_BYTES_DOC = "The maximum amount of data per-partition the server " +
            "will return. Records are fetched in batches by the consumer. If the first record batch in the first non-empty " +
            "partition of the fetch is larger than this limit, the " +
            "batch will still be returned to ensure that the consumer can make progress. The maximum record batch size " +
            "accepted by the broker is defined via <code>message.max.bytes</code> (broker config) or " +
            "<code>max.message.bytes</code> (topic config). See " + FETCH_MAX_BYTES_CONFIG + " for limiting the consumer request size.";
    public static final int DEFAULT_MAX_PARTITION_FETCH_BYTES = 1 * 1024 * 1024;

    /**
     * <code>send.buffer.bytes</code>
     * <p>
     * 用于TCP发送数据时使用的缓冲大小(SO_SNDBUF) -1表示使用OS默认的缓冲区大小
     */
    public static final String SEND_BUFFER_CONFIG = CommonClientConfigs.SEND_BUFFER_CONFIG;

    /**
     * <code>receive.buffer.bytes</code>
     * <p>
     * TCP连接接收数据的缓存(SO_RCVBUF) -1表示使用操作系统的默认值
     */
    public static final String RECEIVE_BUFFER_CONFIG = CommonClientConfigs.RECEIVE_BUFFER_CONFIG;

    /**
     * <code>client.id</code>
     * <p>
     * 当从服务器消费消息的时候向服务器发送的id字符串 在ip/port基础上提供应用的逻辑名称
     * 记录在服务端的请求日志中 用于追踪请求的源
     */
    public static final String CLIENT_ID_CONFIG = CommonClientConfigs.CLIENT_ID_CONFIG;

    /**
     * <code>client.rack</code>
     */
    public static final String CLIENT_RACK_CONFIG = CommonClientConfigs.CLIENT_RACK_CONFIG;

    /**
     * <code>reconnect.backoff.ms</code>
     * <p>
     * 重新连接主机的等待时间 避免了重连的密集循环 该等待时间应用于该客户端到broker的所有连接
     */
    public static final String RECONNECT_BACKOFF_MS_CONFIG = CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG;

    /**
     * <code>reconnect.backoff.max.ms</code>
     * <p>
     * 重新连接到反复连接失败的broker时要等待的最长时间 (以毫秒为单位) 如果提供此选项 则对于每个连续的连接失败
     * 每台主机的退避将成倍增加 直至达到此最大值 在计算退避增量之后 添加20％的随机抖动以避免连接风暴
     */
    public static final String RECONNECT_BACKOFF_MAX_MS_CONFIG = CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG;

    /**
     * <code>retry.backoff.ms</code>
     * <p>
     * 在发生失败的时候如果需要重试 则该配置表示客户端等待多长时间再发起重试 该时间的存在避免了密集循环
     */
    public static final String RETRY_BACKOFF_MS_CONFIG = CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG;

    /**
     * <code>metrics.sample.window.ms</code>
     * <p>
     * 计算指标样本的时间窗口
     */
    public static final String METRICS_SAMPLE_WINDOW_MS_CONFIG = CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG;

    /**
     * <code>metrics.num.samples</code>
     * <p>
     * 用于计算指标而维护的样本数量
     */
    public static final String METRICS_NUM_SAMPLES_CONFIG = CommonClientConfigs.METRICS_NUM_SAMPLES_CONFIG;

    /**
     * <code>metrics.log.level</code>
     */
    public static final String METRICS_RECORDING_LEVEL_CONFIG = CommonClientConfigs.METRICS_RECORDING_LEVEL_CONFIG;

    /**
     * <code>metric.reporters</code>
     */
    public static final String METRIC_REPORTER_CLASSES_CONFIG = CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG;

    /**
     * <code>check.crcs</code>
     * <p>
     * 自动计算被消费的消息的CRC32校验值 可以确保在传输过程中或磁盘存储过程中消息没有被破坏
     * 它会增加额外的负载 在追求极致性能的场合禁用
     */
    public static final String CHECK_CRCS_CONFIG = "check.crcs";
    private static final String CHECK_CRCS_DOC = "Automatically check the CRC32 of the records consumed. This ensures no on-the-wire or on-disk corruption to the messages occurred. This check adds some overhead, so it may be disabled in cases seeking extreme performance.";

    /**
     * <code>key.deserializer</code>
     * <p>
     * key的反序列化类 该类需要实现{@link org.apache.kafka.common.serialization.Deserializer} 接口
     */
    public static final String KEY_DESERIALIZER_CLASS_CONFIG = "key.deserializer";
    public static final String KEY_DESERIALIZER_CLASS_DOC = "Deserializer class for key that implements the <code>org.apache.kafka.common.serialization.Deserializer</code> interface.";

    /**
     * <code>value.deserializer</code>
     * <p>
     * 实现了{@link org.apache.kafka.common.serialization.Deserializer} 接口的反序列化器
     * 用于对消息的value进行反序列化
     */
    public static final String VALUE_DESERIALIZER_CLASS_CONFIG = "value.deserializer";
    public static final String VALUE_DESERIALIZER_CLASS_DOC = "Deserializer class for value that implements the <code>org.apache.kafka.common.serialization.Deserializer</code> interface.";

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
     * 在这个时间之后关闭空闲的连接
     */
    public static final String CONNECTIONS_MAX_IDLE_MS_CONFIG = CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG;

    /**
     * <code>request.timeout.ms</code>
     * <p>
     * 客户端等待服务端响应的最大时间 如果该时间超时 则客户端要么重新发起请求 要么如果重试耗尽 请求失败
     */
    public static final String REQUEST_TIMEOUT_MS_CONFIG = CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG;
    private static final String REQUEST_TIMEOUT_MS_DOC = CommonClientConfigs.REQUEST_TIMEOUT_MS_DOC;

    /**
     * <code>default.api.timeout.ms</code>
     */
    public static final String DEFAULT_API_TIMEOUT_MS_CONFIG = CommonClientConfigs.DEFAULT_API_TIMEOUT_MS_CONFIG;

    /**
     * <code>interceptor.classes</code>
     * <p>
     * 拦截器类的列表 默认没有拦截器 拦截器是消费者的拦截器
     * 该拦截器需要实现 {@link org.apache.kafka.clients.consumer.ConsumerInterceptor} 接口
     * 拦截器可用于对消费者接收到的消息进行拦截处理
     */
    public static final String INTERCEPTOR_CLASSES_CONFIG = "interceptor.classes";
    public static final String INTERCEPTOR_CLASSES_DOC = "A list of classes to use as interceptors. "
            + "Implementing the <code>org.apache.kafka.clients.consumer.ConsumerInterceptor</code> interface allows you to intercept (and possibly mutate) records "
            + "received by the consumer. By default, there are no interceptors.";


    /**
     * <code>exclude.internal.topics</code>
     * <p>
     * 是否内部主题应该暴露给消费者 如果该条目设置为true 则只能先订阅再拉取
     */
    public static final String EXCLUDE_INTERNAL_TOPICS_CONFIG = "exclude.internal.topics";
    private static final String EXCLUDE_INTERNAL_TOPICS_DOC = "Whether internal topics matching a subscribed pattern should " +
            "be excluded from the subscription. It is always possible to explicitly subscribe to an internal topic.";
    public static final boolean DEFAULT_EXCLUDE_INTERNAL_TOPICS = true;

    /**
     * <code>internal.leave.group.on.close</code>
     * Whether or not the consumer should leave the group on close. If set to <code>false</code> then a rebalance
     * won't occur until <code>session.timeout.ms</code> expires.
     *
     * <p>
     * Note: this is an internal configuration and could be changed in the future in a backward incompatible way
     */
    static final String LEAVE_GROUP_ON_CLOSE_CONFIG = "internal.leave.group.on.close";

    /**
     * <code>internal.throw.on.fetch.stable.offset.unsupported</code>
     * Whether or not the consumer should throw when the new stable offset feature is supported.
     * If set to <code>true</code> then the client shall crash upon hitting it.
     * The purpose of this flag is to prevent unexpected broker downgrade which makes
     * the offset fetch protection against pending commit invalid. The safest approach
     * is to fail fast to avoid introducing correctness issue.
     *
     * <p>
     * Note: this is an internal configuration and could be changed in the future in a backward incompatible way
     */
    static final String THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED = "internal.throw.on.fetch.stable.offset.unsupported";

    /**
     * <code>isolation.level</code>
     * <p>
     * 控制如何读取事务消息 如果设置了read_committed 消费者的poll()方法只会返回已经提交的事务消息
     * 如果设置了read_uncommitted(默认值)消费者的poll方法返回所有的消息 即使是已经取消的事务消息
     * <p>
     * 非事务消息以上两种情况都返回 消息总是以偏移量的顺序返回
     * read_committed 只能返回到达LSO的消息 在LSO之后出现的消息只能等待相关的事务提交之后才能看到
     * read_committed 模式 如果有未提交的事务 消费者不能读取到直到HW的消息
     * read_committed 的seekToEnd方法返回LSO
     */
    public static final String ISOLATION_LEVEL_CONFIG = "isolation.level";
    public static final String ISOLATION_LEVEL_DOC = "Controls how to read messages written transactionally. If set to <code>read_committed</code>, consumer.poll() will only return" +
            " transactional messages which have been committed. If set to <code>read_uncommitted</code> (the default), consumer.poll() will return all messages, even transactional messages" +
            " which have been aborted. Non-transactional messages will be returned unconditionally in either mode. <p>Messages will always be returned in offset order. Hence, in " +
            " <code>read_committed</code> mode, consumer.poll() will only return messages up to the last stable offset (LSO), which is the one less than the offset of the first open transaction." +
            " In particular any messages appearing after messages belonging to ongoing transactions will be withheld until the relevant transaction has been completed. As a result, <code>read_committed</code>" +
            " consumers will not be able to read up to the high watermark when there are in flight transactions.</p><p> Further, when in <code>read_committed</code> the seekToEnd method will" +
            " return the LSO";

    public static final String DEFAULT_ISOLATION_LEVEL = IsolationLevel.READ_UNCOMMITTED.toString().toLowerCase(Locale.ROOT);

    /**
     * <code>allow.auto.create.topics</code>
     */
    public static final String ALLOW_AUTO_CREATE_TOPICS_CONFIG = "allow.auto.create.topics";
    private static final String ALLOW_AUTO_CREATE_TOPICS_DOC = "Allow automatic topic creation on the broker when" +
            " subscribing to or assigning a topic. A topic being subscribed to will be automatically created only if the" +
            " broker allows for it using `auto.create.topics.enable` broker configuration. This configuration must" +
            " be set to `false` when using brokers older than 0.11.0";
    public static final boolean DEFAULT_ALLOW_AUTO_CREATE_TOPICS = true;

    /**
     * <code>security.providers</code>
     */
    public static final String SECURITY_PROVIDERS_CONFIG = SecurityConfig.SECURITY_PROVIDERS_CONFIG;
    private static final String SECURITY_PROVIDERS_DOC = SecurityConfig.SECURITY_PROVIDERS_DOC;

    private static final AtomicInteger CONSUMER_CLIENT_ID_SEQUENCE = new AtomicInteger(1);

    static {
        CONFIG = new ConfigDef().define(BOOTSTRAP_SERVERS_CONFIG,
                                        Type.LIST,
                                        Collections.emptyList(),
                                        new ConfigDef.NonNullValidator(),
                                        Importance.HIGH,
                                        CommonClientConfigs.BOOTSTRAP_SERVERS_DOC)
                                .define(CLIENT_DNS_LOOKUP_CONFIG,
                                        Type.STRING,
                                        ClientDnsLookup.USE_ALL_DNS_IPS.toString(),
                                        in(ClientDnsLookup.USE_ALL_DNS_IPS.toString(),
                                           ClientDnsLookup.RESOLVE_CANONICAL_BOOTSTRAP_SERVERS_ONLY.toString()),
                                        Importance.MEDIUM,
                                        CommonClientConfigs.CLIENT_DNS_LOOKUP_DOC)
                                .define(GROUP_ID_CONFIG, Type.STRING, null, Importance.HIGH, GROUP_ID_DOC)
                                .define(GROUP_INSTANCE_ID_CONFIG,
                                        Type.STRING,
                                        null,
                                        Importance.MEDIUM,
                                        GROUP_INSTANCE_ID_DOC)
                                .define(SESSION_TIMEOUT_MS_CONFIG,
                                        Type.INT,
                                        45000,
                                        Importance.HIGH,
                                        SESSION_TIMEOUT_MS_DOC)
                                .define(HEARTBEAT_INTERVAL_MS_CONFIG,
                                        Type.INT,
                                        3000,
                                        Importance.HIGH,
                                        HEARTBEAT_INTERVAL_MS_DOC)
                                .define(PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                                        Type.LIST,
                                        Collections.singletonList(RangeAssignor.class),
                                        new ConfigDef.NonNullValidator(),
                                        Importance.MEDIUM,
                                        PARTITION_ASSIGNMENT_STRATEGY_DOC)
                                .define(METADATA_MAX_AGE_CONFIG,
                                        Type.LONG,
                                        5 * 60 * 1000,
                                        atLeast(0),
                                        Importance.LOW,
                                        CommonClientConfigs.METADATA_MAX_AGE_DOC)
                                .define(ENABLE_AUTO_COMMIT_CONFIG,
                                        Type.BOOLEAN,
                                        true,
                                        Importance.MEDIUM,
                                        ENABLE_AUTO_COMMIT_DOC)
                                .define(AUTO_COMMIT_INTERVAL_MS_CONFIG,
                                        Type.INT,
                                        5000,
                                        atLeast(0),
                                        Importance.LOW,
                                        AUTO_COMMIT_INTERVAL_MS_DOC)
                                .define(CLIENT_ID_CONFIG,
                                        Type.STRING,
                                        "",
                                        Importance.LOW,
                                        CommonClientConfigs.CLIENT_ID_DOC)
                                .define(CLIENT_RACK_CONFIG,
                                        Type.STRING,
                                        "",
                                        Importance.LOW,
                                        CommonClientConfigs.CLIENT_RACK_DOC)
                                .define(MAX_PARTITION_FETCH_BYTES_CONFIG,
                                        Type.INT,
                                        DEFAULT_MAX_PARTITION_FETCH_BYTES,
                                        atLeast(0),
                                        Importance.HIGH,
                                        MAX_PARTITION_FETCH_BYTES_DOC)
                                .define(SEND_BUFFER_CONFIG,
                                        Type.INT,
                                        128 * 1024,
                                        atLeast(CommonClientConfigs.SEND_BUFFER_LOWER_BOUND),
                                        Importance.MEDIUM,
                                        CommonClientConfigs.SEND_BUFFER_DOC)
                                .define(RECEIVE_BUFFER_CONFIG,
                                        Type.INT,
                                        64 * 1024,
                                        atLeast(CommonClientConfigs.RECEIVE_BUFFER_LOWER_BOUND),
                                        Importance.MEDIUM,
                                        CommonClientConfigs.RECEIVE_BUFFER_DOC)
                                .define(FETCH_MIN_BYTES_CONFIG,
                                        Type.INT,
                                        1,
                                        atLeast(0),
                                        Importance.HIGH,
                                        FETCH_MIN_BYTES_DOC)
                                .define(FETCH_MAX_BYTES_CONFIG,
                                        Type.INT,
                                        DEFAULT_FETCH_MAX_BYTES,
                                        atLeast(0),
                                        Importance.MEDIUM,
                                        FETCH_MAX_BYTES_DOC)
                                .define(FETCH_MAX_WAIT_MS_CONFIG,
                                        Type.INT,
                                        500,
                                        atLeast(0),
                                        Importance.LOW,
                                        FETCH_MAX_WAIT_MS_DOC)
                                .define(RECONNECT_BACKOFF_MS_CONFIG,
                                        Type.LONG,
                                        50L,
                                        atLeast(0L),
                                        Importance.LOW,
                                        CommonClientConfigs.RECONNECT_BACKOFF_MS_DOC)
                                .define(RECONNECT_BACKOFF_MAX_MS_CONFIG,
                                        Type.LONG,
                                        1000L,
                                        atLeast(0L),
                                        Importance.LOW,
                                        CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_DOC)
                                .define(RETRY_BACKOFF_MS_CONFIG,
                                        Type.LONG,
                                        100L,
                                        atLeast(0L),
                                        Importance.LOW,
                                        CommonClientConfigs.RETRY_BACKOFF_MS_DOC)
                                .define(AUTO_OFFSET_RESET_CONFIG,
                                        Type.STRING,
                                        "latest",
                                        in("latest", "earliest", "none"),
                                        Importance.MEDIUM,
                                        AUTO_OFFSET_RESET_DOC)
                                .define(CHECK_CRCS_CONFIG,
                                        Type.BOOLEAN,
                                        true,
                                        Importance.LOW,
                                        CHECK_CRCS_DOC)
                                .define(METRICS_SAMPLE_WINDOW_MS_CONFIG,
                                        Type.LONG,
                                        30000,
                                        atLeast(0),
                                        Importance.LOW,
                                        CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_DOC)
                                .define(METRICS_NUM_SAMPLES_CONFIG,
                                        Type.INT,
                                        2,
                                        atLeast(1),
                                        Importance.LOW,
                                        CommonClientConfigs.METRICS_NUM_SAMPLES_DOC)
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
                                .define(KEY_DESERIALIZER_CLASS_CONFIG,
                                        Type.CLASS,
                                        Importance.HIGH,
                                        KEY_DESERIALIZER_CLASS_DOC)
                                .define(VALUE_DESERIALIZER_CLASS_CONFIG,
                                        Type.CLASS,
                                        Importance.HIGH,
                                        VALUE_DESERIALIZER_CLASS_DOC)
                                .define(REQUEST_TIMEOUT_MS_CONFIG,
                                        Type.INT,
                                        30000,
                                        atLeast(0),
                                        Importance.MEDIUM,
                                        REQUEST_TIMEOUT_MS_DOC)
                                .define(DEFAULT_API_TIMEOUT_MS_CONFIG,
                                        Type.INT,
                                        60 * 1000,
                                        atLeast(0),
                                        Importance.MEDIUM,
                                        CommonClientConfigs.DEFAULT_API_TIMEOUT_MS_DOC)
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
                                .define(INTERCEPTOR_CLASSES_CONFIG,
                                        Type.LIST,
                                        Collections.emptyList(),
                                        new ConfigDef.NonNullValidator(),
                                        Importance.LOW,
                                        INTERCEPTOR_CLASSES_DOC)
                                .define(MAX_POLL_RECORDS_CONFIG,
                                        Type.INT,
                                        500,
                                        atLeast(1),
                                        Importance.MEDIUM,
                                        MAX_POLL_RECORDS_DOC)
                                .define(MAX_POLL_INTERVAL_MS_CONFIG,
                                        Type.INT,
                                        300000,
                                        atLeast(1),
                                        Importance.MEDIUM,
                                        MAX_POLL_INTERVAL_MS_DOC)
                                .define(EXCLUDE_INTERNAL_TOPICS_CONFIG,
                                        Type.BOOLEAN,
                                        DEFAULT_EXCLUDE_INTERNAL_TOPICS,
                                        Importance.MEDIUM,
                                        EXCLUDE_INTERNAL_TOPICS_DOC)
                                .defineInternal(LEAVE_GROUP_ON_CLOSE_CONFIG,
                                        Type.BOOLEAN,
                                        true,
                                        Importance.LOW)
                                .defineInternal(THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED,
                                        Type.BOOLEAN,
                                        false,
                                        Importance.LOW)
                                .define(ISOLATION_LEVEL_CONFIG,
                                        Type.STRING,
                                        DEFAULT_ISOLATION_LEVEL,
                                        in(IsolationLevel.READ_COMMITTED.toString().toLowerCase(Locale.ROOT), IsolationLevel.READ_UNCOMMITTED.toString().toLowerCase(Locale.ROOT)),
                                        Importance.MEDIUM,
                                        ISOLATION_LEVEL_DOC)
                                .define(ALLOW_AUTO_CREATE_TOPICS_CONFIG,
                                        Type.BOOLEAN,
                                        DEFAULT_ALLOW_AUTO_CREATE_TOPICS,
                                        Importance.MEDIUM,
                                        ALLOW_AUTO_CREATE_TOPICS_DOC)
                                // security support
                                .define(SECURITY_PROVIDERS_CONFIG,
                                        Type.STRING,
                                        null,
                                        Importance.LOW,
                                        SECURITY_PROVIDERS_DOC)
                                .define(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                                        Type.STRING,
                                        CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL,
                                        Importance.MEDIUM,
                                        CommonClientConfigs.SECURITY_PROTOCOL_DOC)
                                .withClientSslSupport()
                                .withClientSaslSupport();
    }

    @Override
    protected Map<String, Object> postProcessParsedConfig(final Map<String, Object> parsedValues) {
        Map<String, Object> refinedConfigs = CommonClientConfigs.postProcessReconnectBackoffConfigs(this, parsedValues);
        maybeOverrideClientId(refinedConfigs);
        return refinedConfigs;
    }

    private void maybeOverrideClientId(Map<String, Object> configs) {
        final String clientId = this.getString(CLIENT_ID_CONFIG);
        if (clientId == null || clientId.isEmpty()) {
            final String groupId = this.getString(GROUP_ID_CONFIG);
            String groupInstanceId = this.getString(GROUP_INSTANCE_ID_CONFIG);
            if (groupInstanceId != null) {
                JoinGroupRequest.validateGroupInstanceId(groupInstanceId);
            }

            String groupInstanceIdPart = groupInstanceId != null ? groupInstanceId : CONSUMER_CLIENT_ID_SEQUENCE.getAndIncrement() + "";
            String generatedClientId = String.format("consumer-%s-%s", groupId, groupInstanceIdPart);
            configs.put(CLIENT_ID_CONFIG, generatedClientId);
        }
    }

    protected static Map<String, Object> appendDeserializerToConfig(Map<String, Object> configs,
                                                                    Deserializer<?> keyDeserializer,
                                                                    Deserializer<?> valueDeserializer) {
        Map<String, Object> newConfigs = new HashMap<>(configs);
        if (keyDeserializer != null) {
            newConfigs.put(KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.getClass());
        }
        if (valueDeserializer != null) {
            newConfigs.put(VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getClass());
        }
        return newConfigs;
    }

    boolean maybeOverrideEnableAutoCommit() {
        Optional<String> groupId = Optional.ofNullable(getString(CommonClientConfigs.GROUP_ID_CONFIG));
        boolean enableAutoCommit = getBoolean(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
        if (!groupId.isPresent()) { // overwrite in case of default group id where the config is not explicitly provided
            if (!originals().containsKey(ENABLE_AUTO_COMMIT_CONFIG)) {
                enableAutoCommit = false;
            } else if (enableAutoCommit) {
                throw new InvalidConfigurationException(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG + " cannot be set to true when default group id (null) is used.");
            }
        }
        return enableAutoCommit;
    }

    public ConsumerConfig(Properties props) {
        super(CONFIG, props);
    }

    public ConsumerConfig(Map<String, Object> props) {
        super(CONFIG, props);
    }

    protected ConsumerConfig(Map<?, ?> props, boolean doLog) {
        super(CONFIG, props, doLog);
    }

    public static Set<String> configNames() {
        return CONFIG.names();
    }

    public static ConfigDef configDef() {
        return new ConfigDef(CONFIG);
    }

    public static void main(String[] args) {
        System.out.println(CONFIG.toHtml(4, config -> "consumerconfigs_" + config));
    }

}
