/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import java.util.Collections
import java.util.Optional

import kafka.api._
import kafka.cluster.BrokerEndPoint
import kafka.log.{LeaderOffsetIncremented, LogAppendInfo}
import kafka.server.AbstractFetcherThread.ReplicaFetch
import kafka.server.AbstractFetcherThread.ResultWithPartitions
import kafka.utils.Implicits._
import org.apache.kafka.clients.FetchSessionHandler
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.KafkaStorageException
import org.apache.kafka.common.message.ListOffsetRequestData.{ListOffsetPartition, ListOffsetTopic}
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.{MemoryRecords, Records}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.utils.{LogContext, Time}

import scala.jdk.CollectionConverters._
import scala.collection.{Map, mutable}
import scala.compat.java8.OptionConverters._

/**
 * ReplicaFetcherThread 继承了 AbstractFetcherThread 类
 * 是 Follower 副本端创建的线程 用于向 Leader 副本拉取消息数据
 *
 * @param name                       线程名字
 * @param fetcherId                  Follower拉取的线程ID(即线程编号) 单台Broker允许存在多个ReplicaFetcherThread线程
 *                                   Broker 端参数 num.replica.fetchers决定了 Kafka 到底创建多少个 Follower 拉取线程
 * @param sourceBroker               源 Broker 节点信息(源 Broker 是指此线程要从哪个 Broker 上读取数据)
 * @param brokerConfig               KafkaConfig 类实例  封装了 Broker 端所有的参数信息
 *                                   ReplicaFetcherThread 类通过它来获取 Broker 端指定参数的值
 * @param failedPartitions           线程处理过程报错的分区集合
 * @param replicaMgr                 副本管理器 该线程类通过副本管理器来获取分区对象 | 副本对象以及它们下面的日志对象
 * @param metrics
 * @param time
 * @param quota                      用做Follower(拉取速度控制)限流 限流属于高阶用法 如果想深入理解这部分内容的话可以自行阅读 ReplicationQuotaManager 类
 * @param leaderEndpointBlockingSend 用于实现同步发送请求的类(同步发送是指该线程使用它给指定 Broker 发送请求 然后线程处于阻塞状态 直到接收到 Broker 返回的 Response)
 */
class ReplicaFetcherThread(name: String,
                           fetcherId: Int,
                           sourceBroker: BrokerEndPoint,
                           brokerConfig: KafkaConfig,
                           failedPartitions: FailedPartitions,
                           replicaMgr: ReplicaManager,
                           metrics: Metrics,
                           time: Time,
                           quota: ReplicaQuota,
                           leaderEndpointBlockingSend: Option[BlockingSend] = None)
  extends AbstractFetcherThread(name = name,
                                clientId = name,
                                sourceBroker = sourceBroker,
                                failedPartitions,
                                fetchBackOffMs = brokerConfig.replicaFetchBackoffMs,
                                isInterruptible = false,
                                replicaMgr.brokerTopicStats) {

  // 副本Id就是副本所在Broker的Id
  private val replicaId = brokerConfig.brokerId
  private val logContext = new LogContext(s"[ReplicaFetcher replicaId=$replicaId, leaderId=${sourceBroker.id}, " +
    s"fetcherId=$fetcherId] ")
  this.logIdent = logContext.logPrefix

  // 用于执行请求发送的类
  private val leaderEndpoint = leaderEndpointBlockingSend.getOrElse(
    new ReplicaFetcherBlockingSend(sourceBroker, brokerConfig, metrics, time, fetcherId,
      s"broker-$replicaId-fetcher-$fetcherId", logContext))

  // Visible for testing
  private[server] val fetchRequestVersion: Short =
    if (brokerConfig.interBrokerProtocolVersion >= KAFKA_2_7_IV1) 12
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_2_3_IV1) 11
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_2_1_IV2) 10
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_2_0_IV1) 8
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_1_1_IV0) 7
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_11_0_IV1) 5
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_11_0_IV0) 4
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_10_1_IV1) 3
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_10_0_IV0) 2
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_9_0) 1
    else 0

  // Visible for testing
  private[server] val offsetForLeaderEpochRequestVersion: Short =
    if (brokerConfig.interBrokerProtocolVersion >= KAFKA_2_8_IV0) 4
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_2_3_IV1) 3
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_2_1_IV1) 2
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_2_0_IV0) 1
    else 0

  // Visible for testing
  private[server] val listOffsetRequestVersion: Short =
    if (brokerConfig.interBrokerProtocolVersion >= KAFKA_2_8_IV0) 6
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_2_2_IV1) 5
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_2_1_IV1) 4
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_2_0_IV1) 3
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_11_0_IV0) 2
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_10_1_IV2) 1
    else 0

  /*以下4 个参数都是 FETCH 请求的参数 主要控制了 Follower 副本拉取 Leader 副本消息的行为(比如一次请求到底能够获取多少字节的数据 | 或者当未达到累积阈值 FETCH 请求等待多长时间等)*/
  // Follower发送的FETCH请求被处理返回前的最长等待时间 是 Broker 端参数 replica.fetch.wait.max.ms 的值
  private val maxWait = brokerConfig.replicaFetchWaitMaxMs
  // 每个 FETCH Response 返回前必须要累积的最少字节数 是 Broker 端参数 replica.fetch.min.bytes 的值
  private val minBytes = brokerConfig.replicaFetchMinBytes
  // 每个合法 FETCH Response 的最大字节数 是 Broker 端参数 replica.fetch.response.max.bytes 的值
  private val maxBytes = brokerConfig.replicaFetchResponseMaxBytes
  // 单个分区能够获取到的最大字节数 是 Broker 端参数 replica.fetch.max.bytes 的值
  private val fetchSize = brokerConfig.replicaFetchMaxBytes
  override protected val isOffsetForLeaderEpochSupported: Boolean = brokerConfig.interBrokerProtocolVersion >= KAFKA_0_11_0_IV2
  override protected val isTruncationOnFetchSupported = ApiVersion.isTruncationOnFetchSupported(brokerConfig.interBrokerProtocolVersion)
  // 维持某个Broker连接上获取会话状态的类
  val fetchSessionHandler = new FetchSessionHandler(logContext, sourceBroker.id)

  override protected def latestEpoch(topicPartition: TopicPartition): Option[Int] = {
    replicaMgr.localLogOrException(topicPartition).latestEpoch
  }

  override protected def logStartOffset(topicPartition: TopicPartition): Long = {
    replicaMgr.localLogOrException(topicPartition).logStartOffset
  }

  override protected def logEndOffset(topicPartition: TopicPartition): Long = {
    replicaMgr.localLogOrException(topicPartition).logEndOffset
  }

  override protected def endOffsetForEpoch(topicPartition: TopicPartition, epoch: Int): Option[OffsetAndEpoch] = {
    replicaMgr.localLogOrException(topicPartition).endOffsetForEpoch(epoch)
  }

  override def initiateShutdown(): Boolean = {
    val justShutdown = super.initiateShutdown()
    if (justShutdown) {
      // This is thread-safe, so we don't expect any exceptions, but catch and log any errors
      // to avoid failing the caller, especially during shutdown. We will attempt to close
      // leaderEndpoint after the thread terminates.
      try {
        leaderEndpoint.initiateClose()
      } catch {
        case t: Throwable =>
          error(s"Failed to initiate shutdown of leader endpoint $leaderEndpoint after initiating replica fetcher thread shutdown", t)
      }
    }
    justShutdown
  }

  override def awaitShutdown(): Unit = {
    super.awaitShutdown()
    // We don't expect any exceptions here, but catch and log any errors to avoid failing the caller,
    // especially during shutdown. It is safe to catch the exception here without causing correctness
    // issue because we are going to shutdown the thread and will not re-use the leaderEndpoint anyway.
    try {
      leaderEndpoint.close()
    } catch {
      case t: Throwable =>
        error(s"Failed to close leader endpoint $leaderEndpoint after shutting down replica fetcher thread", t)
    }
  }

  /**
   * process fetched data
   * 处理拉取的消息
   *
   * @param topicPartition 读取哪个分区的数据
   * @param fetchOffset    读取到的最新位移值
   * @param partitionData  读取到的分区消息数据
   * @return 写入已读取消息数据前的元数据 对于 Follower 副本读消息写入日志而言 可以忽略这里的 Option 因为肯定会返回具体的 LogAppendInfo 实例而不会是 None
   *         LogAppendInfo封装了很多消息数据被写入到日志前的重要元数据信息 比如首条消息的位移值 | 最后一条消息位移值 |最大时间戳等
   */
  override def processPartitionData(topicPartition: TopicPartition,
                                    fetchOffset: Long,
                                    partitionData: FetchData): Option[LogAppendInfo] = {
    val logTrace = isTraceEnabled
    // 从副本管理器获取指定Topic分区对象
    val partition = replicaMgr.nonOfflinePartition(topicPartition).get
    // 获取日志对象
    val log = partition.localLogOrException
    // 将获取到的数据转换成符合格式要求的消息集合
    val records = toMemoryRecords(partitionData.records)

    maybeWarnIfOversizedRecords(records, topicPartition)

    if (fetchOffset != log.logEndOffset) { // 要读取的起始位移值如果不是本地日志LEO值则视为异常情况
      throw new IllegalStateException("Offset mismatch for partition %s: fetched offset = %d, log end offset = %d.".format(
        topicPartition, fetchOffset, log.logEndOffset))
    }

    if (logTrace)
      trace("Follower has replica log end offset %d for partition %s. Received %d messages and leader hw %d"
        .format(log.logEndOffset, topicPartition, records.sizeInBytes, partitionData.highWatermark))

    // Append the leader's messages to the log
    val logAppendInfo = partition.appendRecordsToFollowerOrFutureReplica(records, isFuture = false) // 写入Follower副本本地日志 沿着这个写入方法一路追下去 它调用的是 appendAsFollower 方法

    if (logTrace)
      trace("Follower has replica log end offset %d after appending %d bytes of messages for partition %s"
        .format(log.logEndOffset, records.sizeInBytes, topicPartition))
    val leaderLogStartOffset = partitionData.logStartOffset

    // For the follower replica, we do not need to keep its segment base offset and physical position.
    // These values will be computed upon becoming leader or handling a preferred read replica fetch.
    val followerHighWatermark = log.updateHighWatermark(partitionData.highWatermark) // 更新Follower副本的高水位值
    // 更新Log Start Offset值的原因: Leader 的 Log Start Offset 可能发生变化(如用户手动执行了删除消息的操作等)
    // Follower 副本的日志需要和 Leader 保持严格的一致 如果 Leader 的该值发生变化 Follower 自然也要发生变化 以保持一致
    log.maybeIncrementLogStartOffset(leaderLogStartOffset, LeaderOffsetIncremented) // 尝试更新Follower副本的Log Start Offset值
    if (logTrace)
      trace(s"Follower set replica high watermark for partition $topicPartition to $followerHighWatermark")

    // Traffic from both in-sync and out of sync replicas are accounted for in replication quota to ensure total replication
    // traffic doesn't exceed quota.
    if (quota.isThrottled(topicPartition)) { // 副本消息拉取限流
      quota.record(records.sizeInBytes)
    }

    if (partition.isReassigning && partition.isAddingLocalReplica) { // 更新统计指标值
      brokerTopicStats.updateReassignmentBytesIn(records.sizeInBytes)
    }
    brokerTopicStats.updateReplicationBytesIn(records.sizeInBytes)
    logAppendInfo // 返回日志写入结果
  }

  def maybeWarnIfOversizedRecords(records: MemoryRecords, topicPartition: TopicPartition): Unit = {
    // oversized messages don't cause replication to fail from fetch request version 3 (KIP-74)
    if (fetchRequestVersion <= 2 && records.sizeInBytes > 0 && records.validBytes <= 0)
      error(s"Replication is failing due to a message that is greater than replica.fetch.max.bytes for partition $topicPartition. " +
        "This generally occurs when the max.message.bytes has been overridden to exceed this value and a suitably large " +
        "message has also been sent. To fix this problem increase replica.fetch.max.bytes in your broker config to be " +
        "equal or larger than your settings for max.message.bytes, both at a broker and topic level.")
  }


  override protected def fetchFromLeader(fetchRequest: FetchRequest.Builder): Map[TopicPartition, FetchData] = {
    try {
      val clientResponse = leaderEndpoint.sendRequest(fetchRequest)
      val fetchResponse = clientResponse.responseBody.asInstanceOf[FetchResponse[Records]]
      if (!fetchSessionHandler.handleResponse(fetchResponse)) {
        Map.empty
      } else {
        fetchResponse.responseData.asScala
      }
    } catch {
      case t: Throwable =>
        fetchSessionHandler.handleError(t)
        throw t
    }
  }

  override protected def fetchEarliestOffsetFromLeader(topicPartition: TopicPartition, currentLeaderEpoch: Int): Long = {
    fetchOffsetFromLeader(topicPartition, currentLeaderEpoch, ListOffsetRequest.EARLIEST_TIMESTAMP)
  }

  override protected def fetchLatestOffsetFromLeader(topicPartition: TopicPartition, currentLeaderEpoch: Int): Long = {
    fetchOffsetFromLeader(topicPartition, currentLeaderEpoch, ListOffsetRequest.LATEST_TIMESTAMP)
  }

  private def fetchOffsetFromLeader(topicPartition: TopicPartition, currentLeaderEpoch: Int, earliestOrLatest: Long): Long = {
    val topic = new ListOffsetTopic()
      .setName(topicPartition.topic)
      .setPartitions(Collections.singletonList(
          new ListOffsetPartition()
            .setPartitionIndex(topicPartition.partition)
            .setCurrentLeaderEpoch(currentLeaderEpoch)
            .setTimestamp(earliestOrLatest)))
    val requestBuilder = ListOffsetRequest.Builder.forReplica(listOffsetRequestVersion, replicaId)
      .setTargetTimes(Collections.singletonList(topic))

    val clientResponse = leaderEndpoint.sendRequest(requestBuilder)
    val response = clientResponse.responseBody.asInstanceOf[ListOffsetResponse]
    val responsePartition = response.topics.asScala.find(_.name == topicPartition.topic).get
      .partitions.asScala.find(_.partitionIndex == topicPartition.partition).get

    Errors.forCode(responsePartition.errorCode) match {
      case Errors.NONE =>
        if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_10_1_IV2)
          responsePartition.offset
        else
          responsePartition.oldStyleOffsets.get(0)
      case error => throw error.exception
    }
  }

  /**
   * 构建发送给 Leader 副本所在 Broker 的 FETCH 请求
   *
   * @param partitionMap key 一组要读取的分区列表 是否被读取取决于PartitionFetchState的状态
   * @return
   */
  override def buildFetch(partitionMap: Map[TopicPartition, PartitionFetchState]): ResultWithPartitions[Option[ReplicaFetch]] = {
    val partitionsWithError = mutable.Set[TopicPartition]()

    // 构造FetchSessionHandler的Builder实例 该对象保存用于向Leader副本请求数据的所有分区
    val builder = fetchSessionHandler.newBuilder(partitionMap.size, false)
    // 遍历每个分区 将处于可获取状态的分区添加到builder后续统一处理  对于有错误的分区加入到出错分区列表
    partitionMap.forKeyValue { (topicPartition, fetchState) =>
      // We will not include a replica in the fetch request if it should be throttled.
      if (fetchState.isReadyForFetch && !shouldFollowerThrottle(quota, fetchState, topicPartition)) {
        try {
          val logStartOffset = this.logStartOffset(topicPartition)
          val lastFetchedEpoch = if (isTruncationOnFetchSupported)
            fetchState.lastFetchedEpoch.map(_.asInstanceOf[Integer]).asJava
          else
            Optional.empty[Integer]
          builder.add(topicPartition, new FetchRequest.PartitionData(
            fetchState.fetchOffset,
            logStartOffset,
            fetchSize,
            Optional.of(fetchState.currentLeaderEpoch),
            lastFetchedEpoch))
        } catch {
          case _: KafkaStorageException =>
            // The replica has already been marked offline due to log directory failure and the original failure should have already been logged.
            // This partition should be removed from ReplicaFetcherThread soon by ReplicaManager.handleLogDirFailure()
            partitionsWithError += topicPartition
        }
      }
    }
    // 构造 Builder 的过程中会用到 ReplicaFetcherThread 类定义的那些与消息获取相关的字段(如 maxWait | minBytes | maxByte)
    val fetchData = builder.build() // 获取构造好的待读取分区数据
    val fetchRequestOpt = if (fetchData.sessionPartitions.isEmpty && fetchData.toForget.isEmpty) { // 是否有数据需要从Leader副本拉取
      None
    } else { // 构造FETCH请求的Builder对象
      val requestBuilder = FetchRequest.Builder
        .forReplica(fetchRequestVersion, replicaId, maxWait, minBytes, fetchData.toSend)
        .setMaxBytes(maxBytes)
        .toForget(fetchData.toForget)
        .metadata(fetchData.metadata)
      Some(ReplicaFetch(fetchData.sessionPartitions(), requestBuilder))
    }
    ResultWithPartitions(fetchRequestOpt, partitionsWithError) // 返回Builder对象以及出错分区列表
  }

  /**
   * Truncate the log for each partition's epoch based on leader's returned epoch and offset.
   * The logic for finding the truncation offset is implemented in AbstractFetcherThread.getOffsetTruncationState
   *
   * 利用给定的 offsetTruncationState 的 offset 值 对给定分区的本地日志进行截断操作
   *
   * @param tp
   * @param offsetTruncationState
   */
  override def truncate(tp: TopicPartition, offsetTruncationState: OffsetTruncationState): Unit = {
    // 获取分区对象
    val partition = replicaMgr.nonOfflinePartition(tp).get
    // 拿到分区本地日志
    val log = partition.localLogOrException

    // truncateTo 方法的主要作用是将日志截断到小于给定值的最大位移值处
    partition.truncateTo(offsetTruncationState.offset, isFuture = false) // 实际上底层调用的是 Log 的 truncateTo 方法

    if (offsetTruncationState.offset < log.highWatermark)
      warn(s"Truncating $tp to offset ${offsetTruncationState.offset} below high watermark " +
        s"${log.highWatermark}")

    // mark the future replica for truncation only when we do last truncation
    if (offsetTruncationState.truncationCompleted)
      replicaMgr.replicaAlterLogDirsManager.markPartitionsForTruncation(brokerConfig.brokerId, tp,
        offsetTruncationState.offset)
  }

  override protected def truncateFullyAndStartAt(topicPartition: TopicPartition, offset: Long): Unit = {
    val partition = replicaMgr.nonOfflinePartition(topicPartition).get
    partition.truncateFullyAndStartAt(offset, isFuture = false)
  }

  override def fetchEpochEndOffsets(partitions: Map[TopicPartition, EpochData]): Map[TopicPartition, EpochEndOffset] = {

    if (partitions.isEmpty) {
      debug("Skipping leaderEpoch request since all partitions do not have an epoch")
      return Map.empty
    }

    val epochRequest = OffsetsForLeaderEpochRequest.Builder.forFollower(offsetForLeaderEpochRequestVersion, partitions.asJava, brokerConfig.brokerId)
    debug(s"Sending offset for leader epoch request $epochRequest")

    try {
      val response = leaderEndpoint.sendRequest(epochRequest)
      val responseBody = response.responseBody.asInstanceOf[OffsetsForLeaderEpochResponse]
      debug(s"Received leaderEpoch response $response")
      responseBody.data.topics.asScala.flatMap { offsetForLeaderTopicResult =>
        offsetForLeaderTopicResult.partitions().asScala.map { offsetForLeaderPartitionResult =>
          val tp = new TopicPartition(offsetForLeaderTopicResult.topic, offsetForLeaderPartitionResult.partition)
          tp -> offsetForLeaderPartitionResult
        }
      }.toMap
    } catch {
      case t: Throwable =>
        warn(s"Error when sending leader epoch request for $partitions", t)

        // if we get any unexpected exception, mark all partitions with an error
        val error = Errors.forException(t)
        partitions.map { case (tp, _) =>
          tp -> new EpochEndOffset()
            .setPartition(tp.partition)
            .setErrorCode(error.code)
        }
    }
  }

  /**
   * To avoid ISR thrashing, we only throttle a replica on the follower if it's in the throttled replica list,
   * the quota is exceeded and the replica is not in sync.
   */
  private def shouldFollowerThrottle(quota: ReplicaQuota, fetchState: PartitionFetchState, topicPartition: TopicPartition): Boolean = {
    !fetchState.isReplicaInSync && quota.isThrottled(topicPartition) && quota.isQuotaExceeded
  }
}
