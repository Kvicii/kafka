/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.yammer.metrics.core.Meter
import kafka.metrics.KafkaMetricsGroup
import kafka.network._
import kafka.utils._
import org.apache.kafka.common.internals.FatalExitError
import org.apache.kafka.common.utils.{KafkaThread, Time}

import scala.collection.mutable
import scala.jdk.CollectionConverters._

/**
 * A thread that answers kafka requests.
 * 请求处理线程类 每个请求处理线程实例 负责从SocketServer的RequestChannel的请求队列中获取请求对象 并进行处理
 * KafkaRequestHandler是一个Runnable对象 因此可以当做是一个线程
 *
 * @param id                  IO线程序号 类似于Processor线程的序号 标识这是IO线程池中的第几个线程
 * @param brokerId            所在Broker序号 即broker.id值 用于标识这是哪个Broker上的请求处理线程
 * @param aggregateIdleMeter
 * @param totalHandlerThreads IO线程池大小
 * @param requestChannel      请求处理通道 该Channel就是SocketServer中的RequestChannel 由于KafkaRequestHandler负责处理请求
 *                            而实际上请求保存在RequestChannel中 因此在构造KafkaRequestHandler时 必须关联SocketServer组件中的RequestChannel实例 也就是让IO线程能够找到请求被保存的地方
 * @param apis                KafkaApis类 用于真正实现请求处理逻辑的类 该类中的handle方法用于执行请求处理逻辑
 * @param time
 */
class KafkaRequestHandler(id: Int,
                          brokerId: Int,
                          val aggregateIdleMeter: Meter,
                          val totalHandlerThreads: AtomicInteger,
                          val requestChannel: RequestChannel,
                          apis: KafkaApis,
                          time: Time) extends Runnable with Logging {
  this.logIdent = "[Kafka Request Handler " + id + " on Broker " + brokerId + "], "
  private val shutdownComplete = new CountDownLatch(1)
  @volatile private var stopped = false

  def run(): Unit = {
    while (!stopped) { // 只要该线程未关闭 就循环执行处理逻辑
      // We use a single meter for aggregate idle percentage for the thread pool.
      // Since meter is calculated as total_recorded_value / time_window and
      // time_window is independent of the number of threads, each recorded idle
      // time should be discounted by # threads.
      val startSelectTime = time.nanoseconds

      // 获取RequestChannel中下一个待处理的请求
      val req = requestChannel.receiveRequest(300)
      val endTime = time.nanoseconds
      // 统计线程空闲时间
      val idleTime = endTime - startSelectTime
      // 更新线程空闲百分比指标
      aggregateIdleMeter.mark(idleTime / totalHandlerThreads.get)

      req match {
        case RequestChannel.ShutdownRequest =>
          // 关闭线程请求
          debug(s"Kafka request handler $id on broker $brokerId received shut down command")
          // 关闭线程
          shutdownComplete.countDown()
          return

        case request: RequestChannel.Request =>
          // 普通请求
          try {
            // 更新请求移出队列的时间戳
            request.requestDequeueTimeNanos = endTime
            trace(s"Kafka request handler $id on broker $brokerId handling request $request")
            // 由KafkaApis中的handle方法执行相应的处理逻辑
            apis.handle(request)
          } catch {
            case e: FatalExitError =>
              // 如果出现了严重错误 立即关闭
              shutdownComplete.countDown()
              Exit.exit(e.statusCode)
            // 如果是普通异常 记录错误日志
            case e: Throwable => error("Exception when handling request", e)
          } finally {
            // 释放请求对象占用的内存缓冲区资源
            request.releaseBuffer()
          }

        case null => // continue
      }
    }
    shutdownComplete.countDown()
  }

  def stop(): Unit = {
    stopped = true
  }

  def initiateShutdown(): Unit = requestChannel.sendShutdownRequest()

  def awaitShutdown(): Unit = shutdownComplete.await()

}

/**
 * 定义了若干个IO线程池 用于执行真正的请求处理逻辑
 * 定义了多个KafkaRequestHandler线程 KafkaRequestHandler线程是真正处理请求逻辑的线程
 * 与他相比 Acceptor线程和Processor线程只是中介
 *
 * 请求处理线程池 负责创建 维护 管理和销毁下辖的请求处理线程
 *
 * Kafka中真正处理请求的地方不是SocketServer 也不是RequestChannel 而是KafkaRequestHandlerPool
 *
 * @param brokerId       所属Broker的序号 broker.id
 * @param requestChannel 请求处理队列 即SocketServer中的RequestChannel 它管理的请求队列为所有IO线程共享
 * @param apis           KafkaApis类 实际请求处理逻辑类
 * @param time
 * @param numThreads     IO线程池初始大小 它是Broker端参数num.io.threads的值 Kafka目前支持动态修改IO线程池大小 因此这里的numThreads是初始线程数 调整后的IO线程池实际大小可以和numThreads不一致
 * @param requestHandlerAvgIdleMetricName
 * @param logAndThreadNamePrefix
 */
class KafkaRequestHandlerPool(val brokerId: Int,
                              val requestChannel: RequestChannel,
                              val apis: KafkaApis,
                              time: Time,
                              numThreads: Int,
                              requestHandlerAvgIdleMetricName: String,
                              logAndThreadNamePrefix: String) extends Logging with KafkaMetricsGroup {

  // IO线程池大小
  // 之所以在存在了numThreads的情况下还定义threadPoolSize的原因是 numThreads是固定的 一旦传入不可变更 因此需要一个口可以动态修改该值
  private val threadPoolSize: AtomicInteger = new AtomicInteger(numThreads)
  /* a meter to track the average free capacity of the request handlers */
  private val aggregateIdleMeter = newMeter(requestHandlerAvgIdleMetricName, "percent", TimeUnit.NANOSECONDS)

  this.logIdent = "[" + logAndThreadNamePrefix + " Kafka Request Handler on Broker " + brokerId + "], "
  // IO线程池
  val runnables = new mutable.ArrayBuffer[KafkaRequestHandler](numThreads)
  for (i <- 0 until numThreads) {
    createHandler(i) // 创建numThreads个IO线程
  }

  /**
   * 创建线程 并将线程加入到IO线程池中
   *
   * @param id
   */
  def createHandler(id: Int): Unit = synchronized {
    // 创建序号为id的IO线程对象并加入到IO线程池runnables中
    runnables += new KafkaRequestHandler(id, brokerId, aggregateIdleMeter, threadPoolSize, requestChannel, apis, time)
    // 启动KafkaRequestHandler线程
    KafkaThread.daemon(logAndThreadNamePrefix + "-kafka-request-handler-" + id, runnables(id)).start()
  }

  /**
   * 将IO线程池的线程数设置为指定的数值
   *
   * @param newSize
   */
  def resizeThreadPool(newSize: Int): Unit = synchronized {
    // 获取当前IO线程池的线程数
    val currentSize = threadPoolSize.get
    info(s"Resizing request handler thread pool size from $currentSize to $newSize")
    if (newSize > currentSize) {
      // 当前线程数 < 待设置的线程数 调用createHandler方法补齐IO线程池中的线程
      for (i <- currentSize until newSize) {
        createHandler(i)
      }
    } else if (newSize < currentSize) {
      // 当前线程数 > 待设置的线程数 将多余的线程从IO线程池中移除 并停止
      for (i <- 1 to (currentSize - newSize)) {
        runnables.remove(currentSize - i).stop()
      }
    }
    // 设置新的IO线程池大小
    threadPoolSize.set(newSize)
  }

  /**
   * Broker关闭 -> 将shutdownRequest写入RequestChannel中 一旦KafkaRequestHandler的run方法中拿到了该标识 将调用shutdownComplete的countdown方法完成关闭
   */
  def shutdown(): Unit = synchronized {
    info("shutting down")
    for (handler <- runnables)
      handler.initiateShutdown()
    for (handler <- runnables)
      handler.awaitShutdown() // shutdownComplete调用countdown之后 将不会阻塞 从而完成关闭
    info("shut down completely")
  }
}

/**
 * Broker端与Topic相关的监控指标的管理类
 *
 * @param name
 */
class BrokerTopicMetrics(name: Option[String]) extends KafkaMetricsGroup {
  val tags: scala.collection.Map[String, String] = name match {
    case None => Map.empty
    case Some(topic) => Map("topic" -> topic)
  }

  case class MeterWrapper(metricType: String, eventType: String) {
    @volatile private var lazyMeter: Meter = _
    private val meterLock = new Object

    def meter(): Meter = {
      var meter = lazyMeter
      if (meter == null) {
        meterLock synchronized {
          meter = lazyMeter
          if (meter == null) {
            meter = newMeter(metricType, eventType, TimeUnit.SECONDS, tags)
            lazyMeter = meter
          }
        }
      }
      meter
    }

    def close(): Unit = meterLock synchronized {
      if (lazyMeter != null) {
        removeMetric(metricType, tags)
        lazyMeter = null
      }
    }

    if (tags.isEmpty) // greedily initialize the general topic metrics
    meter()
  }

  // an internal map for "lazy initialization" of certain metrics
  private val metricTypeMap = new Pool[String, MeterWrapper]()
  metricTypeMap.putAll(Map(
    BrokerTopicStats.MessagesInPerSec -> MeterWrapper(BrokerTopicStats.MessagesInPerSec, "messages"),
    BrokerTopicStats.BytesInPerSec -> MeterWrapper(BrokerTopicStats.BytesInPerSec, "bytes"),
    BrokerTopicStats.BytesOutPerSec -> MeterWrapper(BrokerTopicStats.BytesOutPerSec, "bytes"),
    BrokerTopicStats.BytesRejectedPerSec -> MeterWrapper(BrokerTopicStats.BytesRejectedPerSec, "bytes"),
    BrokerTopicStats.FailedProduceRequestsPerSec -> MeterWrapper(BrokerTopicStats.FailedProduceRequestsPerSec, "requests"),
    BrokerTopicStats.FailedFetchRequestsPerSec -> MeterWrapper(BrokerTopicStats.FailedFetchRequestsPerSec, "requests"),
    BrokerTopicStats.TotalProduceRequestsPerSec -> MeterWrapper(BrokerTopicStats.TotalProduceRequestsPerSec, "requests"),
    BrokerTopicStats.TotalFetchRequestsPerSec -> MeterWrapper(BrokerTopicStats.TotalFetchRequestsPerSec, "requests"),
    BrokerTopicStats.FetchMessageConversionsPerSec -> MeterWrapper(BrokerTopicStats.FetchMessageConversionsPerSec, "requests"),
    BrokerTopicStats.ProduceMessageConversionsPerSec -> MeterWrapper(BrokerTopicStats.ProduceMessageConversionsPerSec, "requests"),
    BrokerTopicStats.NoKeyCompactedTopicRecordsPerSec -> MeterWrapper(BrokerTopicStats.NoKeyCompactedTopicRecordsPerSec, "requests"),
    BrokerTopicStats.InvalidMagicNumberRecordsPerSec -> MeterWrapper(BrokerTopicStats.InvalidMagicNumberRecordsPerSec, "requests"),
    BrokerTopicStats.InvalidMessageCrcRecordsPerSec -> MeterWrapper(BrokerTopicStats.InvalidMessageCrcRecordsPerSec, "requests"),
    BrokerTopicStats.InvalidOffsetOrSequenceRecordsPerSec -> MeterWrapper(BrokerTopicStats.InvalidOffsetOrSequenceRecordsPerSec, "requests")
  ).asJava)
  if (name.isEmpty) {
    metricTypeMap.put(BrokerTopicStats.ReplicationBytesInPerSec, MeterWrapper(BrokerTopicStats.ReplicationBytesInPerSec, "bytes"))
    metricTypeMap.put(BrokerTopicStats.ReplicationBytesOutPerSec, MeterWrapper(BrokerTopicStats.ReplicationBytesOutPerSec, "bytes"))
    metricTypeMap.put(BrokerTopicStats.ReassignmentBytesInPerSec, MeterWrapper(BrokerTopicStats.ReassignmentBytesInPerSec, "bytes"))
    metricTypeMap.put(BrokerTopicStats.ReassignmentBytesOutPerSec, MeterWrapper(BrokerTopicStats.ReassignmentBytesOutPerSec, "bytes"))
  }

  // used for testing only
  def metricMap: Map[String, MeterWrapper] = metricTypeMap.toMap

  def messagesInRate: Meter = metricTypeMap.get(BrokerTopicStats.MessagesInPerSec).meter()

  def bytesInRate: Meter = metricTypeMap.get(BrokerTopicStats.BytesInPerSec).meter()

  def bytesOutRate: Meter = metricTypeMap.get(BrokerTopicStats.BytesOutPerSec).meter()

  def bytesRejectedRate: Meter = metricTypeMap.get(BrokerTopicStats.BytesRejectedPerSec).meter()

  private[server] def replicationBytesInRate: Option[Meter] =
    if (name.isEmpty) Some(metricTypeMap.get(BrokerTopicStats.ReplicationBytesInPerSec).meter())
    else None

  private[server] def replicationBytesOutRate: Option[Meter] =
    if (name.isEmpty) Some(metricTypeMap.get(BrokerTopicStats.ReplicationBytesOutPerSec).meter())
    else None

  private[server] def reassignmentBytesInPerSec: Option[Meter] =
    if (name.isEmpty) Some(metricTypeMap.get(BrokerTopicStats.ReassignmentBytesInPerSec).meter())
    else None

  private[server] def reassignmentBytesOutPerSec: Option[Meter] =
    if (name.isEmpty) Some(metricTypeMap.get(BrokerTopicStats.ReassignmentBytesOutPerSec).meter())
    else None

  def failedProduceRequestRate: Meter = metricTypeMap.get(BrokerTopicStats.FailedProduceRequestsPerSec).meter()

  def failedFetchRequestRate: Meter = metricTypeMap.get(BrokerTopicStats.FailedFetchRequestsPerSec).meter()

  def totalProduceRequestRate: Meter = metricTypeMap.get(BrokerTopicStats.TotalProduceRequestsPerSec).meter()

  def totalFetchRequestRate: Meter = metricTypeMap.get(BrokerTopicStats.TotalFetchRequestsPerSec).meter()

  def fetchMessageConversionsRate: Meter = metricTypeMap.get(BrokerTopicStats.FetchMessageConversionsPerSec).meter()

  def produceMessageConversionsRate: Meter = metricTypeMap.get(BrokerTopicStats.ProduceMessageConversionsPerSec).meter()

  def noKeyCompactedTopicRecordsPerSec: Meter = metricTypeMap.get(BrokerTopicStats.NoKeyCompactedTopicRecordsPerSec).meter()

  def invalidMagicNumberRecordsPerSec: Meter = metricTypeMap.get(BrokerTopicStats.InvalidMagicNumberRecordsPerSec).meter()

  def invalidMessageCrcRecordsPerSec: Meter = metricTypeMap.get(BrokerTopicStats.InvalidMessageCrcRecordsPerSec).meter()

  def invalidOffsetOrSequenceRecordsPerSec: Meter = metricTypeMap.get(BrokerTopicStats.InvalidOffsetOrSequenceRecordsPerSec).meter()

  def closeMetric(metricType: String): Unit = {
    val meter = metricTypeMap.get(metricType)
    if (meter != null)
      meter.close()
  }

  def close(): Unit = metricTypeMap.values.foreach(_.close())
}

/**
 * BrokerTopicStats的伴生对象类 定义Broker端与Topic相关的监控指标 比如常见的MessagesInPerSec和MessagesOutPerSec等
 */
object BrokerTopicStats {
  val MessagesInPerSec = "MessagesInPerSec"
  val BytesInPerSec = "BytesInPerSec"
  val BytesOutPerSec = "BytesOutPerSec"
  val BytesRejectedPerSec = "BytesRejectedPerSec"
  val ReplicationBytesInPerSec = "ReplicationBytesInPerSec"
  val ReplicationBytesOutPerSec = "ReplicationBytesOutPerSec"
  val FailedProduceRequestsPerSec = "FailedProduceRequestsPerSec"
  val FailedFetchRequestsPerSec = "FailedFetchRequestsPerSec"
  val TotalProduceRequestsPerSec = "TotalProduceRequestsPerSec"
  val TotalFetchRequestsPerSec = "TotalFetchRequestsPerSec"
  val FetchMessageConversionsPerSec = "FetchMessageConversionsPerSec"
  val ProduceMessageConversionsPerSec = "ProduceMessageConversionsPerSec"
  val ReassignmentBytesInPerSec = "ReassignmentBytesInPerSec"
  val ReassignmentBytesOutPerSec = "ReassignmentBytesOutPerSec"

  // These following topics are for LogValidator for better debugging on failed records
  val NoKeyCompactedTopicRecordsPerSec = "NoKeyCompactedTopicRecordsPerSec"
  val InvalidMagicNumberRecordsPerSec = "InvalidMagicNumberRecordsPerSec"
  val InvalidMessageCrcRecordsPerSec = "InvalidMessageCrcRecordsPerSec"
  val InvalidOffsetOrSequenceRecordsPerSec = "InvalidOffsetOrSequenceRecordsPerSec"

  private val valueFactory = (k: String) => new BrokerTopicMetrics(Some(k))
}

/**
 * 定义Broker端与TOpic相关的监控指标的管理操作
 */
class BrokerTopicStats extends Logging {
  import BrokerTopicStats._

  private val stats = new Pool[String, BrokerTopicMetrics](Some(valueFactory))
  val allTopicsStats = new BrokerTopicMetrics(None)

  def topicStats(topic: String): BrokerTopicMetrics =
    stats.getAndMaybePut(topic)

  def updateReplicationBytesIn(value: Long): Unit = {
    allTopicsStats.replicationBytesInRate.foreach { metric =>
      metric.mark(value)
    }
  }

  private def updateReplicationBytesOut(value: Long): Unit = {
    allTopicsStats.replicationBytesOutRate.foreach { metric =>
      metric.mark(value)
    }
  }

  def updateReassignmentBytesIn(value: Long): Unit = {
    allTopicsStats.reassignmentBytesInPerSec.foreach { metric =>
      metric.mark(value)
    }
  }

  def updateReassignmentBytesOut(value: Long): Unit = {
    allTopicsStats.reassignmentBytesOutPerSec.foreach { metric =>
      metric.mark(value)
    }
  }

  // This method only removes metrics only used for leader
  def removeOldLeaderMetrics(topic: String): Unit = {
    val topicMetrics = topicStats(topic)
    if (topicMetrics != null) {
      topicMetrics.closeMetric(BrokerTopicStats.MessagesInPerSec)
      topicMetrics.closeMetric(BrokerTopicStats.BytesInPerSec)
      topicMetrics.closeMetric(BrokerTopicStats.BytesRejectedPerSec)
      topicMetrics.closeMetric(BrokerTopicStats.FailedProduceRequestsPerSec)
      topicMetrics.closeMetric(BrokerTopicStats.TotalProduceRequestsPerSec)
      topicMetrics.closeMetric(BrokerTopicStats.ProduceMessageConversionsPerSec)
      topicMetrics.closeMetric(BrokerTopicStats.ReplicationBytesOutPerSec)
      topicMetrics.closeMetric(BrokerTopicStats.ReassignmentBytesOutPerSec)
    }
  }

  // This method only removes metrics only used for follower
  def removeOldFollowerMetrics(topic: String): Unit = {
    val topicMetrics = topicStats(topic)
    if (topicMetrics != null) {
      topicMetrics.closeMetric(BrokerTopicStats.ReplicationBytesInPerSec)
      topicMetrics.closeMetric(BrokerTopicStats.ReassignmentBytesInPerSec)
    }
  }

  def removeMetrics(topic: String): Unit = {
    val metrics = stats.remove(topic)
    if (metrics != null)
      metrics.close()
  }

  def updateBytesOut(topic: String, isFollower: Boolean, isReassignment: Boolean, value: Long): Unit = {
    if (isFollower) {
      if (isReassignment)
        updateReassignmentBytesOut(value)
      updateReplicationBytesOut(value)
    } else {
      topicStats(topic).bytesOutRate.mark(value)
      allTopicsStats.bytesOutRate.mark(value)
    }
  }

  def close(): Unit = {
    allTopicsStats.close()
    stats.values.foreach(_.close())

    info("Broker and topic stats closed")
  }
}
