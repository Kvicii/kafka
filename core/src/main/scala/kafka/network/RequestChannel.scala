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

package kafka.network

import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.concurrent._

import com.typesafe.scalalogging.Logger
import com.yammer.metrics.core.Meter
import kafka.log.LogConfig
import kafka.metrics.KafkaMetricsGroup
import kafka.server.KafkaConfig
import kafka.utils.{Logging, NotNothing, Pool}
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData._
import org.apache.kafka.common.network.Send
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.utils.{Sanitizer, Time}

import scala.annotation.nowarn
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

object RequestChannel extends Logging {
  private val requestLogger = Logger("kafka.request.logger")

  val RequestQueueSizeMetric = "RequestQueueSize"
  val ResponseQueueSizeMetric = "ResponseQueueSize"
  val ProcessorMetricTag = "processor"

  def isRequestLoggingEnabled: Boolean = requestLogger.underlying.isDebugEnabled

  sealed trait BaseRequest // trait关键字类似于Java中的Interface

  case object ShutdownRequest extends BaseRequest // 仅仅起到标识位的作用 当Broker进程关闭时 请求处理器会发送ShutdownRequest到专属的请求处理线程 该线程收到请求后会触发一系列Broker的关闭逻辑

  case class Session(principal: KafkaPrincipal, clientAddress: InetAddress) {
    val sanitizedUser: String = Sanitizer.sanitize(principal.getName)
  }

  class Metrics {

    private val metricsMap = mutable.Map[String, RequestMetrics]()

    (ApiKeys.values.toSeq.map(_.name) ++
      Seq(RequestMetrics.consumerFetchMetricName, RequestMetrics.followFetchMetricName)).foreach { name =>
      metricsMap.put(name, new RequestMetrics(name))
    }

    def apply(metricName: String) = metricsMap(metricName)

    def close(): Unit = {
      metricsMap.values.foreach(_.removeMetrics())
    }
  }

  /**
   * 定义各类Client端或Broker端请求的实现类
   *
   * @param processor      是processor的线程号 即这个请求是由哪个processor线程接收处理的 broker端参数num.network.threads控制了broker每个监听器上创建的processor线程数
   *                       保存processor线程号的原因是 当Request被后面的IO线程处理完毕后 还要依靠processor线程发送Response
   *                       processor线程仅仅是网络接收线程 真正执行Request处理逻辑的是IO线程
   * @param context        标识请求上下文信息
   * @param startTimeNanos 记录Request对象创建的时间 用于各种时间统计指标的计算 以纳秒为单位可以实现非常细粒度的时间统计精度
   * @param memoryPool     非阻塞的内存缓冲区 避免Request对象无限使用内存
   * @param buffer         真正保存Request内容的缓冲区
   * @param metrics        Request 相关的各种监控指标的一个管理类 里面构造了一个Map 封装了所有的请求JMX指标 Request类大部分代码都是与指标监控相关的代码
   */
  class Request(val processor: Int,
                val context: RequestContext,
                val startTimeNanos: Long,
                memoryPool: MemoryPool,
                @volatile private var buffer: ByteBuffer,
                metrics: RequestChannel.Metrics) extends BaseRequest {
    // These need to be volatile because the readers are in the network thread and the writers are in the request
    // handler threads or the purgatory threads
    @volatile var requestDequeueTimeNanos = -1L
    @volatile var apiLocalCompleteTimeNanos = -1L
    @volatile var responseCompleteTimeNanos = -1L
    @volatile var responseDequeueTimeNanos = -1L
    @volatile var messageConversionsTimeNanos = 0L
    @volatile var apiThrottleTimeMs = 0L
    @volatile var temporaryMemoryBytes = 0L
    @volatile var recordNetworkThreadTimeCallback: Option[Long => Unit] = None

    val session = Session(context.principal, context.clientAddress)
    // Request 发送方必须按照 Kafka RPC 协议规定的格式向该缓冲区写入字节 否则将抛出 InvalidRequestException 异常
    private val bodyAndSize: RequestAndSize = context.parseRequest(buffer)

    def header: RequestHeader = context.header

    def sizeOfBodyInBytes: Int = bodyAndSize.size

    //most request types are parsed entirely into objects at this point. for those we can release the underlying buffer.
    //some (like produce, or any time the schema contains fields of types BYTES or NULLABLE_BYTES) retain a reference
    //to the buffer. for those requests we cannot release the buffer early, but only when request processing is done.
    if (!header.apiKey.requiresDelayedAllocation) {
      releaseBuffer()
    }

    def requestDesc(details: Boolean): String = s"$header -- ${loggableRequest.toString(details)}"

    def body[T <: AbstractRequest](implicit classTag: ClassTag[T], @nowarn("cat=unused") nn: NotNothing[T]): T = {
      bodyAndSize.request match {
        case r: T => r
        case r =>
          throw new ClassCastException(s"Expected request with type ${classTag.runtimeClass}, but found ${r.getClass}")
      }
    }

    def loggableRequest: AbstractRequest = {

      def loggableValue(resourceType: ConfigResource.Type, name: String, value: String): String = {
        val maybeSensitive = resourceType match {
          case ConfigResource.Type.BROKER => KafkaConfig.maybeSensitive(KafkaConfig.configType(name))
          case ConfigResource.Type.TOPIC => KafkaConfig.maybeSensitive(LogConfig.configType(name))
          case ConfigResource.Type.BROKER_LOGGER => false
          case _ => true
        }
        if (maybeSensitive) Password.HIDDEN else value
      }

      bodyAndSize.request match {
        case alterConfigs: AlterConfigsRequest =>
          val loggableConfigs = alterConfigs.configs().asScala.map { case (resource, config) =>
            val loggableEntries = new AlterConfigsRequest.Config(config.entries.asScala.map { entry =>
              new AlterConfigsRequest.ConfigEntry(entry.name, loggableValue(resource.`type`, entry.name, entry.value))
            }.asJavaCollection)
            (resource, loggableEntries)
          }.asJava
          new AlterConfigsRequest.Builder(loggableConfigs, alterConfigs.validateOnly).build(alterConfigs.version())

        case alterConfigs: IncrementalAlterConfigsRequest =>
          val resources = new AlterConfigsResourceCollection(alterConfigs.data.resources.size)
          alterConfigs.data.resources.forEach { resource =>
            val newResource = new AlterConfigsResource()
              .setResourceName(resource.resourceName)
              .setResourceType(resource.resourceType)
            resource.configs.forEach { config =>
              newResource.configs.add(new AlterableConfig()
                .setName(config.name)
                .setValue(loggableValue(ConfigResource.Type.forId(resource.resourceType), config.name, config.value))
                .setConfigOperation(config.configOperation))
            }
            resources.add(newResource)
          }
          val data = new IncrementalAlterConfigsRequestData()
            .setValidateOnly(alterConfigs.data().validateOnly())
            .setResources(resources)
          new IncrementalAlterConfigsRequest.Builder(data).build(alterConfigs.version)

        case _ =>
          bodyAndSize.request
      }
    }

    trace(s"Processor $processor received request: ${requestDesc(true)}")

    def requestThreadTimeNanos: Long = {
      if (apiLocalCompleteTimeNanos == -1L) apiLocalCompleteTimeNanos = Time.SYSTEM.nanoseconds
      math.max(apiLocalCompleteTimeNanos - requestDequeueTimeNanos, 0L)
    }

    def updateRequestMetrics(networkThreadTimeNanos: Long, response: Response): Unit = {
      val endTimeNanos = Time.SYSTEM.nanoseconds

      /**
       * Converts nanos to millis with micros precision as additional decimal places in the request log have low
       * signal to noise ratio. When it comes to metrics, there is little difference either way as we round the value
       * to the nearest long.
       */
      def nanosToMs(nanos: Long): Double = {
        val positiveNanos = math.max(nanos, 0)
        TimeUnit.NANOSECONDS.toMicros(positiveNanos).toDouble / TimeUnit.MILLISECONDS.toMicros(1)
      }

      val requestQueueTimeMs = nanosToMs(requestDequeueTimeNanos - startTimeNanos)
      val apiLocalTimeMs = nanosToMs(apiLocalCompleteTimeNanos - requestDequeueTimeNanos)
      val apiRemoteTimeMs = nanosToMs(responseCompleteTimeNanos - apiLocalCompleteTimeNanos)
      val responseQueueTimeMs = nanosToMs(responseDequeueTimeNanos - responseCompleteTimeNanos)
      val responseSendTimeMs = nanosToMs(endTimeNanos - responseDequeueTimeNanos)
      val messageConversionsTimeMs = nanosToMs(messageConversionsTimeNanos)
      val totalTimeMs = nanosToMs(endTimeNanos - startTimeNanos)
      val fetchMetricNames =
        if (header.apiKey == ApiKeys.FETCH) {
          val isFromFollower = body[FetchRequest].isFromFollower
          Seq(
            if (isFromFollower) RequestMetrics.followFetchMetricName
            else RequestMetrics.consumerFetchMetricName
          )
        }
        else Seq.empty
      val metricNames = fetchMetricNames :+ header.apiKey.name
      metricNames.foreach { metricName =>
        val m = metrics(metricName)
        m.requestRate(header.apiVersion).mark()
        m.requestQueueTimeHist.update(Math.round(requestQueueTimeMs))
        m.localTimeHist.update(Math.round(apiLocalTimeMs))
        m.remoteTimeHist.update(Math.round(apiRemoteTimeMs))
        m.throttleTimeHist.update(apiThrottleTimeMs)
        m.responseQueueTimeHist.update(Math.round(responseQueueTimeMs))
        m.responseSendTimeHist.update(Math.round(responseSendTimeMs))
        m.totalTimeHist.update(Math.round(totalTimeMs))
        m.requestBytesHist.update(sizeOfBodyInBytes)
        m.messageConversionsTimeHist.foreach(_.update(Math.round(messageConversionsTimeMs)))
        m.tempMemoryBytesHist.foreach(_.update(temporaryMemoryBytes))
      }

      // Records network handler thread usage. This is included towards the request quota for the
      // user/client. Throttling is only performed when request handler thread usage
      // is recorded, just before responses are queued for delivery.
      // The time recorded here is the time spent on the network thread for receiving this request
      // and sending the response. Note that for the first request on a connection, the time includes
      // the total time spent on authentication, which may be significant for SASL/SSL.
      recordNetworkThreadTimeCallback.foreach(record => record(networkThreadTimeNanos))

      if (isRequestLoggingEnabled) {
        val detailsEnabled = requestLogger.underlying.isTraceEnabled
        val responseString = response.responseString.getOrElse(
          throw new IllegalStateException("responseAsString should always be defined if request logging is enabled"))
        val builder = new StringBuilder(256)
        builder.append("Completed request:").append(requestDesc(detailsEnabled))
          .append(",response:").append(responseString)
          .append(" from connection ").append(context.connectionId)
          .append(";totalTime:").append(totalTimeMs)
          .append(",requestQueueTime:").append(requestQueueTimeMs)
          .append(",localTime:").append(apiLocalTimeMs)
          .append(",remoteTime:").append(apiRemoteTimeMs)
          .append(",throttleTime:").append(apiThrottleTimeMs)
          .append(",responseQueueTime:").append(responseQueueTimeMs)
          .append(",sendTime:").append(responseSendTimeMs)
          .append(",securityProtocol:").append(context.securityProtocol)
          .append(",principal:").append(session.principal)
          .append(",listener:").append(context.listenerName.value)
          .append(",clientInformation:").append(context.clientInformation)
        if (temporaryMemoryBytes > 0)
          builder.append(",temporaryMemoryBytes:").append(temporaryMemoryBytes)
        if (messageConversionsTimeMs > 0)
          builder.append(",messageConversionsTime:").append(messageConversionsTimeMs)
        requestLogger.debug(builder.toString)
      }
    }

    def releaseBuffer(): Unit = {
      if (buffer != null) {
        memoryPool.release(buffer)
        buffer = null
      }
    }

    override def toString = s"Request(processor=$processor, " +
      s"connectionId=${context.connectionId}, " +
      s"session=$session, " +
      s"listenerName=${context.listenerName}, " +
      s"securityProtocol=${context.securityProtocol}, " +
      s"buffer=$buffer)"

  }

  /**
   * Response抽象基类 有5个具体实现类 每个Response对象都包含对应的Request对象
   *
   * @param request
   */
  abstract class Response(val request: Request) {

    def processor: Int = request.processor

    def responseString: Option[String] = Some("")

    /**
     * 实现每一种Response被处理后需要执行的回调逻辑
     *
     * @return
     */
    def onComplete: Option[Send => Unit] = None

    override def toString: String
  }

  /**
   * responseAsString should only be defined if request logging is enabled
   * Kafka 大多数 Request 处理完成后都需要执行一段回调逻辑
   * 用于保存返回结果的Response子类
   */
  class SendResponse(request: Request,
                     val responseSend: Send,
                     val responseAsString: Option[String],
                     val onCompleteCallback: Option[Send => Unit] // 指定处理完成之后的回调逻辑 Scala 中的 Unit 类似于 Java 中的 void
                    ) extends Response(request) {
    override def responseString: Option[String] = responseAsString

    // 函数在 Scala 中是一等公民 因此可以把一个函数作为一个参数传给另一个函数 也可以把函数作为结果返回
    // 把函数赋值给另一个函数并作为结果返回
    override def onComplete: Option[Send => Unit] = onCompleteCallback

    override def toString: String =
      s"Response(type=Send, request=$request, send=$responseSend, asString=$responseAsString)"
  }

  /**
   * 某些Request对象执行完之后不需要执行额外的回调
   *
   * @param request
   */
  class NoOpResponse(request: Request) extends Response(request) {
    override def toString: String =
      s"Response(type=NoOp, request=$request)"
  }

  /**
   * 用于出错后需要关闭TCP连接的场景
   * 此时返回CloseConnectionResponse给Request发送方 显式地通知其关闭TCP连接
   *
   * @param request
   */
  class CloseConnectionResponse(request: Request) extends Response(request) {
    override def toString: String =
      s"Response(type=CloseConnection, request=$request)"
  }

  /**
   * 通知Broker的SocketServer组件某个TCP连接通信通道开始被限流
   *
   * @param request
   */
  class StartThrottlingResponse(request: Request) extends Response(request) {
    override def toString: String =
      s"Response(type=StartThrottling, request=$request)"
  }

  /**
   * 通知Broker的SocketServer组件某个TCP连接通信通道限流已结束
   *
   * @param request
   */
  class EndThrottlingResponse(request: Request) extends Response(request) {
    override def toString: String =
      s"Response(type=EndThrottling, request=$request)"
  }

}

/**
 * 实现了 KafkaMetricsGroup trait 后者封装了许多实用的指标监控方法
 *
 * @param queueSize requestQueue的最大长度 当Broker启动时 SocketServer组件会创建RequestChannel对象 并把Broker端参数queued.max.requests赋值给queueSize 默认500
 * @param metricNamePrefix
 * @param time
 */
class RequestChannel(val queueSize: Int, val metricNamePrefix: String, time: Time) extends KafkaMetricsGroup {

  import RequestChannel._

  val metrics = new RequestChannel.Metrics
  // 每个 RequestChannel 对象实例创建时会定义一个队列来保存 Broker 接收到的各类请求
  // 使用 Java 提供的阻塞队列 ArrayBlockingQueue 实现这个请求队列 并利用它天然提供的线程安全性来保证多个线程能够并发安全高效地访问请求队列
  private val requestQueue = new ArrayBlockingQueue[BaseRequest](queueSize)
  // 封装RequestChannel管理的线程池 每个processor线程负责具体的请求处理逻辑
  // key是processor序号 value是具体的processor线程对象
  // Kafka Broker中所有的网络线程都是在RequestChannel中维护的
  private val processors = new ConcurrentHashMap[Int, Processor]()
  val requestQueueSizeMetricName = metricNamePrefix.concat(RequestQueueSizeMetric)
  val responseQueueSizeMetricName = metricNamePrefix.concat(ResponseQueueSizeMetric)

  newGauge(requestQueueSizeMetricName, () => requestQueue.size)

  newGauge(responseQueueSizeMetricName, () => {
    processors.values.asScala.foldLeft(0) { (total, processor) =>
      total + processor.responseQueueSize
    }
  })

  /**
   * 每当 Broker 启动时 它都会调用 addProcessor 方法 向 RequestChannel 对象添加 num.network.threads 个 Processor 线程
   *
   * @param processor
   */
  def addProcessor(processor: Processor): Unit = {
    // 添加processor到processor线程池
    if (processors.putIfAbsent(processor.id, processor) != null)
      warn(s"Unexpected processor with processorId ${processor.id}")

    // 为processor对象创建对应的监控指标
    newGauge(responseQueueSizeMetricName, () => processor.responseQueueSize,
      Map(ProcessorMetricTag -> processor.id.toString))
  }

  def removeProcessor(processorId: Int): Unit = {
    // 从processor线程池中移除对应的processor线程
    processors.remove(processorId)
    removeMetric(responseQueueSizeMetricName, Map(ProcessorMetricTag -> processorId.toString))
  }

  /**
   * Send a request to be handled, potentially blocking until there is room in the queue for the request
   * 发送Request 仅仅是将 Request 对象放置在 Request 队列中
   */
  def sendRequest(request: RequestChannel.Request): Unit = {
    requestQueue.put(request)
  }

  /**
   * Send a response back to the socket server to be sent over the network
   * 发送Response 将Response添加到Response队列
   */
  def sendResponse(response: RequestChannel.Response): Unit = {

    if (isTraceEnabled) { // 构造要输出的日志内容
      val requestHeader = response.request.header
      val message = response match {
        case sendResponse: SendResponse =>
          s"Sending ${requestHeader.apiKey} response to client ${requestHeader.clientId} of ${sendResponse.responseSend.size} bytes."
        case _: NoOpResponse =>
          s"Not sending ${requestHeader.apiKey} response to client ${requestHeader.clientId} as it's not required."
        case _: CloseConnectionResponse =>
          s"Closing connection for client ${requestHeader.clientId} due to error during ${requestHeader.apiKey}."
        case _: StartThrottlingResponse =>
          s"Notifying channel throttling has started for client ${requestHeader.clientId} for ${requestHeader.apiKey}"
        case _: EndThrottlingResponse =>
          s"Notifying channel throttling has ended for client ${requestHeader.clientId} for ${requestHeader.apiKey}"
      }
      trace(message)
    }

    response match {
      // We should only send one of the following per request
      case _: SendResponse | _: NoOpResponse | _: CloseConnectionResponse =>
        val request = response.request
        val timeNanos = time.nanoseconds()
        request.responseCompleteTimeNanos = timeNanos
        if (request.apiLocalCompleteTimeNanos == -1L)
          request.apiLocalCompleteTimeNanos = timeNanos
      // For a given request, these may happen in addition to one in the previous section, skip updating the metrics
      case _: StartThrottlingResponse | _: EndThrottlingResponse => ()
    }

    // 找到Response对应的processor线程 当processor处理完某个Request后会将processor序号放入response对象
    val processor = processors.get(response.processor)
    // The processor may be null if it was shutdown. In this case, the connections
    // are closed, so the response is dropped.
    if (processor != null) { // 将Response放入对应processor线程的Response队列中等待发送
      processor.enqueueResponse(response)
    }
  }

  /**
   * Get the next request or block until specified time has elapsed
   * 接收Request 从队列中取出 Request
   */
  def receiveRequest(timeout: Long): RequestChannel.BaseRequest =
    requestQueue.poll(timeout, TimeUnit.MILLISECONDS)

  /**
   * Get the next request or block until there is one
   * 接收Request 从队列中取出 Request
   */
  def receiveRequest(): RequestChannel.BaseRequest =
    requestQueue.take()

  def updateErrorMetrics(apiKey: ApiKeys, errors: collection.Map[Errors, Integer]): Unit = {
    errors.foreach { case (error, count) =>
      metrics(apiKey.name).markErrorMeter(error, count)
    }
  }

  def clear(): Unit = {
    requestQueue.clear()
  }

  def shutdown(): Unit = {
    clear()
    metrics.close()
  }

  /**
   * 将shutdownRequest写入到请求队列
   */
  def sendShutdownRequest(): Unit = requestQueue.put(ShutdownRequest)

}

object RequestMetrics {
  val consumerFetchMetricName = ApiKeys.FETCH.name + "Consumer"
  val followFetchMetricName = ApiKeys.FETCH.name + "Follower"

  // 每秒处理的Request数 用来评估Broker的繁忙状态
  val RequestsPerSec = "RequestsPerSec"
  // 计算Request在Request队列中的平均等待时间 单位ms 如果等待时间过长 需要增加后台IO线程数量加快从队列中取Request的速度
  val RequestQueueTimeMs = "RequestQueueTimeMs"
  // 计算Request实际被处理的时间 单位ms
  val LocalTimeMs = "LocalTimeMs"
  // Kafka 的读写请求（PRODUCE 请求和 FETCH 请求）逻辑涉及等待其他 Broker 操作的步骤
  // 等待其他 Broker 完成指定逻辑的时间
  // Kafka 生产环境中设置 acks=all 的 Producer 程序发送消息延时高的主要原因 往往就是 Remote Time 高
  val RemoteTimeMs = "RemoteTimeMs"
  // 计算 Request 被处理的完整流程时间
  val ThrottleTimeMs = "ThrottleTimeMs"
  val ResponseQueueTimeMs = "ResponseQueueTimeMs"
  val ResponseSendTimeMs = "ResponseSendTimeMs"
  val TotalTimeMs = "TotalTimeMs"
  val RequestBytes = "RequestBytes"
  val MessageConversionsTimeMs = "MessageConversionsTimeMs"
  val TemporaryMemoryBytes = "TemporaryMemoryBytes"
  val ErrorsPerSec = "ErrorsPerSec"
}

class RequestMetrics(name: String) extends KafkaMetricsGroup {

  import RequestMetrics._

  val tags = Map("request" -> name)
  val requestRateInternal = new Pool[Short, Meter]()
  // time a request spent in a request queue
  val requestQueueTimeHist = newHistogram(RequestQueueTimeMs, biased = true, tags)
  // time a request takes to be processed at the local broker
  val localTimeHist = newHistogram(LocalTimeMs, biased = true, tags)
  // time a request takes to wait on remote brokers (currently only relevant to fetch and produce requests)
  val remoteTimeHist = newHistogram(RemoteTimeMs, biased = true, tags)
  // time a request is throttled, not part of the request processing time (throttling is done at the client level
  // for clients that support KIP-219 and by muting the channel for the rest)
  val throttleTimeHist = newHistogram(ThrottleTimeMs, biased = true, tags)
  // time a response spent in a response queue
  val responseQueueTimeHist = newHistogram(ResponseQueueTimeMs, biased = true, tags)
  // time to send the response to the requester
  val responseSendTimeHist = newHistogram(ResponseSendTimeMs, biased = true, tags)
  val totalTimeHist = newHistogram(TotalTimeMs, biased = true, tags)
  // request size in bytes
  val requestBytesHist = newHistogram(RequestBytes, biased = true, tags)
  // time for message conversions (only relevant to fetch and produce requests)
  val messageConversionsTimeHist =
    if (name == ApiKeys.FETCH.name || name == ApiKeys.PRODUCE.name)
      Some(newHistogram(MessageConversionsTimeMs, biased = true, tags))
    else
      None
  // Temporary memory allocated for processing request (only populated for fetch and produce requests)
  // This shows the memory allocated for compression/conversions excluding the actual request size
  val tempMemoryBytesHist =
  if (name == ApiKeys.FETCH.name || name == ApiKeys.PRODUCE.name)
    Some(newHistogram(TemporaryMemoryBytes, biased = true, tags))
  else
    None

  private val errorMeters = mutable.Map[Errors, ErrorMeter]()
  Errors.values.foreach(error => errorMeters.put(error, new ErrorMeter(name, error)))

  def requestRate(version: Short): Meter = {
    requestRateInternal.getAndMaybePut(version, newMeter("RequestsPerSec", "requests", TimeUnit.SECONDS, tags + ("version" -> version.toString)))
  }

  class ErrorMeter(name: String, error: Errors) {
    private val tags = Map("request" -> name, "error" -> error.name)

    @volatile private var meter: Meter = null

    def getOrCreateMeter(): Meter = {
      if (meter != null)
        meter
      else {
        synchronized {
          if (meter == null)
            meter = newMeter(ErrorsPerSec, "requests", TimeUnit.SECONDS, tags)
          meter
        }
      }
    }

    def removeMeter(): Unit = {
      synchronized {
        if (meter != null) {
          removeMetric(ErrorsPerSec, tags)
          meter = null
        }
      }
    }
  }

  def markErrorMeter(error: Errors, count: Int): Unit = {
    errorMeters(error).getOrCreateMeter().mark(count.toLong)
  }

  def removeMetrics(): Unit = {
    for (version <- requestRateInternal.keys) removeMetric(RequestsPerSec, tags + ("version" -> version.toString))
    removeMetric(RequestQueueTimeMs, tags)
    removeMetric(LocalTimeMs, tags)
    removeMetric(RemoteTimeMs, tags)
    removeMetric(RequestsPerSec, tags)
    removeMetric(ThrottleTimeMs, tags)
    removeMetric(ResponseQueueTimeMs, tags)
    removeMetric(TotalTimeMs, tags)
    removeMetric(ResponseSendTimeMs, tags)
    removeMetric(RequestBytes, tags)
    removeMetric(ResponseSendTimeMs, tags)
    if (name == ApiKeys.FETCH.name || name == ApiKeys.PRODUCE.name) {
      removeMetric(MessageConversionsTimeMs, tags)
      removeMetric(TemporaryMemoryBytes, tags)
    }
    errorMeters.values.foreach(_.removeMeter())
    errorMeters.clear()
  }
}
