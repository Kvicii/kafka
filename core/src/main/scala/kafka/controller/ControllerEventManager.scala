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

package kafka.controller

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{CountDownLatch, LinkedBlockingQueue}

import kafka.metrics.{KafkaMetricsGroup, KafkaTimer}
import kafka.utils.CoreUtils.inLock
import kafka.utils.ShutdownableThread
import org.apache.kafka.common.utils.Time

import scala.collection._

/**
 * 用于保存字符串属性 如线程名称等
 */
object ControllerEventManager {
  val ControllerEventThreadName = "controller-event-thread"
  val EventQueueTimeMetricName = "EventQueueTimeMs"
  val EventQueueSizeMetricName = "EventQueueSize"
}

/**
 * Controller端事件处理器接口
 * KafkaController是唯一的实现类
 */
trait ControllerEventProcessor {
  /**
   * 支持普通处理Controller事件的接口
   * 接收一个 Controller 事件并进行处理 是实现 Controller 事件处理的主力方法
   *
   * @param event
   */
  def process(event: ControllerEvent): Unit

  /**
   * 支持抢占处理Controller事件的接口
   * 接收一个 Controller 事件 并抢占队列之前的事件进行优先处理 Kafka 使用preempt实现某些高优先级事件的抢占处理
   * 目前在源码中只有两类事件(ShutdownEventThread 和 Expire)需要抢占式处理
   *
   * @param event
   */
  def preempt(event: ControllerEvent): Unit
}

/**
 * 表示的是事件队列上的事件对象
 *
 * @param event         Controller事件
 * @param enqueueTimeMs Controller事件被放入事件队列的时间戳
 */
class QueuedEvent(val event: ControllerEvent, val enqueueTimeMs: Long) {
  // 标识Controller事件是否开始被处理  QueuedEvent使用CountDownLatch的目的是确保Expire事件在建立ZK会话前被处理 如果不是为了处理Expire事件 使用spent来标识该事件已经被处理过了(如果事件已经被处理过 什么都不做直接返回)
  val processingStarted = new CountDownLatch(1)
  // 标识Controller事件是否被处理过
  val spent = new AtomicBoolean(false)

  /**
   * 处理Controller事件
   *
   * @param processor
   */
  def process(processor: ControllerEventProcessor): Unit = {
    if (spent.getAndSet(true))
      return
    processingStarted.countDown()
    processor.process(event)
  }

  /**
   * 抢占式处理Controller事件
   *
   * @param processor
   */
  def preempt(processor: ControllerEventProcessor): Unit = {
    if (spent.getAndSet(true))
      return
    processor.preempt(event)
  }

  /**
   * 阻塞等待事件处理完成
   */
  def awaitProcessing(): Unit = {
    processingStarted.await()
  }

  override def toString: String = {
    s"QueuedEvent(event=$event, enqueueTimeMs=$enqueueTimeMs)"
  }
}

/**
 * Controller事件处理器 用于创建和管理 ControllerEventThread(事件处理线程) 和 事件队列
 *
 * @param controllerId
 * @param processor
 * @param time
 * @param rateAndTimeMetrics
 */
class ControllerEventManager(controllerId: Int,
                             processor: ControllerEventProcessor,
                             time: Time,
                             rateAndTimeMetrics: Map[ControllerState, KafkaTimer]) extends KafkaMetricsGroup {

  import ControllerEventManager._

  @volatile private var _state: ControllerState = ControllerState.Idle
  private val putLock = new ReentrantLock()
  private val queue = new LinkedBlockingQueue[QueuedEvent]
  // Visible for test
  private[controller] val thread = new ControllerEventThread(ControllerEventThreadName)

  private val eventQueueTimeHist = newHistogram(EventQueueTimeMetricName)

  newGauge(EventQueueSizeMetricName, () => queue.size)

  def state: ControllerState = _state

  def start(): Unit = thread.start()

  def close(): Unit = {
    try {
      thread.initiateShutdown()
      clearAndPut(ShutdownEventThread)
      thread.awaitShutdown()
    } finally {
      removeMetric(EventQueueTimeMetricName)
      removeMetric(EventQueueSizeMetricName)
    }
  }

  def put(event: ControllerEvent): QueuedEvent = inLock(putLock) {
    val queuedEvent = new QueuedEvent(event, time.milliseconds())
    queue.put(queuedEvent)
    queuedEvent
  }

  def clearAndPut(event: ControllerEvent): QueuedEvent = inLock(putLock) {
    queue.forEach(_.preempt(processor))
    queue.clear()
    put(event)
  }

  def isEmpty: Boolean = queue.isEmpty

  /**
   * 专属的事件处理线程 唯一的作用是处理不同种类的ControllerEvent
   *
   * @param name
   */
  class ControllerEventThread(name: String) extends ShutdownableThread(name = name, isInterruptible = false) {
    logIdent = s"[ControllerEventThread controllerId=$controllerId] "

    override def doWork(): Unit = {
      val dequeued = queue.take()
      dequeued.event match {
        case ShutdownEventThread => // The shutting down of the thread has been initiated at this point. Ignore this event.
        case controllerEvent =>
          _state = controllerEvent.state

          eventQueueTimeHist.update(time.milliseconds() - dequeued.enqueueTimeMs)

          try {
            def process(): Unit = dequeued.process(processor)

            rateAndTimeMetrics.get(state) match {
              case Some(timer) => timer.time {
                process()
              }
              case None => process()
            }
          } catch {
            case e: Throwable => error(s"Uncaught error processing event $controllerEvent", e)
          }

          _state = ControllerState.Idle
      }
    }
  }

}
