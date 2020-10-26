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

import java.util.ArrayList
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{CountDownLatch, LinkedBlockingQueue, TimeUnit}

import kafka.metrics.{KafkaMetricsGroup, KafkaTimer}
import kafka.utils.CoreUtils.inLock
import kafka.utils.ShutdownableThread
import org.apache.kafka.common.utils.Time

import scala.collection._

/**
 * 用于保存字符串常量(如线程名称)
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
   * 支持普通处理Controller事件的接口 接收一个 Controller 事件并进行处理
   * 是实现 Controller 事件处理的主力方法
   *
   * @param event
   */
  def process(event: ControllerEvent): Unit

  /**
   * 支持抢占处理Controller事件的接口 接收一个 Controller 事件 并抢占队列之前的事件进行优先处理
   * Kafka 使用preempt实现某些高优先级事件的抢占处理 目前在源码中只有两类事件(ShutdownEventThread 和 Expire)需要抢占式处理
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
  // 标识Controller事件是否开始被处理
  // QueuedEvent使用CountDownLatch的目的是确保Expire事件在建立ZK会话前被处理 如果不是为了处理Expire事件 使用spent来标识该事件已经被处理过了(如果事件已经被处理过 什么都不做直接返回)
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
                             rateAndTimeMetrics: Map[ControllerState, KafkaTimer],
                             eventQueueTimeTimeoutMs: Long = 300000) extends KafkaMetricsGroup {

  import ControllerEventManager._

  @volatile private var _state: ControllerState = ControllerState.Idle
  private val putLock = new ReentrantLock()
  private val queue = new LinkedBlockingQueue[QueuedEvent]
  // Visible for test
  private[controller] var thread = new ControllerEventThread(ControllerEventThreadName)

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

  /**
   * 向事件队列中写入Controller事件
   *
   * @param event
   * @return
   */
  def put(event: ControllerEvent): QueuedEvent = inLock(putLock) {
    val queuedEvent = new QueuedEvent(event, time.milliseconds())
    queue.put(queuedEvent)
    queuedEvent
  }

  /**
   * 向事件队列中写入Controller事件 和put不同 该方法会先执行高优先级的抢占事件 之后清空事件队列 最后向事件队列中写入事件
   *
   * @param event
   * @return
   */
  def clearAndPut(event: ControllerEvent): QueuedEvent = inLock(putLock) {
    val preemptedEvents = new ArrayList[QueuedEvent]()
    queue.drainTo(preemptedEvents)
    preemptedEvents.forEach(_.preempt(processor))
    put(event)
  }

  def isEmpty: Boolean = queue.isEmpty

  /**
   * 专属的事件处理线程 唯一的作用是处理不同种类的ControllerEvent 从事件队列中读取Controller事件
   * ShutdownableThread是Kafka为很多线程类定义的公共父类 其父类是 Java Thread 类
   *
   * @param name 由ControllerEventManager对象定义的
   */
  class ControllerEventThread(name: String) extends ShutdownableThread(name = name, isInterruptible = false) {
    logIdent = s"[ControllerEventThread controllerId=$controllerId] "

    override def doWork(): Unit = {
      val dequeued = pollFromEventQueue() // 从事件队列中获取Controller事件 如果事件队列为空将一直阻塞 或者默认阻塞5分钟
      dequeued.event match {
        // 当ControllerEventManager关闭时会向事件队列中写入ShutdownEventThread事件显示通知ControllerEventThread线程关闭 此处什么也不需要做 因为关闭ControllerEventThread的逻辑是由外部调用的
        case ShutdownEventThread => // The shutting down of the thread has been initiated at this point. Ignore this event.
        // 其他事件调用process方法进行处理
        case controllerEvent =>
          _state = controllerEvent.state
          // 更新对应的事件在事件队列中保存的时间
          eventQueueTimeHist.update(time.milliseconds() - dequeued.enqueueTimeMs)

          try {
            def process(): Unit = dequeued.process(processor) // 调用QueueEvent的process方法
            // 处理Controller事件 同时计算处理速率
            rateAndTimeMetrics.get(state) match {
              case Some(timer) => timer.time {
                process()
              }
              case None => process() // 该方法首先调用QueueEvent的process方法判断是否已经被处理过 如果被处理过直接返回 如果没被处理调用ControllerEventProcessor的process方法进行处理
            }
          } catch {
            case e: Throwable => error(s"Uncaught error processing event $controllerEvent", e)
          }

          _state = ControllerState.Idle
      }
    }
  }

  private def pollFromEventQueue(): QueuedEvent = {
    val count = eventQueueTimeHist.count()
    if (count != 0) {
      val event = queue.poll(eventQueueTimeTimeoutMs, TimeUnit.MILLISECONDS)
      if (event == null) {
        eventQueueTimeHist.clear()
        queue.take()
      } else {
        event
      }
    } else {
      queue.take()
    }
  }
}
