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
 *
 * TimerTask 建模的是 Kafka 中的定时任务
 * 是一个 Runnable 类 Kafka 使用一个单独线程异步添加延时请求到时间轮
 */
package kafka.utils.timer

trait TimerTask extends Runnable {

  // 定时任务的超时时间 通常是request.timeout.ms参数值
  val delayMs: Long // timestamp in millisecond

  // 每个TimerTask实例关联一个TimerTaskEntry 每个定时任务需要知道它在哪个Bucket链表下的哪个链表元素上
  private[this] var timerTaskEntry: TimerTaskEntry = null

  /**
   * 取消定时任务 原理就是将关联的timerTaskEntry置空 即把定时任务从链表上摘除
   */
  def cancel(): Unit = {
    synchronized {
      if (timerTaskEntry != null) timerTaskEntry.remove()
      timerTaskEntry = null
    }
  }

  /**
   * 关联timerTaskEntry 原理是给timerTaskEntry字段赋值
   *
   * @param entry
   */
  private[timer] def setTimerTaskEntry(entry: TimerTaskEntry): Unit = {
    synchronized { // Setter 的整个方法体必须由 monitor 锁保护起来 以保证线程安全性
      // if this timerTask is already held by an existing timer task entry,
      // we will remove such an entry first.
      if (timerTaskEntry != null && timerTaskEntry != entry)
        timerTaskEntry.remove() // 如果这个定时任务已经绑定了其他的 timerTaskEntry 就必须先取消绑定

      timerTaskEntry = entry
    }
  }

  /**
   * 获取关联的timerTaskEntry实例
   *
   * @return
   */
  private[timer] def getTimerTaskEntry: TimerTaskEntry = timerTaskEntry
}
