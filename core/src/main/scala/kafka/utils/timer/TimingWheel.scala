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
package kafka.utils.timer

import java.util.concurrent.DelayQueue
import java.util.concurrent.atomic.AtomicInteger

import kafka.utils.nonthreadsafe

/**
 * Hierarchical Timing Wheels
 * A simple timing wheel is a circular list of buckets of timer tasks. Let u be the time unit.
 * A timing wheel with size n has n buckets and can hold timer tasks in n * u time interval.
 * Each bucket holds timer tasks that fall into the corresponding time range. At the beginning,
 * the first bucket holds tasks for [0, u), the second bucket holds tasks for [u, 2u), …,
 * the n-th bucket for [u * (n -1), u * n). Every interval of time unit u, the timer ticks and
 * moved to the next bucket then expire all timer tasks in it. So, the timer never insert a task
 * into the bucket for the current time since it is already expired. The timer immediately runs
 * the expired task. The emptied bucket is then available for the next round, so if the current
 * bucket is for the time t, it becomes the bucket for [t + u * n, t + (n + 1) * u) after a tick.
 * A timing wheel has O(1) cost for insert/delete (start-timer/stop-timer) whereas priority queue
 * based timers, such as java.util.concurrent.DelayQueue and java.util.Timer, have O(log n)
 * insert/delete cost.
 * A major drawback of a simple timing wheel is that it assumes that a timer request is within
 * the time interval of n * u from the current time. If a timer request is out of this interval,
 * it is an overflow. A hierarchical timing wheel deals with such overflows. It is a hierarchically
 * organized timing wheels. The lowest level has the finest time resolution. As moving up the
 * hierarchy, time resolutions become coarser. If the resolution of a wheel at one level is u and
 * the size is n, the resolution of the next level should be n * u. At each level overflows are
 * delegated to the wheel in one level higher. When the wheel in the higher level ticks, it reinsert
 * timer tasks to the lower level. An overflow wheel can be created on-demand. When a bucket in an
 * overflow bucket expires, all tasks in it are reinserted into the timer recursively. The tasks
 * are then moved to the finer grain wheels or be executed. The insert (start-timer) cost is O(m)
 * where m is the number of wheels, which is usually very small compared to the number of requests
 * in the system, and the delete (stop-timer) cost is still O(1).
 * Example
 * Let's say that u is 1 and n is 3. If the start time is c,
 * then the buckets at different levels are:
 * level    buckets
 * 1        [c,c]   [c+1,c+1]  [c+2,c+2]
 * 2        [c,c+2] [c+3,c+5]  [c+6,c+8]
 * 3        [c,c+8] [c+9,c+17] [c+18,c+26]
 * The bucket expiration is at the time of bucket beginning.
 * So at time = c+1, buckets [c,c], [c,c+2] and [c,c+8] are expired.
 * Level 1's clock moves to c+1, and [c+3,c+3] is created.
 * Level 2 and level3's clock stay at c since their clocks move in unit of 3 and 9, respectively.
 * So, no new buckets are created in level 2 and 3.
 * Note that bucket [c,c+2] in level 2 won't receive any task since that range is already covered in level 1.
 * The same is true for the bucket [c,c+8] in level 3 since its range is covered in level 2.
 * This is a bit wasteful, but simplifies the implementation.
 * 1        [c+1,c+1]  [c+2,c+2]  [c+3,c+3]
 * 2        [c,c+2]    [c+3,c+5]  [c+6,c+8]
 * 3        [c,c+8]    [c+9,c+17] [c+18,c+26]
 * At time = c+2, [c+1,c+1] is newly expired.
 * Level 1 moves to c+2, and [c+4,c+4] is created,
 * 1        [c+2,c+2]  [c+3,c+3]  [c+4,c+4]
 * 2        [c,c+2]    [c+3,c+5]  [c+6,c+8]
 * 3        [c,c+8]    [c+9,c+17] [c+18,c+18]
 * At time = c+3, [c+2,c+2] is newly expired.
 * Level 2 moves to c+3, and [c+5,c+5] and [c+9,c+11] are created.
 * Level 3 stay at c.
 * 1        [c+3,c+3]  [c+4,c+4]  [c+5,c+5]
 * 2        [c+3,c+5]  [c+6,c+8]  [c+9,c+11]
 * 3        [c,c+8]    [c+9,c+17] [c+8,c+11]
 * The hierarchical timing wheels works especially well when operations are completed before they time out.
 * Even when everything times out, it still has advantageous when there are many items in the timer.
 * Its insert cost (including reinsert) and delete cost are O(m) and O(1), respectively while priority
 * queue based timers takes O(log N) for both insert and delete where N is the number of items in the queue.
 * This class is not thread-safe. There should not be any add calls while advanceClock is executing.
 * It is caller's responsibility to enforce it. Simultaneous add calls are thread-safe.
 *
 * @param tickMs      滴答一次的时长 类似于手表的例子中向前推进一格的时间
 *                    在 Kafka 中 第 1 层时间轮的 tickMs 被固定为 1 毫秒 也就是说 向前推进一格 Bucket 的时长是 1 毫秒
 * @param wheelSize   每一层时间轮上的 Bucket 数量 第 1 层的 Bucket 数量是 20
 * @param startMs     时间轮对象被创建时的起始时间戳
 * @param taskCounter 这一层时间轮上的总定时任务数
 * @param queue       将所有 Bucket 按照过期时间排序的延迟队列
 *                    随着时间不断向前推进 Kafka 需要依靠这个队列获取那些已过期的 Bucket并清除它们
 */
@nonthreadsafe
private[timer] class TimingWheel(tickMs: Long, wheelSize: Int, startMs: Long, taskCounter: AtomicInteger, queue: DelayQueue[TimerTaskList]) {

  // 这层时间轮总时长 等于tickMs * wheelSize
  // 以第 1 层为例 interval 就是 20 毫秒 由于下一层时间轮的滴答时长就是上一层的总时长 因此第 2 层的滴答时长就是 20 毫秒 总时长是 400 毫秒 以此类推
  private[this] val interval = tickMs * wheelSize
  // 时间轮下的所有 Bucket 对象 也就是所有 TimerTaskList 对象
  private[this] val buckets = Array.tabulate[TimerTaskList](wheelSize) { _ => new TimerTaskList(taskCounter) }

  // 当前时间戳 源码对它进行了一些微调整 将它设置成小于当前时间的最大滴答时长的整数倍
  // 举个例子 假设滴答时长是 20 毫秒 当前时间戳是 123 毫秒 那么currentTime 会被调整为 120 毫秒
  private[this] var currentTime = startMs - (startMs % tickMs) // rounding down to multiple of tickMs

  // overflowWheel can potentially be updated and read by two concurrent threads through add().
  // Therefore, it needs to be volatile due to the issue of Double-Checked Locking pattern with JVM
  // Kafka 是按需创建上层时间轮的 这也就是说当有新的定时任务到达时 会尝试将其放入第 1 层时间轮
  // 如果第 1 层的 interval 无法容纳定时任务的超时时间 就现场创建并配置好第 2 层时间轮 并再次尝试放入
  // 如果依然无法容纳 那么就再创建和配置第 3 层时间轮 以此类推直到找到适合容纳该定时任务的第 N 层时间轮
  // 由于每层时间轮的长度都是倍增的 因此代码并不需要创建太多层的时间轮 就足以容纳绝大部分的延时请求了
  @volatile private[this] var overflowWheel: TimingWheel = null

  /**
   * TimingWheel 类字段 overflowWheel 的创建是按需的 每当需要一个新的上层时间轮时 代码就会调用 addOverflowWheel 方法
   */
  private[this] def addOverflowWheel(): Unit = {
    synchronized {
      // 只有之前没有创建上层时间轮方法才会继续
      if (overflowWheel == null) {
        // 创建新的TimingWheel实例
        overflowWheel = new TimingWheel(
          tickMs = interval, // 滴答时长tickMs等于下层时间轮总时长
          wheelSize = wheelSize, // 每层的轮子数都是相同的
          startMs = currentTime,
          taskCounter = taskCounter,
          queue
        )
      }
    }
  }

  def add(timerTaskEntry: TimerTaskEntry): Boolean = {

    // 获取定时任务的过期时间戳
    val expiration = timerTaskEntry.expirationMs

    // 如果该任务已然被取消了 则无需添加直接返回
    if (timerTaskEntry.cancelled) {
      // Cancelled
      false
    } else if (expiration < currentTime + tickMs) { // 如果该任务超时时间已过期
      // Already expired
      false
    } else if (expiration < currentTime + interval) { // 如果该任务超时时间在本层时间轮覆盖时间范围内
      // Put in its own bucket
      val virtualId = expiration / tickMs
      val bucket = buckets((virtualId % wheelSize.toLong).toInt) // 计算要被放入到哪个Bucket中
      bucket.add(timerTaskEntry) // 添加到Bucket中

      // Set the bucket expiration time
      if (bucket.setExpiration(virtualId * tickMs)) { // 更新这个 Bucket 的过期时间戳 如果该时间变更过 说明Bucket是新建或被重用 将其加回到DelayQueue
        // The bucket needs to be enqueued because it was an expired bucket
        // We only need to enqueue the bucket when its expiration time has changed, i.e. the wheel has advanced
        // and the previous buckets gets reused; further calls to set the expiration within the same wheel cycle
        // will pass in the same value and hence return false, thus the bucket with the same expiration will not
        // be enqueued multiple times.
        queue.offer(bucket)
      }
      true
    } else { // 本层时间轮无法容纳该任务 交由上层时间轮处理
      // Out of the interval. Put it into the parent timer
      if (overflowWheel == null) addOverflowWheel() // 按需创建上层时间轮
      overflowWheel.add(timerTaskEntry) // 加入到上层时间轮中
    }
  }

  // Try to advance the clock
  /**
   * 向前驱动时钟
   *
   * @param timeMs 要把时钟向前推动到这个时点
   */
  def advanceClock(timeMs: Long): Unit = {
    if (timeMs >= currentTime + tickMs) { // 向前驱动到的时点要超过Bucket的时间范围才是有意义的推进 否则什么都不做
      currentTime = timeMs - (timeMs % tickMs) // 更新当前时间currentTime到下一个Bucket的起始时点

      // Try to advance the clock of the overflow wheel if present
      if (overflowWheel != null) overflowWheel.advanceClock(currentTime) // 递归地为上一层间轮做向前推进动作
    }
  }
}
