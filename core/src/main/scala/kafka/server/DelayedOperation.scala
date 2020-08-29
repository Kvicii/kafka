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

import java.util.concurrent._
import java.util.concurrent.atomic._
import java.util.concurrent.locks.{Lock, ReentrantLock}

import kafka.metrics.KafkaMetricsGroup
import kafka.utils.CoreUtils.inLock
import kafka.utils._
import kafka.utils.timer._

import scala.collection._
import scala.collection.mutable.ListBuffer

/**
 * An operation whose processing needs to be delayed for at most the given delayMs. For example
 * a delayed produce operation could be waiting for specified number of acks; or
 * a delayed fetch operation could be waiting for a given number of bytes to accumulate.
 * The logic upon completing a delayed operation is defined in onComplete() and will be called exactly once.
 * Once an operation is completed, isCompleted() will return true. onComplete() can be triggered by either
 * forceComplete(), which forces calling onComplete() after delayMs if the operation is not yet completed,
 * or tryComplete(), which first checks if the operation can be completed or not now, and if yes calls
 * forceComplete().
 * A subclass of DelayedOperation needs to provide an implementation of both onComplete() and tryComplete().
 *
 * 所有 Kafka 延迟请求类的抽象父类
 * 延迟请求的高阶抽象类 提供了完成请求以及请求完成和过期后的回调逻辑实现
 *
 * @param delayMs 超时时间 通常是客户端发出请求的超时时间 也就是客户端参数 request.timeout.ms 的值
 * @param lockOpt
 */
abstract class DelayedOperation(override val delayMs: Long,
                                lockOpt: Option[Lock] = None)
  extends TimerTask with Logging {

  // 标识该延迟操作是否已经完成
  private val completed = new AtomicBoolean(false)
  // 1.1 版本在此之前 只有 completed 参数
  // 可能引发的问题是 当多个线程同时检查某个延迟操作是否满足完成条件时 如果其中一个线程持有了锁 然后执行条件检查 会发现不满足完成条件
  // 与此同时另一个线程执行检查时却发现条件满足了 但是这个线程又没有拿到锁 此时该延迟操作将永远不会有再次被检查的机会 会导致最终超时
  // 防止多个线程同时检查操作是否可完成时发生锁竞争导致操作最终超时
  // 加入 tryCompletePending 字段目的 就是确保拿到锁的线程有机会再次检查条件是否已经满足
  private val tryCompletePending = new AtomicBoolean(false)
  // Visible for testing
  private[server] val lock: Lock = lockOpt.getOrElse(new ReentrantLock)

  /*
   * Force completing the delayed operation, if not already completed.
   * This function can be triggered when
   *
   * 1. The operation has been verified to be completable inside tryComplete()
   * 2. The operation has expired and hence needs to be completed right now
   *
   * Return true iff the operation is completed by the caller: note that
   * concurrent threads can try to complete the same operation, but only
   * the first thread will succeed in completing the operation and return
   * true, others will still return false
   *
   * 强制完成延迟操作 不管它是否满足完成条件 每当操作满足完成条件或已经过期了 就需要调用该方法完成该操作
   */
  def forceComplete(): Boolean = {
    if (completed.compareAndSet(false, true)) {
      // cancel the timeout timer
      cancel()
      onComplete()
      true
    } else {
      false
    }
  }

  /**
   * Check if the delayed operation is already completed
   *
   * 检查延迟操作是否已经完成 源码使用这个方法来决定后续如何处理该操作(比如如果操作已经完成了 那么通常需要取消该操作)
   */
  def isCompleted: Boolean = completed.get()

  /**
   * Call-back to execute when a delayed operation gets expired and hence forced to complete.
   *
   * 强制完成之后执行的过期逻辑回调方法 只有真正完成操作的那个线程才有资格调用这个方法
   */
  def onExpiration(): Unit

  /**
   * Process for completing an operation; This function needs to be defined
   * in subclasses and will be called exactly once in forceComplete()
   *
   * 完成延迟操作所需的处理逻辑 这个方法只会在 forceComplete 方法中被调用
   */
  def onComplete(): Unit

  /**
   * Try to complete the delayed operation by first checking if the operation
   * can be completed by now. If yes execute the completion logic by calling
   * forceComplete() and return true iff forceComplete returns true; otherwise return false
   *
   * This function needs to be defined in subclasses
   *
   * 尝试完成延迟操作的顶层方法 内部会调用 forceComplete 方法
   */
  def tryComplete(): Boolean

  /**
   * Thread-safe variant of tryComplete() that attempts completion only if the lock can be acquired
   * without blocking.
   *
   * If threadA acquires the lock and performs the check for completion before completion criteria is met
   * and threadB satisfies the completion criteria, but fails to acquire the lock because threadA has not
   * yet released the lock, we need to ensure that completion is attempted again without blocking threadA
   * or threadB. `tryCompletePending` is set by threadB when it fails to acquire the lock and at least one
   * of threadA or threadB will attempt completion of the operation if this flag is set. This ensures that
   * every invocation of `maybeTryComplete` is followed by at least one invocation of `tryComplete` until
   * the operation is actually completed.
   *
   * 线程安全版本的 tryComplete 方法 该方法其实是社区后来才加入的 不过已经慢慢地取代了 tryComplete 现在外部代码调用的都是这个方法了
   */
  private[server] def maybeTryComplete(): Boolean = {
    var retry = false // 是否需要重试
    var done = false // 延迟操作是否已完成
    do {
      if (lock.tryLock()) { // 尝试获取锁对象
        try {
          tryCompletePending.set(false) // 清空 tryCompletePending 状态
          done = tryComplete() // 完成延迟请求
        } finally {
          lock.unlock() // 释放锁
        }
        // While we were holding the lock, another thread may have invoked `maybeTryComplete` and set
        // `tryCompletePending`. In this case we should retry.
        // 运行到这里的线程持有锁 其他线程只能运行else分支的代码
        // 如果其他线程将maybeTryComplete设置为true 那么retry = true
        // 这就相当于其他线程给了本线程重试的机会
        retry = tryCompletePending.get()
      } else {
        // Another thread is holding the lock. If `tryCompletePending` is already set and this thread failed to
        // acquire the lock, then the thread that is holding the lock is guaranteed to see the flag and retry.
        // Otherwise, we should set the flag and retry on this thread since the thread holding the lock may have
        // released the lock and returned by the time the flag is set.
        // 运行到这里的线程没有拿到锁 间接影响 retry 值 设置tryCompletePending = true给持有锁的线程一个重试的机会
        retry = !tryCompletePending.getAndSet(true)
      }
    } while (!isCompleted && retry)
    done
  }

  /*
   * run() method defines a task that is executed on timeout
   *
   * 调用延迟操作超时后的过期逻辑 也就是组合调用 forceComplete + onExpiration
   */
  override def run(): Unit = {
    if (forceComplete())
      onExpiration()
  }
}

object DelayedOperationPurgatory {

  // DelayedOperationPurgatory 监控列表的数组长度信息
  private val Shards = 512 // Shard the watcher list to reduce lock contention

  def apply[T <: DelayedOperation](purgatoryName: String,
                                   brokerId: Int = 0,
                                   purgeInterval: Int = 1000,
                                   reaperEnabled: Boolean = true,
                                   timerEnabled: Boolean = true): DelayedOperationPurgatory[T] = {
    val timer = new SystemTimer(purgatoryName)
    new DelayedOperationPurgatory[T](purgatoryName, timer, brokerId, purgeInterval, reaperEnabled, timerEnabled)
  }

}

/**
 * A helper purgatory class for bookkeeping delayed operations with a timeout, and expiring timed out operations.
 *
 * DelayedOperationPurgatory 类是一个泛型类 它的参数类型是 DelayedOperation 的具体子类
 * 通常情况下每一类延迟请求都对应于一个 DelayedOperationPurgatory 实例 这些实例一般都保存在上层的管理器中
 * 如与消费者组相关的心跳请求 | 加入组请求的 Purgatory 实例 就保存在 GroupCoordinator 组件中 而与生产者相关的 PRODUCE 请求的 Purgatory 实例 被保存在分区对象或副本状态机中
 *
 * Purgatory 实现类 该类定义了 WatcherList 对象以及对 WatcherList 的操作方法 而 WatcherList 是实现延迟请求后续自动处理的关键数据结构
 *
 * @param purgatoryName Purgatory 的名字
 * @param timeoutTimer  SystemTimer
 * @param brokerId      Broker 的序号
 * @param purgeInterval 控制删除线程移除 Bucket 中的过期延迟请求的频率 在绝大部分情况下都是 1 秒一次
 *                      对于生产者 | 消费者以及删除消息的 AdminClient 而言 Kafka 分别定义了专属的参数允许你调整这个频率
 *                      比如 生产者参数 producer.purgatory.purge.interval.requests就是做这个用的
 * @param reaperEnabled 是否启动删除线程
 * @param timerEnabled  是否启用分层时间轮
 * @tparam T
 */
final class DelayedOperationPurgatory[T <: DelayedOperation](purgatoryName: String,
                                                             timeoutTimer: Timer,
                                                             brokerId: Int = 0,
                                                             purgeInterval: Int = 1000,
                                                             reaperEnabled: Boolean = true,
                                                             timerEnabled: Boolean = true)
  extends Logging with KafkaMetricsGroup {

  /* a list of operation watching keys */
  private class WatcherList {
    // 定义一组按照Key分组的Watchers对象
    // Pool 是 Kafka 定义的池对象 本质上就是一个 ConcurrentHashMap
    // watchersByKey 的 Key 可以是任何类型 而 Value 就是 Key 对应类型的一组 Watchers 对象
    val watchersByKey = new Pool[Any, Watchers](Some((key: Any) => new Watchers(key)))

    val watchersLock = new ReentrantLock()

    /*
     * Return all the current watcher lists,
     * note that the returned watchers may be removed from the list by other threads
     * 返回所有Watchers对象
     */
    def allWatchers = {
      watchersByKey.values
    }
  }

  private val watcherLists = Array.fill[WatcherList](DelayedOperationPurgatory.Shards)(new WatcherList)

  private def watcherList(key: Any): WatcherList = {
    watcherLists(Math.abs(key.hashCode() % watcherLists.length))
  }

  // the number of estimated total operations in the purgatory
  private[this] val estimatedTotalOperations = new AtomicInteger(0)

  /* background thread expiring operations that have timed out */
  // 将已过期的延迟请求从数据结构中移除掉
  private val expirationReaper = new ExpiredOperationReaper()

  private val metricsTags = Map("delayedOperation" -> purgatoryName)
  newGauge("PurgatorySize", () => watched, metricsTags)
  newGauge("NumDelayedOperations", () => numDelayed, metricsTags)

  if (reaperEnabled)
    expirationReaper.start()

  /**
   * Check if the operation can be completed, if not watch it based on the given watch keys
   *
   * Note that a delayed operation can be watched on multiple keys. It is possible that
   * an operation is completed after it has been added to the watch list for some, but
   * not all of the keys. In this case, the operation is considered completed and won't
   * be added to the watch list of the remaining keys. The expiration reaper thread will
   * remove this operation from any watcher list in which the operation exists.
   *
   * 检查操作是否能够完成 如果不能就把它加入到对应 Key 所在的 WatcherList 中
   *
   * @param operation the delayed operation to be checked
   * @param watchKeys keys for bookkeeping the operation
   * @return true iff the delayed operations can be completed by the caller
   */
  def tryCompleteElseWatch(operation: T, watchKeys: Seq[Any]): Boolean = {
    assert(watchKeys.nonEmpty, "The watch key list can't be empty")

    // The cost of tryComplete() is typically proportional to the number of keys. Calling
    // tryComplete() for each key is going to be expensive if there are many keys. Instead,
    // we do the check in the following way. Call tryComplete(). If the operation is not completed,
    // we just add the operation to all keys. Then we call tryComplete() again. At this time, if
    // the operation is still not completed, we are guaranteed that it won't miss any future triggering
    // event since the operation is already on the watcher list for all keys. This does mean that
    // if the operation is completed (by another thread) between the two tryComplete() calls, the
    // operation is unnecessarily added for watch. However, this is a less severe issue since the
    // expire reaper will clean it up periodically.
    // At this point the only thread that can attempt this operation is this current thread
    // Hence it is safe to tryComplete() without a lock
    var isCompletedByMe = operation.tryComplete()
    // 调用tryComplete完成延迟请求 如果返回true 说明调用tryComplete的线程正常完成了延迟请求 不需要加入WatcherList
    if (isCompletedByMe)
      return true

    var watchCreated = false
    for (key <- watchKeys) { // 遍历所有要监控的Key
      // If the operation is already completed, stop adding it to the rest of the watcher list.
      if (operation.isCompleted) { // 再次查看请求的完成状态 如果已经完成 就说明是被其他线程完成的 返回false
        return false
      }
      watchForOperation(key, operation) // 依然无法完成 将该operation加入到Key所在的WatcherList 等待后续完成

      if (!watchCreated) { // 设置watchCreated标记 表明该任务已经被加入到WatcherList
        watchCreated = true
        estimatedTotalOperations.incrementAndGet() // 更新Purgatory中总请求数
      }
    }

    isCompletedByMe = operation.maybeTryComplete() // 再次尝试完成该延迟请求
    if (isCompletedByMe)
      return true

    // if it cannot be completed by now and hence is watched, add to the expire queue also
    if (!operation.isCompleted) { // 如果依然不能完成此请求 将其加入到过期队列
      if (timerEnabled)
        timeoutTimer.add(operation)
      if (operation.isCompleted) {
        // cancel the timer task
        operation.cancel()
      }
    }
    false
  }

  /**
   * Check if some delayed operations can be completed with the given watch key,
   * and if yes complete them.
   *
   * 检查给定 Key 所在的 WatcherList 中的延迟请求是否满足完成条件 如果是的话则结束掉它们
   *
   * @return the number of completed operations during this process
   */
  def checkAndComplete(key: Any): Int = {
    // 获取给定Key的WatcherList
    val wl = watcherList(key)
    // 获取给定Key的WatcherList
    val watchers = inLock(wl.watchersLock) {
      wl.watchersByKey.get(key)
    }
    // 尝试完成满足完成条件的延迟请求并返回成功完成的请求数
    val numCompleted = if (watchers == null) {
      0
    } else { // 调用 Watchers 的 tryCompleteWatched 方法 去尝试完成那些已满足完成条件的延迟请求
      watchers.tryCompleteWatched()
    }
    debug(s"Request key $key unblocked $numCompleted $purgatoryName operations")
    numCompleted
  }

  /**
   * Return the total size of watch lists the purgatory. Since an operation may be watched
   * on multiple lists, and some of its watched entries may still be in the watch lists
   * even when it has been completed, this number may be larger than the number of real operations watched
   */
  def watched: Int = {
    watcherLists.foldLeft(0) { case (sum, watcherList) => sum + watcherList.allWatchers.map(_.countWatched).sum }
  }

  /**
   * Return the number of delayed operations in the expiry queue
   */
  def numDelayed: Int = timeoutTimer.size

  /**
   * Cancel watching on any delayed operations for the given key. Note the operation will not be completed
   */
  def cancelForKey(key: Any): List[T] = {
    val wl = watcherList(key)
    inLock(wl.watchersLock) {
      val watchers = wl.watchersByKey.remove(key)
      if (watchers != null)
        watchers.cancel()
      else
        Nil
    }
  }

  /*
   * Return the watch list of the given key, note that we need to
   * grab the removeWatchersLock to avoid the operation being added to a removed watcher list
   */
  private def watchForOperation(key: Any, operation: T): Unit = {
    val wl = watcherList(key)
    inLock(wl.watchersLock) {
      val watcher = wl.watchersByKey.getAndMaybePut(key)
      watcher.watch(operation)
    }
  }

  /*
   * Remove the key from watcher lists if its list is empty
   */
  private def removeKeyIfEmpty(key: Any, watchers: Watchers): Unit = {
    val wl = watcherList(key)
    inLock(wl.watchersLock) {
      // if the current key is no longer correlated to the watchers to remove, skip
      if (wl.watchersByKey.get(key) != watchers)
        return

      if (watchers != null && watchers.isEmpty) {
        wl.watchersByKey.remove(key)
      }
    }
  }

  /**
   * Shutdown the expire reaper thread
   */
  def shutdown(): Unit = {
    if (reaperEnabled)
      expirationReaper.shutdown()
    timeoutTimer.shutdown()
    removeMetric("PurgatorySize", metricsTags)
    removeMetric("NumDelayedOperations", metricsTags)
  }

  /**
   * A linked list of watched delayed operations based on some key
   *
   * 基于 Key 的一个延迟请求的监控链表
   */
  private class Watchers(val key: Any) {
    // 每个 Watchers 实例都定义了一个延迟请求链表 而这里的 Key 可以是任何类型
    // Watchers 是一个通用的延迟请求链表 Kafka 利用它来监控保存其中的延迟请求的可完成状态
    private[this] val operations = new ConcurrentLinkedQueue[T]()

    // count the current number of watched operations. This is O(n), so use isEmpty() if possible
    def countWatched: Int = operations.size

    def isEmpty: Boolean = operations.isEmpty

    /**
     * add the element to watch
     * 将延迟请求加入到链表中
     *
     * @param t
     */
    def watch(t: T): Unit = {
      operations.add(t)
    }

    /**
     * traverse the list and try to complete some watched elements
     * 遍历整个链表 并尝试完成其中的延迟请求
     *
     * @return
     */
    def tryCompleteWatched(): Int = {
      var completed = 0

      val iter = operations.iterator()
      while (iter.hasNext) {
        val curr = iter.next()
        if (curr.isCompleted) {
          // another thread has completed this operation, just remove it
          iter.remove()
        } else if (curr.maybeTryComplete()) {
          iter.remove()
          completed += 1
        }
      }

      if (operations.isEmpty)
        removeKeyIfEmpty(key, this)

      completed
    }

    /**
     * 遍历链表 再取消掉里面的延迟请求
     *
     * @return
     */
    def cancel(): List[T] = {
      val iter = operations.iterator()
      val cancelled = new ListBuffer[T]()
      while (iter.hasNext) {
        val curr = iter.next()
        curr.cancel()
        iter.remove()
        cancelled += curr
      }
      cancelled.toList
    }

    // traverse the list and purge elements that are already completed by others
    def purgeCompleted(): Int = {
      var purged = 0

      val iter = operations.iterator()
      while (iter.hasNext) {
        val curr = iter.next()
        if (curr.isCompleted) {
          iter.remove()
          purged += 1
        }
      }

      if (operations.isEmpty)
        removeKeyIfEmpty(key, this)

      purged
    }
  }

  def advanceClock(timeoutMs: Long): Unit = {
    timeoutTimer.advanceClock(timeoutMs)

    // Trigger a purge if the number of completed but still being watched operations is larger than
    // the purge threshold. That number is computed by the difference btw the estimated total number of
    // operations and the number of pending delayed operations.
    if (estimatedTotalOperations.get - numDelayed > purgeInterval) {
      // now set estimatedTotalOperations to delayed (the number of pending operations) since we are going to
      // clean up watchers. Note that, if more operations are completed during the clean up, we may end up with
      // a little overestimated total number of operations.
      estimatedTotalOperations.getAndSet(numDelayed)
      debug("Begin purging watch lists")
      val purged = watcherLists.foldLeft(0) {
        case (sum, watcherList) => sum + watcherList.allWatchers.map(_.purgeCompleted()).sum
      }
      debug("Purged %d elements from watch lists.".format(purged))
    }
  }

  /**
   * A background reaper to expire delayed operations that have timed out
   */
  private class ExpiredOperationReaper extends ShutdownableThread(
    "ExpirationReaper-%d-%s".format(brokerId, purgatoryName),
    false) {

    override def doWork(): Unit = {
      advanceClock(200L)
    }
  }

}
