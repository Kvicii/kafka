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

package kafka.coordinator.group

import java.util

import kafka.utils.nonthreadsafe

/**
 * 组成员概要数据 提取了最核心的元数据信息
 * 一个概要数据类 本质上是一个 POJO 类 仅仅承载数据 没有定义任何逻辑
 *
 * @param memberId        成员ID 由Kafka自动生成 规则是 consumer-组ID-< 序号 >-UUID
 * @param groupInstanceId Consumer端参数group.instance.id值
 *                        消费者组静态成员的 ID 静态成员机制的引入能够规避不必要的消费者组 Rebalance 操作
 * @param clientId        client.id参数值
 *                        由于 memberId 不能被设置 因此可以用这个字段来区分消费者组下的不同成员
 * @param clientHost      Consumer端程序主机名
 *                        记录了这个客户端是从哪台机器发出的消费请求
 * @param metadata        消费者组成员使用的分配策略
 *                        由消费者端参数 partition.assignment.strategy 值设定 默认的 RangeAssignor 策略是按照Topic平均分配分区
 * @param assignment      成员订阅分区
 *                        每个消费者组都要选出一个 Leader 消费者组成员 负责给所有成员分配消费方案
 *                        之后Kafka 将制定好的分配方案序列化成字节数组 赋值给 assignment 分发给各个成员
 */
case class MemberSummary(memberId: String,
                         groupInstanceId: Option[String],
                         clientId: String,
                         clientHost: String,
                         metadata: Array[Byte],
                         assignment: Array[Byte])

/**
 * 仅仅定义了一个工具方法 供上层组件调用
 */
private object MemberMetadata {
  // 从一组给定的分区分配策略详情中提取出分区分配策略的名称 并将其封装成一个集合对象返回
  // 经常被用来统计一个消费者组下的成员到底配置了多少种分区分配策略
  def plainProtocolSet(supportedProtocols: List[(String, Array[Byte])]) = supportedProtocols.map(_._1).toSet
}

/**
 * Member metadata contains the following metadata:
 * Heartbeat metadata:
 * 1. negotiated heartbeat session timeout
 * 2. timestamp of the latest heartbeat
 * Protocol metadata:
 * 1. the list of supported protocols (ordered by preference)
 * 2. the metadata associated with each protocol
 * In addition, it also contains the following state information:
 * 1. Awaiting rebalance callback: when the group is in the prepare-rebalance state,
 * its rebalance callback will be kept in the metadata if the
 * member has sent the join group request
 * 2. Awaiting sync callback: when the group is in the awaiting-sync state, its sync callback
 * is kept in metadata until the leader provides the group assignment
 * and the group transitions to stable
 *
 * 保存消费者组下成员的元数据 Kafka 为消费者组成员定义了很多数据
 *
 * @param memberId
 * @param groupId
 * @param groupInstanceId
 * @param clientId
 * @param clientHost
 * @param rebalanceTimeoutMs Rebalane操作超时时间 即一次 Rebalance 操作必须在这个时间内完成 否则被视为超时
 *                           这个字段的值是 Consumer 端参数 max.poll.interval.ms 的值
 * @param sessionTimeoutMs   会话超时时间 当前消费者组成员依靠心跳机制"保活"
 *                           如果在会话超时时间之内未能成功发送心跳 组成员就被判定成"下线" 从而触发新一轮的 Rebalance
 *                           这个字段的值是 Consumer 端参数 session.timeout.ms 的值
 * @param protocolType       协议类型
 *                           实际上标识的是消费者组被用在了哪个场景 这里的场景具体有两个:
 *                           1. 第一个是作为普通的消费者组使用 该字段对应的值就是 consumer
 *                           2. 第二个是供 Kafka Connect 组件中的消费者使用 该字段对应的值是 connect(不排除后续社区会增加新的协议类型对消费者组而言)
 * @param supportedProtocols 标识成员配置的多组分区分配策略 目前Consumer 端参数 partition.assignment.strategy 的类型是 List 说明你可以为消费者组成员设置多组分配策略
 *                           因此这个字段也是一个 List 类型 每个元素是一个元组(Tuple)。元组的第一个元素是策略名称 第二个元素是序列化后的策略详情
 */
@nonthreadsafe
private[group] class MemberMetadata(var memberId: String,
                                    val groupInstanceId: Option[String],
                                    val clientId: String,
                                    val clientHost: String,
                                    val rebalanceTimeoutMs: Int,
                                    val sessionTimeoutMs: Int,
                                    val protocolType: String,
                                    var supportedProtocols: List[(String, Array[Byte])],
                                    var assignment: Array[Byte] = Array.empty[Byte]) {

  // 保存分配给该成员的分区分配方案
  // var assignment: Array[Byte] = Array.empty[Byte]
  // 表示组成员是否正在等待加入组
  var awaitingJoinCallback: JoinGroupResult => Unit = _
  // 表示组成员是否正在等待 GroupCoordinator 发送分配方案
  var awaitingSyncCallback: SyncGroupResult => Unit = _
  // 表示是否是消费者组下的新成员
  var isNew: Boolean = false

  def isStaticMember: Boolean = groupInstanceId.isDefined

  // This variable is used to track heartbeat completion through the delayed
  // heartbeat purgatory. When scheduling a new heartbeat expiration, we set
  // this value to `false`. Upon receiving the heartbeat (or any other event
  // indicating the liveness of the client), we set it to `true` so that the
  // delayed heartbeat can be completed.
  var heartbeatSatisfied: Boolean = false

  // 组成员是否正在等待加入组
  def isAwaitingJoin: Boolean = awaitingJoinCallback != null
  // 组成员是否正在等待 GroupCoordinator 发送分配方案
  def isAwaitingSync: Boolean = awaitingSyncCallback != null

  /**
   * Get metadata corresponding to the provided protocol.
   */
  def metadata(protocol: String): Array[Byte] = {
    // 从该成员配置的分区分配方案列表中寻找给定策略的详情
    supportedProtocols.find(_._1 == protocol) match {
      case Some((_, metadata)) => metadata // 如果找到就直接返回详情字节数组数据
      case None => // 否则抛出异常
        throw new IllegalArgumentException("Member does not support protocol")
    }
  }

  def hasSatisfiedHeartbeat: Boolean = {
    if (isNew) {
      // New members can be expired while awaiting join, so we have to check this first
      heartbeatSatisfied
    } else if (isAwaitingJoin || isAwaitingSync) {
      // Members that are awaiting a rebalance automatically satisfy expected heartbeats
      true
    } else {
      // Otherwise we require the next heartbeat
      heartbeatSatisfied
    }
  }

  /**
   * Check if the provided protocol metadata matches the currently stored metadata.
   */
  def matches(protocols: List[(String, Array[Byte])]): Boolean = {
    if (protocols.size != this.supportedProtocols.size)
      return false

    for (i <- protocols.indices) {
      val p1 = protocols(i)
      val p2 = supportedProtocols(i)
      if (p1._1 != p2._1 || !util.Arrays.equals(p1._2, p2._2))
        return false
    }
    true
  }

  def summary(protocol: String): MemberSummary = {
    MemberSummary(memberId, groupInstanceId, clientId, clientHost, metadata(protocol), assignment)
  }

  def summaryNoMetadata(): MemberSummary = {
    MemberSummary(memberId, groupInstanceId, clientId, clientHost, Array.empty[Byte], Array.empty[Byte])
  }

  /**
   * Vote for one of the potential group protocols. This takes into account the protocol preference as
   * indicated by the order of supported protocols and returns the first one also contained in the set
   */
  def vote(candidates: Set[String]): String = {
    supportedProtocols.find({ case (protocol, _) => candidates.contains(protocol) }) match {
      case Some((protocol, _)) => protocol
      case None =>
        throw new IllegalArgumentException("Member does not support any of the candidate protocols")
    }
  }

  override def toString: String = {
    "MemberMetadata(" +
      s"memberId=$memberId, " +
      s"groupInstanceId=$groupInstanceId, " +
      s"clientId=$clientId, " +
      s"clientHost=$clientHost, " +
      s"sessionTimeoutMs=$sessionTimeoutMs, " +
      s"rebalanceTimeoutMs=$rebalanceTimeoutMs, " +
      s"supportedProtocols=${supportedProtocols.map(_._1)}" +
      ")"
  }
}
