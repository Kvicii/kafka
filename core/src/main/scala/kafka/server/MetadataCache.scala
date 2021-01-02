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

import java.util
import java.util.Collections
import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.{Seq, Set, mutable}
import scala.jdk.CollectionConverters._
import kafka.cluster.{Broker, EndPoint}
import kafka.api._
import kafka.controller.StateChangeLogger
import kafka.utils.CoreUtils._
import kafka.utils.Logging
import kafka.utils.Implicits._
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataPartitionState
import org.apache.kafka.common.{Cluster, Node, PartitionInfo, TopicPartition, Uuid}
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponsePartition
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{MetadataResponse, UpdateMetadataRequest}
import org.apache.kafka.common.security.auth.SecurityProtocol

/**
 * A cache for the state (e.g., current leader) of each partition. This cache is updated through
 * UpdateMetadataRequest from the controller. Every broker maintains the same cache, asynchronously.
 *
 * 作为集群元数据集散地 保存了集群中关于Topic和 Broker 的所有重要数据
 * Broker 并非是无状态的节点 它需要从 Controller 端异步更新保存集群的元数据信息
 * 由于 Kafka 采用的是 Leader/Follower 模式 与多 Leader 架构和无 Leader 架构相比: 这种分布式架构的一致性是最容易保证的 因此Broker 间元数据的最终一致性是有保证的
 * 但是需要处理 Follower 滞后或数据过期的问题 需要注意的是 这里的 Leader 其实是指 Controller 而 Follower 是指普通的 Broker 节点
 *
 * @param brokerId
 */
class MetadataCache(brokerId: Int) extends Logging {

  // 保护它写入的锁对象
  private val partitionMetadataLock = new ReentrantReadWriteLock()
  //this is the cache state. every MetadataSnapshot instance is immutable, and updates (performed under a lock)
  //replace the value with a completely new one. this means reads (which are not under any lock) need to grab
  //the value of this var (into a val) ONCE and retain that read copy for the duration of their operation.
  //multiple reads of this value risk getting different snapshots.
  // 保存了实际的元数据信息 它是 MetadataCache 类中最重要的字段
  @volatile private var metadataSnapshot: MetadataSnapshot = MetadataSnapshot(partitionStates = mutable.AnyRefMap.empty,
  // 仅仅用于日志输出
    topicIds = Map.empty, controllerId = None, aliveBrokers = mutable.LongMap.empty, aliveNodes = mutable.LongMap.empty)

  this.logIdent = s"[MetadataCache brokerId=$brokerId] "
  // 仅仅用于日志输出
  private val stateChangeLogger = new StateChangeLogger(brokerId, inControllerContext = false, None)

  // This method is the main hotspot when it comes to the performance of metadata requests,
  // we should be careful about adding additional logic here. Relatedly, `brokers` is
  // `List[Integer]` instead of `List[Int]` to avoid a collection copy.
  // filterUnavailableEndpoints exists to support v0 MetadataResponses
  private def maybeFilterAliveReplicas(snapshot: MetadataSnapshot,
                                       brokers: java.util.List[Integer],
                                       listenerName: ListenerName,
                                       filterUnavailableEndpoints: Boolean): java.util.List[Integer] = {
    if (!filterUnavailableEndpoints) {
      brokers
    } else {
      val res = new util.ArrayList[Integer](math.min(snapshot.aliveBrokers.size, brokers.size))
      for (brokerId <- brokers.asScala) {
        if (hasAliveEndpoint(snapshot, brokerId, listenerName))
          res.add(brokerId)
      }
      res
    }
  }

  // errorUnavailableEndpoints exists to support v0 MetadataResponses
  // If errorUnavailableListeners=true, return LISTENER_NOT_FOUND if listener is missing on the broker.
  // Otherwise, return LEADER_NOT_AVAILABLE for broker unavailable and missing listener (Metadata response v5 and below).
  private def getPartitionMetadata(snapshot: MetadataSnapshot, topic: String, listenerName: ListenerName, errorUnavailableEndpoints: Boolean,
                                   errorUnavailableListeners: Boolean): Option[Iterable[MetadataResponsePartition]] = {
    snapshot.partitionStates.get(topic).map { partitions =>
      partitions.map { case (partitionId, partitionState) =>
        val topicPartition = new TopicPartition(topic, partitionId.toInt)
        val leaderBrokerId = partitionState.leader
        val leaderEpoch = partitionState.leaderEpoch
        val maybeLeader = getAliveEndpoint(snapshot, leaderBrokerId, listenerName)

        val replicas = partitionState.replicas
        val filteredReplicas = maybeFilterAliveReplicas(snapshot, replicas, listenerName, errorUnavailableEndpoints)

        val isr = partitionState.isr
        val filteredIsr = maybeFilterAliveReplicas(snapshot, isr, listenerName, errorUnavailableEndpoints)

        val offlineReplicas = partitionState.offlineReplicas

        maybeLeader match {
          case None =>
            val error = if (!snapshot.aliveBrokers.contains(leaderBrokerId)) { // we are already holding the read lock
              debug(s"Error while fetching metadata for $topicPartition: leader not available")
              Errors.LEADER_NOT_AVAILABLE
            } else {
              debug(s"Error while fetching metadata for $topicPartition: listener $listenerName " +
                s"not found on leader $leaderBrokerId")
              if (errorUnavailableListeners) Errors.LISTENER_NOT_FOUND else Errors.LEADER_NOT_AVAILABLE
            }

            new MetadataResponsePartition()
              .setErrorCode(error.code)
              .setPartitionIndex(partitionId.toInt)
              .setLeaderId(MetadataResponse.NO_LEADER_ID)
              .setLeaderEpoch(leaderEpoch)
              .setReplicaNodes(filteredReplicas)
              .setIsrNodes(filteredIsr)
              .setOfflineReplicas(offlineReplicas)

          case Some(_) =>
            val error = if (filteredReplicas.size < replicas.size) {
              debug(s"Error while fetching metadata for $topicPartition: replica information not available for " +
                s"following brokers ${replicas.asScala.filterNot(filteredReplicas.contains).mkString(",")}")
              Errors.REPLICA_NOT_AVAILABLE
            } else if (filteredIsr.size < isr.size) {
              debug(s"Error while fetching metadata for $topicPartition: in sync replica information not available for " +
                s"following brokers ${isr.asScala.filterNot(filteredIsr.contains).mkString(",")}")
              Errors.REPLICA_NOT_AVAILABLE
            } else {
              Errors.NONE
            }

            new MetadataResponsePartition()
              .setErrorCode(error.code)
              .setPartitionIndex(partitionId.toInt)
              .setLeaderId(maybeLeader.map(_.id()).getOrElse(MetadataResponse.NO_LEADER_ID))
              .setLeaderEpoch(leaderEpoch)
              .setReplicaNodes(filteredReplicas)
              .setIsrNodes(filteredIsr)
              .setOfflineReplicas(offlineReplicas)
        }
      }
    }
  }

  /**
   * Check whether a broker is alive and has a registered listener matching the provided name.
   * This method was added to avoid unnecessary allocations in [[maybeFilterAliveReplicas]], which is
   * a hotspot in metadata handling.
   */
  private def hasAliveEndpoint(snapshot: MetadataSnapshot, brokerId: Int, listenerName: ListenerName): Boolean = {
    snapshot.aliveNodes.get(brokerId).exists(_.contains(listenerName))
  }

  /**
   * Get the endpoint matching the provided listener if the broker is alive. Note that listeners can
   * be added dynamically, so a broker with a missing listener could be a transient error.
   *
   * @return None if broker is not alive or if the broker does not have a listener named `listenerName`.
   */
  private def getAliveEndpoint(snapshot: MetadataSnapshot, brokerId: Int, listenerName: ListenerName): Option[Node] = {
    snapshot.aliveNodes.get(brokerId).flatMap(_.get(listenerName))
  }

  // errorUnavailableEndpoints exists to support v0 MetadataResponses
  def getTopicMetadata(topics: Set[String],
                       listenerName: ListenerName,
                       errorUnavailableEndpoints: Boolean = false,
                       errorUnavailableListeners: Boolean = false): Seq[MetadataResponseTopic] = {
    val snapshot = metadataSnapshot
    topics.toSeq.flatMap { topic =>
      getPartitionMetadata(snapshot, topic, listenerName, errorUnavailableEndpoints, errorUnavailableListeners).map { partitionMetadata =>
        new MetadataResponseTopic()
          .setErrorCode(Errors.NONE.code)
          .setName(topic)
          .setTopicId(snapshot.topicIds.getOrElse(topic, Uuid.ZERO_UUID))
          .setIsInternal(Topic.isInternal(topic))
          .setPartitions(partitionMetadata.toBuffer.asJava)
      }
    }
  }

  def getAllTopics(): Set[String] = {
    getAllTopics(metadataSnapshot)
  }

  /**
   * 获取元数据缓存中的分区对象
   *
   * @return
   */
  def getAllPartitions(): Set[TopicPartition] = {
    // 遍历 partitionStates 取出分区号后构建 TopicPartition 实例 并加入到返回集合中返回
    metadataSnapshot.partitionStates.flatMap { case (topicName, partitionsAndStates) =>
      partitionsAndStates.keys.map(partitionId => new TopicPartition(topicName, partitionId.toInt))
    }.toSet
  }

  /**
   * 返回当前集群元数据缓存中的所有Topic
   *
   * @param snapshot
   * @return
   */
  private def getAllTopics(snapshot: MetadataSnapshot): Set[String] = {
    // 仅仅是返回 MetadataSnapshot 数据类型中 partitionStates 字段的所有 Key 字段
    snapshot.partitionStates.keySet
  }

  private def getAllPartitions(snapshot: MetadataSnapshot): Map[TopicPartition, UpdateMetadataPartitionState] = {
    snapshot.partitionStates.flatMap { case (topic, partitionStates) =>
      partitionStates.map { case (partition, state) => (new TopicPartition(topic, partition.toInt), state) }
    }.toMap
  }

  def getNonExistingTopics(topics: Set[String]): Set[String] = {
    topics.diff(metadataSnapshot.partitionStates.keySet)
  }

  def getAliveBroker(brokerId: Int): Option[Broker] = {
    metadataSnapshot.aliveBrokers.get(brokerId)
  }

  def getAliveBrokers: Seq[Broker] = {
    metadataSnapshot.aliveBrokers.values.toBuffer
  }

  private def addOrUpdatePartitionInfo(partitionStates: mutable.AnyRefMap[String, mutable.LongMap[UpdateMetadataPartitionState]],
                                       topic: String,
                                       partitionId: Int,
                                       stateInfo: UpdateMetadataPartitionState): Unit = {
    val infos = partitionStates.getOrElseUpdate(topic, mutable.LongMap.empty)
    infos(partitionId) = stateInfo
  }

  def getPartitionInfo(topic: String, partitionId: Int): Option[UpdateMetadataPartitionState] = {
    metadataSnapshot.partitionStates.get(topic).flatMap(_.get(partitionId))
  }

  def numPartitions(topic: String): Option[Int] = {
    metadataSnapshot.partitionStates.get(topic).map(_.size)
  }

  // if the leader is not known, return None;
  // if the leader is known and corresponding node is available, return Some(node)
  // if the leader is known but corresponding node with the listener name is not available, return Some(NO_NODE)
  def getPartitionLeaderEndpoint(topic: String, partitionId: Int, listenerName: ListenerName): Option[Node] = {
    val snapshot = metadataSnapshot
    snapshot.partitionStates.get(topic).flatMap(_.get(partitionId)) map { partitionInfo =>
      val leaderId = partitionInfo.leader

      snapshot.aliveNodes.get(leaderId) match {
        case Some(nodeMap) =>
          nodeMap.getOrElse(listenerName, Node.noNode)
        case None =>
          Node.noNode
      }
    }
  }

  /**
   * 获取指定监听器类型下该Topic分区所有副本的 Broker 节点对象 并按照 Broker ID 进行分组
   *
   * @param tp
   * @param listenerName
   * @return
   */
  def getPartitionReplicaEndpoints(tp: TopicPartition, listenerName: ListenerName): Map[Int, Node] = {
    // 使用局部变量获取当前元数据缓存 这样做的好处在于不需要使用锁技术
    // 这里有一个可能的问题是读到的数据可能是过期的数据 Kafka
    // 能够自行处理过期元数据的问题 当客户端因为拿到过期元数据而向 Broker 发出错误的指令时 Broker 会显式地通知客户端错误原因 客户端接收到错误后 会尝试再次拉取最新的元数据
    // 这个过程能够保证 客户端最终可以取得最新的元数据信息 过期元数据的不良影响是存在的 但在实际场景中并不是太严重
    val snapshot = metadataSnapshot
    // 获取给定Topic分区的数据
    snapshot.partitionStates.get(tp.topic).flatMap(_.get(tp.partition)).map { partitionInfo =>
      val replicaIds = partitionInfo.replicas // 拿到副本Id列表
      replicaIds.asScala
        .map(replicaId => replicaId.intValue() -> {
          // 获取副本所在的Broker Id
          snapshot.aliveBrokers.get(replicaId.longValue()) match {
            case Some(broker) =>
              // 根据Broker Id去获取对应的Broker节点对象
              broker.getNode(listenerName).getOrElse(Node.noNode())
            case None => // 如果找不到节点
              Node.noNode()
          }
        }).toMap
        .filter(pair => pair match {
          case (_, node) => !node.isEmpty
        })
    }.getOrElse(Map.empty[Int, Node])
  }

  def getControllerId: Option[Int] = metadataSnapshot.controllerId

  def getClusterMetadata(clusterId: String, listenerName: ListenerName): Cluster = {
    val snapshot = metadataSnapshot
    val nodes = snapshot.aliveNodes.map { case (id, nodes) => (id, nodes.get(listenerName).orNull) }

    def node(id: Integer): Node = nodes.get(id.toLong).orNull

    val partitions = getAllPartitions(snapshot)
      .filter { case (_, state) => state.leader != LeaderAndIsr.LeaderDuringDelete }
      .map { case (tp, state) =>
        new PartitionInfo(tp.topic, tp.partition, node(state.leader),
          state.replicas.asScala.map(node).toArray,
          state.isr.asScala.map(node).toArray,
          state.offlineReplicas.asScala.map(node).toArray)
      }
    val unauthorizedTopics = Collections.emptySet[String]
    val internalTopics = getAllTopics(snapshot).filter(Topic.isInternal).asJava
    new Cluster(clusterId, nodes.values.filter(_ != null).toBuffer.asJava,
      partitions.toBuffer.asJava,
      unauthorizedTopics, internalTopics,
      snapshot.controllerId.map(id => node(id)).orNull)
  }

  /**
   * This method returns the deleted TopicPartitions received from UpdateMetadataRequest
   *
   * Controller 给 Broker 发送 UpdateMetadataRequest 请求时 触发更新
   * 读取 UpdateMetadataRequest 请求中的分区数据 然后更新本地元数据缓存
   *
   * @param correlationId
   * @param updateMetadataRequest
   * @return
   */
  def updateMetadata(correlationId: Int, updateMetadataRequest: UpdateMetadataRequest): Seq[TopicPartition] = {
    inWriteLock(partitionMetadataLock) {
      // 第一部分代码的主要作用是给后面的操作准备数据(即 aliveBrokers 和 aliveNodes 两个字段中保存的数据)
      // 保存存活Broker对象 Key是Broker ID Value是Broker对象
      val aliveBrokers = new mutable.LongMap[Broker](metadataSnapshot.aliveBrokers.size)
      // 保存存活节点对象 Key是Broker ID Value是监听器->节点对象
      val aliveNodes = new mutable.LongMap[collection.Map[ListenerName, Node]](metadataSnapshot.aliveNodes.size)
      // 从UpdateMetadataRequest中获取Controller所在的Broker ID
      // 如果当前没有Controller 赋值为None
      val controllerIdOpt = updateMetadataRequest.controllerId match {
        case id if id < 0 => None
        case id => Some(id)
      }
      // 遍历UpdateMetadataRequest请求中的所有存活Broker对象
      updateMetadataRequest.liveBrokers.forEach { broker =>
        // `aliveNodes` is a hot path for metadata requests for large clusters, so we use java.util.HashMap which
        // is a bit faster than scala.collection.mutable.HashMap. When we drop support for Scala 2.10, we could
        // move to `AnyRefMap`, which has comparable performance.
        val nodes = new java.util.HashMap[ListenerName, Node]
        val endPoints = new mutable.ArrayBuffer[EndPoint]
        // 遍历它的所有EndPoint类型 也就是为Broker配置的监听器
        broker.endpoints.forEach { ep =>
          val listenerName = new ListenerName(ep.listener)
          endPoints += new EndPoint(ep.host, ep.port, listenerName, SecurityProtocol.forId(ep.securityProtocol))
          // 将<监听器, Broker节点对象>对保存起来
          nodes.put(listenerName, new Node(broker.id, ep.host, ep.port))
        }
        // 将Broker加入到存活Broker对象集合
        aliveBrokers(broker.id) = Broker(broker.id, endPoints, Option(broker.rack))
        // 将Broker节点加入到存活节点对象集合
        aliveNodes(broker.id) = nodes.asScala
      }
      // 第二部分代码主要工作是确保集群 Broker 配置了相同的监听器 同时初始化已删除分区数组对象 等待下一部分代码逻辑对它进行操作
      // 使用上一部分中的存活Broker节点对象
      // 获取当前Broker所有的<监听器, 节点>对
      aliveNodes.get(brokerId).foreach { listenerMap =>
        val listeners = listenerMap.keySet
        if (!aliveNodes.values.forall(_.keySet == listeners)) { // 如果发现当前Broker配置的监听器与其他Broker有不同之处 记录错误日志
          error(s"Listeners are not identical across brokers: $aliveNodes")
        }
      }

//       构造已删除分区数组 将其作为方法返回结果
//      val deletedPartitions = new mutable.ArrayBuffer[TopicPartition]
//      if (!updateMetadataRequest.partitionStates.iterator.hasNext) { // UpdateMetadataRequest请求没有携带任何分区信息
//         构造新的MetadataSnapshot对象 使用之前的分区信息和新的Broker列表信息
//        metadataSnapshot = MetadataSnapshot(metadataSnapshot.partitionStates, controllerIdOpt, aliveBrokers, aliveNodes)
//      } else { // 否则进入到方法最后一部分  主要工作是提取 UpdateMetadataRequest 请求中的数据 然后填充元数据缓存
      val newTopicIds = updateMetadataRequest.topicStates().asScala
        .map(topicState => (topicState.topicName(), topicState.topicId()))
        .filter(_._2 != Uuid.ZERO_UUID).toMap
      val topicIds = mutable.Map.empty[String, Uuid]
      topicIds ++= metadataSnapshot.topicIds
      topicIds ++= newTopicIds

      val deletedPartitions = new mutable.ArrayBuffer[TopicPartition]
      if (!updateMetadataRequest.partitionStates.iterator.hasNext) {
        metadataSnapshot = MetadataSnapshot(metadataSnapshot.partitionStates, topicIds.toMap, controllerIdOpt, aliveBrokers, aliveNodes)
      } else {
        //since kafka may do partial metadata updates, we start by copying the previous state
        val partitionStates = new mutable.AnyRefMap[String, mutable.LongMap[UpdateMetadataPartitionState]](metadataSnapshot.partitionStates.size)
        // 备份现有元数据缓存中的分区数据
        metadataSnapshot.partitionStates.forKeyValue { (topic, oldPartitionStates) =>
          val copy = new mutable.LongMap[UpdateMetadataPartitionState](oldPartitionStates.size)
          copy ++= oldPartitionStates
          partitionStates(topic) = copy
        }

        val traceEnabled = stateChangeLogger.isTraceEnabled
        val controllerId = updateMetadataRequest.controllerId
        val controllerEpoch = updateMetadataRequest.controllerEpoch
        // 获取UpdateMetadataRequest请求中携带的所有分区数据
        val newStates = updateMetadataRequest.partitionStates.asScala
        // 遍历分区数据
        newStates.foreach { state =>
          // per-partition logging here can be very expensive due going through all partitions in the cluster
          val tp = new TopicPartition(state.topicName, state.partitionIndex)
          if (state.leader == LeaderAndIsr.LeaderDuringDelete) {  // 如果分区处于被删除过程中
            removePartitionInfo(partitionStates, topicIds, tp.topic, tp.partition)  // 将分区从元数据缓存中移除
            if (traceEnabled)
              stateChangeLogger.trace(s"Deleted partition $tp from metadata cache in response to UpdateMetadata " +
                s"request sent by controller $controllerId epoch $controllerEpoch with correlation id $correlationId")
            deletedPartitions += tp // 将分区加入到返回结果数据
          } else { // 将分区加入到元数据缓存
            addOrUpdatePartitionInfo(partitionStates, tp.topic, tp.partition, state)
            if (traceEnabled)
              stateChangeLogger.trace(s"Cached leader info $state for partition $tp in response to " +
                s"UpdateMetadata request sent by controller $controllerId epoch $controllerEpoch with correlation id $correlationId")
          }
        }
        val cachedPartitionsCount = newStates.size - deletedPartitions.size
        stateChangeLogger.info(s"Add $cachedPartitionsCount partitions and deleted ${deletedPartitions.size} partitions from metadata cache " +
          s"in response to UpdateMetadata request sent by controller $controllerId epoch $controllerEpoch with correlation id $correlationId")
        // 使用更新过的分区元数据和第一部分计算的存活Broker列表及节点列表 构建最新的元数据缓存
        metadataSnapshot = MetadataSnapshot(partitionStates, topicIds.toMap, controllerIdOpt, aliveBrokers, aliveNodes)
      }
      deletedPartitions // 返回已删除分区列表数组
    }
  }

  /**
   * 判断给定Topic是否包含在元数据缓存中
   *
   * @param topic
   * @return
   */
  def contains(topic: String): Boolean = {
    // 只需要判断 metadataSnapshot 中 partitionStates 的所有 Key 是否包含指定Topic就行了
    metadataSnapshot.partitionStates.contains(topic)
  }

  /**
   * 首先从 metadataSnapshot 中获取指定Topic分区的分区数据信息 然后根据分区数据是否存在 来判断给定Topic分区是否包含在元数据缓存中
   *
   * @param tp
   * @return
   */
  def contains(tp: TopicPartition): Boolean = getPartitionInfo(tp.topic, tp.partition).isDefined

  private def removePartitionInfo(partitionStates: mutable.AnyRefMap[String, mutable.LongMap[UpdateMetadataPartitionState]],
                                  topicIds: mutable.Map[String, Uuid], topic: String, partitionId: Int): Boolean = {
    partitionStates.get(topic).exists { infos =>
      infos.remove(partitionId)
      if (infos.isEmpty) {
        partitionStates.remove(topic)
        topicIds.remove(topic)
      }
      true
    }
  }

  /**
   * 它是一个 case 类(相当于 Java 中配齐了 Getter 方法的 POJO 类)
   * 同时它也是一个不可变类(Immutable Class) 正因为它的不可变性 其字段值是不允许修改的 所以只能重新创建一个新的实例 来保存更新后的字段值
   *
   * @param partitionStates Map 类型 Key 是Topic名称 Value 又是一个 Map 类型(其 Key 是分区号 Value 是一个 UpdateMetadataPartitionState 类型的字段
   *                        UpdateMetadataPartitionState 类型是 UpdateMetadataRequest 请求内部所需的数据结构)
   * @param controllerId    Controller 所在 Broker 的 ID
   * @param aliveBrokers    当前集群中所有存活着的 Broker 对象列表
   * @param aliveNodes      一个 Map 的 Map 类型 其 Key 是 Broker ID 序号 Value 是 Map 类型(其 Key 是 ListenerName 即 Broker 监听器类型 而 Value 是 Broker 节点对象)
   */
  case class MetadataSnapshot(partitionStates: mutable.AnyRefMap[String, mutable.LongMap[UpdateMetadataPartitionState]],
                              topicIds: Map[String, Uuid],
                              controllerId: Option[Int],
                              aliveBrokers: mutable.LongMap[Broker],
                              aliveNodes: mutable.LongMap[collection.Map[ListenerName, Node]])

}
