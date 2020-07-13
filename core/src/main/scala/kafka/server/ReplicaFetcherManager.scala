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

import kafka.cluster.BrokerEndPoint
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.Time

class ReplicaFetcherManager(brokerConfig: KafkaConfig,
                            protected val replicaManager: ReplicaManager,
                            metrics: Metrics,
                            time: Time,
                            threadNamePrefix: Option[String] = None,
                            quotaManager: ReplicationQuotaManager)
  extends AbstractFetcherManager[ReplicaFetcherThread](
    name = "ReplicaFetcherManager on broker " + brokerConfig.brokerId,
    clientId = "Replica",
    numFetchers = brokerConfig.numReplicaFetchers) {

  /**
   * 创建 ReplicaFetcherThread 实例 供 Follower 副本使用
   * 线程的名字是根据 fetcherId 和 Broker ID 来确定的
   * ReplicaManager 类利用 replicaFetcherManager 字段 对所有 Fetcher 线程进行管理 包括线程的创建 | 启动 | 添加 | 停止和移除
   *
   * @param fetcherId
   * @param sourceBroker
   * @return
   */
  override def createFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint): ReplicaFetcherThread = {
    val prefix = threadNamePrefix.map(tp => s"$tp:").getOrElse("")
    val threadName = s"${prefix}ReplicaFetcherThread-$fetcherId-${sourceBroker.id}"
    // 创建ReplicaFetcherThread线程实例并返回
    new ReplicaFetcherThread(threadName, fetcherId, sourceBroker, brokerConfig, failedPartitions, replicaManager,
      metrics, time, quotaManager)
  }

  def shutdown(): Unit = {
    info("shutting down")
    closeAllFetchers()
    info("shutdown completed")
  }
}
