/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KafkaSplitTask;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.SplitLogCounters;
import org.apache.hadoop.hbase.SplitLogTask;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.ConnectionUtils;
import org.apache.hadoop.hbase.coordination.ZKSplitLogManagerCoordination.TaskFinisher;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.shaded.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.KafkaUtil;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.zookeeper.KeeperException;

/**
 * Class use to do log replay from KAFKA
 */
@InterfaceAudience.Private
public class KafkaRecoveryManager extends LogRecoveryManager {
  private static final Log LOG = LogFactory.getLog(KafkaRecoveryManager.class);
  public static final String RECOVERY_MODE = "KAFKA";
  public static final String KAFKA_LOGSPLIT_TIME_DIFF = "hbase.logsplit.kafka.timebefore";
  public static final long KAFKA_LOGSPLIT_TIME_DIFF_DEFAULT = 600 * 1000;
  public static final String KAFKA_LOGSPLIT_MAX_RETRY = "hbase.logsplit.kafka.retries";
  public static final int KAFKA_LOGSPLIT_MAX_RETRY_DEFAULT = 35;
  private final String kafkaServers;
  private KafkaConsumer<byte[],byte[]> TEST_CONSUMER;
  private AdminClient TEST_ADMIN;
  private final MasterServices masterService;
  private final long timeDiff;
  private final int maxRetry;

  public KafkaRecoveryManager(HMaster master) throws IOException {
    this(master, master, false);
  }

  @VisibleForTesting
  public KafkaRecoveryManager(Server server, MasterServices masterService, boolean initCoordination) throws IOException {
    super(server, server.getConfiguration(), server, masterService, server.getServerName(), initCoordination);
    this.kafkaServers = server.getConfiguration().get(KafkaUtil.KAFKA_BROKER_SERVERS);
    this.masterService = masterService;
    timeDiff = server.getConfiguration().getLong(KAFKA_LOGSPLIT_TIME_DIFF, KAFKA_LOGSPLIT_TIME_DIFF_DEFAULT);
    maxRetry = server.getConfiguration().getInt(KAFKA_LOGSPLIT_MAX_RETRY, KAFKA_LOGSPLIT_MAX_RETRY_DEFAULT);
  }

  public void splitLogs(ServerName serverName, Set<HRegionInfo> regions, ServerManager sm)
      throws IOException {
    if (regions == null || regions.size() == 0) {
      LOG.info("No need to do Region recovery, since there is no regions.");
      return;
    }
    int regionSize = regions.size();
    MonitoredTask status = TaskMonitor.get().createStatus("Doing Region recovery with " +
        regionSize + " regions on Server " + serverName);
    SplitLogCounters.tot_mgr_log_split_batch_start.incrementAndGet();
    LOG.info("Started recovering " + regionSize + " regions in " + serverName);
    long time = EnvironmentEdgeManager.currentTime();

    TaskBatch batch = new TaskBatch();
    int retry = 0;
    boolean genSplitTaskSuccess = false;
    Map<TopicPartition, Long> lastOffsets = new HashMap<>();
    while (retry < maxRetry) {
      try (KafkaConsumer<byte[], byte[]> consumer = getKafkaConsumer();
          AdminClient admin = getKafkaAdminClient()) {
        int totalRecords = 0;
        StringBuilder partitions, startOffsets, endOffsets;
        for (HRegionInfo region : regions) { // TODO adjust task generation granularity
          String table = region.getTable().getNameAsString();
          String kafkaTopic = KafkaUtil.getTableTopic(table);
          String consumerGroup = KafkaUtil.getConsumerGroup(table);
          // iterator all region partition mappings
          partitions = new StringBuilder();
          startOffsets = new StringBuilder();
          endOffsets = new StringBuilder();
          boolean first = true;
          Map<Integer, Long> regionPartitionMapping = getRegionPartitionMappings(consumer, sm, region);
          if (LOG.isDebugEnabled()) {
            StringBuilder info = new StringBuilder("region partition mapping info with ")
                .append(region.getEncodedName()).append(", ");
            for (Map.Entry<Integer, Long> offsetInfo : regionPartitionMapping.entrySet()) {
              info.append(offsetInfo.getKey()).append("->").append(offsetInfo.getValue()).append(";");
            }
            LOG.debug(info.toString());
          }
          for (Map.Entry<Integer, Long> offsetInfo : regionPartitionMapping.entrySet()) {
            TopicPartition topicPartition = new TopicPartition(kafkaTopic, offsetInfo.getKey());
            if (lastOffsets.get(topicPartition) == null) {
              long lastOffset = HConstants.NO_SEQNUM;
              try {
                lastOffset = getConsumerLastOffset(admin, topicPartition, consumerGroup);
              } catch (Throwable e) {
                LOG.warn("error occured when get consumer's last offset." , e);
                // fall back to partition's last offset
                consumer.assign(Arrays.asList(topicPartition));
                consumer.seekToEnd(Collections.singletonList(topicPartition));
                lastOffset = consumer.position(topicPartition) -1;
              }
              lastOffsets.put(topicPartition, lastOffset);
            }
            // kafka partition info
            if (!first) {
            	partitions.append(KafkaSplitTask.FIELD_INNER_SPLITER);
            	startOffsets.append(KafkaSplitTask.FIELD_INNER_SPLITER);
            	endOffsets.append(KafkaSplitTask.FIELD_INNER_SPLITER);
            }
            partitions.append(offsetInfo.getKey());
            startOffsets.append(offsetInfo.getValue());
            endOffsets.append(lastOffsets.get(topicPartition));
            totalRecords += (lastOffsets.get(topicPartition) - offsetInfo.getValue());
            first = false;
          }
          if (partitions.length() > 0) {
            // KAFKASPLITTASK[len]topic[len]partitions[len]startOffsets[len]endOffset[len]regionName[len]startKeyEndkey
            String startKey = Bytes.toString(region.getStartKey());
            String endKey = Bytes.toString(region.getEndKey());
            startKey = "".equals(startKey) ? "null" : startKey;
            endKey = "".equals(endKey) ? "null" : endKey;
            String taskName = new KafkaSplitTask(kafkaTopic, partitions.toString(), startOffsets.toString(),
                endOffsets.toString(), region.getEncodedName(), startKey, endKey).getTaskName();
            if (enqueueSplitTask(taskName.toString(), batch)) {
              LOG.info("enqueued split task : " + taskName);
            }
          }
        }
        LOG.info("total replay kafka records : " + totalRecords);
        genSplitTaskSuccess = true;
        break;
      } catch (Throwable t) {
        LOG.warn("failed when generate log split task, retry num = " + retry, t);
        retry++;
        try {
          Thread.sleep(ConnectionUtils.getPauseTime(100, retry));
        } catch (InterruptedException e) {
          LOG.error("Giving up generate log split task", e);
        }
      }
    }
    if (!genSplitTaskSuccess) {
      throw new IOException("failed when generate log split task.");
    }
    waitForSplittingCompletion(batch, status);

    if (batch.done != batch.installed) {
      batch.isDead = true;
      SplitLogCounters.tot_mgr_log_split_batch_err.incrementAndGet();
      LOG.warn("error while doing kafka log recovery, installed = " + batch.installed
          + " but only " + batch.done + " done");
      String msg = "error or interrupted while doing kafka log recovery, Task = " + batch;
      status.abort(msg);
      throw new IOException(msg);
    }
    SplitLogCounters.tot_mgr_log_split_batch_success.addAndGet(regionSize);
    String msg = "finished kafka recovering with " + regionSize + " regions in "
        + (EnvironmentEdgeManager.currentTime() - time) + "ms";
    status.markComplete(msg);
    LOG.info(msg);
  }

  private void deleteVoliatePartition(String taskName) {
    if (masterService.getZooKeeper() != null) {
      try {
        KafkaSplitTask task = KafkaSplitTask.parseFrom(taskName);
        RegionStates regionStates = masterService.getAssignmentManager().getRegionStates();
        String table = regionStates.getRegionState(task.getRegionName()).getRegion().getTable().getNameAsString();
        String tableZnode = ZKUtil.joinZNode(masterService.getZooKeeper().partitionZnode, table);
        String regionZnode = ZKUtil.joinZNode(tableZnode, task.getRegionName());
        ZKUtil.deleteNodeRecursively(masterService.getZooKeeper(), regionZnode);
      } catch (Throwable e) {
        LOG.error("delete voilate partition mapping failed", e);
      }
    }
  }

  private Map<Integer, Long> getRegionPartitionMappings(KafkaConsumer<byte[], byte[]> consumer,
      ServerManager sm, HRegionInfo region) {
    Map<Integer, Long> offsets = new HashMap<>();
    // heartbeat report
    Map<Integer, Long> heartbeatInfo = sm.getLastFlushedKafkaOffsets(region.getEncodedName().getBytes());
    if (heartbeatInfo != null && !heartbeatInfo.isEmpty()) {
      if (LOG.isDebugEnabled()) {
        for (Map.Entry<Integer, Long> offsetInfo : heartbeatInfo.entrySet()) {
          LOG.debug("get region partition mapping info from heartbeat, partition is"
            + offsetInfo.getKey() + ", offset is " + offsetInfo.getValue());
        }
      }
      offsets.putAll(heartbeatInfo);
    }
    if (masterService.getZooKeeper() != null) {
      try {
        // load from persistance layer
        getMappingFromZK(consumer, offsets, masterService.getZooKeeper().offsetZnode, region, false);
        // load from volatile layer
        getMappingFromZK(consumer, offsets, masterService.getZooKeeper().partitionZnode, region, true);
      } catch (Exception e) {
        LOG.warn("obtain region partition mapping error.", e);
      }
    }
    return offsets;
  }

  private void getMappingFromZK(KafkaConsumer<byte[], byte[]> consumer, Map<Integer, Long> offsets,
      String parent, HRegionInfo region, boolean isVolatile) throws KeeperException {
    if (masterService.getZooKeeper() != null) {
      String tableZnode = ZKUtil.joinZNode(parent, region.getTable().getNameAsString());
      String regionZnode = ZKUtil.joinZNode(tableZnode, region.getEncodedName());
      List<String> partitions = ZKUtil.listChildrenNoWatch(masterService.getZooKeeper(), regionZnode);
      if (partitions != null && !partitions.isEmpty()) {
        for (String partitionStr : partitions) { // iterator each partition
          Integer partition = Integer.parseInt(partitionStr);
          String partitionZnode = ZKUtil.joinZNode(regionZnode, partitionStr);
          Long offset = Bytes.toLong(ZKUtil.getDataNoWatch(masterService.getZooKeeper(), partitionZnode, null));
          if (!isVolatile) {
            if (offsets.putIfAbsent(partition, offset) == null) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("get region partition mapping info from persistance znode, partition is"
                    + partition + ", offset is " + offset);
              }
            }
          } else if (!offsets.containsKey(partition)) {
            long timestamp = System.currentTimeMillis() - timeDiff; // 10 minutes before
            TopicPartition topicPartition = new TopicPartition(KafkaUtil.getTableTopic(
              region.getTable().getNameAsString()), partition);
            OffsetAndTimestamp offsetForTime = consumer.offsetsForTimes(
              Collections.singletonMap(topicPartition, timestamp)).get(topicPartition);
            long startOffset = (offsetForTime == null ? 0 : offsetForTime.offset());
            if (offset >= startOffset) {
              LOG.info("get region partition mapping info from volatile znode, table ="
                  + region.getTable().getNameAsString() + ", region = " + region.getEncodedName()
                  + ", partition = " + partition + ", offset = " + offset);
              offsets.putIfAbsent(partition, offset);
            }
          }
        }
      }
    }
  }

  @VisibleForTesting
  void setKafkaConsumer(KafkaConsumer<byte[], byte[]> consumer) {
    this.TEST_CONSUMER = consumer;
  }

  @VisibleForTesting
  void setKafkaAdminClient(AdminClient adminClient) {
    this.TEST_ADMIN = adminClient;
  }

  private AdminClient getKafkaAdminClient() {
    if (TEST_ADMIN != null) {
      return TEST_ADMIN;
    }
    Properties props = new Properties();
    props.setProperty("bootstrap.servers", kafkaServers);
    return AdminClient.create(props);
  }

  private KafkaConsumer<byte[], byte[]> getKafkaConsumer() {
    if (TEST_CONSUMER != null) {
      return TEST_CONSUMER;
    }
    Properties props = new Properties();
    props.setProperty("bootstrap.servers", kafkaServers);
    props.setProperty("enable.auto.commit", "false");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
    consumer.unsubscribe();
    return consumer;
  }

  private long getConsumerLastOffset(AdminClient client, TopicPartition partition, String group)
      throws Exception {
    ListConsumerGroupOffsetsOptions opts = new ListConsumerGroupOffsetsOptions()
        .topicPartitions(Collections.singletonList(partition)).timeoutMs(10000);
    Map<TopicPartition, OffsetAndMetadata> offsets = client.listConsumerGroupOffsets(group, opts)
        .partitionsToOffsetAndMetadata().get();
    return offsets.get(partition).offset() - 1;
  }

  @Override
  protected TaskFinisher getTaskFinisher() {
    return new TaskFinisher() {
      @Override
      public Status finish(ServerName workerName, String taskName) {
        LOG.info("Log recovery Task has finished, taskName is : " + taskName);
        deleteVoliatePartition(taskName);
        // do old WALs cleanup in ServerCrashProcedure
        return Status.DONE;
      }

      @Override
      public SplitLogTask.Type getTaskType() {
        return SplitLogTask.Type.KAFKA;
      }
    };
  }
}
