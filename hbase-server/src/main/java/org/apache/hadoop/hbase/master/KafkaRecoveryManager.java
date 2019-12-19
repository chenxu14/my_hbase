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
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.SplitLogCounters;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.coordination.ZKSplitLogManagerCoordination.TaskFinisher;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.shaded.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.KafkaUtil;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;

/**
 * Class use to do log replay from KAFKA
 */
@InterfaceAudience.Private
public class KafkaRecoveryManager extends LogRecoveryManager {
  private static final Log LOG = LogFactory.getLog(KafkaRecoveryManager.class);
  public static final String RECOVERY_MODE = "KAFKA";
  private final String kafkaServers;
  private KafkaConsumer<byte[],byte[]> TEST_CONSUMER;
  private final long regionFlushInterval;

  public KafkaRecoveryManager(HMaster master) throws IOException {
    this(master, master);
  }

  @VisibleForTesting
  public KafkaRecoveryManager(Server server, MasterServices masterService) throws IOException {
    super(server, server.getConfiguration(), server, masterService, server.getServerName());
    this.kafkaServers = server.getConfiguration().get(KafkaUtil.KAFKA_BROKER_SERVERS);
    this.regionFlushInterval = server.getConfiguration().getLong(
        HRegion.MEMSTORE_PERIODIC_FLUSH_INTERVAL, HRegion.DEFAULT_CACHE_FLUSH_INTERVAL);
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
    long t = EnvironmentEdgeManager.currentTime();
    TaskBatch batch = new TaskBatch();

    try (KafkaConsumer<byte[], byte[]> consumer = getKafkaConsumer()) {
      Map<String, Integer> tablePartitions = new HashMap<>();
      Map<TopicPartition, Long> lastOffsets = new HashMap<>();
      for (HRegionInfo region : regions) {
        String table = region.getTable().getNameAsString();
        String kafkaTopic = KafkaUtil.getTableTopic(table);
        if (tablePartitions.get(table) == null) {
          tablePartitions.put(table, consumer.partitionsFor(kafkaTopic).size());
        }
        int kafkaPartition = KafkaUtil.getTablePartition(Bytes.toString(region.getStartKey()),
            tablePartitions.get(table));
        TopicPartition topicPartition = new TopicPartition(kafkaTopic, kafkaPartition);
        consumer.assign(Arrays.asList(topicPartition));
        if (lastOffsets.get(topicPartition) == null) {
          consumer.seekToEnd(Collections.singletonList(topicPartition));
          lastOffsets.put(topicPartition, consumer.position(topicPartition));
        }
        long startOffset = sm.getLastFlushedSequenceId(region.getEncodedNameAsBytes()).getLastFlushedSequenceId();
        if (startOffset == -1) {
          long timestamp = System.currentTimeMillis() - regionFlushInterval;
          LOG.warn(region.getRegionName() + "'s target start offset is -1, seek with timestamp " + timestamp);
          OffsetAndTimestamp offset = consumer.offsetsForTimes(
              Collections.singletonMap(topicPartition, timestamp)).get(topicPartition);
          startOffset = (offset == null ? 0 : offset.offset());
        }
        String startKey = Bytes.toString(region.getStartKey());
        String endKey = Bytes.toString(region.getEndKey());
        // taskName like : topic_partition_startOffset_endOffset_regionName-startKey-endKey
        StringBuilder taskName = new StringBuilder(kafkaTopic).append("_").append(kafkaPartition).append("_")
            .append(startOffset)
            .append("_").append(lastOffsets.get(topicPartition))
            .append("_").append(region.getEncodedName()).append("-")
            .append("".equals(startKey) ? "null" : startKey).append("-")
            .append("".equals(endKey) ? "null" : endKey);
        if (!enqueueSplitTask(taskName.toString(), batch)) {
          throw new IOException("duplicate log split scheduled for " + taskName);
        }
        LOG.info("enqueued split task : " + taskName);
      }
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
        + (EnvironmentEdgeManager.currentTime() - t) + "ms";
    status.markComplete(msg);
    LOG.info(msg);
  }

  void setKafkaConsumer(KafkaConsumer<byte[], byte[]> consumer) {
    this.TEST_CONSUMER = consumer;
  }

  KafkaConsumer<byte[], byte[]> getKafkaConsumer() {
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

  @Override
  protected TaskFinisher getTaskFinisher() {
    return new TaskFinisher() {
      @Override
      public Status finish(ServerName workerName, String taskName) {
        LOG.info("Log recovery Task has finished, taskName is : " + taskName);
        // TODO do some cleanup
        return Status.DONE;
      }
    };
  }
}
