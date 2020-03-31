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
package org.apache.hadoop.hbase.wal;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.URLDecoder;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.SplitLogTask;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.shaded.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hbase.shaded.com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.util.KafkaUtil;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

/**
 * Do log split based on Kaka
 */
@InterfaceAudience.Private
public class KafkaWALSplitter extends WALSplitter{
  private static final Log LOG = LogFactory.getLog(KafkaWALSplitter.class);
  private final String kafkaServers;
  private static Map<Integer, KafkaConsumer<byte[],byte[]>> TEST_CONSUMER;

  public KafkaWALSplitter(Configuration conf, FileSystem fs, WALFactory factory, Path rootDir) {
    super(conf, fs, factory, rootDir);
    this.kafkaServers = conf.get(KafkaUtil.KAFKA_BROKER_SERVERS);
  }

  public boolean splitLogFile(String taskName, CancelableProgressable reporter) throws IOException {
    Preconditions.checkState(status == null);
    int interval = conf.getInt(SPLIT_REPORT_INTERVAL, SPLIT_REPORT_INTERVAL_DEFAULT);
    boolean progress_failed = false;
    int editsCount = 0;
    int editsSkipped = 0;
    status = TaskMonitor.get().createStatus("Splitting target task " + taskName
        + "into a temporary staging area.");
    this.taskBeingSplit = taskName;
    outputSink.setReporter(reporter);
    outputSink.startWriterThreads();
    // task format : topic_partition_startOffset_endOffset_regionName-startKey-endKey
    taskName = URLDecoder.decode(taskName, "UTF-8");
    String[] taskInfo = taskName.split(SplitLogTask.TASK_FIELD_SPLITER);
    assert(taskInfo.length == SplitLogTask.KAFKA_TASK_FIELD_LEN);
    String[] partitions = taskInfo[1].split(SplitLogTask.FIELD_INNER_SPLITER);
    String[] startOffsets = taskInfo[2].split(SplitLogTask.FIELD_INNER_SPLITER);
    String[] endOffsets = taskInfo[3].split(SplitLogTask.FIELD_INNER_SPLITER);
    String[] regionInfo = taskInfo[4].split(SplitLogTask.FIELD_INNER_SPLITER); // regionName-startKey-endKey
    assert(regionInfo.length == 3);
    try {
      for (int i = 0; i < partitions.length; i++) {
        try (KafkaConsumer<byte[], byte[]> consumer = getKafkaConsumer(taskInfo[0],
            Integer.parseInt(partitions[i]), Integer.parseInt(startOffsets[i]))) {
          status.setStatus("Opening kafka consumer.");
          if (reporter != null && !reporter.progress()) {
            progress_failed = true;
            return false;
          }
          int loop = 0;
          LOOP : while (true) {
            boolean reachEnd = false;
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(10));
            if (records.isEmpty()) {
              loop++;
              if (loop > 3) {
                LOG.warn(taskName + " can't reach to the target offset " + endOffsets[i] + " after 3 times retry !");
                break LOOP;
              }
            } else {
              loop = 0; // reset loop, since we have records
              for (ConsumerRecord<byte[], byte[]> record : records) {
                if (record.offset() == Integer.parseInt(endOffsets[i])) {
                  reachEnd = true;
                }
                try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(record.value()))) {
                  WALEdit walEdit = new WALEdit();
                  walEdit.readFields(dis);
                  if (walEdit.getCells().size() > 0) {
                    Cell cell = walEdit.getCells().get(0); // With KafkaWAL each WALEntry only corresponds to one record
                    String rowkey = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                    if (!isRowkeyInRange(rowkey, regionInfo[1], regionInfo[2])) {
                      if (LOG.isDebugEnabled()) {
                        LOG.debug("Ignored rowkey:" + rowkey + ", startKey : " + regionInfo[1]
                            + ", endKey : " + regionInfo[2]);
                      }
                      editsSkipped++; // record not belong to this region
                      continue;
                    }
                    WALKey key = new WALKey(Bytes.toBytes(regionInfo[0]), TableName.valueOf(KafkaUtil.getTopicTable(taskInfo[0])),
                        HConstants.NO_SEQNUM, record.timestamp(), HConstants.DEFAULT_CLUSTER_ID);
                    entryBuffers.appendEntry(new Entry(key, walEdit));
                    editsCount++;
                    if (editsCount % interval == 0) {
                      status.setStatus("Split " + editsCount + " edits, skipped " + editsSkipped + " edits.");
                      if (reporter != null && !reporter.progress()) {
                        progress_failed = true;
                        return false;
                      }
                    }
                  } else {
                    editsSkipped++;
                    continue;
                  }
                } finally {
                  if (reachEnd) {
                    break LOOP;
                  }
                }
              }
            }
          }
        }
      }
    } catch (InterruptedException ie) {
      IOException iie = new InterruptedIOException();
      iie.initCause(ie);
      throw iie;
    } catch (IOException e) {
      e = RemoteExceptionHandler.checkIOException(e);
      throw e;
    } finally {
      LOG.debug("Finishing writing output logs and closing down.");
      try {
        // Set progress_failed to true as the immediate following statement will reset its value
        // when finishWritingAndClose() throws exception, progress_failed has the right value
        progress_failed = outputSink.finishWritingAndClose() == null | progress_failed;
      } finally {
        String msg =
            "Processed " + editsCount + " edits across " + outputSink.getNumberOfRecoveredRegions()
                + " regions; edits skipped=" + editsSkipped + "; taskName=" + this.taskBeingSplit
                + ", progress failed=" + progress_failed;
        LOG.info(msg);
        status.markComplete(msg);
      }
    }
    return !progress_failed;
  }

  private boolean isRowkeyInRange(String rowkey, String start, String end) {
    if ("null".equals(start) && "null".equals(end)) { // only one region
      return true;
    } else if ("null".equals(start)) { // first region
      return rowkey.compareTo(end) < 0;
    } else if("null".equals(end)) { // last region
      return rowkey.compareTo(start) >= 0;
    } else {
      return rowkey.compareTo(start) >= 0 && rowkey.compareTo(end) < 0;
    }
  }

  @VisibleForTesting
  public static void setKafkaConsumer(Map<Integer, KafkaConsumer<byte[], byte[]>> consumer) {
    TEST_CONSUMER = consumer;
  }

  private KafkaConsumer<byte[], byte[]> getKafkaConsumer(String topic, int partition, long offset) {
    if (TEST_CONSUMER != null) {
      synchronized (KafkaWALSplitter.class) {
        if (TEST_CONSUMER.containsKey(Integer.valueOf(-1))) {
          return TEST_CONSUMER.remove(Integer.valueOf(-1));
        }
      }
      return TEST_CONSUMER.get(Integer.valueOf(partition));
    }
    Properties props = new Properties();
    props.setProperty("bootstrap.servers", kafkaServers);
    props.setProperty("enable.auto.commit", "false");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
    TopicPartition p = new TopicPartition(topic, partition);
    consumer.assign(Arrays.asList(p));
    consumer.seek(p, offset);
    LOG.info("Instance KafkaConsumer seek the position to " + topic + "_" + partition + "_" + offset);
    return consumer;
  }
}
