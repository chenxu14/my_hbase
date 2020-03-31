/**
 *
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
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.KafkaUtil;
import org.apache.hadoop.hbase.wal.KafkaWALSplitter;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * KAFKA log split base Test
 */
public abstract class KafkaBasedLogSplitTest {
  protected static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  protected static final byte[] FAMILYNAME = Bytes.toBytes("fam");
  protected static final byte[] COLNAME = Bytes.toBytes("col");

  @Before
  public void setUp() throws Exception {
    TEST_UTIL.getConfiguration().set(HConstants.LOG_RECOVERY_MODE, KafkaRecoveryManager.RECOVERY_MODE);
    TEST_UTIL.getConfiguration().set(KafkaUtil.KAFKA_BROKER_SERVERS, "localhost:9092");
    TEST_UTIL.getConfiguration().setBoolean("hbase.assignment.usezk", false);
    TEST_UTIL.getConfiguration().set("hbase.balancer.tablesOnMaster", "hbase:acl,hbase:namespace,hbase:meta");
    TEST_UTIL.startMiniCluster(1);// start master and one regionserver
    TEST_UTIL.getHBaseAdmin().setBalancerRunning(false, true); // turn off the balancer
    TEST_UTIL.getMiniHBaseCluster().abortRegionServer(0);
    TEST_UTIL.getMiniHBaseCluster().getConf().set(WALFactory.WAL_PROVIDER, "kafkaclient");
    TEST_UTIL.getMiniHBaseCluster().startRegionServer();
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @SuppressWarnings("unchecked")
  protected void mockKafkaEnv(KafkaEnvProvider provider) throws Exception {
    AdminClient adminClient = Mockito.mock(AdminClient.class);
    Map<Integer, OffsetAndMetadata> metas = new HashMap<>();
    Map<Integer, KafkaConsumer<byte[], byte[]>> consumers = new HashMap<>();
    for (Map.Entry<Integer, Long> partitionInfo : provider.getPartitionInfo().entrySet()) {
      Integer partition = partitionInfo.getKey();
      Long offset = partitionInfo.getValue();
      OffsetAndMetadata offsetMeta = Mockito.mock(OffsetAndMetadata.class);
      Mockito.when(offsetMeta.offset()).thenReturn(offset);
      metas.put(partition, offsetMeta);
      KafkaConsumer<byte[], byte[]> consumer = Mockito.mock(KafkaConsumer.class);
      if (partition == -1) {
        Mockito.when(consumer.poll(Mockito.any(Duration.class))).thenThrow(KafkaException.class);
      } else {
        Mockito.when(consumer.poll(Mockito.any(Duration.class))).thenReturn(provider.mockKafkaRecords(partition));
      }
      consumers.put(partition, consumer);
    }
    KafkaWALSplitter.setKafkaConsumer(consumers);
    Map<TopicPartition, OffsetAndMetadata> offsets = Mockito.mock(HashMap.class);
    Mockito.when(offsets.get(Mockito.any(TopicPartition.class))).then(new Answer<OffsetAndMetadata>() {
      @Override
      public OffsetAndMetadata answer(InvocationOnMock invocation) throws Throwable {
        TopicPartition topicPartition = invocation.getArgumentAt(0, TopicPartition.class);
        return metas.get(topicPartition.partition());
      }
    });
    ListConsumerGroupOffsetsResult offsetsRes = Mockito.mock(ListConsumerGroupOffsetsResult.class);
    KafkaFuture<Map<TopicPartition,OffsetAndMetadata>> future = Mockito.mock(KafkaFuture.class);
    Mockito.when(future.get()).thenReturn(offsets);
    Mockito.when(offsetsRes.partitionsToOffsetAndMetadata()).thenReturn(future);
    Mockito.when(adminClient.listConsumerGroupOffsets(Mockito.anyString(),
      Mockito.any(ListConsumerGroupOffsetsOptions.class))).thenReturn(offsetsRes);
    KafkaRecoveryManager krm = TEST_UTIL.getHBaseCluster().getMaster().getKafkaRecoveryManager();
    krm.setKafkaConsumer(Mockito.mock(KafkaConsumer.class));
    krm.setKafkaAdminClient(adminClient);
  }

  protected static interface KafkaEnvProvider {
    Map<Integer, Long> getPartitionInfo();
    ConsumerRecords<byte[], byte[]> mockKafkaRecords(int partition) throws IOException;
  }

  protected WALEdit buildWALEdit(Mutation mutation) {
    WALEdit edit = new WALEdit();
    for (List<Cell> cells : mutation.getFamilyCellMap().values()) {
      for (Cell cell : cells) {
        edit.add(cell);
      }
    }
    return edit;
  }

  protected int getCount(Table table) throws IOException {
    int i = 0;
    Scan scan = new Scan();
    try (ResultScanner scanner = table.getScanner(scan)) {
      Result result = scanner.next();
      while (result != null) {
        i++;
        result = scanner.next();
      }
    }
    return i;
  }
}
