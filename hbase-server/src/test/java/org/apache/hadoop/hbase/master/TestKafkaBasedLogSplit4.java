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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.KafkaUtil;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.KafkaWALSplitter;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Test log replay with KAFKA exception
 */
@Category(MediumTests.class)
public class TestKafkaBasedLogSplit4 {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final String TABLENAME = "testLogSplit";
  private static final byte[] FAMILYNAME = Bytes.toBytes("fam");
  private static final byte[] COLNAME = Bytes.toBytes("col");

  @SuppressWarnings("unchecked")
  @BeforeClass
  public static void beforeAllTests() throws Exception {
    TEST_UTIL.getConfiguration().set(HConstants.LOG_RECOVERY_MODE, KafkaRecoveryManager.RECOVERY_MODE);
    TEST_UTIL.getConfiguration().set(KafkaUtil.KAFKA_BROKER_SERVERS, "localhost:9092");
    TEST_UTIL.getConfiguration().setBoolean("hbase.assignment.usezk", false);
    TEST_UTIL.getConfiguration().set("hbase.balancer.tablesOnMaster", "hbase:acl,hbase:namespace,hbase:meta");
    Map<Integer, KafkaConsumer<byte[], byte[]>> consumers = new HashMap<>();
    consumers.put(Integer.valueOf(0), mockKafkaConsumer(0));
    consumers.put(Integer.valueOf(1), mockKafkaConsumer(1));
    consumers.put(Integer.valueOf(-1), mockKafkaConsumer(-1));

    // mock AdminClient
    AdminClient adminClient = Mockito.mock(AdminClient.class);
    OffsetAndMetadata offset = Mockito.mock(OffsetAndMetadata.class);
    Mockito.when(offset.offset()).thenThrow(KafkaException.class);
    OffsetAndMetadata offset_0 = Mockito.mock(OffsetAndMetadata.class);
    Mockito.when(offset_0.offset()).thenReturn(11L);
    OffsetAndMetadata offset_1 = Mockito.mock(OffsetAndMetadata.class);
    Mockito.when(offset_1.offset()).thenReturn(10L);
    Map<TopicPartition, OffsetAndMetadata> offsets = Mockito.mock(HashMap.class);
    Mockito.when(offsets.get(Mockito.any(TopicPartition.class))).then(new Answer<OffsetAndMetadata>() {
      @Override
      public OffsetAndMetadata answer(InvocationOnMock invocation) throws Throwable {
        TopicPartition topicPartition = invocation.getArgumentAt(0, TopicPartition.class);
        int partition = topicPartition.partition();
        return partition == -1 ? offset : (partition == 0 ? offset_0 : offset_1);
      }
    });
    ListConsumerGroupOffsetsResult offsetsRes = Mockito.mock(ListConsumerGroupOffsetsResult.class);
    KafkaFuture<Map<TopicPartition,OffsetAndMetadata>> future = Mockito.mock(KafkaFuture.class);
    Mockito.when(future.get()).thenReturn(offsets);
    Mockito.when(offsetsRes.partitionsToOffsetAndMetadata()).thenReturn(future);
    Mockito.when(adminClient.listConsumerGroupOffsets(Mockito.anyString(),
        Mockito.any(ListConsumerGroupOffsetsOptions.class))).thenReturn(offsetsRes);

    TEST_UTIL.startMiniCluster(1);// start master and one regionserver
    TEST_UTIL.getHBaseAdmin().setBalancerRunning(false, true); // turn off the balancer
    TEST_UTIL.getMiniHBaseCluster().abortRegionServer(0);
    TEST_UTIL.getMiniHBaseCluster().getConf().set(WALFactory.WAL_PROVIDER, "kafkaclient");
    TEST_UTIL.getMiniHBaseCluster().startRegionServer();
    KafkaRecoveryManager krm = TEST_UTIL.getHBaseCluster().getMaster().getKafkaRecoveryManager();
    krm.setKafkaConsumer(consumers.get(0));
    krm.setKafkaAdminClient(adminClient);
    KafkaWALSplitter.setKafkaConsumer(consumers);
  }

  @SuppressWarnings("unchecked")
  private static KafkaConsumer<byte[], byte[]> mockKafkaConsumer(int partition) throws IOException {
    KafkaConsumer<byte[], byte[]> consumer = Mockito.mock(KafkaConsumer.class);
    List<PartitionInfo> partitions = new ArrayList<PartitionInfo>(); // two partitions
    partitions.add(Mockito.mock(PartitionInfo.class));
    partitions.add(Mockito.mock(PartitionInfo.class));
    Mockito.when(consumer.partitionsFor(Mockito.anyString())).thenReturn(partitions);
    Mockito.doNothing().when(consumer).seekToEnd(Mockito.anyCollection());
    Mockito.doNothing().when(consumer).assign(Mockito.anyCollection());
    Mockito.when(consumer.position(Mockito.any(TopicPartition.class))).thenAnswer(new Answer<Long>() {
      @Override
      public Long answer(InvocationOnMock invocation) throws Throwable {
        TopicPartition topicPartition = invocation.getArgumentAt(0, TopicPartition.class);
        return topicPartition.partition() == 0 ? 11L : 10L;
      }
    });
    if (partition == -1) {
      Mockito.when(consumer.poll(Mockito.any(Duration.class))).thenThrow(KafkaException.class);
    } else {
      Mockito.when(consumer.poll(Mockito.any(Duration.class))).thenReturn(mockKafkaRecords(partition));
    }
    Mockito.when(consumer.offsetsForTimes(Mockito.anyMap())).thenAnswer(
      new Answer<Map<TopicPartition, OffsetAndTimestamp>>() {
        @Override
        public Map<TopicPartition, OffsetAndTimestamp> answer(InvocationOnMock invocation) throws Throwable {
          Map<TopicPartition, Long> param = (Map<TopicPartition, Long>) invocation.getArguments()[0];
          TopicPartition key = param.keySet().iterator().next();
          return Collections.singletonMap(key, new OffsetAndTimestamp(0, 0));
        }
      });
    return consumer;
  }

  private static ConsumerRecords<byte[], byte[]> mockKafkaRecords(int partition) throws IOException {
    Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> records = new HashMap<>();
    List<ConsumerRecord<byte[], byte[]>> recordsPerPartition = new ArrayList<>();
    if (partition == 0) {
      for (int i = 5; i < 11; i++) {
        Put put = new Put(Bytes.toBytes("0000_row" + String.format("%02d",i)));
        put.addColumn(FAMILYNAME, COLNAME, System.currentTimeMillis(), Bytes.toBytes("value" + String.format("%02d",i)));
        WALEdit walEdit = buildWALEdit(put);
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream((int)walEdit.heapSize())) {
          walEdit.write(new DataOutputStream(bos));
          recordsPerPartition.add(new ConsumerRecord<byte[], byte[]>(
            TABLENAME, 0, i, HConstants.EMPTY_BYTE_ARRAY, bos.toByteArray()));
        }
      }
    } else {
      for (int i = 6; i < 10; i++) {
        Put put = new Put(Bytes.toBytes("5000_row1" + i));
        put.addColumn(FAMILYNAME, COLNAME, System.currentTimeMillis(), Bytes.toBytes("value1" + i));
        WALEdit walEdit = buildWALEdit(put);
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream((int)walEdit.heapSize())) {
          walEdit.write(new DataOutputStream(bos));
          recordsPerPartition.add(new ConsumerRecord<byte[], byte[]>(
            TABLENAME, 0, i, HConstants.EMPTY_BYTE_ARRAY, bos.toByteArray()));
        }
      }
    }
    records.put(new TopicPartition(TABLENAME, partition), recordsPerPartition);
    return new ConsumerRecords<byte[], byte[]>(records);
  }

  private static WALEdit buildWALEdit(Mutation mutation) {
    WALEdit edit = new WALEdit();
    for (List<Cell> cells : mutation.getFamilyCellMap().values()) {
      for (Cell cell : cells) {
        edit.add(cell);
      }
    }
    return edit;
  }

  @AfterClass
  public static void afterAllTests() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testLogSplit() throws Exception {
    int msgInterval = TEST_UTIL.getConfiguration().getInt("hbase.regionserver.msginterval", 100);
    TableName tn = TableName.valueOf(TABLENAME);
    byte[][] FAMILY = { FAMILYNAME };
    byte[][] SPLIT_KEYS = { Bytes.toBytes("5000") };
    Table table = TEST_UTIL.createTable(tn, FAMILY, SPLIT_KEYS);
    List<HRegion> regions = TEST_UTIL.getHBaseCluster().getRegions(tn);
    assertEquals(2, regions.size());
    HRegion first, second;
    if (regions.get(0).getRegionInfo().getStartKey().length == 0) {
      first = regions.get(0);
      second = regions.get(1);
    } else {
      first = regions.get(1);
      second = regions.get(0);
    }
    ServerName sn = TEST_UTIL.getMiniHBaseCluster().getServerHoldingRegion(tn,
      first.getRegionInfo().getRegionName());
    ServerName sn2 = TEST_UTIL.getMiniHBaseCluster().getServerHoldingRegion(tn,
      second.getRegionInfo().getRegionName());
    assertEquals(sn, sn2);
    // start another RS
    TEST_UTIL.getMiniHBaseCluster().startRegionServer();

    // write some records
    for (long i = 0; i < 5; i++) { // first region
      Put put = new Put(Bytes.toBytes("0000_row" + String.format("%02d",i)));
      put.addColumn(FAMILYNAME, COLNAME, Bytes.toBytes("value" + String.format("%02d",i)));
      put.setAttribute("PART", Bytes.toBytes(0));
      put.setAttribute("OFFSET", Bytes.toBytes(i + 1));
      table.put(put);
    }
    // second region
    for (long i = 0; i < 6; i++) {
      Put put = new Put(Bytes.toBytes("5000_row1" + i));
      put.addColumn(FAMILYNAME, COLNAME, Bytes.toBytes("value1" + i));
      put.setAttribute("PART", Bytes.toBytes(1));
      put.setAttribute("OFFSET", Bytes.toBytes(i + 1));
      table.put(put);
    }
    TEST_UTIL.flush(tn);

    // write some records again
    for (long i = 5; i < 11; i++) { // first region
      Put put = new Put(Bytes.toBytes("0000_row" + String.format("%02d",i)));
      put.addColumn(FAMILYNAME, COLNAME, Bytes.toBytes("value" + String.format("%02d",i)));
      put.setAttribute("PART", Bytes.toBytes(0));
      put.setAttribute("OFFSET", Bytes.toBytes(i + 1));
      table.put(put);
    }
    // second region
    for (long i = 6; i < 10; i++) {
      Put put = new Put(Bytes.toBytes("5000_row1" + i));
      put.addColumn(FAMILYNAME, COLNAME, Bytes.toBytes("value" + i));
      put.setAttribute("PART", Bytes.toBytes(1));
      put.setAttribute("OFFSET", Bytes.toBytes(i + 1));
      table.put(put);
    }

    // make sure RS has report flushId to master 
    Threads.sleep(msgInterval * 2);
    ServerManager serverMgr = TEST_UTIL.getHBaseCluster().getMaster().getServerManager();
    Map<byte[], Long> regionFlushids = serverMgr.getFlushedSequenceIdByRegion();
    assertEquals(4, regionFlushids.size()); // meta + namespace + usertable(2 region)
    long flushId = regionFlushids.get(first.getRegionInfo().getEncodedNameAsBytes());
    long flushId2 = regionFlushids.get(second.getRegionInfo().getEncodedNameAsBytes());
    assertEquals(HConstants.NO_SEQNUM, flushId); // kafka wal dont update sequenceId
    assertEquals(HConstants.NO_SEQNUM, flushId2);
    
    TEST_UTIL.getMiniHBaseCluster().killRegionServer(sn);
    TEST_UTIL.waitTableAvailable(tn);

    Scan scan = new Scan().withStopRow(Bytes.toBytes("5000"));
    scan.addColumn(FAMILYNAME, COLNAME);
    try (ResultScanner scanner = table.getScanner(scan)) {
      Result result = scanner.next();
      int i = 0;
      while (result != null) {
        Cell cell = result.getColumnLatestCell(FAMILYNAME, COLNAME);
        assertNotNull(cell);
        String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
        assertEquals(value, "value" + String.format("%02d",i));
        i++;
        result = scanner.next();
      }
      assertEquals(11, i);
    }

    scan = new Scan().withStartRow(Bytes.toBytes("5000"));
    scan.addColumn(FAMILYNAME, COLNAME);
    try (ResultScanner scanner = table.getScanner(scan)) {
      Result result = scanner.next();
      int i = 0;
      while (result != null) {
        Cell cell = result.getColumnLatestCell(FAMILYNAME, COLNAME);
        assertNotNull(cell);
        String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
        assertEquals(value, "value1" + i);
        i++;
        result = scanner.next();
      }
      assertEquals(10, i);
    }
    assertEquals(0, ZKUtil.listChildrenNoWatch(TEST_UTIL.getZooKeeperWatcher(), "/hbase/splitWAL").size());
  }
}
