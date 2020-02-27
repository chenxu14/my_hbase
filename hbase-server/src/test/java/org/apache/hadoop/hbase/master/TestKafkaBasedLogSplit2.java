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
 * Test do log replay from last flushId
 */
@Category(MediumTests.class)
public class TestKafkaBasedLogSplit2 {
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
    KafkaConsumer<byte[], byte[]> consumer = Mockito.mock(KafkaConsumer.class);
    consumers.put(Integer.valueOf(0), consumer);
    List<PartitionInfo> partitions = new ArrayList<PartitionInfo>();
    partitions.add(Mockito.mock(PartitionInfo.class));
    Mockito.when(consumer.partitionsFor(Mockito.anyString())).thenReturn(partitions);
    Mockito.doNothing().when(consumer).seekToEnd(Mockito.anyCollection());
    Mockito.doNothing().when(consumer).assign(Mockito.anyCollection());
    Mockito.when(consumer.position(Mockito.any(TopicPartition.class))).thenReturn(20L); // end position
    Mockito.when(consumer.poll(Mockito.any(Duration.class))).thenReturn(mockKafkaRecords());
    Mockito.when(consumer.offsetsForTimes(Mockito.anyMap())).thenAnswer(
      new Answer<Map<TopicPartition, OffsetAndTimestamp>>(){
        @Override
        public Map<TopicPartition, OffsetAndTimestamp> answer(InvocationOnMock invocation) throws Throwable {
          Map<TopicPartition, Long> param = (Map<TopicPartition, Long>) invocation.getArguments()[0];
          TopicPartition key = param.keySet().iterator().next();
          return Collections.singletonMap(key, new OffsetAndTimestamp(0, 0));
        }
      });

    // mock AdminClient
    AdminClient adminClient = Mockito.mock(AdminClient.class);
    OffsetAndMetadata offset = Mockito.mock(OffsetAndMetadata.class);
    Mockito.when(offset.offset()).thenReturn(20L);
    Map<TopicPartition, OffsetAndMetadata> offsets = Mockito.mock(HashMap.class);
    Mockito.when(offsets.get(Mockito.any(TopicPartition.class))).thenReturn(offset);
    ListConsumerGroupOffsetsResult offsetsRes = Mockito.mock(ListConsumerGroupOffsetsResult.class);
    KafkaFuture<Map<TopicPartition,OffsetAndMetadata>> future = Mockito.mock(KafkaFuture.class);
    Mockito.when(future.get()).thenReturn(offsets);
    Mockito.when(offsetsRes.partitionsToOffsetAndMetadata()).thenReturn(future);
    Mockito.when(adminClient.listConsumerGroupOffsets(Mockito.anyString(),
        Mockito.any(ListConsumerGroupOffsetsOptions.class))).thenReturn(offsetsRes);

    TEST_UTIL.startMiniCluster(2);// start master and one regionserver
    TEST_UTIL.getMiniHBaseCluster().abortRegionServer(0);
    TEST_UTIL.getMiniHBaseCluster().abortRegionServer(1);
    TEST_UTIL.getMiniHBaseCluster().getConf().set(WALFactory.WAL_PROVIDER, "kafkaclient");
    TEST_UTIL.getMiniHBaseCluster().startRegionServer();
    TEST_UTIL.getMiniHBaseCluster().startRegionServer();
    KafkaRecoveryManager krm = TEST_UTIL.getHBaseCluster().getMaster().getKafkaRecoveryManager();
    krm.setKafkaConsumer(consumer);
    krm.setKafkaAdminClient(adminClient);
    KafkaWALSplitter.setKafkaConsumer(consumers);
  }

  private static ConsumerRecords<byte[], byte[]> mockKafkaRecords() throws IOException {
    Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> records = new HashMap<>();
    List<ConsumerRecord<byte[], byte[]>> recordsPerPartition = new ArrayList<>();
    for (int i = 10; i < 20; i++) {
      Put put = new Put(Bytes.toBytes("row" + String.format("%04d",i)));
      // timestamp must be specified, or get with column specify will return nothing
      put.addColumn(FAMILYNAME, COLNAME, System.currentTimeMillis(), Bytes.toBytes("value" + String.format("%04d",i)));
      WALEdit walEdit = buildWALEdit(put);
      try (ByteArrayOutputStream bos = new ByteArrayOutputStream((int)walEdit.heapSize())) {
        walEdit.write(new DataOutputStream(bos));
        recordsPerPartition.add(new ConsumerRecord<byte[], byte[]>(
            TABLENAME, 0, i, HConstants.EMPTY_BYTE_ARRAY, bos.toByteArray()));
      }
    }
    records.put(new TopicPartition(TABLENAME, 0), recordsPerPartition);
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
    Table table = TEST_UTIL.createTable(tn, "fam");
    List<HRegion> regions = TEST_UTIL.getHBaseCluster().getRegions(tn);
    assertEquals(1, regions.size());
    HRegion region = regions.get(0);
    ServerName sn = TEST_UTIL.getMiniHBaseCluster().getServerHoldingRegion(tn,
        region.getRegionInfo().getRegionName());
    // write some records
    for (long i = 0; i < 10; i++) {
      Put put = new Put(Bytes.toBytes("row" + String.format("%04d",i)));
      put.addColumn(FAMILYNAME, COLNAME, Bytes.toBytes("value" + String.format("%04d",i)));
      put.setAttribute("PART", Bytes.toBytes(0));
      put.setAttribute("OFFSET", Bytes.toBytes(i + 1));
      table.put(put);
    }
    TEST_UTIL.flush(tn);
    // write some records again
    for (long i = 10; i < 20; i++) {
      Put put = new Put(Bytes.toBytes("row" + String.format("%04d",i)));
      put.addColumn(FAMILYNAME, COLNAME, Bytes.toBytes("value" + String.format("%04d",i)));
      put.setAttribute("PART", Bytes.toBytes(0));
      put.setAttribute("OFFSET", Bytes.toBytes(i + 1));
      table.put(put);
    }
    // make sure RS has report flushId to master 
    Threads.sleep(msgInterval * 2);

    ServerManager serverMgr = TEST_UTIL.getHBaseCluster().getMaster().getServerManager();
    Map<byte[], Long> regionFlushids = serverMgr.getFlushedSequenceIdByRegion();
    assertEquals(3, regionFlushids.size()); // meta + namespace + usertable(1 region)
    long flushId = regionFlushids.get(region.getRegionInfo().getEncodedNameAsBytes());
    assertEquals(HConstants.NO_SEQNUM, flushId); // kafka wal dont update sequenceId

    TEST_UTIL.getMiniHBaseCluster().killRegionServer(sn);
    TEST_UTIL.waitTableAvailable(tn);

    Scan scan = new Scan();
    scan.addColumn(FAMILYNAME, COLNAME);
    try (ResultScanner scanner = table.getScanner(scan)) {
      Result result = scanner.next();
      int i = 0;
      while (result != null) {
        Cell cell = result.getColumnLatestCell(FAMILYNAME, COLNAME);
        assertNotNull(cell);
        String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
        assertEquals(value, "value" + String.format("%04d",i));
        i++;
        result = scanner.next();
      }
      assertEquals(20, i);
    }
    assertEquals(0, ZKUtil.listChildrenNoWatch(TEST_UTIL.getZooKeeperWatcher(), "/hbase/splitWAL").size());
  }
}
