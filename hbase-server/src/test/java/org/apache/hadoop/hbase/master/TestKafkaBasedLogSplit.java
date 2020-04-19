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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test do log replay from beginning
 */
@Category(MediumTests.class)
public class TestKafkaBasedLogSplit extends KafkaBasedLogSplitTest {
  @Test
  public void testLogSplitFromBeginning() throws Exception {
    String TABLENAME = "testLogSplitFromBeginning";
    mockKafkaEnv(new KafkaEnvProvider() {
      @Override
      public Map<Integer, Long> getPartitionInfo() {
        Map<Integer, Long> partitionInfo = new HashMap<>();
        partitionInfo.put(Integer.valueOf(0), Long.valueOf(10));
        return partitionInfo;
      }
      @Override
      public ConsumerRecords<byte[], byte[]> mockKafkaRecords(int partition) throws IOException {
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> records = new HashMap<>();
        List<ConsumerRecord<byte[], byte[]>> recordsPerPartition = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
          Put put = new Put(Bytes.toBytes("row" + i));
          // timestamp must be specified, or get with column specify will return nothing
          put.addColumn(FAMILYNAME, COLNAME, System.currentTimeMillis(), Bytes.toBytes("value" + i));
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
    });
    TableName tn = TableName.valueOf(TABLENAME);
    Table table = TEST_UTIL.createTable(tn, "fam");
    List<HRegion> regions = TEST_UTIL.getHBaseCluster().getRegions(tn);
    assertEquals(1, regions.size());
    HRegion region = regions.get(0);
    ServerName sn = TEST_UTIL.getMiniHBaseCluster().getServerHoldingRegion(tn,
        region.getRegionInfo().getRegionName());
    TEST_UTIL.getMiniHBaseCluster().startRegionServer();
    // write some records
    for (long i = 0; i < 10; i++) {
      Put put = new Put(Bytes.toBytes("row" + i));
      put.addColumn(FAMILYNAME, COLNAME, Bytes.toBytes("value" + i));
      put.setAttribute("PART", Bytes.toBytes(0));
      put.setAttribute("OFFSET", Bytes.toBytes(i + 1));
      table.put(put);
    }
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
        assertEquals(value, "value" + i);
        i++;
        result = scanner.next();
      }
      assertEquals(10, i);
    }
    assertEquals(ZKUtil.listChildrenNoWatch(TEST_UTIL.getZooKeeperWatcher(), "/hbase/splitWAL").size(), 0);
  }

  @Test
  public void testLogSplitFromLastFlushId() throws Exception {
    String TABLENAME = "testLogSplitFromLastFlushId";
    mockKafkaEnv(new KafkaEnvProvider() {
      @Override
      public Map<Integer, Long> getPartitionInfo() {
        Map<Integer, Long> partitionInfo = new HashMap<>();
        partitionInfo.put(Integer.valueOf(0), Long.valueOf(20));
        return partitionInfo;
      }
      @Override
      public ConsumerRecords<byte[], byte[]> mockKafkaRecords(int partition) throws IOException {
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
    });
    int msgInterval = TEST_UTIL.getConfiguration().getInt("hbase.regionserver.msginterval", 100);
    TableName tn = TableName.valueOf(TABLENAME);
    Table table = TEST_UTIL.createTable(tn, "fam");
    List<HRegion> regions = TEST_UTIL.getHBaseCluster().getRegions(tn);
    assertEquals(1, regions.size());
    HRegion region = regions.get(0);
    ServerName sn = TEST_UTIL.getMiniHBaseCluster().getServerHoldingRegion(tn,
        region.getRegionInfo().getRegionName());
    TEST_UTIL.getMiniHBaseCluster().startRegionServer();

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