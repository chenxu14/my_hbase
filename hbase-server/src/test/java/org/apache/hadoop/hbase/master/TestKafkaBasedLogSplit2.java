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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
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
 * Test log replay with kafka partition expend
 */
@Category(MediumTests.class)
public class TestKafkaBasedLogSplit2 extends KafkaBasedLogSplitTest {

  @Test
  public void testKafkaPartitionExpend() throws Exception {
    String TABLENAME = "testKafkaPartitionExpend";
    mockKafkaEnv(new KafkaEnvProvider() {
      @Override
      public Map<Integer, Long> getPartitionInfo() {
        Map<Integer, Long> partitionInfo = new HashMap<>();
        partitionInfo.put(Integer.valueOf(0), Long.valueOf(16));
        partitionInfo.put(Integer.valueOf(1), Long.valueOf(6));
        return partitionInfo;
      }
      @Override
      public ConsumerRecords<byte[], byte[]> mockKafkaRecords(int partition) throws IOException {
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> records = new HashMap<>();
        List<ConsumerRecord<byte[], byte[]>> recordsPerPartition = new ArrayList<>();
        if (partition == 0) {
           for (int i = 0; i < 10; i++) {
             String rowkey;
             if (i % 2 == 0) {
               rowkey = "5000_row";
             } else {
               rowkey = "0000_row";
             }
             Put put = new Put(Bytes.toBytes(rowkey + String.format("%02d",i)));
             put.addColumn(FAMILYNAME, COLNAME, System.currentTimeMillis(), Bytes.toBytes("value" + String.format("%02d",i)));
             WALEdit walEdit = buildWALEdit(put);
             try (ByteArrayOutputStream bos = new ByteArrayOutputStream((int)walEdit.heapSize())) {
               walEdit.write(new DataOutputStream(bos));
               recordsPerPartition.add(new ConsumerRecord<byte[], byte[]>(
                   TABLENAME, 0, i + 1, HConstants.EMPTY_BYTE_ARRAY, bos.toByteArray()));
             }
           }
           int offset = 11;
           for (int i = 11; i < 20; i+=2) {
              Put put = new Put(Bytes.toBytes("0000_row" + String.format("%02d",i)));
              put.addColumn(FAMILYNAME, COLNAME, System.currentTimeMillis(), Bytes.toBytes("value" + String.format("%02d",i)));
              WALEdit walEdit = buildWALEdit(put);
              try (ByteArrayOutputStream bos = new ByteArrayOutputStream((int)walEdit.heapSize())) {
                walEdit.write(new DataOutputStream(bos));
                recordsPerPartition.add(new ConsumerRecord<byte[], byte[]>(
                    TABLENAME, 0, offset, HConstants.EMPTY_BYTE_ARRAY, bos.toByteArray()));
              }
              offset++;
           }
        } else if (partition == 1) {
          for (int i = 10; i < 20; i+=2) {
            Put put = new Put(Bytes.toBytes("5000_row" + String.format("%02d",i)));
            put.addColumn(FAMILYNAME, COLNAME, System.currentTimeMillis(), Bytes.toBytes("value" + String.format("%02d",i)));
            WALEdit walEdit = buildWALEdit(put);
            long offset = (i - 10) / 2 + 1;
            try (ByteArrayOutputStream bos = new ByteArrayOutputStream((int)walEdit.heapSize())) {
              walEdit.write(new DataOutputStream(bos));
              recordsPerPartition.add(new ConsumerRecord<byte[], byte[]>(
                  TABLENAME, 1, offset, HConstants.EMPTY_BYTE_ARRAY, bos.toByteArray()));
            }
          }
        }
        records.put(new TopicPartition(TABLENAME, partition), recordsPerPartition);
        return new ConsumerRecords<byte[], byte[]>(records);
      }
    });
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
    for (long i = 0; i < 10; i++) {
      String rowkey;
      if (i % 2 == 0) {
        rowkey = "5000_row" + String.format("%02d",i);
      } else {
        rowkey = "0000_row" + String.format("%02d",i);
      }
      Put put = new Put(Bytes.toBytes(rowkey));
      put.addColumn(FAMILYNAME, COLNAME, Bytes.toBytes("value" + String.format("%02d",i)));
      put.setAttribute("PART", Bytes.toBytes(0));
      put.setAttribute("OFFSET", Bytes.toBytes(i + 1));
      // System.out.println("0 -> " + (i + 1));
      table.put(put);
    }
    // mock partition expend
    for (long i = 10; i < 20; i++) {
      String rowkey;
      int part;
      long offset;
      if (i % 2 == 0) {
        rowkey = "5000_row" + String.format("%02d",i);
        part = 1;
        offset = (i - 10) / 2;
      } else {
        rowkey = "0000_row" + String.format("%02d",i);
        part = 0;
        offset = (i - 9) / 2 + 9;
      }
      Put put = new Put(Bytes.toBytes(rowkey));
      put.addColumn(FAMILYNAME, COLNAME, Bytes.toBytes("value" + String.format("%02d",i)));
      put.setAttribute("PART", Bytes.toBytes(part));
      put.setAttribute("OFFSET", Bytes.toBytes(offset + 1));
      // System.out.println(part + " -> " + (offset + 1));
      table.put(put);
    }

    // make sure RS has report flushId to master 
    Threads.sleep(msgInterval * 2);
    ServerManager serverMgr = TEST_UTIL.getHBaseCluster().getMaster().getServerManager();
    Map<Integer, Long> firstKafkaMapping = serverMgr.getLastFlushedKafkaOffsets(
        first.getRegionInfo().getEncodedName().getBytes());
    assertEquals(firstKafkaMapping.size(), 1);
    for (Map.Entry<Integer, Long> offsets : firstKafkaMapping.entrySet()) {
      System.out.println(offsets.getKey() + " -> " + offsets.getValue());
    }
    Map<Integer, Long> secKafkaMapping = serverMgr.getLastFlushedKafkaOffsets(
        second.getRegionInfo().getEncodedName().getBytes());
    assertEquals(secKafkaMapping.size(), 2);
    for (Map.Entry<Integer, Long> offsets : secKafkaMapping.entrySet()) {
      System.out.println(offsets.getKey() + " -> " + offsets.getValue());
    }

    assertEquals(20, getCount(table));
    TEST_UTIL.getMiniHBaseCluster().killRegionServer(sn);
    TEST_UTIL.waitTableAvailable(tn);
    assertEquals(20, getCount(table));
    assertEquals(0, ZKUtil.listChildrenNoWatch(TEST_UTIL.getZooKeeperWatcher(), "/hbase/splitWAL").size());
  }
}
