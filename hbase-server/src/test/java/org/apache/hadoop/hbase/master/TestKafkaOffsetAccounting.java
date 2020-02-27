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
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.KafkaUtil;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test KAFKA base offset accounting
 */
@Category(MediumTests.class)
public class TestKafkaOffsetAccounting {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final byte[] FAMILYNAME = Bytes.toBytes("fam");
  private static final byte[] COLNAME = Bytes.toBytes("col");
  private static final int msgInterval = 100;

  @BeforeClass
  public static void beforeAllTests() throws Exception {
    TEST_UTIL.getConfiguration().setInt("hbase.regionserver.msginterval", msgInterval);
    TEST_UTIL.getConfiguration().set(HConstants.LOG_RECOVERY_MODE, KafkaRecoveryManager.RECOVERY_MODE);
    TEST_UTIL.getConfiguration().set(KafkaUtil.KAFKA_BROKER_SERVERS, "localhost:9092");
    TEST_UTIL.getConfiguration().setBoolean("hbase.assignment.usezk", false);
    TEST_UTIL.getConfiguration().set("hbase.balancer.tablesOnMaster", "hbase:acl,hbase:namespace,hbase:meta");
    TEST_UTIL.startMiniCluster(1); // start master and one regionserver
    TEST_UTIL.getMiniHBaseCluster().abortRegionServer(0);
    TEST_UTIL.getMiniHBaseCluster().getConf().set(WALFactory.WAL_PROVIDER, "kafkaclient");
    TEST_UTIL.getMiniHBaseCluster().startRegionServer();
  }

  @AfterClass
  public static void afterAllTests() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testHeartbeat() throws Exception {
    HRegion region = buildRegion("testHeartbeat");
    // test volatile znode has created
    ZooKeeperWatcher zkWatcher = TEST_UTIL.getZooKeeperWatcher();
    String tableNode = ZKUtil.joinZNode(zkWatcher.partitionZnode, region.getTableDesc().getNameAsString());
    String regionNode = ZKUtil.joinZNode(tableNode, region.getRegionInfo().getEncodedName());
    List<String> partitions = ZKUtil.listChildrenNoWatch(zkWatcher, regionNode);
    assertEquals(1, partitions.size());
    String partNode = ZKUtil.joinZNode(regionNode, partitions.get(0));
    long offset = Bytes.toLong(ZKUtil.getDataNoWatch(zkWatcher, partNode, null));
    assertEquals(1000, offset);
    // test mapping info has reported to master throw heartbeat
    Thread.sleep(msgInterval * 2);
    ServerManager serverMgr = TEST_UTIL.getHBaseCluster().getMaster().getServerManager();
    Map<Integer, Long> offsets = serverMgr.getLastFlushedKafkaOffsets(
        region.getRegionInfo().getEncodedName().getBytes());
    assertEquals(1, offsets.size()); // only one partition
    assertEquals(1000, offsets.get(Integer.valueOf(0)).longValue());
    // test persistence znode has created
    tableNode = ZKUtil.joinZNode(zkWatcher.offsetZnode, region.getTableDesc().getNameAsString());
    regionNode = ZKUtil.joinZNode(tableNode, region.getRegionInfo().getEncodedName());
    partitions = ZKUtil.listChildrenNoWatch(zkWatcher, regionNode);
    assertEquals(1, partitions.size());
    partNode = ZKUtil.joinZNode(regionNode, partitions.get(0));
    offset = Bytes.toLong(ZKUtil.getDataNoWatch(zkWatcher, partNode, null));
    assertEquals(1000, offsets.get(Integer.valueOf(0)).longValue());
  }

  @Test
  public void testFlushOccur() throws Exception {
    HRegion region = buildRegion("testFlushOccur");
    Thread.sleep(msgInterval * 2);
    ServerManager serverMgr = TEST_UTIL.getHBaseCluster().getMaster().getServerManager();
    Map<Integer, Long> offsets = serverMgr.getLastFlushedKafkaOffsets(
        region.getRegionInfo().getEncodedName().getBytes());
    assertEquals(1, offsets.size()); // only one partition
    assertEquals(1000, offsets.get(Integer.valueOf(0)).longValue());
    region.flush(true); // flush the region
    Thread.sleep(msgInterval * 2);
    // heartbeat will reported nothing after flush
    offsets = serverMgr.getLastFlushedKafkaOffsets(
        region.getRegionInfo().getEncodedName().getBytes());
    assertTrue(offsets == null);
    // persistance znode will delete
    ZooKeeperWatcher zkWatcher = TEST_UTIL.getZooKeeperWatcher();
    String tableNode = ZKUtil.joinZNode(zkWatcher.offsetZnode, region.getTableDesc().getNameAsString());
    String regionNode = ZKUtil.joinZNode(tableNode, region.getRegionInfo().getEncodedName());
    List<String> partitions = ZKUtil.listChildrenNoWatch(zkWatcher, regionNode);
    assertTrue(partitions.isEmpty());
  }

  private HRegion buildRegion(String tableName) throws IOException {
    TableName tn = TableName.valueOf(tableName);
    Table table = TEST_UTIL.createTable(tn, "fam");
    List<HRegion> regions = TEST_UTIL.getHBaseCluster().getRegions(tn);
    assertEquals(1, regions.size());
    HRegion region = regions.get(0);
    // write some records
    for (long i = 0; i < 10; i++) {
      Put put = new Put(Bytes.toBytes("row" + i));
      put.addColumn(FAMILYNAME, COLNAME, Bytes.toBytes("value" + i));
      put.setAttribute("PART", Bytes.toBytes(0));
      put.setAttribute("OFFSET", Bytes.toBytes(i + 1000));
      table.put(put);
    }
    return region;
  }
}
