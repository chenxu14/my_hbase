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
import static org.junit.Assert.assertTrue;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestServerManager {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final byte[] FAMILYNAME = Bytes.toBytes("fam");
  private static final byte[] COLNAME = Bytes.toBytes("col");

  @BeforeClass
  public static void beforeAllTests() throws Exception {
    // we will retry operations when PleaseHoldException is thrown
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 3);
    TEST_UTIL.getConfiguration().setBoolean("hbase.assignment.usezk", false);
    // Start a cluster of two regionservers.
    TEST_UTIL.startMiniCluster(2);
    TEST_UTIL.getHBaseCluster().getMaster().assignmentManager.initializeHandlerTrackers();
  }

  @AfterClass
  public static void afterAllTests() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testSplitRegion() throws Exception {
    TableName TABLENAME = TableName.valueOf("TestSplitRegion");
    Configuration conf = TEST_UTIL.getConfiguration();
    int msgInterval = conf.getInt("hbase.regionserver.msginterval", 100);
    Table table = TEST_UTIL.createTable(TABLENAME, "fam");
    List<HRegion> regions = TEST_UTIL.getHBaseCluster().getRegions(TABLENAME);
    assertEquals(1, regions.size());
    HRegion region = regions.get(0);

    // write some records
    for (int i = 0; i< 10; i++) {
      Put put = new Put(Bytes.toBytes("row" + i));
      put.addColumn(FAMILYNAME, COLNAME, Bytes.toBytes("value"));
      table.put(put);
    }
    TEST_UTIL.flush(TABLENAME);

    Threads.sleep(msgInterval * 2);
    ServerManager serverMgr = TEST_UTIL.getHBaseCluster().getMaster().getServerManager();
    Map<byte[], Long> regionFlushids = serverMgr.getFlushedSequenceIdByRegion();
    long flushId = regionFlushids.get(region.getRegionInfo().getEncodedNameAsBytes());
    assertTrue(flushId != -1);

    TEST_UTIL.getHBaseAdmin().splitRegion(region.getRegionInfo().getRegionName(), Bytes.toBytes("row5"));
    TEST_UTIL.waitTableAvailable(TABLENAME);
    Threads.sleep(msgInterval * 2);

    int count = 0;
    regionFlushids = serverMgr.getFlushedSequenceIdByRegion();
    for (long regionFlushId : regionFlushids.values()) {
      if (regionFlushId == flushId) {
        count++;
      }
    }
    assertEquals(3, count); // parent and daughter has same flushid
    table.close();
  }

  @Test
  public void testMergeRegion() throws Exception {
    byte[][] SPLIT_KEYS = { Bytes.toBytes("row4") };
    byte[][] FAMILY = { FAMILYNAME };
    TableName TABLENAME = TableName.valueOf("TestMergeRegion");
    Configuration conf = TEST_UTIL.getConfiguration();
    int msgInterval = conf.getInt("hbase.regionserver.msginterval", 100);
    Table table = TEST_UTIL.createTable(TABLENAME, FAMILY, SPLIT_KEYS);
    List<HRegion> regions = TEST_UTIL.getHBaseCluster().getRegions(TABLENAME);
    assertEquals(2, regions.size());
    HRegion firstRegion = null;
    HRegion secondRegion = null;
    for (HRegion region : regions) {
      if (Arrays.equals(region.getRegionInfo().getStartKey(), HConstants.EMPTY_START_ROW)) {
        firstRegion = region;
      } else {
        secondRegion = region;
      }
    }
    assertNotNull(firstRegion);
    assertNotNull(secondRegion);

    // write some records
    for (int i = 0; i< 10; i++) {
      Put put = new Put(Bytes.toBytes("row" + i));
      put.addColumn(FAMILYNAME, COLNAME, Bytes.toBytes("value"));
      table.put(put);
    }
    TEST_UTIL.flush(TABLENAME);

    Threads.sleep(msgInterval * 2);
    ServerManager serverMgr = TEST_UTIL.getHBaseCluster().getMaster().getServerManager();
    Map<byte[], Long> regionFlushids = serverMgr.getFlushedSequenceIdByRegion();
    assertEquals(4, regionFlushids.size()); // meta + namespace + usertable(2 region)
    long firstFlushId = regionFlushids.get(firstRegion.getRegionInfo().getEncodedNameAsBytes());

    TEST_UTIL.getHBaseAdmin().mergeRegions(firstRegion.getRegionInfo().getRegionName(),
        secondRegion.getRegionInfo().getRegionName(), false);
    TEST_UTIL.waitTableAvailable(TABLENAME);
    Threads.sleep(msgInterval * 2);

    int count = 0;
    regionFlushids = serverMgr.getFlushedSequenceIdByRegion();
    for (long regionFlushId : regionFlushids.values()) {
      if (regionFlushId == firstFlushId) {
        count++;
      }
    }
    assertEquals(2, count); // parent has the same flushId with first region
    table.close();
  }
}
