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

import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestHdfsBasedLogSplit {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final String TABLENAME = "testLogSplit";
  private static final byte[] FAMILYNAME = Bytes.toBytes("fam");
  private static final byte[] COLNAME = Bytes.toBytes("col");

  @BeforeClass
  public static void beforeAllTests() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean("hbase.assignment.usezk", false);
    TEST_UTIL.getConfiguration().set("hbase.balancer.tablesOnMaster", "hbase:acl,hbase:namespace,hbase:meta");
    TEST_UTIL.startMiniCluster(2);// start master and one regionserver
  }

  @AfterClass
  public static void afterAllTests() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testLogSplit() throws Exception {
    TableName tn = TableName.valueOf(TABLENAME);
    Table table = TEST_UTIL.createTable(tn, "fam");
    List<HRegion> regions = TEST_UTIL.getHBaseCluster().getRegions(tn);
    assertEquals(1, regions.size());
    HRegion region = regions.get(0);
    ServerName sn = TEST_UTIL.getMiniHBaseCluster().getServerHoldingRegion(tn,
        region.getRegionInfo().getRegionName());
    // write some records
    for (long i = 0; i < 10; i++) {
      Put put = new Put(Bytes.toBytes("row" + i));
      put.addColumn(FAMILYNAME, COLNAME, Bytes.toBytes("value" + i));
      put.setAttribute("OFFSET", Bytes.toBytes(i));
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
      assertEquals(i, 10);
    }
    assertEquals(ZKUtil.listChildrenNoWatch(TEST_UTIL.getZooKeeperWatcher(), "/hbase/splitWAL").size(), 0);
  }
}
