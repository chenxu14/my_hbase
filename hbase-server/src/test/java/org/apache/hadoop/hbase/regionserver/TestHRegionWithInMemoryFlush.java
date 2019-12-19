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
package org.apache.hadoop.hbase.regionserver;


import static org.apache.hadoop.hbase.HBaseTestingUtility.START_KEY;
import static org.apache.hadoop.hbase.HBaseTestingUtility.fam1;
import static org.apache.hadoop.hbase.HBaseTestingUtility.fam2;
import static org.apache.hadoop.hbase.HBaseTestingUtility.fam3;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.VerySlowRegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
/**
 * A test similar to TestHRegion, but with in-memory flush families.
 * Also checks wal truncation after in-memory compaction.
 */
@Category({VerySlowRegionServerTests.class, LargeTests.class})
public class TestHRegionWithInMemoryFlush extends TestHRegion {
  // Do not spin up clusters in here. If you need to spin up a cluster, do it
  // over in TestHRegionOnCluster.
  private static final Log LOG = LogFactory.getLog(TestHRegionWithInMemoryFlush.class);
  /**
   * @return A region on which you must call
   *         {@link HBaseTestingUtility#closeRegionAndWAL(HRegion)} when done.
   */
  @Override
  public HRegion initHRegion(byte[] tableName, byte[] startKey, byte[] stopKey,
      String callingMethod, Configuration conf, boolean isReadOnly, Durability durability,
      WAL wal, byte[]... families) throws IOException {
    boolean[] inMemory = new boolean[families.length];
    for(int i = 0; i < inMemory.length; i++) {
      inMemory[i] = true;
    }
    return TEST_UTIL.createLocalHRegionWithInMemoryFlags(tableName, startKey, stopKey,
        isReadOnly, durability, wal, inMemory, families);
  }

  /**
   * A test case of HBASE-21041
   * @throws Exception Exception
   */
  @Override
  @Test
  public void testFlushAndMemstoreSizeCounting() throws Exception {
    byte[] family = Bytes.toBytes("family");
    this.region = initHRegion(tableName, method, CONF, family);
    int count = 0;
    try {
      for (byte[] row : HBaseTestingUtility.ROWS) {
        Put put = new Put(row);
        put.addColumn(family, family, row);
        region.put(put);
        //In memory flush every 1000 puts
        if (count++ % 1000 == 0) {
          HStore store = (HStore)region.getStore(family);
          ((CompactingMemStore)store.memstore).flushInMemory();
        }
      }
      region.flush(true);
      // After flush, data size should be zero
      Assert.assertEquals(0, region.getMemStoreDataSize());
      // After flush, a new active mutable segment is created, so the heap size
      // should equal to MutableSegment.DEEP_OVERHEAD
      Assert.assertEquals(MutableSegment.DEEP_OVERHEAD, region.getMemStoreHeapSize());
      // After flush, offheap size should be zero
      Assert.assertEquals(0, region.getMemStoreOffHeapSize());

    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  /**
   * Splits twice and verifies getting from each of the split regions.
   *
   * @throws Exception
   */
  @Override
  public void testBasicSplit() throws Exception {
    byte[][] families = { fam1, fam2, fam3 };

    Configuration hc = initSplit();
    // Setting up region
    String method = this.getName();
    this.region = initHRegion(tableName, method, hc, families);

    try {
      LOG.info("" + HBaseTestCase.addContent(region, fam3));
      region.flush(true);
      region.compactStores();
      byte[] splitRow = region.checkSplit();
      assertNotNull(splitRow);
      LOG.info("SplitRow: " + Bytes.toString(splitRow));
      HRegion[] regions = splitRegion(region, splitRow);
      try {
        // Need to open the regions.
        // TODO: Add an 'open' to HRegion... don't do open by constructing
        // instance.
        for (int i = 0; i < regions.length; i++) {
          regions[i] = HRegion.openHRegion(regions[i], null);
        }
        // Assert can get rows out of new regions. Should be able to get first
        // row from first region and the midkey from second region.
        assertGet(regions[0], fam3, Bytes.toBytes(START_KEY));
        assertGet(regions[1], fam3, splitRow);
        // Test I can get scanner and that it starts at right place.
        assertScan(regions[0], fam3, Bytes.toBytes(START_KEY));
        assertScan(regions[1], fam3, splitRow);
        // Now prove can't split regions that have references.
        for (int i = 0; i < regions.length; i++) {
          // Add so much data to this region, we create a store file that is >
          // than one of our unsplitable references. it will.
          for (int j = 0; j < 2; j++) {
            HBaseTestCase.addContent(regions[i], fam3);
          }
          HBaseTestCase.addContent(regions[i], fam2);
          HBaseTestCase.addContent(regions[i], fam1);
          regions[i].flush(true);
        }

        byte[][] midkeys = new byte[regions.length][];
        // To make regions splitable force compaction.
        for (int i = 0; i < regions.length; i++) {
          regions[i].compactStores();
          midkeys[i] = regions[i].checkSplit();
        }

        TreeMap<String, HRegion> sortedMap = new TreeMap<String, HRegion>();
        // Split these two daughter regions so then I'll have 4 regions. Will
        // split because added data above.
        for (int i = 0; i < regions.length; i++) {
          HRegion[] rs = null;
          if (midkeys[i] != null) {
            rs = splitRegion(regions[i], midkeys[i]);
            for (int j = 0; j < rs.length; j++) {
              sortedMap.put(Bytes.toString(rs[j].getRegionInfo().getRegionName()),
                  HRegion.openHRegion(rs[j], null));
            }
          }
        }
        LOG.info("Made 4 regions");
      } finally {
        for (int i = 0; i < regions.length; i++) {
          try {
            regions[i].close();
          } catch (IOException e) {
            // Ignore.
          }
        }
      }
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }
}

