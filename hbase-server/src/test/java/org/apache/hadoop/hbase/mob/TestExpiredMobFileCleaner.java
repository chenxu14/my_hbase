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
package org.apache.hadoop.hbase.mob;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Date;
import java.util.Random;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestExpiredMobFileCleaner {

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final static TableName tableName = TableName.valueOf("TestExpiredMobFileCleaner");
  private final static String family = "family";
  private final static byte[] row1 = Bytes.toBytes("row1");
  private final static byte[] row2 = Bytes.toBytes("row2");
  private final static byte[] row3 = Bytes.toBytes("row3");
  private final static byte[] qf = Bytes.toBytes("qf");

  private static BufferedMutator table;
  private static Admin admin;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt("hbase.master.info.port", 0);
    TEST_UTIL.getConfiguration().setBoolean("hbase.regionserver.info.port.auto", true);

    TEST_UTIL.getConfiguration().setInt("hfile.format.version", 3);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

  }

  @Before
  public void setUp() throws Exception {
    TEST_UTIL.startMiniCluster(1);
    init();
  }

  @After
  public void tearDown() throws Exception {
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
    admin.close();
    TEST_UTIL.shutdownMiniCluster();
    TEST_UTIL.getTestFileSystem().delete(TEST_UTIL.getDataTestDir(), true);
  }

  private void init() throws Exception {
    HTableDescriptor desc = new HTableDescriptor(tableName);
    HColumnDescriptor hcd = new HColumnDescriptor(family);
    hcd.setMobEnabled(true);
    hcd.setMobThreshold(3L);
    hcd.setMaxVersions(4);
    desc.addFamily(hcd);

    admin = TEST_UTIL.getHBaseAdmin();
    admin.createTable(desc);
    table = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration())
            .getBufferedMutator(tableName);
  }

  private void truncateTable() throws IOException{
    admin.disableTable(tableName);
    admin.truncateTable(tableName, false);
  }

  private void modifyColumnExpiryDays(int expireDays) throws Exception {
    HColumnDescriptor hcd = new HColumnDescriptor(family);
    hcd.setMobEnabled(true);
    hcd.setMobThreshold(3L);
    // change ttl as expire days to make some row expired
    int timeToLive = expireDays * secondsOfDay();
    hcd.setTimeToLive(timeToLive);

    admin.modifyColumn(tableName, hcd);
  }

  private void putKVAndFlush(BufferedMutator table, byte[] row, byte[] value, long ts)
      throws Exception {

    Put put = new Put(row, ts);
    put.addColumn(Bytes.toBytes(family), qf, value);
    table.mutate(put);

    table.flush();
    admin.flush(tableName);
  }

  @Test
  public void testMobFileTTLClean() throws Exception {
    truncateTable();
    Path mobDirPath = MobUtils.getMobFamilyPath(TEST_UTIL.getConfiguration(), tableName, family);
    byte[] dummyData = makeDummyData(600);
    long ts = System.currentTimeMillis() - 3 * secondsOfDay() * 1000; // 3 days before
    Path expirePath = new Path(mobDirPath, MobUtils.formatDate(new Date(ts)));
    putKVAndFlush(table, row1, dummyData, ts);
    FileStatus[] files = TEST_UTIL.getTestFileSystem().listStatus(mobDirPath);
    //the first mob file
    assertEquals("Before cleanup without delay 1", 1, files.length);

    ts = System.currentTimeMillis() - 2 * secondsOfDay() * 1000; // 2 day before
    putKVAndFlush(table, row2, dummyData, ts);
    files = TEST_UTIL.getTestFileSystem().listStatus(mobDirPath);
    //now there are 2 mob files
    assertEquals("Before cleanup without delay 2", 2, files.length);
    //the third mob file
    ts = System.currentTimeMillis() - 1 * secondsOfDay() * 1000; // 1 day before
    putKVAndFlush(table, row3, dummyData, ts);
    files = TEST_UTIL.getTestFileSystem().listStatus(mobDirPath);
    assertEquals("Before cleanup without delay 3", 3, files.length);

    modifyColumnExpiryDays(2); // ttl = 2, make the first row expired

    //run the cleaner
    String[] args = new String[2];
    args[0] = tableName.getNameAsString();
    args[1] = family;
    ToolRunner.run(TEST_UTIL.getConfiguration(), new ExpiredMobFileCleaner(), args);

    files = TEST_UTIL.getTestFileSystem().listStatus(expirePath);
    //the first mob fie is removed
    assertEquals("After cleanup without delay 1", 0, files.length);
  }

  private int secondsOfDay() {
    return 24 * 3600;
  }

  private byte[] makeDummyData(int size) {
    byte [] dummyData = new byte[size];
    new Random().nextBytes(dummyData);
    return dummyData;
  }
}