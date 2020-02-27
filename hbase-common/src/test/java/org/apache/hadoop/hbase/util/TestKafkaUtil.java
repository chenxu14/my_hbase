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
package org.apache.hadoop.hbase.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestKafkaUtil {
  @Test
  public void testGetTablePartition() {
    assertEquals(1, KafkaUtil.getTablePartition("1247_rowkey", 10));
    assertEquals(12, KafkaUtil.getTablePartition("1247_rowkey", 100));
    assertEquals(124, KafkaUtil.getTablePartition("1247_rowkey", 1000));
    assertEquals(1247, KafkaUtil.getTablePartition("1247_rowkey", 10000));

    // partition count is 100
    for (int i = 0; i < 10000; i++) {
      String rowkey = String.format("%04d", i);
      int partition = KafkaUtil.getTablePartition(rowkey, 100);
      if (partition > 9) {
        assertTrue(rowkey.startsWith(String.valueOf(partition)));
      } else {
        assertTrue(rowkey.startsWith("0" + String.valueOf(partition)));
      }
    }

    // partition count is 200
    Map<Integer, Integer> partInfo = new HashMap<>();
    int interval = 10000 / 200;
    for (int i = 0; i < 200; i++) {
      partInfo.put(i, i * interval);
    }
    partInfo.put(200, Integer.MAX_VALUE);
    for (int i = 0; i < 10000; i++) {
      String rowkey = String.format("%04d", i);
      int partition = KafkaUtil.getTablePartition(rowkey, 200);
      assertTrue(partInfo.get(partition) <= i && i < partInfo.get(partition + 1));
    }
  }

  @Test
  public void testGetConsumerGroup() throws UnknownHostException {
    String table = "usertable";
    String hostname = InetAddress.getLocalHost().getHostName();
    String groupName = "connect-" + table + "-" + hostname.substring(0, hostname.indexOf("-"));
    assertEquals(groupName, KafkaUtil.getConsumerGroup("usertable"));
  }

  @Test
  public void testConstrains() {
    int[] partitionCnt = new int[]{1,2,5,10,20,50,100,200,500,1000,2000,5000};
    Set<Integer> partitions;
    for (int cnt : partitionCnt) {
      partitions = new HashSet<>();
      for (int i = 0; i < KafkaUtil.PART_UPPER_LIMMIT; i++) {
        if (i % (KafkaUtil.PART_UPPER_LIMMIT / cnt) == 0) {
          String startkey = String.format("%04d", i);
          partitions.add(KafkaUtil.getTablePartition(startkey, cnt));
        }
      }
      assertEquals(cnt, partitions.size());
    }
  }

  public static void main(String[] args) {
    int regionCount = 5000;
    for (int i = 0; i < 10000; i++) {
      String rowkey = String.format("%04d", i);
      if (i % (10000 / regionCount) == 0) {
        System.out.println("rowkey : " + rowkey);
        System.out.println("partition : " + KafkaUtil.getTablePartition(rowkey, regionCount));
      }
    }
  }
}
