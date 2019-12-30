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
package org.apache.hadoop.hbase.util;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class KafkaUtil {
  private static final Log LOG = LogFactory.getLog(KafkaUtil.class);
  public static final String KAFKA_BROKER_SERVERS = "kafka.bootstrap.servers";

  private static final String LOCAL_ZONE;
  static {
    String hostname = null;
    try {
      hostname = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }
    int index = hostname == null ? -1 : hostname.indexOf("-");
    LOCAL_ZONE = index == -1 ? "rz" : hostname.substring(0, index);
  }

  /**
   * Each HBase table corresponding to an KAFKA topic
   */
  public static String getTableTopic(String tableName) {
    return tableName;
  }

  public static String getConsumerGroup(String tableName) {
    return new StringBuilder("connect-").append(tableName).append("-").append(LOCAL_ZONE).toString();
  }

  public static String getTopicTable(String topicName) {
    return topicName;
  }

  /**
   * return the partition this row will go to
   * Each HBase Region corresponding to an KAFKA partition,
   * rowkey should contains partition info, split by '_'
   */
  public static int getTablePartition(String rowkey, int partitionCount) {
    String cntStr = String.valueOf(partitionCount);
    int len = cntStr.length();
    assert(len <= 5); // partitionCount's upper limit is 10000, len is 5
    int interval = (partitionCount == Math.pow(10, len-1)) ? 1 : (int) Math.pow(10, len) / partitionCount;
    int prefix = getPrefix(rowkey);
    if (interval == 1) { // power of 10
      return prefix / (int) Math.pow(10, 5 - len);
    } else {
      String preStr = String.format("%04d", prefix).substring(0, len);
      int value = Integer.parseInt(preStr) / interval;
      return value;
    }
  }

  public static int getPrefix(String rowkey) {
    if (rowkey == null || "".equals(rowkey)) {
      return 0;
    }
    try {
      int index = rowkey.indexOf("_");
      String prefix = (index == -1) ? rowkey : rowkey.substring(0, index);
      for (int i = prefix.length(); i < 4; i++) { // rowkey has 4 prefix
        prefix += "0";
      }
      return Integer.parseInt(prefix.substring(0, 4)); // only retain 4 num
    } catch (NumberFormatException e) {
      LOG.warn(e.getMessage(), e);
      return 0;
    }
  }

  static int DJBHash(String rowkey) {
    int hash = 5381;
    for (int i = 0; i < rowkey.length(); i++) {
      hash = ((hash << 5) + hash) + rowkey.charAt(i);
    }
    return (hash & 0x7FFFFFFF);
  }
}
