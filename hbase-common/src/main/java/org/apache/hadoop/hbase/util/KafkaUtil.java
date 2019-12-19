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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class KafkaUtil {
  private static final Log LOG = LogFactory.getLog(KafkaUtil.class);
  public static final String KAFKA_BROKER_SERVERS = "kafka.bootstrap.servers";
  /**
   * Each HBase table corresponding to an KAFKA topic
   */
  public static String getTableTopic(String tableName) {
    return tableName;
  }

  public static String getTopicTable(String topicName) {
    return topicName;
  }

  /**
   * Each HBase Region corresponding to an KAFKA partition,
   * rowkey should contains partition info, split by '_'
   */
  public static int getTablePartition(String rowkey, int partitionCount) {
    return getPrefix(rowkey) % partitionCount;
  }

  public static int getPrefix(String rowkey) {
    if (rowkey == null || "".equals(rowkey)) {
      return 0;
    }
    try {
      int index = rowkey.indexOf("_");
      String prefix = (index == -1) ? rowkey : rowkey.substring(0, index);
      return Integer.parseInt(prefix);
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
