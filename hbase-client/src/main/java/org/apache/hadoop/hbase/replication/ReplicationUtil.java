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
package org.apache.hadoop.hbase.replication;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ReplicationUtil {
  private static final Log LOG = LogFactory.getLog(ReplicationUtil.class);
  public static final String GROUP_PREFIX = "[GROUP]";
  public static boolean targetPeerNotBelongTo(String peerId, String group) {
    return group != null && !peerId.startsWith(GROUP_PREFIX + group);
  }

  public static String getGroupNameOfServer(ZooKeeperWatcher zkCli, ServerName sn) {
    byte[] data = null;
    try {
      String groupServersZNode = zkCli.baseZNode + "/rsgroup/servers/" + sn.getHostAndPort();
      if(ZKUtil.checkExists(zkCli, groupServersZNode) != -1) {
        data = ZKUtil.getDataNoWatch(zkCli, groupServersZNode, null);
      }
    } catch (KeeperException e) {
      LOG.error(e.getMessage(), e);
    }
    String group = data == null ? null : Bytes.toString(data);
    return group;
  }
}
