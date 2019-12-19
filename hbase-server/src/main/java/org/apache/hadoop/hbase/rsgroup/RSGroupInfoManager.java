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

package org.apache.hadoop.hbase.rsgroup;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.net.Address;
/**
 * Interface used to manage RSGroupInfo storage. An implementation
 * has the option to support offline mode.
 * See {@link RSGroupBasedLoadBalancer}
 */
@InterfaceAudience.Private
public interface RSGroupInfoManager {

  public static final String GROUPS_CONF_FILE = "hbase.rsgroup.conf.file";
  public static final String GROUPS_CONF_FILE_DEFAULT = "${hbase.tmp.dir}/rsgroups";
  public static final String DEFAULT_GROUP_NAME = "hbase.rsgroup.default.groupname";
  public static final String DEFAULT_GROUP_NAME_DEFAULT = "default";

  /**
   * Gets the group info of server.
   */
  RSGroupInfo getRSGroupOfServer(Address serverHostPort) throws IOException;

  /**
   * Gets {@code RSGroupInfo} for the given group name.
   */
  RSGroupInfo getRSGroup(String groupName) throws IOException;

  /**
   * Get the group membership of a table
   */
  String getRSGroupOfTable(TableName tableName) throws IOException;

  /**
   * List the existing {@code RSGroupInfo}s.
   */
  List<RSGroupInfo> listRSGroups() throws IOException;

  /**
   * Refresh/reload the group information from the persistent store
   */
  void refreshGroupServers() throws IOException;

  void moveTableToGroup(TableName table, String group) throws IOException;

  void moveServerToGroup(Address serverHostPort, String group) throws IOException;

  void removeTable(TableName tableName) throws IOException;
}
