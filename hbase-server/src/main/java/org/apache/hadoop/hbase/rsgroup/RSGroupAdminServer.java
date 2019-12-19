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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.constraint.ConstraintException;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.RegionStates;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.shaded.com.google.common.collect.Maps;

/**
 * Service to support Region Server Grouping (HBase-6721).
 */
@InterfaceAudience.Private
public class RSGroupAdminServer implements RSGroupAdmin {
  private static final Log LOG = LogFactory.getLog(RSGroupAdminServer.class);

  private MasterServices master;
  private final RSGroupInfoManager rsGroupInfoManager;

  public RSGroupAdminServer(MasterServices master, RSGroupInfoManager rsGroupInfoManager)
      throws IOException {
    this.master = master;
    this.rsGroupInfoManager = rsGroupInfoManager;
  }

  @Override
  public RSGroupInfo getRSGroupInfo(String groupName) throws IOException {
    return rsGroupInfoManager.getRSGroup(groupName);
  }

  @Override
  public RSGroupInfo getRSGroupInfoOfTable(TableName tableName) throws IOException {
    // We are reading across two Maps in the below with out synchronizing across
    // them; should be safe most of the time.
    String groupName = rsGroupInfoManager.getRSGroupOfTable(tableName);
    return groupName == null? null: rsGroupInfoManager.getRSGroup(groupName);
  }

  @Override
  public boolean balanceRSGroup(String groupName) throws IOException {
    ServerManager serverManager = master.getServerManager();
    AssignmentManager assignmentManager = master.getAssignmentManager();
    LoadBalancer balancer = master.getAssignmentManager().getBalancer();

    boolean balancerRan;
    synchronized (balancer) {
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().preBalanceRSGroup(groupName);
      }
      if (getRSGroupInfo(groupName) == null) {
        throw new ConstraintException("RSGroup does not exist: "+groupName);
      }
      // Only allow one balance run at a time.
      Map<String, RegionState> groupRIT = rsGroupGetRegionsInTransition(groupName);
      if (groupRIT.size() > 0) {
        LOG.debug("Not running balancer because " + groupRIT.size() + " region(s) in transition: " +
          StringUtils.abbreviate(
              master.getAssignmentManager().getRegionStates().getRegionsInTransition().toString(),
              256));
        return false;
      }
      if (serverManager.areDeadServersInProgress()) {
        LOG.debug("Not running balancer because processing dead regionserver(s): " +
            serverManager.getDeadServers());
        return false;
      }

      // We balance per group instead of per table
      List<RegionPlan> plans = balancer.balanceCluster(getAssignmentsByGroup(groupName), true);
      long startTime = System.currentTimeMillis();
      balancerRan = plans != null;
      if (plans != null && !plans.isEmpty()) {
        LOG.info("RSGroup balance " + groupName + " starting with plan count: " + plans.size());
        for (RegionPlan plan: plans) {
          LOG.info("balance " + plan);
          assignmentManager.balance(plan);
        }
        LOG.info("RSGroup balance " + groupName + " completed after "
            + (System.currentTimeMillis() - startTime) + " seconds");
      }
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().postBalanceRSGroup(groupName, balancerRan);
      }
    }
    return balancerRan;
  }

  @Override
  public List<RSGroupInfo> listRSGroups() throws IOException {
    return rsGroupInfoManager.listRSGroups();
  }

  @Override
  public RSGroupInfo getRSGroupOfServer(Address hostPort) throws IOException {
    return rsGroupInfoManager.getRSGroupOfServer(hostPort);
  }

  private Map<String, RegionState> rsGroupGetRegionsInTransition(String groupName)
      throws IOException {
    Map<String, RegionState> rit = Maps.newTreeMap();
    AssignmentManager am = master.getAssignmentManager();
    for(TableName tableName : getRSGroupInfo(groupName).getTables()) {
      for(HRegionInfo regionInfo: am.getRegionStates().getRegionsOfTable(tableName)) {
        RegionState state = am.getRegionStates().getRegionTransitionState(regionInfo);
        if(state != null) {
          rit.put(regionInfo.getEncodedName(), state);
        }
      }
    }
    return rit;
  }

  private Map<ServerName, List<HRegionInfo>> getAssignmentsByGroup(String groupName) throws IOException {
    Map<ServerName, List<HRegionInfo>> result = Maps.newHashMap();
    RSGroupInfo rsGroupInfo = getRSGroupInfo(groupName);
    Set<Address> groupServers = rsGroupInfo.getServers();
    RegionStates states = master.getAssignmentManager().getRegionStates();
    for(ServerName serverName: master.getServerManager().getOnlineServers().keySet()) {
      if(groupServers.contains(Address.fromString(serverName.getHostAndPort()))) { // belong to our group
        List<HRegionInfo> regions = new ArrayList<HRegionInfo>();
        Set<HRegionInfo> regionsOnSn = states.getServerRegions(serverName);
        if(regionsOnSn != null) {
          regions.addAll(regionsOnSn);
        }
        result.put(serverName, regions);
      }
    }
    return result;
  }

  @Override
  public void refreshGroupServers() throws IOException {
    if (master.getMasterCoprocessorHost() != null) {
      master.getMasterCoprocessorHost().preRefreshGroupServers();
    }
    rsGroupInfoManager.refreshGroupServers();
    if (master.getMasterCoprocessorHost() != null) {
      master.getMasterCoprocessorHost().postRefreshGroupServers();
    }
  }

  @Override
  public void moveTableToGroup(TableName tableName, String groupName) throws IOException {
    if (master.getMasterCoprocessorHost() != null) {
      master.getMasterCoprocessorHost().preMoveTableToGroup(tableName, groupName);
    }
    rsGroupInfoManager.moveTableToGroup(tableName, groupName);
    if (master.getMasterCoprocessorHost() != null) {
      master.getMasterCoprocessorHost().postMoveTableToGroup(tableName, groupName);
    }
  }

  @Override
  public void moveServerToGroup(Address address, String groupName) throws IOException {
    rsGroupInfoManager.moveServerToGroup(address, groupName);
  }
}
