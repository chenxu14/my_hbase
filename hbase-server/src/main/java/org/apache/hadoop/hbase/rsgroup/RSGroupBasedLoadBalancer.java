package org.apache.hadoop.hbase.rsgroup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.balancer.BaseLoadBalancer;
import org.apache.hadoop.hbase.master.balancer.StochasticLoadBalancer;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.util.ReflectionUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * GroupBasedLoadBalancer, used when Region Server Grouping is configured (HBase-6721)
 * It does region balance based on a table's group membership.
 */
@InterfaceAudience.Private
public class RSGroupBasedLoadBalancer implements RSGroupableBalancer {
  private static final Log LOG = LogFactory.getLog(RSGroupBasedLoadBalancer.class);
  
  private Configuration config;
  private ClusterStatus clusterStatus;
  private MasterServices masterServices;
  private volatile RSGroupInfoManager rsGroupInfoManager;
  private LoadBalancer internalBalancer;
  private Map<String, LoadBalancer> specialBalancers;
  private ServerName masterServerName;
  private ExecutorService service;
  private boolean bulkBalance = false;
  private Set<String> excludeGroups = null;

  /**
   * Used by reflection in {@link org.apache.hadoop.hbase.master.balancer.LoadBalancerFactory}.
   */
  @InterfaceAudience.Private
  public RSGroupBasedLoadBalancer() {}

  @Override
  public void setClusterStatus(ClusterStatus st) {
    this.clusterStatus = st;
    if(internalBalancer != null) {
      internalBalancer.setClusterStatus(st);
    }
    if(specialBalancers != null) {
      for(LoadBalancer balancer : specialBalancers.values()) {
        balancer.setClusterStatus(st);
      }
    }
  }

  @Override
  public void setMasterServices(MasterServices masterServices) {
    this.masterServices = masterServices;
    this.masterServerName = masterServices.getServerName();
  }

  @Override
  public List<RegionPlan> balanceCluster(Map<ServerName, List<HRegionInfo>> clusterState) throws HBaseIOException {
    return balanceCluster(clusterState, false);
  }

  @Override
  public List<RegionPlan> balanceCluster(Map<ServerName, List<HRegionInfo>> clusterState,
      boolean mandatory) throws HBaseIOException {
    // process master regions
    List<RegionPlan> regionPlans = balanceMasterRegions(clusterState);
    if (regionPlans != null || clusterState == null || clusterState.size() <= 1) {
      return regionPlans;
    }
    if (masterServerName != null && clusterState.containsKey(masterServerName)) {
      if (clusterState.size() <= 2) {
        return null;
      }
      clusterState = new HashMap<ServerName, List<HRegionInfo>>(clusterState);
      clusterState.remove(masterServerName);
    }
    regionPlans = new ArrayList<RegionPlan>();
    Map<ServerName,List<HRegionInfo>> correctedState = correctAssignments(clusterState);
    try {
      Map<String, Map<ServerName, List<HRegionInfo>>> groupClusterStates = 
          new HashMap<String, Map<ServerName, List<HRegionInfo>>>(); // group -> (server -> regions)
      for(Map.Entry<ServerName,List<HRegionInfo>> entry : correctedState.entrySet()){ // server -> regions
        ServerName sName = entry.getKey();
        List<HRegionInfo> regions = entry.getValue();
        if(LoadBalancer.BOGUS_SERVER_NAME.equals(sName)){
          for (HRegionInfo regionInfo : regions) {
            regionPlans.add(new RegionPlan(regionInfo, null, null));
          }
          continue;
        }
        RSGroupInfo info = rsGroupInfoManager.getRSGroupOfServer(Address.fromString(sName.getHostAndPort()));
        Map<ServerName, List<HRegionInfo>> groupClusterState = groupClusterStates.get(info.getName());
        if(groupClusterState == null){
          groupClusterState = new HashMap<ServerName, List<HRegionInfo>>();
          groupClusterStates.put(info.getName(), groupClusterState);
        }
        groupClusterState.put(sName, regions);
      }
      // bulk balance
      if(bulkBalance && this.service != null){
        List<Future<List<RegionPlan>>> results = new ArrayList<Future<List<RegionPlan>>>(groupClusterStates.size());
        for(Map.Entry<String, Map<ServerName, List<HRegionInfo>>> info : groupClusterStates.entrySet()) {
          String groupName = info.getKey();
          if(groupClusterStates.size() > 1 // if size = 1, mandatory balance on the target group happen 
              && excludeGroups != null && excludeGroups.contains(groupName)) { // target group in exclude list
            LOG.info("bypass the balance, because target group " + groupName + " in the exclude list.");
            continue;
          }
          Map<ServerName, List<HRegionInfo>> groupClusterState = info.getValue();
          Future<List<RegionPlan>> res = service.submit(new Callable<List<RegionPlan>>(){
            @Override
            public List<RegionPlan> call() throws Exception {
              if (specialBalancers != null && specialBalancers.containsKey(groupName)) {
                LOG.info("use exclusive balancer, because target group " + groupName + " in the special list.");
                return specialBalancers.get(groupName).balanceCluster(groupClusterState, mandatory);	  
              } else {
                return internalBalancer.balanceCluster(groupClusterState, mandatory);
              }
            }
          });
          results.add(res);
        }
        for(Future<List<RegionPlan>> res : results) {
          List<RegionPlan> groupPlans = res.get();
          if (groupPlans != null) {
            regionPlans.addAll(groupPlans);
          }
        }
      } else {
        for(Map.Entry<String, Map<ServerName, List<HRegionInfo>>> info : groupClusterStates.entrySet()) {
          String groupName = info.getKey();
          if(groupClusterStates.size() > 1 // if size = 1, mandatory balance on the target group happen 
              && excludeGroups != null && excludeGroups.contains(groupName)) { // target group in exclude list
            LOG.info("bypass the balance, because target group " + groupName + " in the exclude list.");
            continue;
          }
          Map<ServerName, List<HRegionInfo>> groupClusterState = info.getValue();
          List<RegionPlan> groupPlans = null;
          if (specialBalancers != null && specialBalancers.containsKey(groupName)) {
            LOG.info("use exclusive balancer, because target group " + groupName + " in the special list.");
            groupPlans = specialBalancers.get(groupName).balanceCluster(groupClusterState, mandatory);
          } else {
            groupPlans = this.internalBalancer.balanceCluster(groupClusterState, mandatory);
          }
          if (groupPlans != null) {
            regionPlans.addAll(groupPlans);
          }
        }
      }
    } catch (Exception exp) {
      LOG.warn("Exception while balancing cluster.", exp);
      regionPlans.clear();
    }
    return regionPlans;
  }

  private List<RegionPlan> balanceMasterRegions(Map<ServerName, List<HRegionInfo>> clusterMap) {
    if (masterServerName == null || clusterMap == null || clusterMap.size() <= 1){ 
      return null;
    }
    List<RegionPlan> plans = null;
    List<HRegionInfo> regions = clusterMap.get(masterServerName);
    if (regions != null) {
      Iterator<ServerName> keyIt = null;
      for (HRegionInfo region: regions) {
        if (shouldBeOnMaster(region)) continue;

        // Find a non-master regionserver to host the region
        if (keyIt == null || !keyIt.hasNext()) {
          keyIt = clusterMap.keySet().iterator();
        }
        ServerName dest = keyIt.next();
        if (masterServerName.equals(dest)) {
          if (!keyIt.hasNext()) {
            keyIt = clusterMap.keySet().iterator();
          }
          dest = keyIt.next();
        }

        // Move this region away from the master regionserver
        RegionPlan plan = new RegionPlan(region, masterServerName, dest);
        if (plans == null) {
          plans = new ArrayList<RegionPlan>();
        }
        plans.add(plan);
      }
    }
    for (Map.Entry<ServerName, List<HRegionInfo>> server: clusterMap.entrySet()) {
      if (masterServerName.equals(server.getKey())) continue;
      for (HRegionInfo region: server.getValue()) {
        if (!shouldBeOnMaster(region)) continue;

        // Move this region to the master regionserver
        RegionPlan plan = new RegionPlan(region, server.getKey(), masterServerName);
        if (plans == null) {
          plans = new ArrayList<RegionPlan>();
        }
        plans.add(plan);
      }
    }
    return plans;
  }

  private Map<ServerName, List<HRegionInfo>> correctAssignments(Map<ServerName, List<HRegionInfo>> existingAssignments) {
    Map<ServerName, List<HRegionInfo>> correctAssignments = new TreeMap<ServerName, List<HRegionInfo>>();
    correctAssignments.put(LoadBalancer.BOGUS_SERVER_NAME, new LinkedList<HRegionInfo>());
    for (Map.Entry<ServerName, List<HRegionInfo>> assignments : existingAssignments.entrySet()){
      ServerName sName = assignments.getKey();
      correctAssignments.put(sName, new LinkedList<HRegionInfo>());
      List<HRegionInfo> regions = assignments.getValue();
      for (HRegionInfo region : regions) {
        RSGroupInfo info = null;
        try {
          info = rsGroupInfoManager.getRSGroup(rsGroupInfoManager.getRSGroupOfTable(region.getTable()));
        } catch (IOException exp) {
          LOG.info("RSGroup information null for region of table " + region.getTable(), exp);
        }
        if ((info == null) || (!info.containsServer(Address.fromString(sName.getHostAndPort())))) {
          correctAssignments.get(LoadBalancer.BOGUS_SERVER_NAME).add(region);
        } else {
          correctAssignments.get(sName).add(region);
        }
      }
    }
    return correctAssignments;
  }

  @Override
  public Map<ServerName, List<HRegionInfo>> roundRobinAssignment(List<HRegionInfo> regions, List<ServerName> servers)
      throws HBaseIOException {
    Map<ServerName, List<HRegionInfo>> assignments = assignMasterRegions(regions, servers);
    if (assignments != null && !assignments.isEmpty()) {
      servers = new ArrayList<ServerName>(servers);
      // Guarantee not to put other regions on master
      servers.remove(masterServerName);
      List<HRegionInfo> masterRegions = assignments.get(masterServerName);
      if (!masterRegions.isEmpty()) {
        regions = new ArrayList<HRegionInfo>(regions);
        for (HRegionInfo region: masterRegions) {
          regions.remove(region);
        }
      }
    }
    if (regions == null || regions.isEmpty()) {
      return assignments;
    }

    int numServers = servers == null ? 0 : servers.size();
    if (numServers == 0) {
      LOG.warn("Wanted to do round robin assignment but no servers to assign to");
      return null;
    }

    ListMultimap<String,HRegionInfo> regionMap = ArrayListMultimap.create(); // group -> regions
    ListMultimap<String,ServerName> serverMap = ArrayListMultimap.create();  // group -> servers
    generateGroupMaps(regions, servers, regionMap, serverMap);
    for(String groupKey : regionMap.keySet()) {
      if (regionMap.get(groupKey).size() > 0) {
        Map<ServerName, List<HRegionInfo>> result =
            this.internalBalancer.roundRobinAssignment(
                regionMap.get(groupKey),
                serverMap.get(groupKey));
        if(result != null) {
          if(result.containsKey(LoadBalancer.BOGUS_SERVER_NAME) && assignments.containsKey(LoadBalancer.BOGUS_SERVER_NAME)){
            assignments.get(LoadBalancer.BOGUS_SERVER_NAME).addAll(result.get(LoadBalancer.BOGUS_SERVER_NAME));
          }else{
            assignments.putAll(result);
          }
        }
      }
    }
    return assignments;
  }

  private void generateGroupMaps(List<HRegionInfo> regions, List<ServerName> servers,
      ListMultimap<String, HRegionInfo> regionMap, ListMultimap<String, ServerName> serverMap)  throws HBaseIOException {
    try {
      for (HRegionInfo region : regions) {
        String groupName = rsGroupInfoManager.getRSGroupOfTable(region.getTable());
        if (groupName == null) {
          LOG.warn("Group for table "+region.getTable()+" is null");
        }
        regionMap.put(groupName, region);
      }
      for (String groupKey : regionMap.keySet()) {
        RSGroupInfo info = rsGroupInfoManager.getRSGroup(groupKey);
        serverMap.putAll(groupKey, filterOfflineServers(info, servers));
        if(serverMap.get(groupKey).size() < 1) {
          serverMap.put(groupKey, LoadBalancer.BOGUS_SERVER_NAME);
        }
      }
    } catch(IOException e) {
      throw new HBaseIOException("Failed to generate group maps", e);
    }
  }

  private List<ServerName> filterOfflineServers(RSGroupInfo info, List<ServerName> onlineServers) {
    if (info != null) {
      return filterServers(info.getServers(), onlineServers);
    } else {
      LOG.warn("RSGroup Information found to be null. Some regions might be unassigned.");
      return Collections.emptyList();
    }
  }

  private List<ServerName> filterServers(Set<Address> servers, List<ServerName> onlineServers) {
    ArrayList<ServerName> finalList = new ArrayList<ServerName>();
    for (Address server : servers) {
      for(ServerName curr: onlineServers) {
        if(Address.fromString(curr.getHostAndPort()).equals(server)) {
          finalList.add(curr);
          break;
        }
      }
    }
    return finalList;
  }

  @Override
  public Map<ServerName, List<HRegionInfo>> retainAssignment(Map<HRegionInfo, ServerName> regions,
      List<ServerName> servers) throws HBaseIOException {
    try {
      Map<ServerName, List<HRegionInfo>> assignments = assignMasterRegions(regions.keySet(), servers);
      if (assignments != null && !assignments.isEmpty()) {
        servers = new ArrayList<ServerName>(servers);
        // Guarantee not to put other regions on master
        servers.remove(masterServerName);
        List<HRegionInfo> masterRegions = assignments.get(masterServerName);
        if (!masterRegions.isEmpty()) {
          regions = new HashMap<HRegionInfo, ServerName>(regions);
          for (HRegionInfo region: masterRegions) {
            regions.remove(region);
          }
        }
      }
      if (regions == null || regions.isEmpty()) {
        return assignments;
      }

      int numServers = servers == null ? 0 : servers.size();
      if (numServers == 0) {
        LOG.warn("Wanted to do retain assignment but no servers to assign to");
        return null;
      }

      ListMultimap<String, HRegionInfo> groupToRegion = ArrayListMultimap.create();
      Set<HRegionInfo> misplacedRegions = new HashSet<HRegionInfo>();

      for(Map.Entry<HRegionInfo, ServerName> region : regions.entrySet()) {
        HRegionInfo regionInfo = region.getKey();
        ServerName assignedServer = region.getValue();
        String group = rsGroupInfoManager.getRSGroupOfTable(regionInfo.getTable());
        RSGroupInfo info = rsGroupInfoManager.getRSGroup(group);
        if (assignedServer != null &&
            (info == null || !info.containsServer(Address.fromString(assignedServer.getHostAndPort())))) {
          misplacedRegions.add(regionInfo);
        } else {
          groupToRegion.put(group, regionInfo);
        }
      }

      // Now the "groupToRegion" map has only the regions which have correct
      // assignments.
      for (String key : groupToRegion.keySet()) {
        Map<HRegionInfo, ServerName> currentAssignmentMap = new TreeMap<HRegionInfo, ServerName>();
        List<HRegionInfo> regionList = groupToRegion.get(key);
        RSGroupInfo info = rsGroupInfoManager.getRSGroup(key);
        List<ServerName> candidateList = filterOfflineServers(info, servers);
        for (HRegionInfo region : regionList) {
          currentAssignmentMap.put(region, regions.get(region));
        }
        if(candidateList.size() > 0) {
          assignments.putAll(this.internalBalancer.retainAssignment(
              currentAssignmentMap, candidateList));
        }
      }
      for (HRegionInfo region : misplacedRegions) {
        String groupName = rsGroupInfoManager.getRSGroupOfTable(region.getTable());;
        RSGroupInfo info = rsGroupInfoManager.getRSGroup(groupName);
        List<ServerName> candidateList = filterOfflineServers(info, servers);
        ServerName server = this.internalBalancer.randomAssignment(region, candidateList);
        if (server != null) {
          if (!assignments.containsKey(server)) {
            assignments.put(server, new ArrayList<HRegionInfo>());
          }
          assignments.get(server).add(region);
        } else {
          //if not server is available assign to bogus so it ends up in RIT
          if(!assignments.containsKey(LoadBalancer.BOGUS_SERVER_NAME)) {
            assignments.put(LoadBalancer.BOGUS_SERVER_NAME, new ArrayList<HRegionInfo>());
          }
          assignments.get(LoadBalancer.BOGUS_SERVER_NAME).add(region);
        }
      }
      return assignments;
    } catch (IOException e) {
      throw new HBaseIOException("Failed to do online retain assignment", e);
    }
  }

  private Map<ServerName, List<HRegionInfo>> assignMasterRegions(Collection<HRegionInfo> regions, List<ServerName> servers) {
    Map<ServerName, List<HRegionInfo>> assignments = new TreeMap<ServerName, List<HRegionInfo>>();
    if (servers == null || regions == null || regions.isEmpty()) {
      return assignments;
    }
    if (masterServerName != null && servers.contains(masterServerName)) {
      assignments.put(masterServerName, new ArrayList<HRegionInfo>());
      for (HRegionInfo region: regions) {
        if (shouldBeOnMaster(region)) {
          assignments.get(masterServerName).add(region);
        }
      }
    }
    return assignments;
  }

  @Override
  public Map<HRegionInfo, ServerName> immediateAssignment(List<HRegionInfo> regions, List<ServerName> servers)
      throws HBaseIOException {
    return null;
  }

  @Override
  public ServerName randomAssignment(HRegionInfo region, List<ServerName> servers) throws HBaseIOException {
    if (servers != null && servers.contains(masterServerName)) {
      if (shouldBeOnMaster(region)) {
        return masterServerName;
      }
      servers = new ArrayList<>(servers);
      // Guarantee not to put other regions on master
      servers.remove(masterServerName);
    }
    int numServers = servers == null ? 0 : servers.size();
    if (numServers == 0) {
      LOG.warn("Wanted to retain assignment but no servers to assign to");
      return null;
    }

    ListMultimap<String,HRegionInfo> regionMap = LinkedListMultimap.create();
    ListMultimap<String,ServerName> serverMap = LinkedListMultimap.create();
    generateGroupMaps(Lists.newArrayList(region), servers, regionMap, serverMap);
    List<ServerName> filteredServers = serverMap.get(regionMap.keySet().iterator().next());
    ServerName res = this.internalBalancer.randomAssignment(region, filteredServers);
    return LoadBalancer.BOGUS_SERVER_NAME.equals(res) ? null : res;
  }

  private boolean shouldBeOnMaster(HRegionInfo region) {
    String[] tables = BaseLoadBalancer.getTablesOnMaster(config);
    return Arrays.asList(tables).contains(region.getTable().getNameAsString()) 
        && region.getReplicaId() == HRegionInfo.DEFAULT_REPLICA_ID;
  }

  @Override
  public void initialize() throws HBaseIOException {
    try {
      if (rsGroupInfoManager == null) {
        List<RSGroupAdminEndpoint> cps = masterServices.getMasterCoprocessorHost().findCoprocessors(RSGroupAdminEndpoint.class);
        if (cps.size() != 1) {
          String msg = "Expected one implementation of GroupAdminEndpoint but found " + cps.size();
          LOG.error(msg);
          throw new HBaseIOException(msg);
        }
        rsGroupInfoManager = cps.get(0).getGroupInfoManager();
      }
    } catch (IOException e) {
      throw new HBaseIOException("Failed to initialize GroupInfoManagerImpl", e);
    }
    // Create the balancer
    Class<? extends LoadBalancer> balancerKlass = config.getClass(HBASE_RSGROUP_LOADBALANCER_CLASS,
        StochasticLoadBalancer.class, LoadBalancer.class);

    String[] speicalGroups = config.getStrings(HBASE_RSGROUP_LOADBALANCER_SPECIAL_GROUPS);
    if (speicalGroups != null && speicalGroups.length > 0) {
      LOG.info("groups in special list : " + config.get(HBASE_RSGROUP_LOADBALANCER_SPECIAL_GROUPS));
      this.specialBalancers = new HashMap<String,LoadBalancer>();
      for(String group : speicalGroups){
        Class<? extends LoadBalancer> groupBalancer =
            config.getClass(HBASE_RSGROUP_LOADBALANCER_CLASS + "." + group, balancerKlass, LoadBalancer.class);
        LoadBalancer tempBalancer = ReflectionUtils.newInstance(groupBalancer, config);
        tempBalancer.setMasterServices(masterServices);
        tempBalancer.setClusterStatus(clusterStatus);
        tempBalancer.setOwner(group);
        tempBalancer.setConf(config);
        tempBalancer.initialize();
        specialBalancers.put(group, tempBalancer);
      }
    }

    internalBalancer = ReflectionUtils.newInstance(balancerKlass, config);
    internalBalancer.setMasterServices(masterServices);
    internalBalancer.setClusterStatus(clusterStatus);
    internalBalancer.setConf(config);
    internalBalancer.initialize();
  }

  @VisibleForTesting
  public void setRsGroupInfoManager(RSGroupInfoManager rsGroupInfoManager) {
    this.rsGroupInfoManager = rsGroupInfoManager;
  }

  public RSGroupInfoManager getRsGroupInfoManager() {
    return rsGroupInfoManager;
  }

  @Override
  public void regionOnline(HRegionInfo regionInfo, ServerName sn) {
  }

  @Override
  public void regionOffline(HRegionInfo regionInfo) {
  }

  @Override
  public void onConfigurationChange(Configuration conf) {
    setConf(conf);
  }

  @Override
  public synchronized void setConf(Configuration conf) {
    this.config = conf;
    this.bulkBalance = this.config.getBoolean(HBASE_RSGROUP_BULK_BALANCE, false);
    LOG.info("bulk balance flag is " + bulkBalance);
    excludeGroups = new HashSet<String>();
    String[] groups = config.getStrings(HBASE_RSGROUP_LOADBALANCER_EXCLUDE);
    if(groups != null && groups.length > 0) {
      LOG.info("groups that exclude from default balance are : " + config.get(HBASE_RSGROUP_LOADBALANCER_EXCLUDE));
      for(String group : groups) {
        excludeGroups.add(group);
      }
    }
    if(bulkBalance && service == null){
      service = Executors.newCachedThreadPool(
        new ThreadFactoryBuilder().setNameFormat("BalanceGroup #%d").build());
    } else {
      service = null;
    }
    if(internalBalancer != null) {
      internalBalancer.setConf(conf);
    }
    if(specialBalancers != null) {
      for(LoadBalancer balancer : specialBalancers.values()) {
        balancer.setConf(conf);
      }
    }
  }

  @Override
  public Configuration getConf() {
    return config;
  }

  @Override
  public void stop(String why) {
  }

  @Override
  public boolean isStopped() {
    return false;
  }

  @Override
  public void setOwner(String group) {
    throw new UnsupportedOperationException("RSGroupBasedLoadBalancer not support setOwner!");
  }
}