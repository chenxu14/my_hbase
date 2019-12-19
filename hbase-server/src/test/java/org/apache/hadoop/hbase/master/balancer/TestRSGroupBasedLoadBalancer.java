package org.apache.hadoop.hbase.master.balancer;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.rsgroup.RSGroupBasedLoadBalancer;
import org.apache.hadoop.hbase.rsgroup.RSGroupInfo;
import org.apache.hadoop.hbase.rsgroup.RSGroupInfoManager;
import org.apache.hadoop.hbase.rsgroup.RSGroupInfoManagerImpl;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.balancer.SimpleLoadBalancer;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@Category(SmallTests.class)
public class TestRSGroupBasedLoadBalancer {

  private static final Log LOG = LogFactory.getLog(TestRSGroupBasedLoadBalancer.class);
  private static HBaseTestingUtility TEST_UTIL;
  private static RSGroupBasedLoadBalancer loadBalancer;
  private static RSGroupInfoManager infoMgr;
  private static SecureRandom rand;

  static TableName[] tables =
      new TableName[] { TableName.valueOf("table01"),
          TableName.valueOf("table02"),
          TableName.valueOf("table03"),
          TableName.valueOf("table04"),
          TableName.valueOf("table05")};
  static List<ServerName> servers;
  static Map<TableName, String> tableMap;
  static List<HTableDescriptor> tableDescs;
  int[] regionAssignment = new int[] { 2, 5, 7, 10, 4, 3, 1 };
  static int regionId = 0;

  @BeforeClass
  public static void beforeAllTests() throws Exception {
    rand = new SecureRandom();
    tableDescs = Lists.newArrayList();
    tableDescs.add(new HTableDescriptor(TableName.valueOf("table01")));
    tableDescs.add(new HTableDescriptor(TableName.valueOf("table02")));
    tableDescs.add(new HTableDescriptor(TableName.valueOf("table03")));
    tableDescs.add(new HTableDescriptor(TableName.valueOf("table04")));
    tableDescs.add(new HTableDescriptor(TableName.valueOf("table05")));
    
    servers = Lists.newArrayList();
    servers.add(ServerName.valueOf("host01.test.com", 16020, -1));
    servers.add(ServerName.valueOf("host02.test.com", 16020, -1));
    servers.add(ServerName.valueOf("host03.test.com", 16020, -1));
    servers.add(ServerName.valueOf("host04.test.com", 16020, -1));
    servers.add(ServerName.valueOf("host05.test.com", 16020, -1));
    servers.add(ServerName.valueOf("host06.test.com", 16020, -1));
    servers.add(ServerName.valueOf("host07.test.com", 16020, -1));
    
    TEST_UTIL = new HBaseTestingUtility();
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set("hbase.balancer.tablesOnMaster", "hbase:acl,hbase:namespace,hbase:meta,hbase:quota");
    conf.set("hbase.assignment.usezk", "false");
    conf.set("hbase.regions.slop", "0");
    conf.setBoolean("hbase.rsgroup.bulk.balance", false);
    conf.set("hbase.rsgroup.grouploadbalancer.class", SimpleLoadBalancer.class.getCanonicalName());

    RecoverableZooKeeper zkcli = Mockito.mock(RecoverableZooKeeper.class);
    ZooKeeperWatcher zkWatcher = Mockito.mock(ZooKeeperWatcher.class);
    zkWatcher.baseZNode = "/hbase";

    when(zkcli.getChildren(Mockito.any(String.class), Mockito.any(Watcher.class)))
      .thenAnswer(new Answer<List<String>>() {
        public List<String> answer(InvocationOnMock invocation) throws Throwable {
          String path = invocation.getArgumentAt(0, String.class);
          List<String> childs = new ArrayList<String>();
          if ("/hbase/rsgroup/tables".equals(path)) {
            childs.add("table01");
            childs.add("table02");
            childs.add("table03");
            childs.add("table04");
            childs.add("table05");
          } else if ("/hbase/rsgroup/servers".equals(path)) {
            childs.add("host01.test.com:16020");
            childs.add("host02.test.com:16020");
            childs.add("host03.test.com:16020");
            childs.add("host04.test.com:16020");
            childs.add("host05.test.com:16020");
            childs.add("host06.test.com:16020");
            childs.add("host07.test.com:16020");
          }
          return childs;
        }
      });

    when(zkcli.getData(Mockito.any(String.class), Mockito.any(Watcher.class), Mockito.any(Stat.class)))
      .thenAnswer(new Answer<byte[]>() {
        public byte[] answer(InvocationOnMock invocation) throws Throwable {
          String path = invocation.getArgumentAt(0, String.class);
          byte[] res = null;
          if ("/hbase/rsgroup/servers/host01.test.com:16020".equals(path)) {
            res = Bytes.toBytes("default");
          } else if ("/hbase/rsgroup/servers/host02.test.com:16020".equals(path)) {
            res = Bytes.toBytes("default");
          } else if ("/hbase/rsgroup/servers/host03.test.com:16020".equals(path)) {
            res = Bytes.toBytes("test");
          } else if ("/hbase/rsgroup/servers/host04.test.com:16020".equals(path)) {
            res = Bytes.toBytes("test");
          } else if ("/hbase/rsgroup/servers/host05.test.com:16020".equals(path)) {
            res = Bytes.toBytes("produce");
          } else if ("/hbase/rsgroup/servers/host06.test.com:16020".equals(path)) {
            res = Bytes.toBytes("produce");
          } else if ("/hbase/rsgroup/servers/host07.test.com:16020".equals(path)) {
            res = Bytes.toBytes("produce");
          } else if ("/hbase/rsgroup/tables/table01".equals(path)) {
            res = Bytes.toBytes("default");
          } else if ("/hbase/rsgroup/tables/table02".equals(path)) {
            res = Bytes.toBytes("default");
          } else if ("/hbase/rsgroup/tables/table03".equals(path)) {
            res = Bytes.toBytes("test");
          } else if ("/hbase/rsgroup/tables/table04".equals(path)) {
            res = Bytes.toBytes("produce");
          } else if ("/hbase/rsgroup/tables/table05".equals(path)) {
            res = Bytes.toBytes("produce");
          }
          return res;
        }
      });
    when(zkWatcher.getRecoverableZooKeeper()).thenReturn(zkcli);
    infoMgr = new RSGroupInfoManagerImpl(conf, zkWatcher);
    loadBalancer = new RSGroupBasedLoadBalancer();
    loadBalancer.setRsGroupInfoManager(infoMgr);
    loadBalancer.setMasterServices(getMockedMaster());
    loadBalancer.setConf(conf);
    loadBalancer.initialize();
  }

  /**
   * Test the load balancing algorithm.
   *
   * Invariant is that all servers of the group should be hosting either floor(average) or
   * ceiling(average)
   *
   * @throws Exception
   */
  @Test
  public void testBalanceCluster() throws Exception {
    Map<ServerName, List<HRegionInfo>> servers = mockClusterServers();
    ArrayListMultimap<String, ServerAndLoad> list = convertToGroupBasedMap(servers);
    LOG.info("Mock Cluster :  " + printStats(list));
    List<RegionPlan> plans = loadBalancer.balanceCluster(servers);
    ArrayListMultimap<String, ServerAndLoad> balancedCluster = reconcile(list, plans);
    LOG.info("Mock Balance : " + printStats(balancedCluster));
    assertClusterAsBalanced(balancedCluster);
  }

  /**
   * Invariant is that all servers of a group have load between floor(avg) and
   * ceiling(avg) number of regions.
   */
  private void assertClusterAsBalanced(
      ArrayListMultimap<String, ServerAndLoad> groupLoadMap) {
    for (String gName : groupLoadMap.keySet()) {
      List<ServerAndLoad> groupLoad = groupLoadMap.get(gName);
      int numServers = groupLoad.size();
      int numRegions = 0;
      int maxRegions = 0;
      int minRegions = Integer.MAX_VALUE;
      for (ServerAndLoad server : groupLoad) {
        int nr = server.getLoad();
        if (nr > maxRegions) {
          maxRegions = nr;
        }
        if (nr < minRegions) {
          minRegions = nr;
        }
        numRegions += nr;
      }
      if (maxRegions - minRegions < 2) {
        // less than 2 between max and min, can't balance
        return;
      }
      int min = numRegions / numServers;
      int max = numRegions % numServers == 0 ? min : min + 1;

      for (ServerAndLoad server : groupLoad) {
        assertTrue(server.getLoad() <= max);
        assertTrue(server.getLoad() >= min);
      }
    }
  }

  /**
   * Tests the bulk assignment used during cluster startup.
   *
   * Round-robin. Should yield a balanced cluster so same invariant as the
   * load balancer holds, all servers holding either floor(avg) or
   * ceiling(avg).
   *
   * @throws Exception
   */
  @Test
  public void testBulkAssignment() throws Exception {
    List<HRegionInfo> regions = randomRegions(25);
    Map<ServerName, List<HRegionInfo>> assignments = loadBalancer.roundRobinAssignment(regions, servers);
    //test regular scenario
    assertTrue(assignments.keySet().size() == servers.size());
    for (ServerName sn : assignments.keySet()) {
      List<HRegionInfo> regionAssigned = assignments.get(sn);
      for (HRegionInfo region : regionAssigned) {
        TableName tableName = region.getTable();
        String groupName = infoMgr.getRSGroupOfTable(tableName);
        assertTrue(StringUtils.isNotEmpty(groupName));
        RSGroupInfo gInfo = infoMgr.getRSGroup(groupName);
        assertTrue("Region is not correctly assigned to group servers.",
          gInfo.containsServer(Address.fromString(sn.getHostAndPort())));
      }
    }
    ArrayListMultimap<String, ServerAndLoad> loadMap = convertToGroupBasedMap(assignments);
    assertClusterAsBalanced(loadMap);
  }

  /**
   * Test the cluster startup bulk assignment which attempts to retain
   * assignment info.
   *
   * @throws Exception
   */
  @Test
  public void testRetainAssignment() throws Exception {
    // Test simple case where all same servers are there
    Map<ServerName, List<HRegionInfo>> currentAssignments = mockClusterServers();
    Map<HRegionInfo, ServerName> inputForTest = new HashMap<>();
    for (ServerName sn : currentAssignments.keySet()) {
      for (HRegionInfo region : currentAssignments.get(sn)) {
        inputForTest.put(region, sn);
      }
    }
    //verify region->null server assignment is handled
    inputForTest.put(randomRegions(1).get(0), null);
    Map<ServerName, List<HRegionInfo>> newAssignment = loadBalancer.retainAssignment(inputForTest, servers);
    assertRetainedAssignment(inputForTest, servers, newAssignment);
  }

  /**
   * Asserts a valid retained assignment plan.
   * <p>
   * Must meet the following conditions:
   * <ul>
   * <li>Every input region has an assignment, and to an online server
   * <li>If a region had an existing assignment to a server with the same
   * address a a currently online server, it will be assigned to it
   * </ul>
   *
   * @param existing
   * @param assignment
   * @throws java.io.IOException
   * @throws java.io.FileNotFoundException
   */
  private void assertRetainedAssignment(
      Map<HRegionInfo, ServerName> existing, List<ServerName> servers,
      Map<ServerName, List<HRegionInfo>> assignment)
      throws FileNotFoundException, IOException {
    // Verify condition 1, every region assigned, and to online server
    Set<ServerName> onlineServerSet = new TreeSet<>(servers);
    Set<HRegionInfo> assignedRegions = new TreeSet<>();
    for (Map.Entry<ServerName, List<HRegionInfo>> a : assignment.entrySet()) {
      assertTrue(
          "Region assigned to server that was not listed as online",
          onlineServerSet.contains(a.getKey()));
      for (HRegionInfo r : a.getValue())
        assignedRegions.add(r);
    }
    assertEquals(existing.size(), assignedRegions.size());

    // Verify condition 2, every region must be assigned to correct server.
    Set<String> onlineHostNames = new TreeSet<>();
    for (ServerName s : servers) {
      onlineHostNames.add(s.getHostname());
    }

    for (Map.Entry<ServerName, List<HRegionInfo>> a : assignment.entrySet()) {
      ServerName currentServer = a.getKey();
      for (HRegionInfo r : a.getValue()) {
        ServerName oldAssignedServer = existing.get(r);
        TableName tableName = r.getTable();
        String groupName = infoMgr.getRSGroupOfTable(tableName);
        assertTrue(StringUtils.isNotEmpty(groupName));
        RSGroupInfo gInfo = infoMgr.getRSGroup(groupName);
        assertTrue("Region is not correctly assigned to group servers.",
            gInfo.containsServer(Address.fromString(currentServer.getHostAndPort())));
        if (oldAssignedServer != null && onlineHostNames.contains(oldAssignedServer.getHostname())) {
          // this region was previously assigned somewhere, and that
          // host is still around, then the host must have been is a
          // different group.
          if (!oldAssignedServer.getHostAndPort().equals(currentServer.getHostAndPort())) {
            assertFalse(gInfo.containsServer(Address.fromString(oldAssignedServer.getHostAndPort())));
          }
        }
      }
    }
  }

  private String printStats(
      ArrayListMultimap<String, ServerAndLoad> groupBasedLoad) throws IOException {
    StringBuffer sb = new StringBuffer();
    sb.append("\n");
    for (String groupName : groupBasedLoad.keySet()) {
      sb.append("Stats for group: " + groupName);
      sb.append("\n");
      sb.append(infoMgr.getRSGroup(groupName).getServers());
      sb.append("\n");
      List<ServerAndLoad> groupLoad = groupBasedLoad.get(groupName);
      int numServers = groupLoad.size();
      int totalRegions = 0;
      sb.append("Per Server Load: \n");
      for (ServerAndLoad sLoad : groupLoad) {
        sb.append("Server :" + sLoad.getServerName() + " Load : "
            + sLoad.getLoad() + "\n");
        totalRegions += sLoad.getLoad();
      }
      sb.append(" Group Statistics : \n");
      float average = (float) totalRegions / numServers;
      int max = (int) Math.ceil(average);
      int min = (int) Math.floor(average);
      sb.append("[srvr=" + numServers + " rgns=" + totalRegions + " avg="
          + average + " max=" + max + " min=" + min + "]");
      sb.append("\n");
      sb.append("===============================");
      sb.append("\n");
    }
    return sb.toString();
  }

  private ArrayListMultimap<String, ServerAndLoad> convertToGroupBasedMap(
      final Map<ServerName, List<HRegionInfo>> serversMap) throws IOException {
    ArrayListMultimap<String, ServerAndLoad> loadMap = ArrayListMultimap.create();
    for (RSGroupInfo gInfo : infoMgr.listRSGroups()) {
      Set<Address> groupServers = gInfo.getServers();
      for (Address hostPort : groupServers) {
        ServerName actual = null;
        for(ServerName entry: servers) {
          if(Address.fromString(entry.getHostAndPort()).equals(hostPort)) {
            actual = entry;
            break;
          }
        }
        List<HRegionInfo> regions = serversMap.get(actual);
        assertTrue("No load for " + actual, regions != null);
        loadMap.put(gInfo.getName(), new ServerAndLoad(actual, regions.size()));
      }
    }
    return loadMap;
  }

  private ArrayListMultimap<String, ServerAndLoad> reconcile(
      ArrayListMultimap<String, ServerAndLoad> previousLoad,List<RegionPlan> plans) {
    ArrayListMultimap<String, ServerAndLoad> result = ArrayListMultimap.create();
    result.putAll(previousLoad);
    if (plans != null) {
      for (RegionPlan plan : plans) {
        ServerName source = plan.getSource();
        updateLoad(result, source, -1);
        ServerName destination = plan.getDestination();
        updateLoad(result, destination, +1);
      }
    }
    return result;
  }

  private void updateLoad(
      ArrayListMultimap<String, ServerAndLoad> previousLoad,
      final ServerName sn, final int diff) {
    for (String groupName : previousLoad.keySet()) {
      ServerAndLoad newSAL = null;
      ServerAndLoad oldSAL = null;
      for (ServerAndLoad sal : previousLoad.get(groupName)) {
        if (ServerName.isSameHostnameAndPort(sn, sal.getServerName())) {
          oldSAL = sal;
          newSAL = new ServerAndLoad(sn, sal.getLoad() + diff);
          break;
        }
      }
      if (newSAL != null) {
        previousLoad.remove(groupName, oldSAL);
        previousLoad.put(groupName, newSAL);
        break;
      }
    }
  }

  private Map<ServerName, List<HRegionInfo>> mockClusterServers() throws IOException {
    assertTrue(servers.size() == regionAssignment.length);
    Map<ServerName, List<HRegionInfo>> assignment = new TreeMap<>();
    for (int i = 0; i < servers.size(); i++) {
      int numRegions = regionAssignment[i];
      List<HRegionInfo> regions = assignedRegions(numRegions, servers.get(i));
      assignment.put(servers.get(i), regions);
    }
    return assignment;
  }

  /**
   * Generate a list of regions evenly distributed between the tables.
   *
   * @param numRegions The number of regions to be generated.
   * @return List of HRegionInfo.
   */
  private List<HRegionInfo> randomRegions(int numRegions) {
    List<HRegionInfo> regions = new ArrayList<>(numRegions);
    byte[] start = new byte[16];
    byte[] end = new byte[16];
    rand.nextBytes(start);
    rand.nextBytes(end);
    int regionIdx = rand.nextInt(tables.length);
    for (int i = 0; i < numRegions; i++) {
      Bytes.putInt(start, 0, numRegions << 1);
      Bytes.putInt(end, 0, (numRegions << 1) + 1);
      int tableIndex = (i + regionIdx) % tables.length;
      HRegionInfo hri = new HRegionInfo(
          tables[tableIndex], start, end, false, regionId++);
      regions.add(hri);
    }
    return regions;
  }

  /**
   * Generate assigned regions to a given server using group information.
   *
   * @param numRegions the num regions to generate
   * @param sn the servername
   * @return the list of regions
   * @throws java.io.IOException Signals that an I/O exception has occurred.
   */
  private List<HRegionInfo> assignedRegions(int numRegions, ServerName sn) throws IOException {
    List<HRegionInfo> regions = new ArrayList<>(numRegions);
    RSGroupInfo info = infoMgr.getRSGroupOfServer(Address.fromString(sn.getHostAndPort()));
    Set<TableName> tables = info.getTables();
    TableName[] tableArrays = new TableName[tables.size()];
    tables.toArray(tableArrays);
    
    byte[] start = new byte[16];
    byte[] end = new byte[16];
    Bytes.putInt(start, 0, numRegions << 1);
    Bytes.putInt(end, 0, (numRegions << 1) + 1);
    
    for (int i = 0; i < numRegions; i++) {
      TableName tableName = tableArrays[i % tables.size()];
      HRegionInfo hri = new HRegionInfo(tableName, start, end, false,regionId++);
      regions.add(hri);
    }
    return regions;
  }

  private static MasterServices getMockedMaster() throws IOException {
    TableDescriptors tds = Mockito.mock(TableDescriptors.class);
    Mockito.when(tds.get(tables[0])).thenReturn(tableDescs.get(0));
    Mockito.when(tds.get(tables[1])).thenReturn(tableDescs.get(1));
    Mockito.when(tds.get(tables[2])).thenReturn(tableDescs.get(2));
    Mockito.when(tds.get(tables[3])).thenReturn(tableDescs.get(3));
    Mockito.when(tds.get(tables[3])).thenReturn(tableDescs.get(4));
    MasterServices services = Mockito.mock(HMaster.class);
    Mockito.when(services.getTableDescriptors()).thenReturn(tds);
    AssignmentManager am = Mockito.mock(AssignmentManager.class);
    Mockito.when(services.getAssignmentManager()).thenReturn(am);
    return services;
  }

}
