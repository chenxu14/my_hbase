package org.apache.hadoop.hbase.rsgroup;

import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseCluster;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.CreateMode;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MediumTests.class })
public class TestRSGroupAdmin {

  public final static long WAIT_TIMEOUT = 60000 * 5;
  private static HBaseTestingUtility TEST_UTIL;
  private static HMaster master;
  private static RSGroupAdmin rsGroupAdmin;

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL = new HBaseTestingUtility();
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(HConstants.HBASE_MASTER_LOADBALANCER_CLASS, RSGroupBasedLoadBalancer.class.getName());
    conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, RSGroupAdminEndpoint.class.getName());
    conf.set(RSGroupInfoManager.DEFAULT_GROUP_NAME, RSGroupInfoManager.DEFAULT_GROUP_NAME_DEFAULT);
    conf.set("hbase.balancer.tablesOnMaster", "hbase:acl,hbase:namespace,hbase:meta,hbase:quota");
    conf.set("hbase.assignment.usezk", "false");

    TEST_UTIL.startMiniZKCluster();
    ZooKeeperWatcher zkw = TEST_UTIL.getZooKeeperWatcher();
    ZKUtil.createNodeIfNotExistsNoWatch(zkw, "/hbase", null, CreateMode.PERSISTENT);
    ZKUtil.createNodeIfNotExistsNoWatch(zkw, "/hbase/rsgroup", null, CreateMode.PERSISTENT);
    ZKUtil.createNodeIfNotExistsNoWatch(zkw, "/hbase/rsgroup/servers", null, CreateMode.PERSISTENT);
    ZKUtil.createNodeIfNotExistsNoWatch(zkw, "/hbase/rsgroup/servers/host01.test.com:16020", 
        Bytes.toBytes("default"), CreateMode.PERSISTENT);
    ZKUtil.createNodeIfNotExistsNoWatch(zkw, "/hbase/rsgroup/servers/host02.test.com:16020", 
        Bytes.toBytes("default"), CreateMode.PERSISTENT);
    ZKUtil.createNodeIfNotExistsNoWatch(zkw, "/hbase/rsgroup/servers/host03.test.com:16020", 
        Bytes.toBytes("test"), CreateMode.PERSISTENT);
    ZKUtil.createNodeIfNotExistsNoWatch(zkw, "/hbase/rsgroup/tables", null, CreateMode.PERSISTENT);
    ZKUtil.createNodeIfNotExistsNoWatch(zkw, "/hbase/rsgroup/tables/table01", Bytes.toBytes("default"), CreateMode.PERSISTENT);
    ZKUtil.createNodeIfNotExistsNoWatch(zkw, "/hbase/rsgroup/tables/table02", Bytes.toBytes("default"), CreateMode.PERSISTENT);
    ZKUtil.createNodeIfNotExistsNoWatch(zkw, "/hbase/rsgroup/tables/table03", Bytes.toBytes("test"), CreateMode.PERSISTENT);
    ZKUtil.createNodeIfNotExistsNoWatch(zkw, "/hbase/rsgroup/tables/table04", Bytes.toBytes("test"), CreateMode.PERSISTENT);
    
    TEST_UTIL.startMiniCluster();
    HBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    master = ((MiniHBaseCluster) cluster).getMaster();
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return master.isInitialized();
      }
    });
    HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
    admin.setBalancerRunning(false, true);
    rsGroupAdmin = new RSGroupAdminClient(TEST_UTIL.getConnection());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testRSGroupAdmin() {
    try {
      assertEquals(2, rsGroupAdmin.listRSGroups().size());
      RSGroupInfo group = rsGroupAdmin.getRSGroupInfo("test");
      assertEquals(1, group.getServers().size());
      assertEquals(2, group.getTables().size());

      group = rsGroupAdmin.getRSGroupInfoOfTable(TableName.valueOf("table03"));
      assertEquals(1, group.getServers().size());
      assertEquals(2, group.getTables().size());

      group = rsGroupAdmin.getRSGroupOfServer(Address.fromString("host03.test.com:16020"));
      assertEquals(1, group.getServers().size());
      assertEquals(2, group.getTables().size());

      rsGroupAdmin.moveTableToGroup(TableName.valueOf("table01"), "test");
      group = rsGroupAdmin.getRSGroupInfo("test");
      assertEquals(3, group.getTables().size());

      rsGroupAdmin.moveServerToGroup(Address.fromParts("host02.test.com", 16020), "test");
      rsGroupAdmin.refreshGroupServers();
      group = rsGroupAdmin.getRSGroupInfo("test");
      assertEquals(2, group.getServers().size());
    } catch (IOException e) {
      fail(e.getMessage());
    }
  }
}
