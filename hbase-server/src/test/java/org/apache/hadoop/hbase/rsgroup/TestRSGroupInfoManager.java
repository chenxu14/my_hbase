package org.apache.hadoop.hbase.rsgroup;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
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

@Category({ SmallTests.class })
public class TestRSGroupInfoManager {
  final static Log LOG = LogFactory.getLog(TestRSGroupInfoManager.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Configuration conf;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    conf.set(RSGroupInfoManager.DEFAULT_GROUP_NAME, "group1");
  }

  @Test
  public void testGroupNoServerButTable() {
    RecoverableZooKeeper zkcli = Mockito.mock(RecoverableZooKeeper.class);
    ZooKeeperWatcher zkWatcher = Mockito.mock(ZooKeeperWatcher.class);
    zkWatcher.baseZNode = "/hbase";
    try {
      when(zkcli.getChildren(Mockito.any(String.class), Mockito.any(Watcher.class)))
          .thenAnswer(new Answer<List<String>>() {
            public List<String> answer(InvocationOnMock invocation) throws Throwable {
              String path = invocation.getArgumentAt(0, String.class);
              List<String> childs = new ArrayList<String>();
              if ("/hbase/rsgroup/tables".equals(path)) {
                childs.add("table1");
              } else if ("/hbase/rsgroup/servers".equals(path)) {
                childs.add("hostname1");
                childs.add("hostname2");
              }
              return childs;
            }
          });
      when(zkcli.getData(Mockito.any(String.class), Mockito.any(Watcher.class), Mockito.any(Stat.class)))
          .thenAnswer(new Answer<byte[]>() {
            public byte[] answer(InvocationOnMock invocation) throws Throwable {
              String path = invocation.getArgumentAt(0, String.class);
              byte[] res = null;
              if ("/hbase/rsgroup/tables/table1".equals(path)) {
                res = Bytes.toBytes("group2");
              } else if ("/hbase/rsgroup/servers/hostname1".equals(path)) {
                res = Bytes.toBytes("group1");
              } else if ("/hbase/rsgroup/servers/hostname2".equals(path)) {
                res = Bytes.toBytes("group1");
              }
              return res;
            }
          });
      when(zkWatcher.getRecoverableZooKeeper()).thenReturn(zkcli);
      RSGroupInfoManager infoMgr = new RSGroupInfoManagerImpl(conf, zkWatcher);
      infoMgr.refreshGroupServers();
    } catch (Exception e) {
      assertEquals("group2 has no server, but has table on it!", e.getMessage());
    }
  }

  @Test
  public void testDefaultNoServer() {
    RecoverableZooKeeper zkcli = Mockito.mock(RecoverableZooKeeper.class);
    ZooKeeperWatcher zkWatcher = Mockito.mock(ZooKeeperWatcher.class);
    zkWatcher.baseZNode = "/hbase";
    try {
      when(zkcli.getChildren(Mockito.any(String.class), Mockito.any(Watcher.class)))
          .thenAnswer(new Answer<List<String>>() {
            public List<String> answer(InvocationOnMock invocation) throws Throwable {
              String path = invocation.getArgumentAt(0, String.class);
              List<String> childs = new ArrayList<String>();
              if ("/hbase/rsgroup/servers".equals(path)) {
                childs.add("hostname1");
              }
              return childs;
            }
          });
      when(zkcli.getData(Mockito.any(String.class), Mockito.any(Watcher.class), Mockito.any(Stat.class)))
          .thenAnswer(new Answer<byte[]>() {
            public byte[] answer(InvocationOnMock invocation) throws Throwable {
              String path = invocation.getArgumentAt(0, String.class);
              byte[] res = null;
              if ("/hbase/rsgroup/servers/hostname1".equals(path)) {
                res = Bytes.toBytes("group2");
              }
              return res;
            }
          });
      when(zkWatcher.getRecoverableZooKeeper()).thenReturn(zkcli);
      RSGroupInfoManager infoMgr = new RSGroupInfoManagerImpl(conf, zkWatcher);
      infoMgr.refreshGroupServers();
    } catch (Exception e) {
      assertEquals("group1 is default group, but has no server on it!", e.getMessage());
    }
  }

  @Test
  public void testCURD() {
    RecoverableZooKeeper zkcli = Mockito.mock(RecoverableZooKeeper.class);
    ZooKeeperWatcher zkWatcher = Mockito.mock(ZooKeeperWatcher.class);
    zkWatcher.baseZNode = "/hbase";
    try {
      when(zkcli.getChildren(Mockito.any(String.class), Mockito.any(Watcher.class)))
          .thenAnswer(new Answer<List<String>>() {
            public List<String> answer(InvocationOnMock invocation) throws Throwable {
              String path = invocation.getArgumentAt(0, String.class);
              List<String> childs = new ArrayList<String>();
              if ("/hbase/rsgroup/tables".equals(path)) {
                childs.add("table1");
                childs.add("table2");
              } else if ("/hbase/rsgroup/servers".equals(path)) {
                childs.add("hostname1:16020");
                childs.add("hostname2");
                childs.add("hostname3:16020");
              }
              return childs;
            }
          });
      when(zkcli.getData(Mockito.any(String.class), Mockito.any(Watcher.class), Mockito.any(Stat.class)))
          .thenAnswer(new Answer<byte[]>() {
            public byte[] answer(InvocationOnMock invocation) throws Throwable {
              String path = invocation.getArgumentAt(0, String.class);
              byte[] res = null;
              if ("/hbase/rsgroup/servers/hostname1:16020".equals(path)) {
                res = Bytes.toBytes("group1");
              } else if ("/hbase/rsgroup/servers/hostname2".equals(path)) {
                res = Bytes.toBytes("group1");
              } else if ("/hbase/rsgroup/servers/hostname3:16020".equals(path)) {
                res = Bytes.toBytes("group2");
              } else if ("/hbase/rsgroup/tables/table1".equals(path)) {
                res = Bytes.toBytes("group1");
              } else if ("/hbase/rsgroup/tables/table2".equals(path)) {
                res = Bytes.toBytes("group2");
              }
              return res;
            }
          });
      when(zkWatcher.getRecoverableZooKeeper()).thenReturn(zkcli);
      RSGroupInfoManager infoMgr = new RSGroupInfoManagerImpl(conf, zkWatcher);
      infoMgr.refreshGroupServers();
      RSGroupInfo info = infoMgr.getRSGroupOfServer(Address.fromString("hostname1:16020"));
      assertEquals("group1",info.getName());
      assertEquals(2,info.getServers().size());
      assertEquals(1,info.getTables().size());
      List<RSGroupInfo> groups = infoMgr.listRSGroups();
      assertEquals(2, groups.size());
      
      info = infoMgr.getRSGroup("group2");
      assertEquals(1,info.getServers().size());
      assertEquals(1,info.getTables().size());

      String group = infoMgr.getRSGroupOfTable(TableName.valueOf("table1"));
      assertEquals("group1",group);
      // test move table
      infoMgr.moveTableToGroup(TableName.valueOf("table1"), "group2");
      info = infoMgr.getRSGroup("group2");
      assertEquals(2,info.getTables().size());
      // test delete table
      infoMgr.removeTable(TableName.valueOf("table1"));
      info = infoMgr.getRSGroup("group2");
      assertEquals(1,info.getTables().size());
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

}
