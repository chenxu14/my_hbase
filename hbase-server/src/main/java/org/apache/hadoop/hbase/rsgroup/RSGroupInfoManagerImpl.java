package org.apache.hadoop.hbase.rsgroup;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import com.google.common.collect.Lists;

/**
 * This is an implementation of {@link RSGroupInfoManager} which makes use of an
 * local File as the persistence store for the group information.
 *
 * <h2>Concurrency</h2> RSGroup state is kept locally in Maps. There is a
 * rsgroup name to cached RSGroupInfo Map at {@link #rsGroupMap} and a Map of
 * tables to the name of the rsgroup they belong too (in {@link #tableMap}).
 */
@InterfaceAudience.Private
public class RSGroupInfoManagerImpl implements RSGroupInfoManager {
  private static final Log LOG = LogFactory.getLog(RSGroupInfoManager.class);
  public static final String GROUP_NODE = "rsgroup";
  public static final String GROUP_TABLES_NODE = "tables";
  public static final String GROUP_SERVERS_NODE = "servers";
  private Map<String, RSGroupInfo> rsGroupMap;
  private Map<TableName, String> tableMap;
  private final ReentrantReadWriteLock lock;
  private String defaultGroup;
  private ZooKeeperWatcher zkWatcher;
  private String groupTablesZNode;
  private String groupServersZNode;

  public RSGroupInfoManagerImpl(Configuration conf, ZooKeeperWatcher zkWatcher) throws IOException {
    this.rsGroupMap = new HashMap<String, RSGroupInfo>();
    this.tableMap = new HashMap<TableName, String>();
    this.lock = new ReentrantReadWriteLock();
    this.zkWatcher = zkWatcher;
    this.defaultGroup = conf.get(DEFAULT_GROUP_NAME, DEFAULT_GROUP_NAME_DEFAULT);
    String groupZnodeParent = conf.get("zookeeper.znode.group.parent", GROUP_NODE);
    String groupZNode = ZKUtil.joinZNode(zkWatcher.baseZNode, groupZnodeParent);
    this.groupTablesZNode = ZKUtil.joinZNode(groupZNode, GROUP_TABLES_NODE);
    this.groupServersZNode = ZKUtil.joinZNode(groupZNode, GROUP_SERVERS_NODE);
    try{
      ZKUtil.createAndFailSilent(zkWatcher, groupZNode);
      ZKUtil.createAndFailSilent(zkWatcher, groupTablesZNode);
      ZKUtil.createAndFailSilent(zkWatcher, groupServersZNode);
    } catch (KeeperException e) {
      throw new ZooKeeperConnectionException(
          zkWatcher.prefix("Unexpected KeeperException creating base node"), e);
    }
    refreshGroupServers();
  }

  @Override
  public RSGroupInfo getRSGroupOfServer(Address serverHostPort) throws IOException {
    try {
      lock.readLock().lock();
      for (RSGroupInfo info : rsGroupMap.values()) {
        if (info.containsServer(serverHostPort)) {
          return info;
        }
      }
    } finally {
      lock.readLock().unlock();
    }
    return rsGroupMap.get(defaultGroup);
  }

  @Override
  public RSGroupInfo getRSGroup(String groupName) throws IOException {
    RSGroupInfo group = null;
    try {
      lock.readLock().lock();
      group = rsGroupMap.get(groupName);
    } finally {
      lock.readLock().unlock();
    }
    return group;
  }

  @Override
  public String getRSGroupOfTable(TableName tableName) throws IOException {
    String group = null;
    try {
      lock.readLock().lock();
      group = tableMap.get(tableName);
    } finally {
      lock.readLock().unlock();
    }
    return group == null ? defaultGroup : group;
  }

  @Override
  public List<RSGroupInfo> listRSGroups() throws IOException {
    List<RSGroupInfo> groups = null;
    try {
      lock.readLock().lock();
      groups = Lists.newLinkedList(rsGroupMap.values());
    } finally {
      lock.readLock().unlock();
    }
    return groups;
  }

  @Override
  public void refreshGroupServers() throws IOException {
    Map<String, String> tables = new HashMap<String, String>();
    Map<Address, String> servers = new HashMap<Address, String>();
    try {
      // refresh table nodes
      List<String> tableNodes = ZKUtil.listChildrenNoWatch(zkWatcher, groupTablesZNode);
      if (tableNodes != null) {
        for (String tableName : tableNodes) {
          byte[] data = ZKUtil.getDataNoWatch(zkWatcher, ZKUtil.joinZNode(groupTablesZNode, tableName), null);
          if (data != null) {
            tables.put(tableName, Bytes.toString(data));
          } else {
            tables.put(tableName, this.defaultGroup);
          }
        }
      }
      // refresh server nodes
      List<String> serverNodes = ZKUtil.listChildrenNoWatch(zkWatcher, groupServersZNode);
      if (serverNodes != null) {
        for (String server : serverNodes) {
          String[] serverInfo = server.split(":");
          Address address;
          if (serverInfo.length == 1) {
            address = Address.fromParts(serverInfo[0].trim(), HConstants.DEFAULT_REGIONSERVER_PORT);
          } else if (serverInfo.length == 2) {
            address = Address.fromParts(serverInfo[0].trim(), Integer.parseInt(serverInfo[1].trim()));
          } else {
            throw new IOException(server + " should be hostname,port or hostname!");
          }
          byte[] data = ZKUtil.getDataNoWatch(zkWatcher, ZKUtil.joinZNode(groupServersZNode, server), null);
          if (data != null) {
            servers.put(address, Bytes.toString(data));
          } else {
            servers.put(address, this.defaultGroup);
          }
        }
      }
    } catch (KeeperException e) {
      LOG.error("get group info from ZK failed!", e);
      throw new IOException("get group info from ZK failed!");
    }
    sanityCheck(tables, servers);
    syncGroups(tables, servers);
  }

  private void sanityCheck(Map<String, String> tables, Map<Address, String> servers) throws IOException {
    Collection<String> groups = servers.values();
    if (groups.size() > 0 && !groups.contains(defaultGroup)) {
      throw new IOException(defaultGroup + " is default group, but has no server on it!");
    }
    for (String group : tables.values()) {
      if (!groups.contains(group)) {
        throw new IOException(group + " has no server, but has table on it!");
      }
    }
  }

  private void syncGroups(Map<String, String> tables, Map<Address, String> servers) {
    try {
      lock.writeLock().lock();
      this.tableMap.clear();
      this.rsGroupMap.clear();
      for (Map.Entry<String, String> tableEntry : tables.entrySet()) {
        TableName table = TableName.valueOf(tableEntry.getKey());
        String group = tableEntry.getValue();
        this.tableMap.put(table, group);
        RSGroupInfo groupInfo = this.rsGroupMap.get(group);
        if (groupInfo == null) {
          groupInfo = new RSGroupInfo(group);
          this.rsGroupMap.put(group, groupInfo);
        }
        groupInfo.addTable(table);
      }
      for (Map.Entry<Address, String> serverEntry : servers.entrySet()) {
        Address server = serverEntry.getKey();
        String group = serverEntry.getValue();
        RSGroupInfo groupInfo = this.rsGroupMap.get(group);
        if (groupInfo == null) {
          groupInfo = new RSGroupInfo(group);
          this.rsGroupMap.put(group, groupInfo);
        }
        groupInfo.addServer(server);
      }
    } finally {
      lock.writeLock().unlock();
    }
    LOG.info("rsgroups refresh success!");
  }

  @Override
  public void moveTableToGroup(TableName table, String group) throws IOException {
    if(group == null){
      group = this.defaultGroup;
    }
    RSGroupInfo groupInfo = this.rsGroupMap.get(group);
    // do sanity check
    if (groupInfo == null) {
      throw new IOException("target group " + group + " not exist!");
    }
    if (groupInfo.getServers().size() < 1) {
      throw new IOException("target group " + group + " has no servers!");
    }
    String znode = ZKUtil.joinZNode(groupTablesZNode, table.getNameAsString());
    // persistence to ZK first
    try {
      int version = ZKUtil.checkExists(zkWatcher, znode);
      if (version != -1) { // already exits
        ZKUtil.setData(zkWatcher, znode, Bytes.toBytes(group), version);
      } else {
        ZKUtil.createNodeIfNotExistsNoWatch(zkWatcher, znode, Bytes.toBytes(group), CreateMode.PERSISTENT);
      }
      lock.writeLock().lock();
      if(this.tableMap.containsKey(table)){
        String groupBefore = this.tableMap.remove(table);
        this.rsGroupMap.get(groupBefore).getTables().remove(table);
      }
      this.tableMap.put(table, group);
      groupInfo.getTables().add(table);
    } catch (KeeperException e) {
      LOG.error("create table znode " + znode + " failed!", e);
      // if persistence error, give up sync to memory
      throw new IOException("create table znode " + znode + " failed!");
    } finally {
      if (lock.writeLock().isHeldByCurrentThread()) {
        lock.writeLock().unlock();
      }
    }
  }

  @Override
  public void moveServerToGroup(Address address, String group) throws IOException {
    if(group == null){
      group = this.defaultGroup;
    }
    String server = address.getHostname() + ":" + address.getPort();
    String znode = ZKUtil.joinZNode(groupServersZNode, server);
    try {
      int version = ZKUtil.checkExists(zkWatcher, znode);
      if (version != -1) { // already exits
        ZKUtil.setData(zkWatcher, znode, Bytes.toBytes(group), version);
      } else {
        ZKUtil.createNodeIfNotExistsNoWatch(zkWatcher, znode, Bytes.toBytes(group), CreateMode.PERSISTENT);
      }
    } catch (KeeperException e) {
      LOG.error("create server znode " + znode + " failed!", e);
      throw new IOException("create server znode " + znode + " failed!");
    }
  }

  @Override
  public void removeTable(TableName table) throws IOException {
    String znode = ZKUtil.joinZNode(groupTablesZNode, table.getNameAsString());
    try {
      int version = ZKUtil.checkExists(zkWatcher, znode);
      if (version != -1) {
        ZKUtil.deleteNode(zkWatcher, znode, version);
      }
      lock.writeLock().lock();
      String groupBefore = this.tableMap.remove(table);
      if(groupBefore != null){
        this.rsGroupMap.get(groupBefore).getTables().remove(table);
      }
    } catch (KeeperException e) {
      LOG.error("delete table znode " + znode + " failed!", e);
      throw new IOException("delete table znode " + znode + " failed!");
    } finally {
      if (lock.writeLock().isHeldByCurrentThread()) {
        lock.writeLock().unlock();
      }
    }
  }

}
