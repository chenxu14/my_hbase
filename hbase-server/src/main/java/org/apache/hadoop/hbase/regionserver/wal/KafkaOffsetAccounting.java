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
package org.apache.hadoop.hbase.regionserver.wal;

import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ImmutableByteArray;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

/**
 * Accounting of offset per partition
 */
@InterfaceAudience.Private
class KafkaOffsetAccounting {
  private static final Log LOG = LogFactory.getLog(KafkaOffsetAccounting.class);
  private final ConcurrentHashMap<byte[], ConcurrentHashMap<ImmutableByteArray, ConcurrentHashMap<Integer, Long>>>
      unflushedKakfaOffsets = new ConcurrentHashMap<>(); // region -> (family -> (partition -> offset))
  private final ConcurrentHashMap<byte[], ConcurrentHashMap<ImmutableByteArray, ConcurrentHashMap<Integer, Long>>>
      flushingKafkaOffsets = new ConcurrentHashMap<>();
  private Map<byte[], Map<Integer, Long>> currentHeartbeat = new HashMap<>();
   private AtomicReference<Map<byte[], Map<Integer, Long>>> lastHeartbeat = new AtomicReference<>();

  void updateKafkaOffset(String table, byte[] encodedRegionName, String encodeName,
      ConcurrentHashMap<ImmutableByteArray, ConcurrentHashMap<Integer, Long>> kafkaOffsets, // family -> (partition -> offset)
      ZooKeeperWatcher zkWatcher) throws IOException {
    // since each partition consumed only by one consumer, kafkaOffsets can guaranteed to be incrementally
    ConcurrentHashMap<ImmutableByteArray, ConcurrentHashMap<Integer, Long>> familyLevel =
        unflushedKakfaOffsets.putIfAbsent(encodedRegionName, kafkaOffsets);
    boolean needPersistance = false;
    try {
      if (familyLevel == null) { // new region add
        if (LOG.isDebugEnabled()) {
          LOG.debug("new region add, should persistance the region partition mapping in voliate znode,"
              + " region is " + encodeName);
        }
        needPersistance = true;
        return;
      }
      for(ImmutableByteArray fam : kafkaOffsets.keySet()) {
        Map<Integer, Long> partitionLevel = familyLevel.putIfAbsent(fam, kafkaOffsets.get(fam));
        if (partitionLevel == null) { // new family add
          if (LOG.isDebugEnabled()) {
            LOG.debug("new family add, should persistance the region partition mapping in voliate znode."
                + " region is " + encodeName + ", family is " + fam.toString());
          }
          needPersistance = true;
          continue;
        }
        for (Map.Entry<Integer, Long> offsetInfo : kafkaOffsets.get(fam).entrySet()) {
          // since offsets will be update incrementally, use putIfAbsent is enough
          Long preValue = partitionLevel.putIfAbsent(offsetInfo.getKey(), offsetInfo.getValue());
          if (preValue == null) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("first KV come into memstore, should persistance the region partition mapping in voliate znode,"
                  + " region is " + encodeName + ", family is " + fam.toString());
            }
            needPersistance = true;
          }
        }
      }
    } finally {
      if (needPersistance && zkWatcher != null) {
        Map<Integer, Long> offsets = new HashMap<>();
        gettEarliestKakfaOffset(offsets, kafkaOffsets);
        uploadToZookeeper(zkWatcher, table, encodedRegionName, encodeName, offsets);
      }
    }
  }

  /**
   * upload the first coming region-partition-mapping to ZK
   * in order to avoid the mapping info missing
   */
  private void uploadToZookeeper(ZooKeeperWatcher zkWatcher, String table, byte[] encodedRegionName,
      String encodeName, Map<Integer, Long> offsets) throws IOException {
    if (lastHeartbeat.get() != null) {
      Map<Integer, Long> partitionInfo = lastHeartbeat.get().get(encodedRegionName);
      if (partitionInfo!= null && !partitionInfo.isEmpty()) {
        for (Integer alreadyHave : partitionInfo.keySet()) {
          if (offsets.containsKey(alreadyHave)) { // has reported before
            offsets.remove(alreadyHave);
          }
        }
      }
    }
    // update region-partition-volatile info to ZK
    try {
      for (Map.Entry<Integer, Long> offsetInfo : offsets.entrySet()) {
        String regionZnode = createRegionZNode(zkWatcher, zkWatcher.partitionZnode, table, encodeName);
        LOG.info("new region partition mapping find, persisitance it as voliate znode.");
        updatePartitionOffset(zkWatcher, regionZnode, offsetInfo.getKey(), offsetInfo.getValue());
      }
    } catch (KeeperException e) {
      throw new ZooKeeperConnectionException(
        zkWatcher.prefix("Unexpected KeeperException creating region-partition-mapping node"), e);
    }
  }

  private void updatePartitionOffset(ZooKeeperWatcher zkWatcher, String regionZNode,
      Integer partition, Long offset) throws KeeperException {
    String partitionNode = ZKUtil.joinZNode(regionZNode, partition.toString());
    int version = ZKUtil.checkExists(zkWatcher, partitionNode);
    if (version != -1) { // already exits
      ZKUtil.setData(zkWatcher, partitionNode, Bytes.toBytes(offset), version);
    } else { // persist it
      ZKUtil.createNodeIfNotExistsNoWatch(zkWatcher, partitionNode,
          Bytes.toBytes(offset), CreateMode.PERSISTENT);
    }
  }

  public void startKafkaCacheFlush(byte[] encodedRegionName, Set<byte[]> families) {
    ConcurrentHashMap<ImmutableByteArray, ConcurrentHashMap<Integer, Long>> oldKafkaOffsets = null;
    ConcurrentHashMap<ImmutableByteArray, ConcurrentHashMap<Integer, Long>> familyLevel =
        unflushedKakfaOffsets.get(encodedRegionName);
    if (familyLevel != null && !familyLevel.isEmpty()) { // transfer to flushingKafkaOffsets
      oldKafkaOffsets = new ConcurrentHashMap<>();
      for (byte[] family : families) {
        // region -> partition mapping will be remove with this family
        ImmutableByteArray familyObj = ImmutableByteArray.wrap(family);
        ConcurrentHashMap<Integer, Long> partitionLevel = familyLevel.remove(familyObj);
        if (partitionLevel != null && !partitionLevel.isEmpty()) {
          oldKafkaOffsets.put(familyObj, partitionLevel);
        }
      }
      if (!oldKafkaOffsets.isEmpty()) {
        if (this.flushingKafkaOffsets.put(encodedRegionName, oldKafkaOffsets) != null) {
          LOG.warn("Flushing Map not cleaned up for " + Bytes.toString(encodedRegionName));
        }
      }
    }
  }

  void completeKafkaCacheFlush(byte[] encodedRegionName) {
    this.flushingKafkaOffsets.remove(encodedRegionName);
  }

  void abortKafkaCacheFlush(byte[] encodedRegionName) {
    ConcurrentHashMap<ImmutableByteArray, ConcurrentHashMap<Integer, Long>> oldKafkaOffsets =
        this.flushingKafkaOffsets.remove(encodedRegionName);
    if (oldKafkaOffsets != null && !oldKafkaOffsets.isEmpty()) {
      ConcurrentHashMap<ImmutableByteArray, ConcurrentHashMap<Integer, Long>> familyLevel =
          this.unflushedKakfaOffsets.putIfAbsent(encodedRegionName, oldKafkaOffsets);
      if (familyLevel == null) {
        return;
      }
      // trasfer failed flush offset to unflushedKakfaOffsets
      for (Enumeration<ImmutableByteArray> families = oldKafkaOffsets.keys(); families.hasMoreElements();) {
        ImmutableByteArray family = families.nextElement();
        familyLevel.put(family, oldKafkaOffsets.get(family));
      }
    }
  }

  void onRegionClose(byte[] encodedRegionName) {
    this.unflushedKakfaOffsets.remove(encodedRegionName);
    if (this.flushingKafkaOffsets.remove(encodedRegionName) != null) {
      LOG.warn("Still have flushing records when closing " + Bytes.toString(encodedRegionName));
    }
  }

  Map<Integer, Long> getEarliestKakfaOffset(ZooKeeperWatcher zkWatcher, String table,
      String encodedName, byte[] encodedRegionName) {
    Map<Integer, Long> offsets = new HashMap<>();
    gettEarliestKakfaOffset(offsets, flushingKafkaOffsets.get(encodedRegionName));
    gettEarliestKakfaOffset(offsets, unflushedKakfaOffsets.get(encodedRegionName));
    currentHeartbeat.put(encodedRegionName, offsets);
    try {
      if (zkWatcher != null &&
          (lastHeartbeat.get() == null || lastHeartbeat.get().get(encodedRegionName) == null)) {
        // first heartbeat, upload mapping info to ZK
        String regionZnode = createRegionZNode(zkWatcher, zkWatcher.offsetZnode, table, encodedName);
        LOG.info("first heartbeat to HMaster, persistance the regopn partition mapping, regions is : "
          + encodedName + ", table is : " + table);
        for (Map.Entry<Integer, Long> offsetInfo : offsets.entrySet()) {
          updatePartitionOffset(zkWatcher, regionZnode, offsetInfo.getKey(), offsetInfo.getValue());
        }
      } else {
        Map<Integer, Long> oldOffsets = lastHeartbeat.get().get(encodedRegionName);
        Set<Integer> oldParts = new HashSet<>(oldOffsets.size());
        oldParts.addAll(oldOffsets.keySet());
        String regionZnode = null;
        for (Integer part : offsets.keySet()) {
          if (oldParts.contains(part) && offsets.get(part) <= oldOffsets.get(part)) {
            // no need to persist, most cases
            oldParts.remove(part);
            continue;
          } else {
            regionZnode = createRegionZNode(zkWatcher, zkWatcher.offsetZnode, table, encodedName);
            if (!oldParts.contains(part)) {
              // new add mapping, should persist it
              LOG.info("new region partition mapping find during heartbeat, persist it to ZK, regions is : "
                  + encodedName);
              updatePartitionOffset(zkWatcher, regionZnode, part, offsets.get(part));
            } else if (offsets.get(part) > oldOffsets.get(part)) {
              // new offset bigger than before, persist it
              oldParts.remove(part);
              if (LOG.isDebugEnabled()) {
                LOG.debug("new offset bigger than before, persist it, regions is : "
                    + encodedName + ", table is : " + table);
              }
              updatePartitionOffset(zkWatcher, regionZnode, part, offsets.get(part));
            }
          }
        }
        if (!oldParts.isEmpty()) { // delete invalid mapping
          if (regionZnode == null) {
            regionZnode = createRegionZNode(zkWatcher, zkWatcher.offsetZnode, table, encodedName);
          }
          for (Integer toDel : oldParts) {
            LOG.info("invalid region partition mapping find during heartbeat, delete it from ZK, regions is : "
                + encodedName + ", partition is " + toDel);
            ZKUtil.deleteNodeFailSilent(zkWatcher, ZKUtil.joinZNode(regionZnode, toDel.toString()));
          }
        }
      }
    } catch (KeeperException e) {
      LOG.warn(zkWatcher.prefix("Unexpected KeeperException creating region-partition-mapping node"), e);
    } 
    return offsets;
  }

  private String createRegionZNode(ZooKeeperWatcher zkWatcher, String parent, String table,
      String region) throws KeeperException {
    String tableZnode = ZKUtil.joinZNode(parent, table);
    String regionZnode = ZKUtil.joinZNode(tableZnode, region);
    ZKUtil.createAndFailSilent(zkWatcher, regionZnode);
    return regionZnode;
  }

  /**
   * when RS heartbeat to HMaster successed, should triger this
   */
  void heartbeatAcked() {
    lastHeartbeat.set(currentHeartbeat);
    currentHeartbeat = new HashMap<>();
  }

  private void gettEarliestKakfaOffset(Map<Integer, Long> target,
      Map<ImmutableByteArray, ConcurrentHashMap<Integer, Long>> source) {
    if (source != null && !source.isEmpty()) {
      for (Map<Integer, Long> partitionLevel : source.values()) { // each family
        for (Map.Entry<Integer, Long> offsetInfo : partitionLevel.entrySet()) { // each partition
          Integer partition = offsetInfo.getKey();
          Long offset = offsetInfo.getValue();
          target.putIfAbsent(partition, offset);
          if (target.get(partition) > offset) {
            target.put(partition, offset);
          }
        }
      }
    }
  }
}
