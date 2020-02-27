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
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.shaded.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hbase.util.ImmutableByteArray;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;

/**
 * WAL implements on Kafka, WALEntry will append on Client side
 */
@InterfaceAudience.Private
public class KafkaClientWAL implements WAL {
  private final KafkaOffsetAccounting offsetAccounting = new KafkaOffsetAccounting();
  private final WALCoprocessorHost coprocessorHost;
  private FSHLog hdfsWal;
  private AtomicReference<ZooKeeperWatcher> zkWatcher = new AtomicReference<>();

  public KafkaClientWAL(final Configuration conf, FSHLog hdfsWal) {
    this.coprocessorHost = new WALCoprocessorHost(this, conf);
    this.hdfsWal = hdfsWal;
  }

  @Override
  public OptionalLong getLogFileSizeIfBeingWritten(Path path) {
    return OptionalLong.empty();
  }

  @Override
  public void registerWALActionsListener(WALActionsListener listener) {
    // since WAL append in Client side, no need listener for KafkaWal
    hdfsWal.registerWALActionsListener(listener);
  }

  @Override
  public boolean unregisterWALActionsListener(WALActionsListener listener) {
    // since WAL append in Client side, no need listener for KafkaWal
    return hdfsWal.unregisterWALActionsListener(listener);
  }

  @Override
  public byte[][] rollWriter() throws FailedLogCloseException, IOException {
    // no need to do log roll with kafka wal
    return hdfsWal.rollWriter();
  }

  @Override
  public byte[][] rollWriter(boolean force, boolean syncFailed)
      throws FailedLogCloseException, IOException {
    return hdfsWal.rollWriter(force, syncFailed);
  }

  @Override
  public void shutdown() throws IOException {
    hdfsWal.shutdown();
  }

  @Override
  public void close() throws IOException {
    hdfsWal.close();
  }

  @Override
  public long appendData(HRegionInfo info, WALKey key, WALEdit edits)
      throws IOException {
    // Client will pass offset infos by Mutation#setAttribute
    ConcurrentHashMap<ImmutableByteArray, ConcurrentHashMap<Integer, Long>> kafkaOffsets =
        key.getKafkaOffsets(); // famliy -> (partition -> offset)
    // begin mvcc, set WriteEntry to WALKey
    MultiVersionConcurrencyControl.WriteEntry we = key.getMvcc().begin();
    long regionSequenceId = we.getWriteNumber();
    if (edits != null && !edits.isReplay()) {
      for (Cell c : edits.getCells()) {
        // set seqId in order not to violate MVCC
        CellUtil.setSequenceId(c, regionSequenceId);
      }
    }
    key.setWriteEntry(we); // this will set WALKey's seqId to MVCC txid
    if (kafkaOffsets != null && kafkaOffsets.size() > 0) {
      offsetAccounting.updateKafkaOffset(info.getTable().getNameAsString(), key.getEncodedRegionName(),
        info.getEncodedName(), kafkaOffsets, zkWatcher.get());
    }
    return -1; // since WALEntry already write to KAFKA(client side), no need to do sync
  }

  @Override
  public long appendMarker(HRegionInfo info, WALKey key, WALEdit edit) throws IOException {
    if (edit.isRegionCloseMarker()) {
      this.offsetAccounting.onRegionClose(info.getEncodedNameAsBytes());
    }
    // append Marker edit to hdfsWal, the Marker is needed (see HBASE-2231)
    return hdfsWal.append(info, key, edit, false);
  }

  @Override
  public void updateStore(byte[] encodedRegionName, byte[] familyName, Long sequenceid, boolean onlyIfGreater) {
    // MemStoreCompactor call this to truncated the WAL in order not get too long
    // no need to do with KAFKA WAL
  }

  @Override
  public void sync() throws IOException {
    // no need to do sync, since WAL sync in Client side
    hdfsWal.sync();
  }

  @Override
  public void sync(long txid) throws IOException {
    hdfsWal.sync(txid);
  }

  @Override
  public WALCoprocessorHost getCoprocessorHost() {
    return this.coprocessorHost;
  }

  @Override
  @VisibleForTesting
  public Long startCacheFlush(byte[] encodedRegionName, Set<byte[]> families) {
    this.offsetAccounting.startKafkaCacheFlush(encodedRegionName, families);
    return HConstants.NO_SEQNUM;
  }

  @Override
  public Long startCacheFlush(byte[] encodedRegionName, Map<byte[], Long> familyToSeq) {
    this.offsetAccounting.startKafkaCacheFlush(encodedRegionName, familyToSeq.keySet());
    return HConstants.NO_SEQNUM;
  }

  @Override
  public void completeCacheFlush(byte[] encodedRegionName) {
    this.offsetAccounting.completeKafkaCacheFlush(encodedRegionName);
  }

  @Override
  public void abortCacheFlush(byte[] encodedRegionName) {
    this.offsetAccounting.abortKafkaCacheFlush(encodedRegionName);
  }

  @Override
  @VisibleForTesting
  public long getEarliestMemstoreSeqNum(byte[] encodedRegionName) {
    // do not use this with kafka wal
    return HConstants.NO_SEQNUM;
  }

  @Override
  public long getEarliestMemstoreSeqNum(byte[] encodedRegionName, byte[] familyName) {
    // do not use this with kafka wal
    return HConstants.NO_SEQNUM;
  }

  /**
   * return the earliest offset of each partition after region flush
   */
  public Map<Integer, Long> getEarliestKakfaOffset(String table, String encodedName,
      byte[] encodedRegionName) {
    return this.offsetAccounting.getEarliestKakfaOffset(this.zkWatcher.get(), table,
      encodedName, encodedRegionName);
  }

  public void setZooKeeperWatcher(ZooKeeperWatcher zkWatcher) {
    this.zkWatcher.compareAndSet(null, zkWatcher);
  }

  public void heartbeatAcked() {
    this.offsetAccounting.heartbeatAcked();
  }
}
