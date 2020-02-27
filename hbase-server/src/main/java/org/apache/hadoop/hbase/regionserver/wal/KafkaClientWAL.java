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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.shaded.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALKey;

/**
 * WAL implements on Kafka, WALEntry will append on Client side
 */
@InterfaceAudience.Private
public class KafkaClientWAL implements WAL {
  // TODO SequenceIdAccounting can be global? since each WAL corresponding only one KAFKA partition
  private final SequenceIdAccounting sequenceIdAccounting = new SequenceIdAccounting();
  private final WALCoprocessorHost coprocessorHost;
  private FSHLog hdfsWal;

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
  public byte[][] rollWriter(boolean force, boolean syncFailed) throws FailedLogCloseException, IOException {
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
    // Client will pass OFFSET info by Mutation#setAttribute, see HRegion#doMiniBatchMutate
    long kafkaOffset = key.getSequenceId();
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
    key.setSequenceId(kafkaOffset); // change back to KAFKA offset
    if (kafkaOffset != WALKey.NO_SEQUENCE_ID) {
      // Since KAFKA partition consume serially, each region's lastSeqId will increment serially
      sequenceIdAccounting.update(key.getEncodedRegionName(),
          FSWALEntry.collectFamilies(edits.getCells()), kafkaOffset, true);
    }
    return -1; // since WALEntry already write to KAFKA(client side), no need to do sync
  }

  @Override
  public long appendMarker(HRegionInfo info, WALKey key, WALEdit edits)
      throws IOException {
    // append Marker edit to hdfsWal, the Marker is needed (see HBASE-2231)
    return hdfsWal.append(info, key, edits, false);
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
  public Long startCacheFlush(byte[] encodedRegionName, Set<byte[]> families) {
    return this.sequenceIdAccounting.startCacheFlush(encodedRegionName, families);
  }

  @Override
  public Long startCacheFlush(byte[] encodedRegionName, Map<byte[], Long> familyToSeq) {
    for (byte[] family : familyToSeq.keySet()) {
      // values of familyToSeq are mvcc txid, replace it with HConstants.NO_SEQNUM
      familyToSeq.put(family, HConstants.NO_SEQNUM);
    }
    return this.sequenceIdAccounting.startCacheFlush(encodedRegionName, familyToSeq);
  }

  @Override
  public void completeCacheFlush(byte[] encodedRegionName) {
    this.sequenceIdAccounting.completeCacheFlush(encodedRegionName);
  }

  @Override
  public void abortCacheFlush(byte[] encodedRegionName) {
    this.sequenceIdAccounting.abortCacheFlush(encodedRegionName);
  }

  @Override
  @VisibleForTesting
  public long getEarliestMemstoreSeqNum(byte[] encodedRegionName) {
    return this.sequenceIdAccounting.getLowestSequenceId(encodedRegionName);
  }

  @Override
  public long getEarliestMemstoreSeqNum(byte[] encodedRegionName, byte[] familyName) {
    return this.sequenceIdAccounting.getLowestSequenceId(encodedRegionName, familyName);
  }
}
