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
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.ipc.ServerCall;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.shaded.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CollectionUtils;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALKey;

/**
 * A WAL Entry for {@link AbstractFSWAL} implementation.  Immutable.
 * A subclass of {@link Entry} that carries extra info across the ring buffer such as
 * region sequenceid (we want to use this later, just before we write the WAL to ensure region
 * edits maintain order).  The extra info added here is not 'serialized' as part of the WALEdit
 * hence marked 'transient' to underline this fact.  It also adds mechanism so we can wait on
 * the assign of the region sequence id.  See #stampRegionSequenceId().
 */
@InterfaceAudience.Private
class FSWALEntry extends Entry {
  // The below data members are denoted 'transient' just to highlight these are not persisted;
  // they are only in memory and held here while passing over the ring buffer.
  private final transient long txid;

  /**
   * If false, means this is a meta edit written by the hbase system itself. It was not in
   * memstore. HBase uses these edit types to note in the log operational transitions such
   * as compactions, flushes, or region open/closes.
   */
  private final transient boolean inMemstore;

  /**
   * Set if this is a meta edit and it is of close region type.
   */
  private final transient boolean closeRegion;

  private final transient HRegionInfo hri;
  private final transient Set<byte[]> familyNames;
  private final transient Optional<ServerCall<?>> rpcCall;

  /**
   * @param inMemstore If true, then this is a data edit, one that came from client. If false, it
   *   is a meta edit made by the hbase system itself and is for the WAL only.
   */
  FSWALEntry(final long txid, final WALKey key, final WALEdit edit,
      final HRegionInfo hri, final boolean inMemstore, ServerCall<?> rpcCall) {
    super(key, edit);
    this.inMemstore = inMemstore;
    this.closeRegion = !inMemstore && edit.isRegionCloseMarker();
    this.hri = hri;
    this.txid = txid;
    if (inMemstore) {
      // construct familyNames here to reduce the work of log sinker.
      this.familyNames = collectFamilies(edit.getCells());
    } else {
      this.familyNames = Collections.emptySet();
    }
    this.rpcCall = Optional.ofNullable(rpcCall);
    if (rpcCall != null) {
      rpcCall.retainByWAL();
    }
  }

  @VisibleForTesting
  static Set<byte[]> collectFamilies(List<Cell> cells) {
    if (CollectionUtils.isEmpty(cells)) {
      return Collections.<byte[]> emptySet();
    } else {
      Set<byte[]> set = new TreeSet<>(Bytes.BYTES_COMPARATOR);
      for (Cell cell: cells) {
        if (!WALEdit.isMetaEditFamily(cell)) {
          set.add(CellUtil.cloneFamily(cell));
        }
      }
      return set;
    }
  }

  public String toString() {
    return "sequence=" + this.txid + ", " + super.toString();
  };

  boolean isInMemstore() {
    return this.inMemstore;
  }

  boolean isCloseRegion() {
    return closeRegion;
  }

  HRegionInfo getHRegionInfo() {
    return this.hri;
  }

  /**
   * @return The transaction id of this edit.
   */
  long getTxid() {
    return this.txid;
  }

  /**
   * Here is where a WAL edit gets its sequenceid.
   * @param we after HBASE-17471 we already get the mvcc number
   * in WriteEntry, just stamp the writenumber to cells and walkey
   * @return The sequenceid we stamped on this edit.
   * @throws IOException
   */
  long stampRegionSequenceId(MultiVersionConcurrencyControl.WriteEntry we) throws IOException {
    long regionSequenceId = we.getWriteNumber();
    if (!this.getEdit().isReplay() && inMemstore) {
      for (Cell c : getEdit().getCells()) {
        CellUtil.setSequenceId(c, regionSequenceId);
      }
    }

    getKey().setWriteEntry(we);
    return regionSequenceId;
  }

  /**
   * @return the family names which are effected by this edit.
   */
  Set<byte[]> getFamilyNames() {
    return familyNames;
  }

  void release() {
    rpcCall.ifPresent(ServerCall::releaseByWAL);
  }
}
