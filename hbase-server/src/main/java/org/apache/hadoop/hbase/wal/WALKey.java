/**
 *
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
package org.apache.hadoop.hbase.wal;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.regionserver.SequenceId;
// imports for things that haven't moved from regionserver.wal yet.
import org.apache.hadoop.hbase.regionserver.wal.CompressionContext;
import org.apache.hadoop.hbase.regionserver.wal.WALCellCodec;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.NameBytesPair;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.FamilyScope;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.ScopeType;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * A Key for an entry in the change log.
 *
 * The log intermingles edits to many tables and rows, so each log entry
 * identifies the appropriate table and row.  Within a table and row, they're
 * also sorted.
 *
 * <p>Some Transactional edits (START, COMMIT, ABORT) will not have an
 * associated row.
 *
 * Note that protected members marked @InterfaceAudience.Private are only protected
 * to support the legacy HLogKey class, which is in a different package.
 * 
 * <p>
 */
// TODO: Key and WALEdit are never used separately, or in one-to-many relation, for practical
//       purposes. They need to be merged into WALEntry.
// TODO: Cleanup. We have logSeqNum and then WriteEntry, both are sequence id'ing. Fix.
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.REPLICATION)
public class WALKey implements SequenceId, Comparable<WALKey> {
  @InterfaceAudience.Private // For internal use only.
  public MultiVersionConcurrencyControl getMvcc() {
    return mvcc;
  }

  /**
   * Will block until a write entry has been assigned by they WAL subsystem.
   * @return A WriteEntry gotten from local WAL subsystem. Must be completed by calling
   * mvcc#complete or mvcc#completeAndWait.
   * @throws InterruptedIOException
   * @see
   * #setWriteEntry(org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl.WriteEntry)
   */
  @InterfaceAudience.Private // For internal use only.
  public MultiVersionConcurrencyControl.WriteEntry getWriteEntry() throws InterruptedIOException {
    assert this.writeEntry != null;
    return this.writeEntry;
  }

  @InterfaceAudience.Private // For internal use only.
  public void setWriteEntry(MultiVersionConcurrencyControl.WriteEntry writeEntry) {
    assert this.writeEntry == null;
    this.writeEntry = writeEntry;
    // Set our sequenceid now using WriteEntry.
    this.sequenceId = this.writeEntry.getWriteNumber();
  }

  // REMOVE!!!! No more Writables!!!!
  // Should be < 0 (@see HLogKey#readFields(DataInput))
  // version 2 supports WAL compression
  // public members here are only public because of HLogKey
  @InterfaceAudience.Private
  protected enum Version {
    UNVERSIONED(0),
    // Initial number we put on WALKey when we introduced versioning.
    INITIAL(-1),
    // Version -2 introduced a dictionary compression facility.  Only this
    // dictionary-based compression is available in version -2.
    COMPRESSED(-2);

    public final int code;
    static final Version[] byCode;
    static {
      byCode = Version.values();
      for (int i = 0; i < byCode.length; i++) {
        if (byCode[i].code != -1 * i) {
          throw new AssertionError("Values in this enum should be descending by one");
        }
      }
    }

    Version(int code) {
      this.code = code;
    }

    public boolean atLeast(Version other) {
      return code <= other.code;
    }

    public static Version fromCode(int code) {
      return byCode[code * -1];
    }
  }

  /*
   * This is used for reading the log entries created by the previous releases
   * (0.94.11) which write the clusters information to the scopes of WALEdit.
   */
  private static final String PREFIX_CLUSTER_KEY = ".";


  // visible for deprecated HLogKey
  @InterfaceAudience.Private
  protected static final Version VERSION = Version.COMPRESSED;

  /** Used to represent when a particular wal key doesn't know/care about the sequence ordering. */
  public static final long NO_SEQUENCE_ID = -1;


  // visible for deprecated HLogKey
  @InterfaceAudience.Private
  protected byte [] encodedRegionName;
  // visible for deprecated HLogKey
  @InterfaceAudience.Private
  protected TableName tablename;
  /**
   * SequenceId for this edit. Set post-construction at write-to-WAL time. Until then it is
   * NO_SEQUENCE_ID. Change it so multiple threads can read it -- e.g. access is synchronized.
   */
  private long sequenceId;

  /**
   * Used during WAL replay; the sequenceId of the edit when it came into the system.
   */
  private long origLogSeqNum = 0;
  // Time at which this edit was written.
  // visible for deprecated HLogKey
  @InterfaceAudience.Private
  protected long writeTime;

  // The first element in the list is the cluster id on which the change has originated
  // visible for deprecated HLogKey
  @InterfaceAudience.Private
  protected List<UUID> clusterIds;
  private Map<String, byte[]> attributes;
  private NavigableMap<byte[], Integer> replicationScope;

  private long nonceGroup = HConstants.NO_NONCE;
  private long nonce = HConstants.NO_NONCE;
  private MultiVersionConcurrencyControl mvcc;
  /**
   * Set in a way visible to multiple threads; e.g. synchronized getter/setters.
   */
  private MultiVersionConcurrencyControl.WriteEntry writeEntry;
  public static final List<UUID> EMPTY_UUIDS = Collections.unmodifiableList(new ArrayList<UUID>());

  // visible for deprecated HLogKey
  @InterfaceAudience.Private
  protected CompressionContext compressionContext;

  public WALKey() {
    init(null, null, 0L, HConstants.LATEST_TIMESTAMP,
      new ArrayList<UUID>(), HConstants.NO_NONCE, HConstants.NO_NONCE, null, null);
  }

  public WALKey(final NavigableMap<byte[], Integer> replicationScope) {
    init(null, null, 0L, HConstants.LATEST_TIMESTAMP,
        new ArrayList<UUID>(), HConstants.NO_NONCE, HConstants.NO_NONCE, null, replicationScope);
  }

  @VisibleForTesting
  public WALKey(final byte[] encodedRegionName, final TableName tablename,
                long logSeqNum,
      final long now, UUID clusterId) {
    List<UUID> clusterIds = new ArrayList<UUID>(1);
    clusterIds.add(clusterId);
    init(encodedRegionName, tablename, logSeqNum, now, clusterIds,
        HConstants.NO_NONCE, HConstants.NO_NONCE, null, null);
  }

  /**
   * @deprecated Remove. Useless.
   */
  @Deprecated // REMOVE
  public WALKey(final byte[] encodedRegionName, final TableName tablename,
      final NavigableMap<byte[], Integer> replicationScope) {
    this(encodedRegionName, tablename, System.currentTimeMillis(), replicationScope);
  }

  // TODO: Fix being able to pass in sequenceid.
  public WALKey(final byte[] encodedRegionName, final TableName tablename, final long now) {
    init(encodedRegionName,
        tablename,
        NO_SEQUENCE_ID,
        now,
        EMPTY_UUIDS,
        HConstants.NO_NONCE,
        HConstants.NO_NONCE,
        null, null);
  }

  // TODO: Fix being able to pass in sequenceid.
  public WALKey(final byte[] encodedRegionName, final TableName tablename, final long now,
      final NavigableMap<byte[], Integer> replicationScope) {
    init(encodedRegionName, tablename, NO_SEQUENCE_ID, now, EMPTY_UUIDS, HConstants.NO_NONCE,
        HConstants.NO_NONCE, null, replicationScope);
  }

  public WALKey(final byte[] encodedRegionName, final TableName tablename, final long now,
      MultiVersionConcurrencyControl mvcc, final NavigableMap<byte[], Integer> replicationScope) {
    init(encodedRegionName, tablename, NO_SEQUENCE_ID, now, EMPTY_UUIDS, HConstants.NO_NONCE,
        HConstants.NO_NONCE, mvcc, replicationScope);
  }

  // TODO: Fix being able to pass in sequenceid.
  public WALKey(final byte[] encodedRegionName,
                final TableName tablename,
                final long now,
                MultiVersionConcurrencyControl mvcc) {
    init(encodedRegionName,
        tablename,
        NO_SEQUENCE_ID,
        now,
        EMPTY_UUIDS,
        HConstants.NO_NONCE,
        HConstants.NO_NONCE,
        mvcc, null);
  }

  /**
   * Create the log key for writing to somewhere.
   * We maintain the tablename mainly for debugging purposes.
   * A regionName is always a sub-table object.
   * <p>Used by log splitting and snapshots.
   *
   * @param encodedRegionName Encoded name of the region as returned by
   *                         <code>HRegionInfo#getEncodedNameAsBytes()</code>.
   * @param tablename         - name of table
   * @param logSeqNum         - log sequence number
   * @param now               Time at which this edit was written.
   * @param clusterIds        the clusters that have consumed the change(used in Replication)
   * @param nonceGroup        the nonceGroup
   * @param nonce             the nonce
   * @param mvcc              the mvcc associate the WALKey
   * @param replicationScope  the non-default replication scope
   *                          associated with the region's column families
   */
  // TODO: Fix being able to pass in sequenceid.
  public WALKey(final byte[] encodedRegionName, final TableName tablename, long logSeqNum,
      final long now, List<UUID> clusterIds, long nonceGroup, long nonce,
      MultiVersionConcurrencyControl mvcc, final NavigableMap<byte[], Integer> replicationScope) {
    init(encodedRegionName, tablename, logSeqNum, now, clusterIds, nonceGroup, nonce, mvcc,
        replicationScope);
  }

  /**
   * Create the log key for writing to somewhere.
   * We maintain the tablename mainly for debugging purposes.
   * A regionName is always a sub-table object.
   * <p>Used by log splitting and snapshots.
   *
   * @param encodedRegionName Encoded name of the region as returned by
   *                          <code>HRegionInfo#getEncodedNameAsBytes()</code>.
   * @param tablename         - name of table
   * @param logSeqNum         - log sequence number
   * @param now               Time at which this edit was written.
   * @param clusterIds        the clusters that have consumed the change(used in Replication)
   */
  // TODO: Fix being able to pass in sequenceid.
  public WALKey(final byte[] encodedRegionName,
                final TableName tablename,
                long logSeqNum,
                final long now,
                List<UUID> clusterIds,
                long nonceGroup,
                long nonce,
                MultiVersionConcurrencyControl mvcc) {
    init(encodedRegionName, tablename, logSeqNum, now, clusterIds, nonceGroup, nonce, mvcc, null);
  }

  /**
   * Create the log key for writing to somewhere.
   * We maintain the tablename mainly for debugging purposes.
   * A regionName is always a sub-table object.
   *
   * @param encodedRegionName Encoded name of the region as returned by
   *                          <code>HRegionInfo#getEncodedNameAsBytes()</code>.
   * @param tablename         the tablename
   * @param now               Time at which this edit was written.
   * @param clusterIds        the clusters that have consumed the change(used in Replication)
   * @param nonceGroup
   * @param nonce
   * @param mvcc mvcc control used to generate sequence numbers and control read/write points
   */
  public WALKey(final byte[] encodedRegionName, final TableName tablename,
                final long now, List<UUID> clusterIds, long nonceGroup,
                final long nonce, final MultiVersionConcurrencyControl mvcc) {
    init(encodedRegionName, tablename, NO_SEQUENCE_ID, now, clusterIds, nonceGroup, nonce, mvcc,
        null);
  }

  /**
   * Create the log key for writing to somewhere.
   * We maintain the tablename mainly for debugging purposes.
   * A regionName is always a sub-table object.
   *
   * @param encodedRegionName Encoded name of the region as returned by
   *                          <code>HRegionInfo#getEncodedNameAsBytes()</code>.
   * @param tablename
   * @param now               Time at which this edit was written.
   * @param clusterIds        the clusters that have consumed the change(used in Replication)
   * @param nonceGroup        the nonceGroup
   * @param nonce             the nonce
   * @param mvcc mvcc control used to generate sequence numbers and control read/write points
   * @param replicationScope  the non-default replication scope of the column families
   */
  public WALKey(final byte[] encodedRegionName, final TableName tablename,
                final long now, List<UUID> clusterIds, long nonceGroup,
                final long nonce, final MultiVersionConcurrencyControl mvcc,
                NavigableMap<byte[], Integer> replicationScope) {
    init(encodedRegionName, tablename, NO_SEQUENCE_ID, now, clusterIds, nonceGroup, nonce, mvcc,
        replicationScope);
  }

  /**
   * Create the log key for writing to somewhere.
   * We maintain the tablename mainly for debugging purposes.
   * A regionName is always a sub-table object.
   *
   * @param encodedRegionName Encoded name of the region as returned by
   *                          <code>HRegionInfo#getEncodedNameAsBytes()</code>.
   * @param tablename
   * @param logSeqNum
   * @param nonceGroup
   * @param nonce
   */
  public WALKey(final byte[] encodedRegionName,
                final TableName tablename,
                long logSeqNum,
                long nonceGroup,
                long nonce,
                final MultiVersionConcurrencyControl mvcc) {
    init(encodedRegionName,
        tablename,
        logSeqNum,
        EnvironmentEdgeManager.currentTime(),
        EMPTY_UUIDS,
        nonceGroup,
        nonce,
        mvcc, null);
  }

  @InterfaceAudience.Private
  protected void init(final byte[] encodedRegionName,
                      final TableName tablename,
                      long logSeqNum,
                      final long now,
                      List<UUID> clusterIds,
                      long nonceGroup,
                      long nonce,
                      MultiVersionConcurrencyControl mvcc,
                      NavigableMap<byte[], Integer> replicationScope) {
    this.sequenceId = logSeqNum;
    this.writeTime = now;
    this.clusterIds = clusterIds;
    this.encodedRegionName = encodedRegionName;
    this.tablename = tablename;
    this.nonceGroup = nonceGroup;
    this.nonce = nonce;
    this.mvcc = mvcc;
    if (logSeqNum != NO_SEQUENCE_ID) {
      setSequenceId(logSeqNum);
    }
    this.replicationScope = replicationScope;
  }

  // For HLogKey and deserialization. DO NOT USE. See setWriteEntry below.
  @InterfaceAudience.Private
  public void setSequenceId(long sequenceId) {
    this.sequenceId = sequenceId;
  }

  /**
   * @param compressionContext Compression context to use
   */
  public void setCompressionContext(CompressionContext compressionContext) {
    this.compressionContext = compressionContext;
  }

  /** @return encoded region name */
  public byte [] getEncodedRegionName() {
    return encodedRegionName;
  }

  /** @return table name */
  public TableName getTablename() {
    return tablename;
  }

  /** @return log sequence number
   * @deprecated Use {@link #getSequenceId()}
   */
  @Deprecated
  public long getLogSeqNum() {
    return getSequenceId();
  }

  /**
   * Used to set original seq Id for WALKey during wal replay
   * @param seqId
   */
  public void setOrigLogSeqNum(final long seqId) {
    this.origLogSeqNum = seqId;
  }
  
  /**
   * Return a positive long if current WALKey is created from a replay edit
   * @return original sequence number of the WALEdit
   */
  public long getOrigLogSeqNum() {
    return this.origLogSeqNum;
  }
  
  public long getSequenceId() {
    return this.sequenceId;
  }

  /**
   * @return the write time
   */
  public long getWriteTime() {
    return this.writeTime;
  }

  public NavigableMap<byte[], Integer> getReplicationScopes() {
    return replicationScope;
  }

  /** @return The nonce group */
  public long getNonceGroup() {
    return nonceGroup;
  }

  /** @return The nonce */
  public long getNonce() {
    return nonce;
  }

  private void setReplicationScope(NavigableMap<byte[], Integer> replicationScope) {
    this.replicationScope = replicationScope;
  }

  public void serializeReplicationScope(boolean serialize) {
    if (!serialize) {
      setReplicationScope(null);
    }
  }

  public void setAttribute(String name, byte[] value) {
    if (this.attributes == null) {
      attributes = new HashMap<String, byte[]>();
    }
    if (value == null) {
      attributes.remove(name);
      if (attributes.isEmpty()) {
        this.attributes = null;
      }
    } else {
      attributes.put(name, value);
    }
  }

  public Map<String, byte[]> getAttributeMap() {
    return attributes; 
  }

  public void readOlderScopes(NavigableMap<byte[], Integer> scopes) {
    if (scopes != null) {
      Iterator<Map.Entry<byte[], Integer>> iterator = scopes.entrySet()
          .iterator();
      while (iterator.hasNext()) {
        Map.Entry<byte[], Integer> scope = iterator.next();
        String key = Bytes.toString(scope.getKey());
        if (key.startsWith(PREFIX_CLUSTER_KEY)) {
          addClusterId(UUID.fromString(key.substring(PREFIX_CLUSTER_KEY
              .length())));
          iterator.remove();
        }
      }
      if (scopes.size() > 0) {
        this.replicationScope = scopes;
      }
    }
  }

  /**
   * Marks that the cluster with the given clusterId has consumed the change
   */
  public void addClusterId(UUID clusterId) {
    if (!clusterIds.contains(clusterId)) {
      clusterIds.add(clusterId);
    }
  }

  /**
   * @return the set of cluster Ids that have consumed the change
   */
  public List<UUID> getClusterIds() {
    return clusterIds;
  }

  /**
   * @return the cluster id on which the change has originated. It there is no such cluster, it
   *         returns DEFAULT_CLUSTER_ID (cases where replication is not enabled)
   */
  public UUID getOriginatingClusterId(){
    return clusterIds.isEmpty() ? HConstants.DEFAULT_CLUSTER_ID : clusterIds.get(0);
  }

  @Override
  public String toString() {
    return tablename + "/" + Bytes.toString(encodedRegionName) + "/" +
        sequenceId;
  }

  /**
   * Produces a string map for this key. Useful for programmatic use and
   * manipulation of the data stored in an WALKey, for example, printing
   * as JSON.
   *
   * @return a Map containing data from this key
   */
  public Map<String, Object> toStringMap() {
    Map<String, Object> stringMap = new HashMap<String, Object>();
    stringMap.put("table", tablename);
    stringMap.put("region", Bytes.toStringBinary(encodedRegionName));
    stringMap.put("sequence", getSequenceId());
    return stringMap;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    return compareTo((WALKey)obj) == 0;
  }

  @Override
  public int hashCode() {
    int result = Bytes.hashCode(this.encodedRegionName);
    result ^= getSequenceId();
    result ^= this.writeTime;
    return result;
  }

  @Override
  public int compareTo(WALKey o) {
    int result = Bytes.compareTo(this.encodedRegionName, o.encodedRegionName);
    if (result == 0) {
      long sid = getSequenceId();
      long otherSid = o.getSequenceId();
      if (sid < otherSid) {
        result = -1;
      } else if (sid  > otherSid) {
        result = 1;
      }
      if (result == 0) {
        if (this.writeTime < o.writeTime) {
          result = -1;
        } else if (this.writeTime > o.writeTime) {
          return 1;
        }
      }
    }
    // why isn't cluster id accounted for?
    return result;
  }

  /**
   * Drop this instance's tablename byte array and instead
   * hold a reference to the provided tablename. This is not
   * meant to be a general purpose setter - it's only used
   * to collapse references to conserve memory.
   */
  void internTableName(TableName tablename) {
    // We should not use this as a setter - only to swap
    // in a new reference to the same table name.
    assert tablename.equals(this.tablename);
    this.tablename = tablename;
  }

  /**
   * Drop this instance's region name byte array and instead
   * hold a reference to the provided region name. This is not
   * meant to be a general purpose setter - it's only used
   * to collapse references to conserve memory.
   */
  void internEncodedRegionName(byte []encodedRegionName) {
    // We should not use this as a setter - only to swap
    // in a new reference to the same table name.
    assert Bytes.equals(this.encodedRegionName, encodedRegionName);
    this.encodedRegionName = encodedRegionName;
  }

  public WALProtos.WALKey.Builder getBuilder(
      WALCellCodec.ByteStringCompressor compressor) throws IOException {
    WALProtos.WALKey.Builder builder = WALProtos.WALKey.newBuilder();
    if (compressionContext == null) {
      builder.setEncodedRegionName(ByteStringer.wrap(this.encodedRegionName));
      builder.setTableName(ByteStringer.wrap(this.tablename.getName()));
    } else {
      builder.setEncodedRegionName(compressor.compress(this.encodedRegionName,
          compressionContext.regionDict));
      builder.setTableName(compressor.compress(this.tablename.getName(),
          compressionContext.tableDict));
    }
    builder.setLogSequenceNumber(getSequenceId());
    builder.setWriteTime(writeTime);
    if (this.origLogSeqNum > 0) {
      builder.setOrigSequenceNumber(this.origLogSeqNum);
    }
    if (this.nonce != HConstants.NO_NONCE) {
      builder.setNonce(nonce);
    }
    if (this.nonceGroup != HConstants.NO_NONCE) {
      builder.setNonceGroup(nonceGroup);
    }
    HBaseProtos.UUID.Builder uuidBuilder = HBaseProtos.UUID.newBuilder();
    for (UUID clusterId : clusterIds) {
      uuidBuilder.setLeastSigBits(clusterId.getLeastSignificantBits());
      uuidBuilder.setMostSigBits(clusterId.getMostSignificantBits());
      builder.addClusterIds(uuidBuilder.build());
    }
    if (this.attributes != null && attributes.size() > 0) {
      HBaseProtos.NameBytesPair.Builder attrBuilder = HBaseProtos.NameBytesPair.newBuilder();
      for (Map.Entry<String, byte[]> attrEntry : attributes.entrySet()) {
        attrBuilder.setName(attrEntry.getKey());
        attrBuilder.setValue(ByteString.copyFrom(attrEntry.getValue()));
        builder.addAttribute(attrBuilder.build());
      }
    }
    if (replicationScope != null) {
      for (Map.Entry<byte[], Integer> e : replicationScope.entrySet()) {
        ByteString family = (compressionContext == null)
            ? ByteStringer.wrap(e.getKey())
            : compressor.compress(e.getKey(), compressionContext.familyDict);
        builder.addScopes(FamilyScope.newBuilder()
            .setFamily(family).setScopeType(ScopeType.valueOf(e.getValue())));
      }
    }
    return builder;
  }

  public void readFieldsFromPb(WALProtos.WALKey walKey,
                               WALCellCodec.ByteStringUncompressor uncompressor)
      throws IOException {
    if (this.compressionContext != null) {
      this.encodedRegionName = uncompressor.uncompress(
          walKey.getEncodedRegionName(), compressionContext.regionDict);
      byte[] tablenameBytes = uncompressor.uncompress(
          walKey.getTableName(), compressionContext.tableDict);
      this.tablename = TableName.valueOf(tablenameBytes);
    } else {
      this.encodedRegionName = walKey.getEncodedRegionName().toByteArray();
      this.tablename = TableName.valueOf(walKey.getTableName().toByteArray());
    }
    clusterIds.clear();
    if (walKey.hasClusterId()) {
      //When we are reading the older log (0.95.1 release)
      //This is definitely the originating cluster
      clusterIds.add(new UUID(walKey.getClusterId().getMostSigBits(), walKey.getClusterId()
          .getLeastSigBits()));
    }
    for (HBaseProtos.UUID clusterId : walKey.getClusterIdsList()) {
      clusterIds.add(new UUID(clusterId.getMostSigBits(), clusterId.getLeastSigBits()));
    }
    if (walKey.hasNonceGroup()) {
      this.nonceGroup = walKey.getNonceGroup();
    }
    if (walKey.hasNonce()) {
      this.nonce = walKey.getNonce();
    }
    this.attributes = null;
    List<NameBytesPair> attrList = walKey.getAttributeList();
    if (attrList != null && attrList.size() > 0) {
      attributes = new HashMap<String, byte[]>();
      for (NameBytesPair attr : attrList) {
        attributes.put(attr.getName(), attr.getValue().toByteArray());
      }
    }
    this.replicationScope = null;
    if (walKey.getScopesCount() > 0) {
      this.replicationScope = new TreeMap<byte[], Integer>(Bytes.BYTES_COMPARATOR);
      for (FamilyScope scope : walKey.getScopesList()) {
        byte[] family = (compressionContext == null) ? scope.getFamily().toByteArray() :
          uncompressor.uncompress(scope.getFamily(), compressionContext.familyDict);
        this.replicationScope.put(family, scope.getScopeType().getNumber());
      }
    }
    setSequenceId(walKey.getLogSequenceNumber());
    this.writeTime = walKey.getWriteTime();
    if(walKey.hasOrigSequenceNumber()) {
      this.origLogSeqNum = walKey.getOrigSequenceNumber();
    }
  }
}
