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
package org.apache.hadoop.hbase.regionserver;

import java.io.DataInput;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileBlock;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileInfo;
import org.apache.hadoop.hbase.io.hfile.HFileReader;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.io.hfile.HFileWriter;
import org.apache.hadoop.hbase.io.hfile.ReaderContext;
import org.apache.hadoop.hbase.io.hfile.ReaderContext.ReaderType;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.shaded.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hbase.shaded.com.google.common.base.Function;
import org.apache.hadoop.hbase.shaded.com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.shaded.com.google.common.collect.ImmutableList;
import org.apache.hadoop.hbase.shaded.com.google.common.collect.Ordering;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.BloomContext;
import org.apache.hadoop.hbase.util.BloomFilter;
import org.apache.hadoop.hbase.util.BloomFilterFactory;
import org.apache.hadoop.hbase.util.BloomFilterWriter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.RowBloomContext;
import org.apache.hadoop.hbase.util.RowColBloomContext;

/**
 * A Store data file.  Stores usually have one or more of these files.  They
 * are produced by flushing the memstore to disk.  To
 * create, instantiate a writer using {@link StoreFile.WriterBuilder}
 * and append data. Be sure to add any metadata before calling close on the
 * Writer (Use the appendMetadata convenience methods). On close, a StoreFile
 * is sitting in the Filesystem.  To refer to it, create a StoreFile instance
 * passing filesystem and path.  To read, call {@link #initReader()}
 * <p>StoreFiles may also reference store files in another Store.
 *
 * The reason for this weird pattern where you use a different instance for the
 * writer and a reader is that we write once but read a lot more.
 */
@InterfaceAudience.LimitedPrivate("Coprocessor")
public class StoreFile {
  private static final Log LOG = LogFactory.getLog(StoreFile.class.getName());

  // Keys for fileinfo values in HFile

  /** Max Sequence ID in FileInfo */
  public static final byte [] MAX_SEQ_ID_KEY = Bytes.toBytes("MAX_SEQ_ID_KEY");

  /** Major compaction flag in FileInfo */
  public static final byte[] MAJOR_COMPACTION_KEY =
      Bytes.toBytes("MAJOR_COMPACTION_KEY");

  /** Minor compaction flag in FileInfo */
  public static final byte[] EXCLUDE_FROM_MINOR_COMPACTION_KEY =
      Bytes.toBytes("EXCLUDE_FROM_MINOR_COMPACTION");

  /**
   * Key for compaction event which contains the compacted storefiles in FileInfo
   */
  public static final byte[] COMPACTION_EVENT_KEY = Bytes.toBytes("COMPACTION_EVENT_KEY");

  /** Bloom filter Type in FileInfo */
  public static final byte[] BLOOM_FILTER_TYPE_KEY =
      Bytes.toBytes("BLOOM_FILTER_TYPE");

  /** Delete Family Count in FileInfo */
  public static final byte[] DELETE_FAMILY_COUNT =
      Bytes.toBytes("DELETE_FAMILY_COUNT");

  /** Last Bloom filter key in FileInfo */
  public static final byte[] LAST_BLOOM_KEY = Bytes.toBytes("LAST_BLOOM_KEY");

  /** Key for Timerange information in metadata*/
  public static final byte[] TIMERANGE_KEY = Bytes.toBytes("TIMERANGE");

  /** Key for timestamp of earliest-put in metadata*/
  public static final byte[] EARLIEST_PUT_TS = Bytes.toBytes("EARLIEST_PUT_TS");

  /** Key for the number of mob cells in metadata*/
  public static final byte[] MOB_CELLS_COUNT = Bytes.toBytes("MOB_CELLS_COUNT");

  private final StoreFileInfo fileInfo;

  // StoreFile.Reader
  private volatile Reader initialReader;

  // Block cache configuration and reference.
  private final CacheConfig cacheConf;

  // Indicates if the file got compacted
  private volatile boolean compactedAway = false;

  // Keys for metadata stored in backing HFile.
  // Set when we obtain a Reader.
  private long sequenceid = -1;

  // max of the MemstoreTS in the KV's in this store
  // Set when we obtain a Reader.
  private long maxMemstoreTS = -1;
  private boolean bulkload = false;

  // firstKey, lastkey and cellComparator will be set when openReader.
  private Cell firstKey;

  private Cell lastKey;

  private CellComparator comparator;

  CacheConfig getCacheConf() {
    return this.cacheConf;
  }

  public Cell getFirstKey() {
    return firstKey;
  }

  public Cell getLastKey() {
    return lastKey;
  }

  public CellComparator getComparator() {
    return comparator;
  }

  public long getMaxMemstoreTS() {
    return maxMemstoreTS;
  }

  public void setMaxMemstoreTS(long maxMemstoreTS) {
    this.maxMemstoreTS = maxMemstoreTS;
  }

  public void setBulkload(boolean bulkload) {
    this.bulkload = bulkload;
  }

  public boolean isBulkload() {
    return bulkload;
  }

  // If true, this file was product of a major compaction.  Its then set
  // whenever you get a Reader.
  private AtomicBoolean majorCompaction = null;

  // If true, this file should not be included in minor compactions.
  // It's set whenever you get a Reader.
  private boolean excludeFromMinorCompaction = false;

  // This file was product of these compacted store files
  private final Set<String> compactedStoreFiles = new HashSet<>();

  /** Meta key set when store file is a result of a bulk load */
  public static final byte[] BULKLOAD_TASK_KEY =
    Bytes.toBytes("BULKLOAD_SOURCE_TASK");
  public static final byte[] BULKLOAD_TIME_KEY =
    Bytes.toBytes("BULKLOAD_TIMESTAMP");

  /**
   * Map of the metadata entries in the corresponding HFile. Populated when Reader is opened
   * after which it is not modified again.
   */
  private Map<byte[], byte[]> metadataMap;

  /**
   * Bloom filter type specified in column family configuration. Does not
   * necessarily correspond to the Bloom filter type present in the HFile.
   */
  private final BloomType cfBloomType;

  /**
   * Key for skipping resetting sequence id in metadata.
   * For bulk loaded hfiles, the scanner resets the cell seqId with the latest one,
   * if this metadata is set as true, the reset is skipped.
   */
  public static final byte[] SKIP_RESET_SEQ_ID = Bytes.toBytes("SKIP_RESET_SEQ_ID");

  /**
   * Constructor, loads a reader and it's indices, etc. May allocate a substantial amount of ram
   * depending on the underlying files (10-20MB?).
   * @param fs The current file system to use.
   * @param p The path of the file.
   * @param conf The current configuration.
   * @param cacheConf The cache configuration and block cache reference.
   * @param cfBloomType The bloom type to use for this store file as specified by column family
   *          configuration. This may or may not be the same as the Bloom filter type actually
   *          present in the HFile, because column family configuration might change. If this is
   *          {@link BloomType#NONE}, the existing Bloom filter is ignored.
   * @param primaryReplica true if this is a store file for primary replica, otherwise false.
   * @throws IOException
   */
  public StoreFile(FileSystem fs, Path p, Configuration conf, CacheConfig cacheConf,
      BloomType cfBloomType, boolean primaryReplica) throws IOException {
    this(new StoreFileInfo(conf, fs, p, primaryReplica), cfBloomType, cacheConf);
  }

  /**
   * Constructor, loads a reader and it's indices, etc. May allocate a substantial amount of ram
   * depending on the underlying files (10-20MB?).
   * @param fs fs The current file system to use.
   * @param fileInfo The store file information.
   * @param conf The current configuration.
   * @param cfBloomType cfBloomType The bloom type to use for this store file as specified by column
   *          family configuration. This may or may not be the same as the Bloom filter type
   *          actually present in the HFile, because column family configuration might change. If
   *          this is {@link BloomType#NONE}, the existing Bloom filter is ignored.
   * @param cacheConf The cache configuration and block cache reference.
   */
  public StoreFile(StoreFileInfo fileInfo, BloomType cfBloomType, CacheConfig cacheConf) {
    this.fileInfo = fileInfo;
    this.cacheConf = cacheConf;
    if (BloomFilterFactory.isGeneralBloomEnabled(fileInfo.getConf())) {
      this.cfBloomType = cfBloomType;
    } else {
      LOG.info("Ignoring bloom filter check for file " + this.getPath() + ": " + "cfBloomType=" +
          cfBloomType + " (disabled in config)");
      this.cfBloomType = BloomType.NONE;
    }
  }

  /**
   * @return the StoreFile object associated to this StoreFile.
   *         null if the StoreFile is not a reference.
   */
  public StoreFileInfo getFileInfo() {
    return this.fileInfo;
  }

  /**
   * @return Path or null if this StoreFile was made with a Stream.
   */
  public Path getPath() {
    return this.fileInfo.getPath();
  }

  /**
   * @return Returns the qualified path of this StoreFile
   */
  public Path getQualifiedPath() {
    FileSystem fs = fileInfo.getFileSystem();
    return this.fileInfo.getPath().makeQualified(fs.getUri(), fs.getWorkingDirectory());
  }

  /**
   * @return True if this is a StoreFile Reference; call
   * after {@link #open()} else may get wrong answer.
   */
  public boolean isReference() {
    return this.fileInfo.isReference();
  }

  /**
   * @return True if this file was made by a major compaction.
   */
  public boolean isMajorCompaction() {
    if (this.majorCompaction == null) {
      throw new NullPointerException("This has not been set yet");
    }
    return this.majorCompaction.get();
  }

  /**
   * @return True if this file should not be part of a minor compaction.
   */
  public boolean excludeFromMinorCompaction() {
    return this.excludeFromMinorCompaction;
  }

  /**
   * @return This files maximum edit sequence id.
   */
  public long getMaxSequenceId() {
    return this.sequenceid;
  }

  public long getModificationTimeStamp() throws IOException {
    return (fileInfo == null) ? 0 : fileInfo.getModificationTime();
  }

  /**
   * Only used by the Striped Compaction Policy
   * @param key
   * @return value associated with the metadata key
   */
  public byte[] getMetadataValue(byte[] key) {
    return metadataMap.get(key);
  }

  /**
   * Return the largest memstoreTS found across all storefiles in
   * the given list. Store files that were created by a mapreduce
   * bulk load are ignored, as they do not correspond to any specific
   * put operation, and thus do not have a memstoreTS associated with them.
   * @return 0 if no non-bulk-load files are provided or, this is Store that
   * does not yet have any store files.
   */
  public static long getMaxMemstoreTSInList(Collection<StoreFile> sfs) {
    long max = 0;
    for (StoreFile sf : sfs) {
      if (!sf.isBulkLoadResult()) {
        max = Math.max(max, sf.getMaxMemstoreTS());
      }
    }
    return max;
  }

  /**
   * Return the highest sequence ID found across all storefiles in
   * the given list.
   * @param sfs
   * @return 0 if no non-bulk-load files are provided or, this is Store that
   * does not yet have any store files.
   */
  public static long getMaxSequenceIdInList(Collection<StoreFile> sfs) {
    long max = 0;
    for (StoreFile sf : sfs) {
      max = Math.max(max, sf.getMaxSequenceId());
    }
    return max;
  }

  /**
   * Check if this storefile was created by bulk load.
   * When a hfile is bulk loaded into HBase, we append
   * '_SeqId_<id-when-loaded>' to the hfile name, unless
   * "hbase.mapreduce.bulkload.assign.sequenceNumbers" is
   * explicitly turned off.
   * If "hbase.mapreduce.bulkload.assign.sequenceNumbers"
   * is turned off, fall back to BULKLOAD_TIME_KEY.
   * @return true if this storefile was created by bulk load.
   */
  boolean isBulkLoadResult() {
    boolean bulkLoadedHFile = false;
    String fileName = this.getPath().getName();
    int startPos = fileName.indexOf("SeqId_");
    if (startPos != -1) {
      bulkLoadedHFile = true;
    }
    return bulkLoadedHFile || (metadataMap != null && metadataMap.containsKey(BULKLOAD_TIME_KEY));
  }

  @VisibleForTesting
  public boolean isCompactedAway() {
    return compactedAway;
  }

  @VisibleForTesting
  public int getRefCount() {
    return fileInfo.refCount.get();
  }

  /**
   * @return true if the file is still used in reads
   */
  public boolean isReferencedInReads() {
    int rc = fileInfo.refCount.get();
    assert rc >= 0; // we should not go negative.
    return rc > 0;
  }

  /**
   * Return the timestamp at which this bulk load file was generated.
   */
  public long getBulkLoadTimestamp() {
    byte[] bulkLoadTimestamp = metadataMap.get(BULKLOAD_TIME_KEY);
    return (bulkLoadTimestamp == null) ? 0 : Bytes.toLong(bulkLoadTimestamp);
  }

  /**
   * @return the cached value of HDFS blocks distribution. The cached value is
   * calculated when store file is opened.
   */
  public HDFSBlocksDistribution getHDFSBlockDistribution() {
    return this.fileInfo.getHDFSBlockDistribution();
  }

  /**
   * Opens reader on this store file. Called by Constructor.
   * @throws IOException
   * @see #closeReader(boolean)
   */
  private void open() throws IOException {
    fileInfo.initHDFSBlocksDistribution();
    long readahead = fileInfo.isNoReadahead() ? 0L : -1L;
    ReaderContext context = fileInfo.createReaderContext(false, readahead, ReaderType.PREAD);
    fileInfo.initHFileInfo(context);
    Reader reader = fileInfo.preStoreFileReaderOpen(context, cacheConf);
    if (reader == null) {
      reader = fileInfo.createReader(context, cacheConf);
      fileInfo.getHFileInfo().initMetaAndIndex(reader.getHFileReader());
    }
    this.initialReader = fileInfo.postStoreFileReaderOpen(context, cacheConf, reader);

    // Load up indices and fileinfo. This also loads Bloom filter type.
    metadataMap = Collections.unmodifiableMap(initialReader.loadFileInfo());

    // Read in our metadata.
    byte [] b = metadataMap.get(MAX_SEQ_ID_KEY);
    if (b != null) {
      // By convention, if halfhfile, top half has a sequence number > bottom
      // half. Thats why we add one in below. Its done for case the two halves
      // are ever merged back together --rare.  Without it, on open of store,
      // since store files are distinguished by sequence id, the one half would
      // subsume the other.
      this.sequenceid = Bytes.toLong(b);
      if (fileInfo.isTopReference()) {
        this.sequenceid += 1;
      }
    }

    if (isBulkLoadResult()){
      // generate the sequenceId from the fileName
      // fileName is of the form <randomName>_SeqId_<id-when-loaded>_
      String fileName = this.getPath().getName();
      // Use lastIndexOf() to get the last, most recent bulk load seqId.
      int startPos = fileName.lastIndexOf("SeqId_");
      if (startPos != -1) {
        this.sequenceid = Long.parseLong(fileName.substring(startPos + 6,
            fileName.indexOf('_', startPos + 6)));
        // Handle reference files as done above.
        if (fileInfo.isTopReference()) {
          this.sequenceid += 1;
        }
      }
      // SKIP_RESET_SEQ_ID only works in bulk loaded file.
      // In mob compaction, the hfile where the cells contain the path of a new mob file is bulk
      // loaded to hbase, these cells have the same seqIds with the old ones. We do not want
      // to reset new seqIds for them since this might make a mess of the visibility of cells that
      // have the same row key but different seqIds.
      boolean skipResetSeqId = isSkipResetSeqId(metadataMap.get(SKIP_RESET_SEQ_ID));
      if (skipResetSeqId) {
        // increase the seqId when it is a bulk loaded file from mob compaction.
        this.sequenceid += 1;
      }
      initialReader.setSkipResetSeqId(skipResetSeqId);
      initialReader.setBulkLoaded(true);
    }
    initialReader.setSequenceID(this.sequenceid);

    b = metadataMap.get(HFileWriter.MAX_MEMSTORE_TS_KEY);
    if (b != null) {
      this.maxMemstoreTS = Bytes.toLong(b);
    }

    b = metadataMap.get(MAJOR_COMPACTION_KEY);
    if (b != null) {
      boolean mc = Bytes.toBoolean(b);
      if (this.majorCompaction == null) {
        this.majorCompaction = new AtomicBoolean(mc);
      } else {
        this.majorCompaction.set(mc);
      }
    } else {
      // Presume it is not major compacted if it doesn't explicity say so
      // HFileOutputFormat explicitly sets the major compacted key.
      this.majorCompaction = new AtomicBoolean(false);
    }

    b = metadataMap.get(EXCLUDE_FROM_MINOR_COMPACTION_KEY);
    this.excludeFromMinorCompaction = (b != null && Bytes.toBoolean(b));

    BloomType hfileBloomType = initialReader.getBloomFilterType();
    if (cfBloomType != BloomType.NONE) {
      initialReader.loadBloomfilter(BlockType.GENERAL_BLOOM_META);
      if (hfileBloomType != cfBloomType) {
        LOG.info("HFile Bloom filter type for "
            + initialReader.getHFileReader().getName() + ": " + hfileBloomType
            + ", but " + cfBloomType + " specified in column family "
            + "configuration");
      }
    } else if (hfileBloomType != BloomType.NONE) {
      LOG.info("Bloom filter turned off by CF config for "
          + initialReader.getHFileReader().getName());
    }

    // load delete family bloom filter
    initialReader.loadBloomfilter(BlockType.DELETE_FAMILY_BLOOM_META);

    try {
      byte[] data = metadataMap.get(TIMERANGE_KEY);
      initialReader.timeRange = data == null ? null : TimeRangeTracker.parseFrom(data).toTimeRange();
    } catch (IllegalArgumentException e) {
      LOG.error("Error reading timestamp range data from meta -- " +
          "proceeding without", e);
      this.initialReader.timeRange = null;
    }
    try {
      byte[] data = metadataMap.get(COMPACTION_EVENT_KEY);
      this.compactedStoreFiles.addAll(ProtobufUtil.toCompactedStoreFiles(data));
    } catch (IOException e) {
      LOG.error("Error reading compacted storefiles from meta data", e);
    }
    // initialize so we can reuse them after reader closed.
    firstKey = initialReader.getFirstKey();
    lastKey = initialReader.getLastKey();
    comparator = initialReader.getComparator();
  }

  /**
   * Initialize the reader used for pread.
   */
  public void initReader() throws IOException {
    if (initialReader == null) {
      synchronized (this) {
        if (initialReader == null) {
          try {
            open();
          } catch (Exception e) {
            try {
              boolean evictOnClose = cacheConf != null ? cacheConf.shouldEvictOnClose() : true;
              this.closeReader(evictOnClose);
            } catch (IOException ee) {
              LOG.warn("failed to close reader", ee);
            }
            throw e;
          }
        }
      }
    }
  }

  private Reader createStreamReader(boolean canUseDropBehind) throws IOException {
    initReader();
    final boolean doDropBehind = canUseDropBehind && cacheConf.shouldDropBehindCompaction();
    ReaderContext context = fileInfo.createReaderContext(doDropBehind, -1, ReaderType.STREAM);
    Reader reader = fileInfo.preStoreFileReaderOpen(context, cacheConf);
    if (reader == null) {
      reader = fileInfo.createReader(context, cacheConf);
      // steam reader need copy stuffs from pread reader
      reader.copyFields(initialReader);
    }
    return fileInfo.postStoreFileReaderOpen(context, cacheConf, reader);
  }

  public StoreFileScanner getPreadScanner(boolean cacheBlocks, long readPt, long scannerOrder,
      boolean canOptimizeForNonNullColumn) {
    return getReader().getStoreFileScanner(cacheBlocks, true, false, readPt, scannerOrder,
      canOptimizeForNonNullColumn);
  }

  public StoreFileScanner getStreamScanner(boolean canUseDropBehind, boolean cacheBlocks,
      boolean isCompaction, long readPt, long scannerOrder, boolean canOptimizeForNonNullColumn)
      throws IOException {
    return createStreamReader(canUseDropBehind).getStoreFileScanner(cacheBlocks, false,
      isCompaction, readPt, scannerOrder, canOptimizeForNonNullColumn);
  }

  /**
   * @return Current reader.  Must call initReader first else returns null.
   * @see #initReader()
   */
  public Reader getReader() {
    return this.initialReader;
  }

  /**
   * @param evictOnClose whether to evict blocks belonging to this file
   * @throws IOException
   */
  public synchronized void closeReader(boolean evictOnClose)
      throws IOException {
    if (this.initialReader != null) {
      this.initialReader.close(evictOnClose);
      this.initialReader = null;
    }
  }

  /**
   * Marks the status of the file as compactedAway.
   */
  public void markCompactedAway() {
    this.compactedAway = true;
  }

  /**
   * Delete this file
   * @throws IOException
   */
  public void deleteReader() throws IOException {
    boolean evictOnClose =
        cacheConf != null? cacheConf.shouldEvictOnClose(): true;
    closeReader(evictOnClose);
    this.fileInfo.getFileSystem().delete(getPath(), true);
  }

  @Override
  public String toString() {
    return this.fileInfo.toString();
  }

  /**
   * @return a length description of this StoreFile, suitable for debug output
   */
  public String toStringDetailed() {
    StringBuilder sb = new StringBuilder();
    sb.append(this.getPath().toString());
    sb.append(", isReference=").append(isReference());
    sb.append(", isBulkLoadResult=").append(isBulkLoadResult());
    if (isBulkLoadResult()) {
      sb.append(", bulkLoadTS=").append(getBulkLoadTimestamp());
    } else {
      sb.append(", seqid=").append(getMaxSequenceId());
    }
    sb.append(", majorCompaction=").append(isMajorCompaction());

    return sb.toString();
  }

  /**
   * Gets whether to skip resetting the sequence id for cells.
   * @param skipResetSeqId The byte array of boolean.
   * @return Whether to skip resetting the sequence id.
   */
  private boolean isSkipResetSeqId(byte[] skipResetSeqId) {
    if (skipResetSeqId != null && skipResetSeqId.length == 1) {
      return Bytes.toBoolean(skipResetSeqId);
    }
    return false;
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="ICAST_INTEGER_MULTIPLY_CAST_TO_LONG",
      justification="Will not overflow")
  public static class WriterBuilder {
    private final Configuration conf;
    private final CacheConfig cacheConf;
    private final FileSystem fs;

    private CellComparator comparator = CellComparator.COMPARATOR;
    private BloomType bloomType = BloomType.NONE;
    private long maxKeyCount = 0;
    private Path dir;
    private Path filePath;
    private InetSocketAddress[] favoredNodes;
    private HFileContext fileContext;
    private boolean shouldDropCacheBehind;
    private Supplier<Collection<StoreFile>> compactedFilesSupplier = () -> Collections.emptySet();

    public WriterBuilder(Configuration conf, CacheConfig cacheConf,
        FileSystem fs) {
      this.conf = conf;
      this.cacheConf = cacheConf;
      this.fs = fs;
    }

    /**
     * Creates Builder with cache configuration disabled
     */
    public WriterBuilder(Configuration conf, FileSystem fs) {
      this.conf = conf;
      this.cacheConf = CacheConfig.DISABLED;
      this.fs = fs;
    }

    /**
     * Use either this method or {@link #withFilePath}, but not both.
     * @param dir Path to column family directory. The directory is created if
     *          does not exist. The file is given a unique name within this
     *          directory.
     * @return this (for chained invocation)
     */
    public WriterBuilder withOutputDir(Path dir) {
      Preconditions.checkNotNull(dir);
      this.dir = dir;
      return this;
    }

    /**
     * Use either this method or {@link #withOutputDir}, but not both.
     * @param filePath the StoreFile path to write
     * @return this (for chained invocation)
     */
    public WriterBuilder withFilePath(Path filePath) {
      Preconditions.checkNotNull(filePath);
      this.filePath = filePath;
      return this;
    }

    /**
     * @param favoredNodes an array of favored nodes or possibly null
     * @return this (for chained invocation)
     */
    public WriterBuilder withFavoredNodes(InetSocketAddress[] favoredNodes) {
      this.favoredNodes = favoredNodes;
      return this;
    }

    public WriterBuilder withComparator(CellComparator comparator) {
      Preconditions.checkNotNull(comparator);
      this.comparator = comparator;
      return this;
    }

    public WriterBuilder withBloomType(BloomType bloomType) {
      Preconditions.checkNotNull(bloomType);
      this.bloomType = bloomType;
      return this;
    }

    /**
     * @param maxKeyCount estimated maximum number of keys we expect to add
     * @return this (for chained invocation)
     */
    public WriterBuilder withMaxKeyCount(long maxKeyCount) {
      this.maxKeyCount = maxKeyCount;
      return this;
    }

    public WriterBuilder withFileContext(HFileContext fileContext) {
      this.fileContext = fileContext;
      return this;
    }

    public WriterBuilder withShouldDropCacheBehind(boolean shouldDropCacheBehind) {
      this.shouldDropCacheBehind = shouldDropCacheBehind;
      return this;
    }

    public WriterBuilder withCompactedFilesSupplier(
        Supplier<Collection<StoreFile>> compactedFilesSupplier) {
      this.compactedFilesSupplier = compactedFilesSupplier;
      return this;
    }

    /**
     * Create a store file writer. Client is responsible for closing file when
     * done. If metadata, add BEFORE closing using
     * {@link Writer#appendMetadata}.
     */
    public Writer build() throws IOException {
      if ((dir == null ? 0 : 1) + (filePath == null ? 0 : 1) != 1) {
        throw new IllegalArgumentException("Either specify parent directory " +
            "or file path");
      }

      if (dir == null) {
        dir = filePath.getParent();
      }

      if (!fs.exists(dir)) {
        fs.mkdirs(dir);
      }

      // set block storage policy for temp path
      String policyName = this.conf.get(HColumnDescriptor.STORAGE_POLICY);
      if (null == policyName) {
        policyName = this.conf.get(HStore.BLOCK_STORAGE_POLICY_KEY);
      }
      FSUtils.setStoragePolicy(this.fs, dir, policyName);

      if (filePath == null) {
        filePath = getUniqueFile(fs, dir);
        if (!BloomFilterFactory.isGeneralBloomEnabled(conf)) {
          bloomType = BloomType.NONE;
        }
      }

      if (comparator == null) {
        comparator = CellComparator.COMPARATOR;
      }
      return new Writer(fs, filePath,
          conf, cacheConf, comparator, bloomType, maxKeyCount, favoredNodes, fileContext,
          shouldDropCacheBehind, compactedFilesSupplier);
    }
  }

  /**
   * @param fs
   * @param dir Directory to create file in.
   * @return random filename inside passed <code>dir</code>
   */
  public static Path getUniqueFile(final FileSystem fs, final Path dir)
      throws IOException {
    if (!fs.getFileStatus(dir).isDirectory()) {
      throw new IOException("Expecting " + dir.toString() +
        " to be a directory");
    }
    return new Path(dir, UUID.randomUUID().toString().replaceAll("-", ""));
  }

  public Long getMinimumTimestamp() {
    return getReader().timeRange == null? null: getReader().timeRange.getMin();
  }

  public Long getMaximumTimestamp() {
    return getReader().timeRange == null? null: getReader().timeRange.getMax();
  }

  /**
   * Gets the approximate mid-point of this file that is optimal for use in splitting it.
   * @param comparator Comparator used to compare KVs.
   * @return The split point row, or null if splitting is not possible, or reader is null.
   */
  byte[] getFileSplitPoint(CellComparator comparator) throws IOException {
    if (initialReader == null) {
      LOG.warn("Storefile " + this + " Reader is null; cannot get split point");
      return null;
    }
    // Get first, last, and mid keys.  Midkey is the key that starts block
    // in middle of hfile.  Has column and timestamp.  Need to return just
    // the row we want to split on as midkey.
    Cell midkey = initialReader.midkey();
    if (midkey != null) {
      Cell firstKey = initialReader.getFirstKey();
      Cell lastKey = initialReader.getLastKey();
      // if the midkey is the same as the first or last keys, we cannot (ever) split this region.
      if (comparator.compareRows(midkey, firstKey) == 0
          || comparator.compareRows(midkey, lastKey) == 0) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("cannot split because midkey is the same as first or last row");
        }
        return null;
      }
      return CellUtil.cloneRow(midkey);
    }
    return null;
  }

  /**
   * A StoreFile writer.  Use this to read/write HBase Store Files. It is package
   * local because it is an implementation detail of the HBase regionserver.
   */
  public static class Writer implements CellSink, ShipperListener {
    private final BloomFilterWriter generalBloomFilterWriter;
    private final BloomFilterWriter deleteFamilyBloomFilterWriter;
    private final BloomType bloomType;
    private long earliestPutTs = HConstants.LATEST_TIMESTAMP;
    private long deleteFamilyCnt = 0;
    private BloomContext bloomContext = null;
    private BloomContext deleteFamilyBloomContext = null;

    private final TimeRangeTracker timeRangeTracker;
    private final Supplier<Collection<StoreFile>> compactedFilesSupplier;

    protected HFileWriter writer;

    /**
     * Creates an HFile.Writer that also write helpful meta data.
     * @param fs file system to write to
     * @param path file name to create
     * @param conf user configuration
     * @param comparator key comparator
     * @param bloomType bloom filter setting
     * @param maxKeys the expected maximum number of keys to be added. Was used
     *        for Bloom filter size in {@link HFile} format version 1.
     * @param favoredNodes
     * @param fileContext - The HFile context
     * @param shouldDropCacheBehind Drop pages written to page cache after writing the store file.
     * @throws IOException problem writing to FS
     */
    private Writer(FileSystem fs, Path path,
        final Configuration conf,
        CacheConfig cacheConf,
        final CellComparator comparator, BloomType bloomType, long maxKeys,
        InetSocketAddress[] favoredNodes, HFileContext fileContext,
        boolean shouldDropCacheBehind,
        Supplier<Collection<StoreFile>> compactedFilesSupplier) throws IOException {
      this.compactedFilesSupplier = compactedFilesSupplier;
      this.timeRangeTracker = TimeRangeTracker.create(TimeRangeTracker.Type.NON_SYNC);
      // TODO : Change all writers to be specifically created for compaction context
      writer = HFile.getWriterFactory(conf, cacheConf)
          .withPath(fs, path)
          .withComparator(comparator)
          .withFavoredNodes(favoredNodes)
          .withFileContext(fileContext)
          .withShouldDropCacheBehind(shouldDropCacheBehind)
          .create();
      generalBloomFilterWriter = BloomFilterFactory.createGeneralBloomAtWrite(
          conf, cacheConf, bloomType,
          (int) Math.min(maxKeys, Integer.MAX_VALUE), writer);

      if (generalBloomFilterWriter != null) {
        this.bloomType = bloomType;
        if (LOG.isTraceEnabled()) {
          LOG.trace("Bloom filter type for " + path + ": " + this.bloomType + ", " +
              generalBloomFilterWriter.getClass().getSimpleName());
        }
        // init bloom context
        switch (bloomType) {
        case ROW:
          bloomContext = new RowBloomContext(generalBloomFilterWriter, comparator);
          break;
        case ROWCOL:
          bloomContext = new RowColBloomContext(generalBloomFilterWriter, comparator);
          break;
        default:
          throw new IOException(
              "Invalid Bloom filter type: " + bloomType + " (ROW or ROWCOL expected)");
        }
      } else {
        // Not using Bloom filters.
        this.bloomType = BloomType.NONE;
      }

      // initialize delete family Bloom filter when there is NO RowCol Bloom
      // filter
      if (this.bloomType != BloomType.ROWCOL) {
        this.deleteFamilyBloomFilterWriter = BloomFilterFactory
            .createDeleteBloomAtWrite(conf, cacheConf,
                (int) Math.min(maxKeys, Integer.MAX_VALUE), writer);
        deleteFamilyBloomContext = new RowBloomContext(deleteFamilyBloomFilterWriter, comparator);
      } else {
        deleteFamilyBloomFilterWriter = null;
      }
      if (deleteFamilyBloomFilterWriter != null) {
        if (LOG.isTraceEnabled()) LOG.trace("Delete Family Bloom filter type for " + path + ": "
            + deleteFamilyBloomFilterWriter.getClass().getSimpleName());
      }
    }

    /**
     * Writes meta data.
     * Call before {@link #close()} since its written as meta data to this file.
     * @param maxSequenceId Maximum sequence id.
     * @param majorCompaction True if this file is product of a major compaction
     * @throws IOException problem writing to FS
     */
    public void appendMetadata(final long maxSequenceId, final boolean majorCompaction)
    throws IOException {
      appendMetadata(maxSequenceId, majorCompaction, Collections.emptySet());
    }

    /**
     * Writes meta data.
     * Call before {@link #close()} since its written as meta data to this file.
     * @param maxSequenceId Maximum sequence id.
     * @param majorCompaction True if this file is product of a major compaction
     * @param storeFiles The compacted store files to generate this new file
     * @throws IOException problem writing to FS
     */
    public void appendMetadata(final long maxSequenceId, final boolean majorCompaction,
        final Collection<StoreFile> storeFiles) throws IOException {
      writer.appendFileInfo(MAX_SEQ_ID_KEY, Bytes.toBytes(maxSequenceId));
      writer.appendFileInfo(MAJOR_COMPACTION_KEY, Bytes.toBytes(majorCompaction));
      writer.appendFileInfo(COMPACTION_EVENT_KEY, toCompactionEventTrackerBytes(storeFiles));
      appendTrackedTimestampsToMetadata();
    }

    /**
     * Used when write {@link HStoreFile#COMPACTION_EVENT_KEY} to new file's file info. The compacted
     * store files's name is needed. But if the compacted store file is a result of compaction, it's
     * compacted files which still not archived is needed, too. And don't need to add compacted files
     * recursively. If file A, B, C compacted to new file D, and file D compacted to new file E, will
     * write A, B, C, D to file E's compacted files. So if file E compacted to new file F, will add E
     * to F's compacted files first, then add E's compacted files: A, B, C, D to it. And no need to
     * add D's compacted file, as D's compacted files has been in E's compacted files, too.
     * See HBASE-20724 for more details.
     *
     * @param storeFiles The compacted store files to generate this new file
     * @return bytes of CompactionEventTracker
     */
    private byte[] toCompactionEventTrackerBytes(Collection<StoreFile> storeFiles) {
      Set<String> notArchivedCompactedStoreFiles =
          this.compactedFilesSupplier.get().stream().map(sf -> sf.getPath().getName())
              .collect(Collectors.toSet());
      Set<String> compactedStoreFiles = new HashSet<>();
      for (StoreFile storeFile : storeFiles) {
        compactedStoreFiles.add(storeFile.getFileInfo().getPath().getName());
        for (String csf : storeFile.getCompactedStoreFiles()) {
          if (notArchivedCompactedStoreFiles.contains(csf)) {
            compactedStoreFiles.add(csf);
          }
        }
      }
      return ProtobufUtil.toCompactionEventTrackerBytes(compactedStoreFiles);
    }

    /**
     * Writes meta data.
     * Call before {@link #close()} since its written as meta data to this file.
     * @param maxSequenceId Maximum sequence id.
     * @param majorCompaction True if this file is product of a major compaction
     * @param mobCellsCount The number of mob cells.
     * @throws IOException problem writing to FS
     */
    public void appendMetadata(final long maxSequenceId, final boolean majorCompaction,
        final long mobCellsCount) throws IOException {
      writer.appendFileInfo(MAX_SEQ_ID_KEY, Bytes.toBytes(maxSequenceId));
      writer.appendFileInfo(MAJOR_COMPACTION_KEY, Bytes.toBytes(majorCompaction));
      writer.appendFileInfo(MOB_CELLS_COUNT, Bytes.toBytes(mobCellsCount));
      appendTrackedTimestampsToMetadata();
    }

    /**
     * Add TimestampRange and earliest put timestamp to Metadata
     */
    public void appendTrackedTimestampsToMetadata() throws IOException {
      // TODO: The StoreFileReader always converts the byte[] to TimeRange
      // via TimeRangeTracker, so we should write the serialization data of TimeRange directly.
      appendFileInfo(TIMERANGE_KEY, TimeRangeTracker.toByteArray(timeRangeTracker));
      appendFileInfo(EARLIEST_PUT_TS, Bytes.toBytes(earliestPutTs));
    }

    /**
     * Record the earlest Put timestamp.
     *
     * If the timeRangeTracker is not set,
     * update TimeRangeTracker to include the timestamp of this key
     * @param cell
     */
    public void trackTimestamps(final Cell cell) {
      if (KeyValue.Type.Put.getCode() == cell.getTypeByte()) {
        earliestPutTs = Math.min(earliestPutTs, cell.getTimestamp());
      }
      timeRangeTracker.includeTimestamp(cell);
    }

    private void appendGeneralBloomfilter(final Cell cell) throws IOException {
      if (this.generalBloomFilterWriter != null) {
        /*
         * http://2.bp.blogspot.com/_Cib_A77V54U/StZMrzaKufI/AAAAAAAAADo/ZhK7bGoJdMQ/s400/KeyValue.png
         * Key = RowLen + Row + FamilyLen + Column [Family + Qualifier] + TimeStamp
         *
         * 2 Types of Filtering:
         *  1. Row = Row
         *  2. RowCol = Row + Qualifier
         */
      bloomContext.writeBloom(cell);
      }
    }

    private void appendDeleteFamilyBloomFilter(final Cell cell)
        throws IOException {
      if (!CellUtil.isDeleteFamily(cell) && !CellUtil.isDeleteFamilyVersion(cell)) {
        return;
      }

      // increase the number of delete family in the store file
      deleteFamilyCnt++;
      if (this.deleteFamilyBloomFilterWriter != null) {
        deleteFamilyBloomContext.writeBloom(cell);
      }
    }

    @Override
    public void append(final Cell cell) throws IOException {
      appendGeneralBloomfilter(cell);
      appendDeleteFamilyBloomFilter(cell);
      writer.append(cell);
      trackTimestamps(cell);
    }

    @Override
    public void beforeShipped() throws IOException {
      // For now these writer will always be of type ShipperListener true.
      // TODO : Change all writers to be specifically created for compaction context
      writer.beforeShipped();
      if (generalBloomFilterWriter != null) {
        generalBloomFilterWriter.beforeShipped();
      }
      if (deleteFamilyBloomFilterWriter != null) {
        deleteFamilyBloomFilterWriter.beforeShipped();
      }
    }

    public Path getPath() {
      return this.writer.getPath();
    }

    public boolean hasGeneralBloom() {
      return this.generalBloomFilterWriter != null;
    }

    /**
     * For unit testing only.
     *
     * @return the Bloom filter used by this writer.
     */
    BloomFilterWriter getGeneralBloomWriter() {
      return generalBloomFilterWriter;
    }

    private boolean closeBloomFilter(BloomFilterWriter bfw) throws IOException {
      boolean haveBloom = (bfw != null && bfw.getKeyCount() > 0);
      if (haveBloom) {
        bfw.compactBloom();
      }
      return haveBloom;
    }

    private boolean closeGeneralBloomFilter() throws IOException {
      boolean hasGeneralBloom = closeBloomFilter(generalBloomFilterWriter);

      // add the general Bloom filter writer and append file info
      if (hasGeneralBloom) {
        writer.addGeneralBloomFilter(generalBloomFilterWriter);
        writer.appendFileInfo(BLOOM_FILTER_TYPE_KEY,
            Bytes.toBytes(bloomType.toString()));
        bloomContext.addLastBloomKey(writer);
      }
      return hasGeneralBloom;
    }

    private boolean closeDeleteFamilyBloomFilter() throws IOException {
      boolean hasDeleteFamilyBloom = closeBloomFilter(deleteFamilyBloomFilterWriter);

      // add the delete family Bloom filter writer
      if (hasDeleteFamilyBloom) {
        writer.addDeleteFamilyBloomFilter(deleteFamilyBloomFilterWriter);
      }

      // append file info about the number of delete family kvs
      // even if there is no delete family Bloom.
      writer.appendFileInfo(DELETE_FAMILY_COUNT,
          Bytes.toBytes(this.deleteFamilyCnt));

      return hasDeleteFamilyBloom;
    }

    public void close() throws IOException {
      boolean hasGeneralBloom = this.closeGeneralBloomFilter();
      boolean hasDeleteFamilyBloom = this.closeDeleteFamilyBloomFilter();

      writer.close();

      // Log final Bloom filter statistics. This needs to be done after close()
      // because compound Bloom filters might be finalized as part of closing.
      if (StoreFile.LOG.isTraceEnabled()) {
        StoreFile.LOG.trace((hasGeneralBloom ? "" : "NO ") + "General Bloom and " +
          (hasDeleteFamilyBloom ? "" : "NO ") + "DeleteFamily" + " was added to HFile " +
          getPath());
      }

    }

    public void appendFileInfo(byte[] key, byte[] value) throws IOException {
      writer.appendFileInfo(key, value);
    }

    /** For use in testing, e.g. {@link org.apache.hadoop.hbase.regionserver.CreateRandomStoreFile}
     */
    HFileWriter getHFileWriter() {
      return writer;
    }
  }

  /**
   * Reader for a StoreFile.
   */
  public static class Reader {
    private static final Log LOG = LogFactory.getLog(Reader.class.getName());

    protected BloomFilter generalBloomFilter = null;
    protected BloomFilter deleteFamilyBloomFilter = null;
    protected BloomType bloomFilterType;
    private final HFileReader reader;
    protected long sequenceID = -1;
    protected TimeRange timeRange = null;
    private byte[] lastBloomKey;
    private long deleteFamilyCnt = -1;
    private boolean bulkLoadResult = false;
    private KeyValue.KeyOnlyKeyValue lastBloomKeyOnlyKV = null;
    private boolean skipResetSeqId = true;
    // Counter that is incremented every time a scanner is created on the
    // store file. It is decremented when the scan on the store file is
    // done. All StoreFileReader for the same StoreFile will share this counter.
    private final AtomicInteger refCount;

    private final CacheConfig cacheConf;
    private final ReaderContext context;

    private Reader(HFileReader reader, AtomicInteger refCount, CacheConfig cacheConf, ReaderContext context) {
      this.reader = reader;
      bloomFilterType = BloomType.NONE;
      this.refCount = refCount;
      this.cacheConf = cacheConf;
      this.context = context;
    }

    public Reader(ReaderContext context, HFileInfo fileInfo, CacheConfig cacheConf,
        AtomicInteger refCount, Configuration conf) throws IOException {
      this(HFile.createReader(context, fileInfo, cacheConf, conf), refCount, cacheConf, context);
    }

    void copyFields(Reader storeFileReader) throws IOException {
      this.generalBloomFilter = storeFileReader.generalBloomFilter;
      this.deleteFamilyBloomFilter = storeFileReader.deleteFamilyBloomFilter;
      this.bloomFilterType = storeFileReader.bloomFilterType;
      this.sequenceID = storeFileReader.sequenceID;
      this.timeRange = storeFileReader.timeRange;
      this.lastBloomKey = storeFileReader.lastBloomKey;
      this.bulkLoadResult = storeFileReader.bulkLoadResult;
      this.lastBloomKeyOnlyKV = storeFileReader.lastBloomKeyOnlyKV;
      this.skipResetSeqId = storeFileReader.skipResetSeqId;
    }

    public boolean isPrimaryReplicaReader() {
      return reader.isPrimaryReplicaReader();
    }

    /**
     * ONLY USE DEFAULT CONSTRUCTOR FOR UNIT TESTS
     */
    @VisibleForTesting
    Reader() {
      this.refCount = new AtomicInteger(0);
      this.reader = null;
      this.cacheConf = null;
      this.context = null;
    }

    public CellComparator getComparator() {
      return reader.getComparator();
    }

    /**
     * Get a scanner to scan over this StoreFile.
     *
     * @param cacheBlocks should this scanner cache blocks?
     * @param pread use pread (for highly concurrent small readers)
     * @param isCompaction is scanner being used for compaction?
     * @param scannerOrder Order of this scanner relative to other scanners. See
     *        {@link KeyValueScanner#getScannerOrder()}.
     * @param canOptimizeForNonNullColumn {@code true} if we can make sure there is no null column,
     *          otherwise {@code false}. This is a hint for optimization.
     * @return a scanner
     */
    public StoreFileScanner getStoreFileScanner(boolean cacheBlocks, boolean pread,
      boolean isCompaction, long readPt, long scannerOrder, boolean canOptimizeForNonNullColumn) {
      // Increment the ref count
      refCount.incrementAndGet();
      return new StoreFileScanner(this, getScanner(cacheBlocks, pread, isCompaction),
        !isCompaction, reader.hasMVCCInfo(), readPt, scannerOrder, canOptimizeForNonNullColumn);
    }

    /**
     * Indicate that the scanner has finished reading with this reader. We need to decrement the ref
     * count, and also, if this is not the common pread reader, we should close it.
     */
    void readCompleted() {
      refCount.decrementAndGet();
      if (context.getReaderType() == ReaderType.STREAM) {
        try {
          reader.close(false);
        } catch (IOException e) {
          LOG.warn("failed to close stream reader", e);
        }
      }
    }

    /**
     * @deprecated Do not write further code which depends on this call. Instead
     * use getStoreFileScanner() which uses the StoreFileScanner class/interface
     * which is the preferred way to scan a store with higher level concepts.
     *
     * @param cacheBlocks should we cache the blocks?
     * @param pread use pread (for concurrent small readers)
     * @return the underlying HFileScanner
     */
    @Deprecated
    public HFileScanner getScanner(boolean cacheBlocks, boolean pread) {
      return getScanner(cacheBlocks, pread, false);
    }

    /**
     * @deprecated Do not write further code which depends on this call. Instead
     * use getStoreFileScanner() which uses the StoreFileScanner class/interface
     * which is the preferred way to scan a store with higher level concepts.
     *
     * @param cacheBlocks
     *          should we cache the blocks?
     * @param pread
     *          use pread (for concurrent small readers)
     * @param isCompaction
     *          is scanner being used for compaction?
     * @return the underlying HFileScanner
     */
    @Deprecated
    public HFileScanner getScanner(boolean cacheBlocks, boolean pread,
        boolean isCompaction) {
      return reader.getScanner(cacheBlocks, pread, isCompaction);
    }

    public void close(boolean evictOnClose) throws IOException {
      reader.close(evictOnClose);
    }

    /**
     * Check if this storeFile may contain keys within the TimeRange that
     * have not expired (i.e. not older than oldestUnexpiredTS).
     * @param timeRange the timeRange to restrict
     * @param oldestUnexpiredTS the oldest timestamp that is not expired, as
     *          determined by the column family's TTL
     * @return false if queried keys definitely don't exist in this StoreFile
     */
    boolean passesTimerangeFilter(TimeRange tr, long oldestUnexpiredTS) {
      return this.timeRange == null? true:
        this.timeRange.includesTimeRange(tr) && this.timeRange.getMax() >= oldestUnexpiredTS;
    }

    /**
     * Checks whether the given scan passes the Bloom filter (if present). Only
     * checks Bloom filters for single-row or single-row-column scans. Bloom
     * filter checking for multi-gets is implemented as part of the store
     * scanner system (see {@link StoreFileScanner#seekExactly}) and uses
     * the lower-level API {@link #passesGeneralRowBloomFilter(byte[], int, int)}
     * and {@link #passesGeneralRowColBloomFilter(Cell)}.
     *
     * @param scan the scan specification. Used to determine the row, and to
     *          check whether this is a single-row ("get") scan.
     * @param columns the set of columns. Only used for row-column Bloom
     *          filters.
     * @return true if the scan with the given column set passes the Bloom
     *         filter, or if the Bloom filter is not applicable for the scan.
     *         False if the Bloom filter is applicable and the scan fails it.
     */
     boolean passesBloomFilter(Scan scan,
        final SortedSet<byte[]> columns) {
      // Multi-column non-get scans will use Bloom filters through the
      // lower-level API function that this function calls.
      if (!scan.isGetScan()) {
        return true;
      }

      byte[] row = scan.getStartRow();
      switch (this.bloomFilterType) {
        case ROW:
          return passesGeneralRowBloomFilter(row, 0, row.length);

        case ROWCOL:
          if (columns != null && columns.size() == 1) {
            byte[] column = columns.first();
            // create the required fake key
            Cell kvKey = CellUtil.createFirstOnRow(row, HConstants.EMPTY_BYTE_ARRAY, column);
            return passesGeneralRowColBloomFilter(kvKey);
          }

          // For multi-column queries the Bloom filter is checked from the
          // seekExact operation.
          return true;

        default:
          return true;
      }
    }

    public boolean passesDeleteFamilyBloomFilter(byte[] row, int rowOffset,
        int rowLen) {
      // Cache Bloom filter as a local variable in case it is set to null by
      // another thread on an IO error.
      BloomFilter bloomFilter = this.deleteFamilyBloomFilter;

      // Empty file or there is no delete family at all
      if (reader.getTrailer().getEntryCount() == 0 || deleteFamilyCnt == 0) {
        return false;
      }

      if (bloomFilter == null) {
        return true;
      }

      try {
        if (!bloomFilter.supportsAutoLoading()) {
          return true;
        }
        return bloomFilter.contains(row, rowOffset, rowLen, null);
      } catch (IllegalArgumentException e) {
        LOG.error("Bad Delete Family bloom filter data -- proceeding without",
            e);
        setDeleteFamilyBloomFilterFaulty();
      }

      return true;
    }

    /**
     * A method for checking Bloom filters. Called directly from
     * StoreFileScanner in case of a multi-column query.
     *
     * @param row
     * @param rowOffset
     * @param rowLen
     * @return True if passes
     */
    public boolean passesGeneralRowBloomFilter(byte[] row, int rowOffset, int rowLen) {
      BloomFilter bloomFilter = this.generalBloomFilter;
      if (bloomFilter == null) {
        return true;
      }

      // Used in ROW bloom
      byte[] key = null;
      if (rowOffset != 0 || rowLen != row.length) {
        throw new AssertionError(
            "For row-only Bloom filters the row " + "must occupy the whole array");
      }
      key = row;
      return checkGeneralBloomFilter(key, null, bloomFilter);
    }

    /**
     * A method for checking Bloom filters. Called directly from
     * StoreFileScanner in case of a multi-column query.
     *
     * @param cell
     *          the cell to check if present in BloomFilter
     * @return True if passes
     */
    public boolean passesGeneralRowColBloomFilter(Cell cell) {
      BloomFilter bloomFilter = this.generalBloomFilter;
      if (bloomFilter == null) {
        return true;
      }
      // Used in ROW_COL bloom
      Cell kvKey = null;
      // Already if the incoming key is a fake rowcol key then use it as it is
      if (cell.getTypeByte() == KeyValue.Type.Maximum.getCode() && cell.getFamilyLength() == 0) {
        kvKey = cell;
      } else {
        kvKey = CellUtil.createFirstOnRowCol(cell);
      }
      return checkGeneralBloomFilter(null, kvKey, bloomFilter);
    }

    private boolean checkGeneralBloomFilter(byte[] key, Cell kvKey, BloomFilter bloomFilter) {
      // Empty file
      if (reader.getTrailer().getEntryCount() == 0)
        return false;

      HFileBlock bloomBlock = null;
      try {
        boolean shouldCheckBloom;
        ByteBuff bloom;
        if (bloomFilter.supportsAutoLoading()) {
          bloom = null;
          shouldCheckBloom = true;
        } else {
          bloomBlock = reader.getMetaBlock(HFile.BLOOM_FILTER_DATA_KEY, true);
          bloom = bloomBlock.getBufferWithoutHeader();
          shouldCheckBloom = bloom != null;
        }

        if (shouldCheckBloom) {
          boolean exists;

          // Whether the primary Bloom key is greater than the last Bloom key
          // from the file info. For row-column Bloom filters this is not yet
          // a sufficient condition to return false.
          boolean keyIsAfterLast = (lastBloomKey != null);
          // hbase:meta does not have blooms. So we need not have special interpretation
          // of the hbase:meta cells.  We can safely use Bytes.BYTES_RAWCOMPARATOR for ROW Bloom
          if (keyIsAfterLast) {
            if (bloomFilterType == BloomType.ROW) {
              keyIsAfterLast = (Bytes.BYTES_RAWCOMPARATOR.compare(key, lastBloomKey) > 0);
            } else {
              keyIsAfterLast = (CellComparator.COMPARATOR.compare(kvKey, lastBloomKeyOnlyKV)) > 0;
            }
          }

          if (bloomFilterType == BloomType.ROWCOL) {
            // Since a Row Delete is essentially a DeleteFamily applied to all
            // columns, a file might be skipped if using row+col Bloom filter.
            // In order to ensure this file is included an additional check is
            // required looking only for a row bloom.
            Cell rowBloomKey = CellUtil.createFirstOnRow(kvKey);
            // hbase:meta does not have blooms. So we need not have special interpretation
            // of the hbase:meta cells.  We can safely use Bytes.BYTES_RAWCOMPARATOR for ROW Bloom
            if (keyIsAfterLast
                && (CellComparator.COMPARATOR.compare(rowBloomKey, lastBloomKeyOnlyKV)) > 0) {
              exists = false;
            } else {
              exists = bloomFilter.contains(kvKey, bloom, BloomType.ROWCOL) ||
                  bloomFilter.contains(rowBloomKey, bloom, BloomType.ROWCOL);
            }
          } else {
            exists = !keyIsAfterLast
                && bloomFilter.contains(key, 0, key.length, bloom);
          }

          return exists;
        }
      } catch (IOException e) {
        LOG.error("Error reading bloom filter data -- proceeding without",
            e);
        setGeneralBloomFilterFaulty();
      } catch (IllegalArgumentException e) {
        LOG.error("Bad bloom filter data -- proceeding without", e);
        setGeneralBloomFilterFaulty();
      } finally {
        // Release the bloom block so that its ref count can be decremented.
        if (bloomBlock != null) {
          bloomBlock.release();
        }
      }
      return true;
    }

    /**
     * Checks whether the given scan rowkey range overlaps with the current storefile's
     * @param scan the scan specification. Used to determine the rowkey range.
     * @return true if there is overlap, false otherwise
     */
    public boolean passesKeyRangeFilter(Scan scan) {
      if (this.getFirstKey() == null || this.getLastKey() == null) {
        // the file is empty
        return false;
      }
      if (Bytes.equals(scan.getStartRow(), HConstants.EMPTY_START_ROW)
          && Bytes.equals(scan.getStopRow(), HConstants.EMPTY_END_ROW)) {
        return true;
      }
      byte[] smallestScanRow = scan.isReversed() ? scan.getStopRow() : scan.getStartRow();
      byte[] largestScanRow = scan.isReversed() ? scan.getStartRow() : scan.getStopRow();
      Cell firstKeyKV = this.getFirstKey();
      Cell lastKeyKV = this.getLastKey();
      boolean nonOverLapping = (getComparator().compareRows(firstKeyKV,
          largestScanRow, 0, largestScanRow.length) > 0 
          && !Bytes.equals(scan.isReversed() ? scan.getStartRow() : scan.getStopRow(),
              HConstants.EMPTY_END_ROW))
          || getComparator().compareRows(lastKeyKV, smallestScanRow, 0, smallestScanRow.length) < 0;
      return !nonOverLapping;
    }

    public Map<byte[], byte[]> loadFileInfo() throws IOException {
      Map<byte [], byte []> fi = reader.getHFileInfo();

      byte[] b = fi.get(BLOOM_FILTER_TYPE_KEY);
      if (b != null) {
        bloomFilterType = BloomType.valueOf(Bytes.toString(b));
      }

      lastBloomKey = fi.get(LAST_BLOOM_KEY);
      if(bloomFilterType == BloomType.ROWCOL) {
        lastBloomKeyOnlyKV = new KeyValue.KeyOnlyKeyValue(lastBloomKey, 0, lastBloomKey.length);
      }
      byte[] cnt = fi.get(DELETE_FAMILY_COUNT);
      if (cnt != null) {
        deleteFamilyCnt = Bytes.toLong(cnt);
      }

      return fi;
    }

    public void loadBloomfilter() {
      this.loadBloomfilter(BlockType.GENERAL_BLOOM_META);
      this.loadBloomfilter(BlockType.DELETE_FAMILY_BLOOM_META);
    }

    private void loadBloomfilter(BlockType blockType) {
      try {
        if (blockType == BlockType.GENERAL_BLOOM_META) {
          if (this.generalBloomFilter != null)
            return; // Bloom has been loaded

          DataInput bloomMeta = reader.getGeneralBloomFilterMetadata();
          if (bloomMeta != null) {
            // sanity check for NONE Bloom filter
            if (bloomFilterType == BloomType.NONE) {
              throw new IOException(
                  "valid bloom filter type not found in FileInfo");
            } else {
              generalBloomFilter = BloomFilterFactory.createFromMeta(bloomMeta, reader);
              if (!cacheConf.shouldPrefetchOnOpen(reader.length()) && cacheConf.shouldPrefetchBloom()) {
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Prefetch general bloom, " + context.getFilePath());
                }
                generalBloomFilter.warmup();
              }
              if (LOG.isTraceEnabled()) {
                LOG.trace("Loaded " + bloomFilterType.toString() + " "
                  + generalBloomFilter.getClass().getSimpleName()
                  + " metadata for " + reader.getName());
              }
            }
          }
        } else if (blockType == BlockType.DELETE_FAMILY_BLOOM_META) {
          if (this.deleteFamilyBloomFilter != null)
            return; // Bloom has been loaded

          DataInput bloomMeta = reader.getDeleteBloomFilterMetadata();
          if (bloomMeta != null) {
            deleteFamilyBloomFilter = BloomFilterFactory.createFromMeta(bloomMeta, reader);
            if (!cacheConf.shouldPrefetchOnOpen(reader.length()) && cacheConf.shouldPrefetchBloom()) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Prefetch deleteFamily bloom, " + context.getFilePath());
              }
              deleteFamilyBloomFilter.warmup();
            }
            LOG.info("Loaded Delete Family Bloom ("
                + deleteFamilyBloomFilter.getClass().getSimpleName()
                + ") metadata for " + reader.getName());
          }
        } else {
          throw new RuntimeException("Block Type: " + blockType.toString()
              + "is not supported for Bloom filter");
        }
      } catch (IOException e) {
        LOG.error("Error reading bloom filter meta for " + blockType
            + " -- proceeding without", e);
        setBloomFilterFaulty(blockType);
      } catch (IllegalArgumentException e) {
        LOG.error("Bad bloom filter meta " + blockType
            + " -- proceeding without", e);
        setBloomFilterFaulty(blockType);
      }
    }

    private void setBloomFilterFaulty(BlockType blockType) {
      if (blockType == BlockType.GENERAL_BLOOM_META) {
        setGeneralBloomFilterFaulty();
      } else if (blockType == BlockType.DELETE_FAMILY_BLOOM_META) {
        setDeleteFamilyBloomFilterFaulty();
      }
    }

    /**
     * The number of Bloom filter entries in this store file, or an estimate
     * thereof, if the Bloom filter is not loaded. This always returns an upper
     * bound of the number of Bloom filter entries.
     *
     * @return an estimate of the number of Bloom filter entries in this file
     */
    public long getFilterEntries() {
      return generalBloomFilter != null ? generalBloomFilter.getKeyCount()
          : reader.getEntries();
    }

    public void setGeneralBloomFilterFaulty() {
      generalBloomFilter = null;
    }

    public void setDeleteFamilyBloomFilterFaulty() {
      this.deleteFamilyBloomFilter = null;
    }

    public Cell getLastKey() {
      return reader.getLastKey();
    }

    public byte[] getLastRowKey() {
      return reader.getLastRowKey();
    }

    public Cell midkey() throws IOException {
      return reader.midkey();
    }

    public long length() {
      return reader.length();
    }

    public long getTotalUncompressedBytes() {
      return reader.getTrailer().getTotalUncompressedBytes();
    }

    public long getEntries() {
      return reader.getEntries();
    }

    public long getDeleteFamilyCnt() {
      return deleteFamilyCnt;
    }

    public Cell getFirstKey() {
      return reader.getFirstKey();
    }

    public long indexSize() {
      return reader.indexSize();
    }

    public BloomType getBloomFilterType() {
      return this.bloomFilterType;
    }

    public long getSequenceID() {
      return sequenceID;
    }

    public void setSequenceID(long sequenceID) {
      this.sequenceID = sequenceID;
    }

    public void setBulkLoaded(boolean bulkLoadResult) {
      this.bulkLoadResult = bulkLoadResult;
    }

    public boolean isBulkLoaded() {
      return this.bulkLoadResult;
    }

    BloomFilter getGeneralBloomFilter() {
      return generalBloomFilter;
    }

    long getUncompressedDataIndexSize() {
      return reader.getTrailer().getUncompressedDataIndexSize();
    }

    public long getTotalBloomSize() {
      if (generalBloomFilter == null)
        return 0;
      return generalBloomFilter.getByteSize();
    }

    public int getHFileVersion() {
      return reader.getTrailer().getMajorVersion();
    }

    public int getHFileMinorVersion() {
      return reader.getTrailer().getMinorVersion();
    }

    public HFileReader getHFileReader() {
      return reader;
    }

    void disableBloomFilterForTesting() {
      generalBloomFilter = null;
      this.deleteFamilyBloomFilter = null;
    }

    public long getMaxTimestamp() {
      return timeRange == null ? TimeRange.INITIAL_MAX_TIMESTAMP: timeRange.getMax();
    }

    boolean isSkipResetSeqId() {
      return skipResetSeqId;
    }

    void setSkipResetSeqId(boolean skipResetSeqId) {
      this.skipResetSeqId = skipResetSeqId;
    }

    public ReaderContext getReaderContext() {
      return this.context;
    }
  }

  /**
   * Useful comparators for comparing StoreFiles.
   */
  public abstract static class Comparators {
    /**
     * Comparator that compares based on the Sequence Ids of the
     * the StoreFiles. Bulk loads that did not request a seq ID
     * are given a seq id of -1; thus, they are placed before all non-
     * bulk loads, and bulk loads with sequence Id. Among these files,
     * the size is used to determine the ordering, then bulkLoadTime.
     * If there are ties, the path name is used as a tie-breaker.
     */
    public static final Comparator<StoreFile> SEQ_ID =
      Ordering.compound(ImmutableList.of(
          Ordering.natural().onResultOf(new GetSeqId()),
          Ordering.natural().onResultOf(new GetFileSize()).reverse(),
          Ordering.natural().onResultOf(new GetBulkTime()),
          Ordering.natural().onResultOf(new GetPathName())
      ));

    private static class GetSeqId implements Function<StoreFile, Long> {
      @Override
      public Long apply(StoreFile sf) {
        return sf.getMaxSequenceId();
      }
    }

    private static class GetFileSize implements Function<StoreFile, Long> {
      @Override
      public Long apply(StoreFile sf) {
        if (sf.getReader() != null) {
          return sf.getReader().length();
        } else {
          // the reader may be null for the compacted files and if the archiving
          // had failed.
          return -1L;
        }
      }
    }

    private static class GetBulkTime implements Function<StoreFile, Long> {
      @Override
      public Long apply(StoreFile sf) {
        if (!sf.isBulkLoadResult()) return Long.MAX_VALUE;
        return sf.getBulkLoadTimestamp();
      }
    }

    private static class GetPathName implements Function<StoreFile, String> {
      @Override
      public String apply(StoreFile sf) {
        return sf.getPath().getName();
      }
    }
  }

  Set<String> getCompactedStoreFiles() {
    return Collections.unmodifiableSet(this.compactedStoreFiles);
  }
}
