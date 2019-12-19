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
package org.apache.hadoop.hbase.regionserver.compactions;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFile.FileInfo;
import org.apache.hadoop.hbase.mob.MobCell;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.regionserver.CellSink;
import org.apache.hadoop.hbase.regionserver.HMobStore;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.regionserver.ShipperListener;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFile.Writer;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;
import org.apache.hadoop.hbase.regionserver.StoreScanner;
import org.apache.hadoop.hbase.regionserver.TimeRangeTracker;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.shaded.com.google.common.io.Closeables;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix;

/**
 * A compactor is a compaction algorithm associated a given policy. Base class also contains
 * reusable parts for implementing compactors (what is common and what isn't is evolving).
 */
@InterfaceAudience.Private
public abstract class Compactor<T extends CellSink> {
  private static final Log LOG = LogFactory.getLog(Compactor.class);
  private static final long COMPACTION_PROGRESS_LOG_INTERVAL = 60 * 1000;
  protected volatile CompactionProgress progress;
  protected final Configuration conf;
  protected final Store store;

  protected final int compactionKVMax;
  protected final Compression.Algorithm compactionCompression;

  /** specify how many days to keep MVCC values during major compaction **/ 
  protected int keepSeqIdPeriod;
  // Configs that drive whether we drop page cache behind compactions
  protected static final String  MAJOR_COMPACTION_DROP_CACHE =
      "hbase.regionserver.majorcompaction.pagecache.drop";
  protected static final String MINOR_COMPACTION_DROP_CACHE =
      "hbase.regionserver.minorcompaction.pagecache.drop";

  private boolean dropCacheMajor;
  private boolean dropCacheMinor;

  private long mobSizeThreshold;
  //TODO: depending on Store is not good but, realistically, all compactors currently do.
  Compactor(final Configuration conf, final Store store) {
    this.conf = conf;
    this.store = store;
    this.compactionKVMax =
      this.conf.getInt(HConstants.COMPACTION_KV_MAX, HConstants.COMPACTION_KV_MAX_DEFAULT);
    this.compactionCompression = (this.store.getFamily() == null) ?
        Compression.Algorithm.NONE : this.store.getFamily().getCompactionCompression();
    this.keepSeqIdPeriod = Math.max(this.conf.getInt(HConstants.KEEP_SEQID_PERIOD, 
      HConstants.MIN_KEEP_SEQID_PERIOD), HConstants.MIN_KEEP_SEQID_PERIOD);
    this.dropCacheMajor = conf.getBoolean(MAJOR_COMPACTION_DROP_CACHE, true);
    this.dropCacheMinor = conf.getBoolean(MINOR_COMPACTION_DROP_CACHE, true);
    if (store instanceof HMobStore) {
      this.mobSizeThreshold = store.getFamily().getMobThreshold();
    }
  }

  protected interface CellSinkFactory<S> {
    S createWriter(InternalScanner scanner, FileDetails fd, boolean shouldDropBehind)
        throws IOException;
  }

  public CompactionProgress getProgress() {
    return this.progress;
  }

  /** The sole reason this class exists is that java has no ref/out/pointer parameters. */
  protected static class FileDetails {
    /** Maximum key count after compaction (for blooms) */
    public long maxKeyCount = 0;
    /** Earliest put timestamp if major compaction */
    public long earliestPutTs = HConstants.LATEST_TIMESTAMP;
    /** Latest put timestamp */
    public long latestPutTs = HConstants.LATEST_TIMESTAMP;
    /** The last key in the files we're compacting. */
    public long maxSeqId = 0;
    /** Latest memstore read point found in any of the involved files */
    public long maxMVCCReadpoint = 0;
    /** Max tags length**/
    public int maxTagsLength = 0;
    /** Min SeqId to keep during a major compaction **/
    public long minSeqIdToKeep = 0;
  }

  /**
   * Extracts some details about the files to compact that are commonly needed by compactors.
   * @param filesToCompact Files.
   * @param allFiles Whether all files are included for compaction
   * @return The result.
   */
  protected FileDetails getFileDetails(
      Collection<StoreFile> filesToCompact, boolean allFiles) throws IOException {
    FileDetails fd = new FileDetails();
    long oldestHFileTimeStampToKeepMVCC = System.currentTimeMillis() - 
      (1000L * 60 * 60 * 24 * this.keepSeqIdPeriod);  

    for (StoreFile file : filesToCompact) {
      if(allFiles && (file.getModificationTimeStamp() < oldestHFileTimeStampToKeepMVCC)) {
        // when isAllFiles is true, all files are compacted so we can calculate the smallest 
        // MVCC value to keep
        if(fd.minSeqIdToKeep < file.getMaxMemstoreTS()) {
          fd.minSeqIdToKeep = file.getMaxMemstoreTS();
        }
      }
      long seqNum = file.getMaxSequenceId();
      fd.maxSeqId = Math.max(fd.maxSeqId, seqNum);
      StoreFile.Reader r = file.getReader();
      if (r == null) {
        LOG.warn("Null reader for " + file.getPath());
        continue;
      }
      // NOTE: use getEntries when compacting instead of getFilterEntries, otherwise under-sized
      // blooms can cause progress to be miscalculated or if the user switches bloom
      // type (e.g. from ROW to ROWCOL)
      long keyCount = r.getEntries();
      fd.maxKeyCount += keyCount;
      // calculate the latest MVCC readpoint in any of the involved store files
      Map<byte[], byte[]> fileInfo = r.loadFileInfo();
      byte[] tmp = null;
      // Get and set the real MVCCReadpoint for bulk loaded files, which is the
      // SeqId number.
      if (r.isBulkLoaded()) {
        fd.maxMVCCReadpoint = Math.max(fd.maxMVCCReadpoint, r.getSequenceID());
      }
      else {
        tmp = fileInfo.get(HFile.Writer.MAX_MEMSTORE_TS_KEY);
        if (tmp != null) {
          fd.maxMVCCReadpoint = Math.max(fd.maxMVCCReadpoint, Bytes.toLong(tmp));
        }
      }
      tmp = fileInfo.get(FileInfo.MAX_TAGS_LEN);
      if (tmp != null) {
        fd.maxTagsLength = Math.max(fd.maxTagsLength, Bytes.toInt(tmp));
      }
      // If required, calculate the earliest put timestamp of all involved storefiles.
      // This is used to remove family delete marker during compaction.
      long earliestPutTs = 0;
      if (allFiles) {
        tmp = fileInfo.get(StoreFile.EARLIEST_PUT_TS);
        if (tmp == null) {
          // There's a file with no information, must be an old one
          // assume we have very old puts
          fd.earliestPutTs = earliestPutTs = HConstants.OLDEST_TIMESTAMP;
        } else {
          earliestPutTs = Bytes.toLong(tmp);
          fd.earliestPutTs = Math.min(fd.earliestPutTs, earliestPutTs);
        }
      }
      tmp = fileInfo.get(StoreFile.TIMERANGE_KEY);
      fd.latestPutTs = tmp == null ? HConstants.LATEST_TIMESTAMP: TimeRangeTracker.parseFrom(tmp).getMax();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Compacting " + file +
          ", keycount=" + keyCount +
          ", bloomtype=" + r.getBloomFilterType().toString() +
          ", size=" + TraditionalBinaryPrefix.long2String(r.length(), "", 1) +
          ", encoding=" + r.getHFileReader().getDataBlockEncoding() +
          ", seqNum=" + seqNum +
          (allFiles ? ", earliestPutTs=" + earliestPutTs: ""));
      }
    }
    return fd;
  }

  /**
   * Creates file scanners for compaction.
   * @param filesToCompact Files.
   * @return Scanners.
   */
  protected List<StoreFileScanner> createFileScanners(Collection<StoreFile> filesToCompact,
      long smallestReadPoint, boolean useDropBehind) throws IOException {
    return StoreFileScanner.getScannersForCompaction(filesToCompact, useDropBehind,
      smallestReadPoint);
  }

  protected long getSmallestReadPoint() {
    return store.getSmallestReadPoint();
  }

  protected interface InternalScannerFactory {

    ScanType getScanType(CompactionRequest request);

    InternalScanner createScanner(List<StoreFileScanner> scanners, ScanType scanType,
        FileDetails fd, long smallestReadPoint) throws IOException;
  }

  protected final InternalScannerFactory defaultScannerFactory = new InternalScannerFactory() {
    @Override
    public ScanType getScanType(CompactionRequest request) {
      return request.isAllFiles() ? ScanType.COMPACT_DROP_DELETES
          : ScanType.COMPACT_RETAIN_DELETES;
    }
    @Override
    public InternalScanner createScanner(List<StoreFileScanner> scanners, ScanType scanType,
        FileDetails fd, long smallestReadPoint) throws IOException {
      return Compactor.this.createScanner(store, scanners, scanType, smallestReadPoint,
        fd.earliestPutTs);
    }
  };

  protected final InternalScannerFactory mobScannerFactory = new InternalScannerFactory() {
    @Override
    public ScanType getScanType(CompactionRequest request) {
      // retain the delete markers until they are expired.
      return ScanType.COMPACT_RETAIN_DELETES;
    }
    @Override
    public InternalScanner createScanner(List<StoreFileScanner> scanners,
        ScanType scanType, FileDetails fd, long smallestReadPoint) throws IOException {
      return Compactor.this.createScanner(store, scanners, scanType, smallestReadPoint,
        fd.earliestPutTs);
    }
  };

  /**
   * Creates a writer for a new file in a temporary directory.
   * @param fd The file details.
   * @return Writer for a new StoreFile in the tmp dir.
   * @throws IOException if creation failed
   */
  protected Writer createTmpWriter(FileDetails fd, boolean shouldDropBehind) throws IOException {
    boolean includeMVCCReadpoint = (store instanceof HMobStore) ? true : fd.maxMVCCReadpoint > 0;
    boolean includesTags = (store instanceof HMobStore) ? true : fd.maxTagsLength > 0;
    // When all MVCC readpoints are 0, don't write them.
    // See HBASE-8166, HBASE-12600, and HBASE-13389.
    return store.createWriterInTmp(fd.maxKeyCount, this.compactionCompression,
    /* isCompaction = */true,
    /* includeMVCCReadpoint = */includeMVCCReadpoint,
    /* includesTags = */includesTags, shouldDropBehind);
  }

  protected List<Path> compact(final CompactionRequest request,
      InternalScannerFactory scannerFactory, CellSinkFactory<T> sinkFactory,
      CompactionThroughputController throughputController, User user) throws IOException {
    List<Path> res = null;
    FileDetails fd = getFileDetails(request.getFiles(), request.isAllFiles());
    this.progress = new CompactionProgress(fd.maxKeyCount);

    // Find the smallest read point across all the Scanners.
    long smallestReadPoint = getSmallestReadPoint();

    T writer = null;
    boolean dropCache;
    if (request.isMajor() || request.isAllFiles()) {
      dropCache = this.dropCacheMajor;
    } else {
      dropCache = this.dropCacheMinor;
    }

    List<StoreFileScanner> scanners =
        createFileScanners(request.getFiles(), smallestReadPoint, dropCache);
    InternalScanner scanner = null;
    boolean finished = false;
    try {
      /* Include deletes, unless we are doing a major compaction */
      ScanType scanType = scannerFactory.getScanType(request);
      scanner = preCreateCoprocScanner(request, scanType, fd.earliestPutTs, scanners, user,
        smallestReadPoint);
      if (scanner == null) {
        scanner = scannerFactory.createScanner(scanners, scanType, fd, smallestReadPoint);
      }
      scanner = postCreateCoprocScanner(request, scanType, scanner, user);
      if (scanner == null) {
        // NULL scanner returned from coprocessor hooks means skip normal processing.
        return new ArrayList<Path>();
      }
      boolean cleanSeqId = false;
      if (fd.minSeqIdToKeep > 0) {
        smallestReadPoint = Math.min(fd.minSeqIdToKeep, smallestReadPoint);
        cleanSeqId = true;
      }
      writer = sinkFactory.createWriter(scanner, fd, dropCache);
      if (store instanceof HMobStore) {
        finished = performMobCompaction(fd, scanner, writer, smallestReadPoint, cleanSeqId,
            throughputController, request.isAllFiles());
      } else {
        finished = performCompaction(fd, scanner, writer, smallestReadPoint, cleanSeqId,
            throughputController, request.isAllFiles());
      }

      if (!finished) {
        throw new InterruptedIOException("Aborting compaction of store " + store + " in region "
            + store.getRegionInfo().getRegionNameAsString() + " because it was interrupted.");
      }
      res = commitWriter(writer, fd, request);
    } finally {
      if (!finished && writer != null) {
        abortWriter(writer);
      }
      Closeables.close(scanner, true);
    }
    assert finished : "We should have exited the method on all error paths";
    assert writer != null : "Writer should be non-null if no error";
    return res;
  }

  protected abstract List<Path> commitWriter(T writer, FileDetails fd, CompactionRequest request)
      throws IOException;

  protected abstract void abortWriter(T writer) throws IOException;
  
  /**
   * Calls coprocessor, if any, to create compaction scanner - before normal scanner creation.
   * @param request Compaction request.
   * @param scanType Scan type.
   * @param earliestPutTs Earliest put ts.
   * @param scanners File scanners for compaction files.
   * @param user the User
   * @param readPoint the read point to help create scanner by Coprocessor if required.
   * @return Scanner override by coprocessor; null if not overriding.
   */
  protected InternalScanner preCreateCoprocScanner(final CompactionRequest request,
      final ScanType scanType, final long earliestPutTs, final List<StoreFileScanner> scanners,
      User user, final long readPoint) throws IOException {
    if (store.getCoprocessorHost() == null) {
      return null;
    }
    if (user == null) {
      return store.getCoprocessorHost().preCompactScannerOpen(store, scanners, scanType,
        earliestPutTs, request, readPoint);
    } else {
      try {
        return user.getUGI().doAs(new PrivilegedExceptionAction<InternalScanner>() {
          @Override
          public InternalScanner run() throws Exception {
            return store.getCoprocessorHost().preCompactScannerOpen(store, scanners,
              scanType, earliestPutTs, request, readPoint);
          }
        });
      } catch (InterruptedException ie) {
        InterruptedIOException iioe = new InterruptedIOException();
        iioe.initCause(ie);
        throw iioe;
      }
    }
  }

  /**
   * Calls coprocessor, if any, to create scanners - after normal scanner creation.
   * @param request Compaction request.
   * @param scanType Scan type.
   * @param scanner The default scanner created for compaction.
   * @return Scanner scanner to use (usually the default); null if compaction should not proceed.
   */
  protected InternalScanner postCreateCoprocScanner(final CompactionRequest request,
      final ScanType scanType, final InternalScanner scanner, User user) throws IOException {
    if (store.getCoprocessorHost() == null) {
      return scanner;
    }
    if (user == null) {
      return store.getCoprocessorHost().preCompact(store, scanner, scanType, request);
    } else {
      try {
        return user.getUGI().doAs(new PrivilegedExceptionAction<InternalScanner>() {
          @Override
          public InternalScanner run() throws Exception {
            return store.getCoprocessorHost().preCompact(store, scanner, scanType, request);
          }
        });
      } catch (InterruptedException ie) {
        InterruptedIOException iioe = new InterruptedIOException();
        iioe.initCause(ie);
        throw iioe;
      }
    }
  }

  /**
   * Used to prevent compaction name conflict when multiple compactions running parallel on the
   * same store.
   */
  private static final AtomicInteger NAME_COUNTER = new AtomicInteger(0);

  private String generateCompactionName() {
    int counter;
    for (;;) {
      counter = NAME_COUNTER.get();
      int next = counter == Integer.MAX_VALUE ? 0 : counter + 1;
      if (NAME_COUNTER.compareAndSet(counter, next)) {
        break;
      }
    }
    return store.getRegionInfo().getRegionNameAsString() + "#"
        + store.getFamily().getNameAsString() + "#" + counter;
  }

  /**
   * Performs the compaction.
   * @param fd FileDetails of cell sink writer
   * @param scanner Where to read from.
   * @param writer Where to write to.
   * @param smallestReadPoint Smallest read point.
   * @param cleanSeqId When true, remove seqId(used to be mvcc) value which is &lt;= smallestReadPoint
   * @param major Is a major compaction.
   * @return Whether compaction ended; false if it was interrupted for some reason.
   */
  protected boolean performCompaction(FileDetails fd, InternalScanner scanner, CellSink writer,
      long smallestReadPoint, boolean cleanSeqId,
      CompactionThroughputController throughputController, boolean major) throws IOException {
    assert writer instanceof ShipperListener;
    long bytesWrittenProgressForCloseCheck = 0;
    long bytesWrittenProgressForLog = 0;
    // Since scanner.next() can return 'false' but still be delivering data,
    // we have to use a do/while loop.
    List<Cell> cells = new ArrayList<Cell>();
    long closeCheckSizeLimit = HStore.getCloseCheckInterval();
    long lastMillis = 0;
    if (LOG.isDebugEnabled()) {
      lastMillis = EnvironmentEdgeManager.currentTime();
    }
    String compactionName = generateCompactionName();
    long now = 0;
    boolean hasMore;
    ScannerContext scannerContext =
        ScannerContext.newBuilder().setBatchLimit(compactionKVMax).build();

    throughputController.start(compactionName);
    KeyValueScanner kvs = (scanner instanceof KeyValueScanner)? (KeyValueScanner)scanner : null;
    try {
      do {
        hasMore = scanner.next(cells, scannerContext);
        if (LOG.isDebugEnabled()) {
          now = EnvironmentEdgeManager.currentTime();
        }
        // output to writer:
        Cell lastCleanCell = null;
        long lastCleanCellSeqId = 0;
        for (Cell c : cells) {
          if (cleanSeqId && c.getSequenceId() <= smallestReadPoint) {
            lastCleanCell = c;
            lastCleanCellSeqId = c.getSequenceId();
            CellUtil.setSequenceId(c, 0);
          } else {
            lastCleanCell = null;
            lastCleanCellSeqId = 0;
          }
          writer.append(c);
          int len = KeyValueUtil.length(c);
          ++progress.currentCompactedKVs;
          progress.totalCompactedSize += len;
          if (LOG.isDebugEnabled()) {
            bytesWrittenProgressForLog += len;
          }
          throughputController.control(compactionName, len);
          // check periodically to see if a system stop is requested
          if (closeCheckSizeLimit > 0) {
            bytesWrittenProgressForCloseCheck += len;
            if (bytesWrittenProgressForCloseCheck > closeCheckSizeLimit) {
              bytesWrittenProgressForCloseCheck = 0;
              if (!store.areWritesEnabled()) {
                progress.cancel();
                return false;
              }
            }
          }
        }
        if (kvs != null) {
          if (lastCleanCell != null) {
            // HBASE-16931, set back sequence id to avoid affecting scan order unexpectedly.
            // ShipperListener will do a clone of the last cells it refer, so need to set back
            // sequence id before ShipperListener.beforeShipped
            CellUtil.setSequenceId(lastCleanCell, lastCleanCellSeqId);
          }
          // Clone the cells that are in the writer so that they are freed of references,
          // if they are holding any.
          ((ShipperListener)writer).beforeShipped();
          // The SHARED block references, being read for compaction, will be kept in prevBlocks
          // list(See HFileScannerImpl#prevBlocks). In case of scan flow, after each set of cells
          // being returned to client, we will call shipped() which can clear this list. Here by
          // we are doing the similar thing. In between the compaction (after every N cells
          // written with collective size of 'shippedCallSizeLimit') we will call shipped which
          // may clear prevBlocks list.
          kvs.shipped();
        }
        if (lastCleanCell != null) {
          // HBASE-16931, set back sequence id to avoid affecting scan order unexpectedly
          CellUtil.setSequenceId(lastCleanCell, lastCleanCellSeqId);
        }
        // Log the progress of long running compactions every minute if
        // logging at DEBUG level
        if (LOG.isDebugEnabled()) {
          if ((now - lastMillis) >= COMPACTION_PROGRESS_LOG_INTERVAL) {
            LOG.debug("Compaction progress: "
                + compactionName
                + " "
                + progress
                + String.format(", rate=%.2f kB/sec", (bytesWrittenProgressForLog / 1024.0)
                    / ((now - lastMillis) / 1000.0)) + ", throughputController is "
                + throughputController);
            lastMillis = now;
            bytesWrittenProgressForLog = 0;
          }
        }
        cells.clear();
      } while (hasMore);
    } catch (InterruptedException e) {
      progress.cancel();
      throw new InterruptedIOException("Interrupted while control throughput of compacting "
          + compactionName);
    } finally {
      throughputController.finish(compactionName);
    }
    progress.complete();
    return true;
  }

  /**
   * Performs compaction on a column family with the mob flag enabled.
   * This is for when the mob threshold size has changed or if the mob
   * column family mode has been toggled via an alter table statement.
   * Compacts the files by the following rules.
   * 1. If the Put cell has a mob reference tag, the cell's value is the path of the mob file.
   * <ol>
   * <li>
   * If the value size of a cell is larger than the threshold, this cell is regarded as a mob,
   * directly copy the (with mob tag) cell into the new store file.
   * </li>
   * <li>
   * Otherwise, retrieve the mob cell from the mob file, and writes a copy of the cell into
   * the new store file.
   * </li>
   * </ol>
   * 2. If the Put cell doesn't have a reference tag.
   * <ol>
   * <li>
   * If the value size of a cell is larger than the threshold, this cell is regarded as a mob,
   * write this cell to a mob file, and write the path of this mob file to the store file.
   * </li>
   * <li>
   * Otherwise, directly write this cell into the store file.
   * </li>
   * </ol>
   * 3. Decide how to write a Delete cell.
   * <ol>
   * <li>
   * If a Delete cell does not have a mob reference tag which means this delete marker have not
   * been written to the mob del file, write this cell to the mob del file, and write this cell
   * with a ref tag to a store file.
   * </li>
   * <li>
   * Otherwise, directly write it to a store file.
   * </li>
   * </ol>
   * After the major compaction on the normal hfiles, we have a guarantee that we have purged all
   * deleted or old version mob refs, and the delete markers are written to a del file with the
   * suffix _del. Because of this, it is safe to use the del file in the mob compaction.
   * The mob compaction doesn't take place in the normal hfiles, it occurs directly in the
   * mob files. When the small mob files are merged into bigger ones, the del file is added into
   * the scanner to filter the deleted cells.
   * @param fd File details
   * @param scanner Where to read from.
   * @param writer Where to write to.
   * @param smallestReadPoint Smallest read point.
   * @param cleanSeqId When true, remove seqId(used to be mvcc) value which is <= smallestReadPoint
   * @param throughputController The compaction throughput controller.
   * @param major Is a major compaction.
   * @param numofFilesToCompact the number of files to compact
   * @return Whether compaction ended; false if it was interrupted for any reason.
   */
  protected boolean performMobCompaction(FileDetails fd, InternalScanner scanner, CellSink writer,
      long smallestReadPoint, boolean cleanSeqId, CompactionThroughputController throughputController,
      boolean major) throws IOException {
    int bytesWritten = 0;
    // Since scanner.next() can return 'false' but still be delivering data,
    // we have to use a do/while loop.
    List<Cell> cells = new ArrayList<Cell>();
    // Limit to "hbase.hstore.compaction.kv.max" (default 10) to avoid OOME
    int closeCheckInterval = HStore.getCloseCheckInterval();
    boolean hasMore;
    Path familyPath = MobUtils.getMobFamilyPath(conf, store.getTableName(), store.getColumnFamilyName());
    String date = MobUtils.formatDate(new Date(fd.latestPutTs));
    Path path = new Path(familyPath, date);
    Path delpath = new Path(familyPath, MobConstants.MOB_DELFILE_DIR_NAME);
    byte[] fileName = null;
    StoreFile.Writer mobFileWriter = null, delFileWriter = null;
    long mobCells = 0, deleteMarkersCount = 0;
    long cellsCountCompactedToMob = 0, cellsCountCompactedFromMob = 0;
    long cellsSizeCompactedToMob = 0, cellsSizeCompactedFromMob = 0;
    try {
      try {
        // If the mob file writer could not be created, directly write the cell to the store file.
        mobFileWriter = ((HMobStore)store).createWriterInTmp(new Date(fd.latestPutTs), fd.maxKeyCount,
            store.getFamily().getCompression(), store.getRegionInfo().getStartKey());
        fileName = Bytes.toBytes(mobFileWriter.getPath().getName());
      } catch (IOException e) {
        LOG.error("Failed to create mob writer, "
               + "we will continue the compaction by writing MOB cells directly in store files", e);
      }
      delFileWriter = ((HMobStore)store).createDelFileWriterInTmp(new Date(fd.latestPutTs), fd.maxKeyCount,
          store.getFamily().getCompression(), store.getRegionInfo().getStartKey());
      ScannerContext scannerContext =
              ScannerContext.newBuilder().setBatchLimit(compactionKVMax).build();
      do {
        hasMore = scanner.next(cells, scannerContext);
        for (Cell c : cells) {
          if (major && CellUtil.isDelete(c)) {
            if (MobUtils.isMobReferenceCell(c)) {
              // Directly write it to a store file
              writer.append(c);
            } else {
              // Add a ref tag to this cell and write it to a store file.
              writer.append(MobUtils.createMobRefDeleteMarker(c));
              // Write the cell to a del file
              delFileWriter.append(c);
              deleteMarkersCount++;
            }
          } else if (mobFileWriter == null || c.getTypeByte() != KeyValue.Type.Put.getCode()) {
            // If the mob file writer is null or the kv type is not put, directly write the cell
            // to the store file.
            writer.append(c);
          } else if (MobUtils.isMobReferenceCell(c)) {
            if (MobUtils.hasValidMobRefCellValue(c)) {
              int size = MobUtils.getMobValueLength(c);
              if (size > mobSizeThreshold) {
                // If the value size is larger than the threshold, it's regarded as a mob. Since
                // its value is already in the mob file, directly write this cell to the store file
                writer.append(c);
              } else {
                // If the value is not larger than the threshold, it's not regarded a mob. Retrieve
                // the mob cell from the mob file, and write it back to the store file. Must
                // close the mob scanner once the life cycle finished.
                try (MobCell mobCell = ((HMobStore)store).resolve(c, false)) {
                  if (mobCell.getCell().getValueLength() != 0) {
                    // put the mob data back to the store file
                    CellUtil.setSequenceId(mobCell.getCell(), c.getSequenceId());
                    writer.append(mobCell.getCell());
                    cellsCountCompactedFromMob++;
                    cellsSizeCompactedFromMob += mobCell.getCell().getValueLength();
                  } else {
                    // If the value of a file is empty, there might be issues when retrieving,
                    // directly write the cell to the store file, and leave it to be handled by the
                    // next compaction.
                    writer.append(c);
                  }
                }
              }
            } else {
              LOG.warn("The value format of the KeyValue " + c
                  + " is wrong, its length is less than " + Bytes.SIZEOF_INT);
              writer.append(c);
            }
          } else if (c.getValueLength() <= mobSizeThreshold) {
            //If value size of a cell is not larger than the threshold, directly write to store file
            writer.append(c);
          } else {
            // If the value size of a cell is larger than the threshold, it's regarded as a mob,
            // write this cell to a mob file, and write the path to the store file.
            mobCells++;
            // append the original keyValue in the mob file.
            mobFileWriter.append(c);
            Cell reference = MobUtils.createMobRefCell(c, fileName,
                ((HMobStore)store).getRefCellTags());
            // write the cell whose value is the path of a mob file to the store file.
            writer.append(reference);
            cellsCountCompactedToMob++;
            cellsSizeCompactedToMob += c.getValueLength();
          }
          ++progress.currentCompactedKVs;
          // check periodically to see if a system stop is requested
          if (closeCheckInterval > 0) {
            bytesWritten += KeyValueUtil.length(c);
            if (bytesWritten > closeCheckInterval) {
              bytesWritten = 0;
              if (!store.areWritesEnabled()) {
                progress.cancel();
                return false;
              }
            }
          }
        }
        cells.clear();
      } while (hasMore);
    } finally {
      if (mobFileWriter != null) {
        mobFileWriter.appendMetadata(fd.maxSeqId, major, mobCells);
        mobFileWriter.close();
      }
      if (delFileWriter != null) {
        delFileWriter.appendMetadata(fd.maxSeqId, major, deleteMarkersCount);
        delFileWriter.close();
      }
    }
    if (mobFileWriter != null) {
      if (mobCells > 0) {
        // If the mob file is not empty, commit it.
        ((HMobStore)store).commitFile(mobFileWriter.getPath(), path);
      } else {
        try {
          // If the mob file is empty, delete it instead of committing.
          store.getFileSystem().delete(mobFileWriter.getPath(), true);
        } catch (IOException e) {
          LOG.error("Failed to delete the temp mob file", e);
        }
      }
    }
    if (delFileWriter != null) {
      if (deleteMarkersCount > 0) {
        // If the del file is not empty, commit it.
        // If the commit fails, the compaction is re-performed again.
        ((HMobStore)store).commitFile(delFileWriter.getPath(), delpath);
      } else {
        try {
          // If the del file is empty, delete it instead of committing.
          store.getFileSystem().delete(delFileWriter.getPath(), true);
        } catch (IOException e) {
          LOG.error("Failed to delete the temp del file", e);
        }
      }
    }
    ((HMobStore)store).updateCellsCountCompactedFromMob(cellsCountCompactedFromMob);
    ((HMobStore)store).updateCellsCountCompactedToMob(cellsCountCompactedToMob);
    ((HMobStore)store).updateCellsSizeCompactedFromMob(cellsSizeCompactedFromMob);
    ((HMobStore)store).updateCellsSizeCompactedToMob(cellsSizeCompactedToMob);
    progress.complete();
    return true;
  }

  /**
   * @param store store
   * @param scanners Store file scanners.
   * @param scanType Scan type.
   * @param smallestReadPoint Smallest MVCC read point.
   * @param earliestPutTs Earliest put across all files.
   * @return A compaction scanner.
   */
  protected InternalScanner createScanner(Store store, List<StoreFileScanner> scanners,
      ScanType scanType, long smallestReadPoint, long earliestPutTs) throws IOException {
    Scan scan = new Scan();
    scan.setMaxVersions(store.getFamily().getMaxVersions());
    return new StoreScanner(store, store.getScanInfo(), scan, scanners,
        scanType, smallestReadPoint, earliestPutTs);
  }

  /**
   * @param store The store.
   * @param scanners Store file scanners.
   * @param smallestReadPoint Smallest MVCC read point.
   * @param earliestPutTs Earliest put across all files.
   * @param dropDeletesFromRow Drop deletes starting with this row, inclusive. Can be null.
   * @param dropDeletesToRow Drop deletes ending with this row, exclusive. Can be null.
   * @return A compaction scanner.
   */
  protected InternalScanner createScanner(Store store, List<StoreFileScanner> scanners,
     long smallestReadPoint, long earliestPutTs, byte[] dropDeletesFromRow,
     byte[] dropDeletesToRow) throws IOException {
    Scan scan = new Scan();
    scan.setMaxVersions(store.getFamily().getMaxVersions());
    return new StoreScanner(store, store.getScanInfo(), scan, scanners, smallestReadPoint,
        earliestPutTs, dropDeletesFromRow, dropDeletesToRow);
  }

  /**
   * Appends the metadata and closes the writer.
   * @param writer The current store writer.
   * @param fd The file details.
   * @param isMajor Is a major compaction.
   * @throws IOException
   */
  protected void appendMetadataAndCloseWriter(StoreFile.Writer writer, FileDetails fd,
      boolean isMajor) throws IOException {
    writer.appendMetadata(fd.maxSeqId, isMajor);
    writer.close();
  }
}
