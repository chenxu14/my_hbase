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

import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.shaded.com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.shaded.com.google.common.collect.Lists;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALProvider.Writer;
import org.apache.hadoop.hbase.wal.WALSplitter.RegionEntryBuffer;
import org.apache.hadoop.hbase.wal.WALSplitter.SinkWriter;
import org.apache.hadoop.hbase.wal.WALSplitter.WriterAndPath;
import org.apache.hadoop.io.MultipleIOException;

/**
 * Class that manages the output streams from the log splitting process.
 */
@InterfaceAudience.Private
public class LogRecoveredEditsOutputSink extends LogRecoveryOutputSink {
  private static final Log LOG = LogFactory.getLog(LogRecoveredEditsOutputSink.class);

  public LogRecoveredEditsOutputSink(WALSplitter spliter) {
    super(spliter);
  }

  /**
   * @return null if failed to report progress
   * @throws IOException
   */
  @Override
  public List<Path> finishWritingAndClose() throws IOException {
    boolean isSuccessful = false;
    List<Path> result = null;
    try {
      isSuccessful = finishWriting(false);
    } finally {
      result = close();
      List<IOException> thrown = closeLogWriters(null);
      if (thrown != null && !thrown.isEmpty()) {
        throw MultipleIOException.createIOException(thrown);
      }
    }
    if (isSuccessful) {
      splits = result;
    }
    return splits;
  }

  // delete the one with fewer wal entries
  void deleteOneWithFewerEntries(WriterAndPath wap, Path dst) throws IOException {
    long dstMinLogSeqNum = -1L;
    try (WAL.Reader reader = spliter.walFactory.createReader(spliter.fs, dst)) {
      WAL.Entry entry = reader.next();
      if (entry != null) {
        dstMinLogSeqNum = entry.getKey().getSequenceId();
      }
    } catch (EOFException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(
          "Got EOF when reading first WAL entry from " + dst + ", an empty or broken WAL file?",
          e);
      }
    }
    if (wap.minLogSeqNum < dstMinLogSeqNum) {
      LOG.warn("Found existing old edits file. It could be the result of a previous failed"
          + " split attempt or we have duplicated wal entries. Deleting " + dst + ", length="
          + spliter.fs.getFileStatus(dst).getLen());
      if (!spliter.fs.delete(dst, false)) {
        LOG.warn("Failed deleting of old " + dst);
        throw new IOException("Failed deleting of old " + dst);
      }
    } else {
      LOG.warn("Found existing old edits file and we have less entries. Deleting " + wap.p
          + ", length=" + spliter.fs.getFileStatus(wap.p).getLen());
      if (!spliter.fs.delete(wap.p, false)) {
        LOG.warn("Failed deleting of " + wap.p);
        throw new IOException("Failed deleting of " + wap.p);
      }
    }
  }

  /**
   * Close all of the output streams.
   * @return the list of paths written.
   */
  List<Path> close() throws IOException {
    Preconditions.checkState(!closeAndCleanCompleted);

    final List<Path> paths = new ArrayList<Path>();
    final List<IOException> thrown = Lists.newArrayList();
    ThreadPoolExecutor closeThreadPool = Threads.getBoundedCachedThreadPool(spliter.numWriterThreads, 30L,
      TimeUnit.SECONDS, new ThreadFactory() {
        private int count = 1;

        @Override
        public Thread newThread(Runnable r) {
          Thread t = new Thread(r, "split-log-closeStream-" + count++);
          return t;
        }
      });
    CompletionService<Void> completionService =
      new ExecutorCompletionService<Void>(closeThreadPool);
    boolean progress_failed;
    try{
      progress_failed = executeCloseTask(completionService, thrown, paths);
    } catch (InterruptedException e) {
      IOException iie = new InterruptedIOException();
      iie.initCause(e);
      throw iie;
    } catch (ExecutionException e) {
      throw new IOException(e.getCause());
    } finally {
      closeThreadPool.shutdownNow();
    }

    if (!thrown.isEmpty()) {
      throw MultipleIOException.createIOException(thrown);
    }
    writersClosed = true;
    closeAndCleanCompleted = true;
    if (progress_failed) {
      return null;
    }
    return paths;
  }

  boolean executeCloseTask(CompletionService<Void> completionService,
      final List<IOException> thrown, final List<Path> paths)
      throws InterruptedException, ExecutionException {
    for (final Map.Entry<String, SinkWriter> writersEntry : writers.entrySet()) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Submitting close of " + ((WriterAndPath) writersEntry.getValue()).p);
      }
      completionService.submit(new Callable<Void>() {
        @Override public Void call() throws Exception {
          WriterAndPath wap = (WriterAndPath) writersEntry.getValue();
          Path dst = closeWriter(writersEntry.getKey(), wap, thrown);
          paths.add(dst);
          return null;
        }
      });
    }
    boolean progress_failed = false;
    for (int i = 0, n = this.writers.size(); i < n; i++) {
      Future<Void> future = completionService.take();
      future.get();
      if (!progress_failed && reporter != null && !reporter.progress()) {
        progress_failed = true;
      }
    }
    return progress_failed;
  }

  Path closeWriter(String encodedRegionName, WriterAndPath wap, List<IOException> thrown)
      throws IOException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Closing " + wap.p);
    }
    try {
      wap.w.close();
    } catch (IOException ioe) {
      LOG.error("Couldn't close log at " + wap.p, ioe);
      thrown.add(ioe);
      return null;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Closed wap " + wap.p + " (wrote " + wap.editsWritten
          + " edits, skipped " + wap.editsSkipped + " edits in "
          + (wap.nanosSpent / 1000 / 1000) + "ms");
    }
    if (wap.editsWritten == 0) {
      // just remove the empty recovered.edits file
      if (spliter.fs.exists(wap.p) && !spliter.fs.delete(wap.p, false)) {
        LOG.warn("Failed deleting empty " + wap.p);
        throw new IOException("Failed deleting empty  " + wap.p);
      }
      return null;
    }

    Path dst = getCompletedRecoveredEditsFilePath(wap.p,
        regionMaximumEditLogSeqNum.get(encodedRegionName));
    try {
      if (!dst.equals(wap.p) && spliter.fs.exists(dst)) {
        deleteOneWithFewerEntries(wap, dst);
      }
      // Skip the unit tests which create a splitter that reads and
      // writes the data without touching disk.
      // TestHLogSplit#testThreading is an example.
      if (spliter.fs.exists(wap.p)) {
        if (!spliter.fs.rename(wap.p, dst)) {
          throw new IOException("Failed renaming " + wap.p + " to " + dst);
        }
        LOG.info("Rename " + wap.p + " to " + dst);
      }
    } catch (IOException ioe) {
      LOG.error("Couldn't rename " + wap.p + " to " + dst, ioe);
      thrown.add(ioe);
      return null;
    }
    return dst;
  }

  private List<IOException> closeLogWriters(List<IOException> thrown) throws IOException {
    if (writersClosed) {
      return thrown;
    }

    if (thrown == null) {
      thrown = Lists.newArrayList();
    }
    try {
      for (WriterThread t : writerThreads) {
        while (t.isAlive()) {
          t.shouldStop = true;
          t.interrupt();
          try {
            t.join(10);
          } catch (InterruptedException e) {
            IOException iie = new InterruptedIOException();
            iie.initCause(e);
            throw iie;
          }
        }
      }
    } finally {
      WriterAndPath wap = null;
        for (SinkWriter tmpWAP : writers.values()) {
        try {
          wap = (WriterAndPath) tmpWAP;
          wap.w.close();
        } catch (IOException ioe) {
          LOG.error("Couldn't close log at " + wap.p, ioe);
          thrown.add(ioe);
          continue;
        }
        LOG.info(
            "Closed log " + wap.p + " (wrote " + wap.editsWritten + " edits in " + (wap.nanosSpent
                / 1000 / 1000) + "ms)");
      }
      writersClosed = true;
    }

    return thrown;
  }

  /**
   * Get a writer and path for a log starting at the given entry. This function is threadsafe so
   * long as multiple threads are always acting on different regions.
   * @return null if this region shouldn't output any logs
   */
  WriterAndPath getWriterAndPath(Entry entry, boolean reusable) throws IOException {
    byte region[] = entry.getKey().getEncodedRegionName();
    String regionName = Bytes.toString(region);
    WriterAndPath ret = (WriterAndPath) writers.get(regionName);
    if (ret != null) {
      return ret;
    }
    // If we already decided that this region doesn't get any output
    // we don't need to check again.
    if (blacklistedRegions.contains(region)) {
      return null;
    }
    ret = createWAP(region, entry, spliter.rootDir);
    if (ret == null) {
      blacklistedRegions.add(region);
      return null;
    }
    if (reusable) {
      writers.put(regionName, ret);
    }
    return ret;
  }

  /**
   * @return a path with a write for that path. caller should close.
   */
  WriterAndPath createWAP(byte[] region, Entry entry, Path rootdir) throws IOException {
    Path regionedits = WALSplitter.getRegionSplitEditsPath(spliter.fs, entry, rootdir, spliter.taskBeingSplit);
    if (regionedits == null) {
      return null;
    }
    if (spliter.fs.exists(regionedits)) {
      LOG.warn("Found old edits file. It could be the "
          + "result of a previous failed split attempt. Deleting " + regionedits + ", length="
          + spliter.fs.getFileStatus(regionedits).getLen());
      if (!spliter.fs.delete(regionedits, false)) {
        LOG.warn("Failed delete of old " + regionedits);
      }
    }
    Writer w = spliter.createWriter(regionedits);
    LOG.debug("Creating writer path=" + regionedits);
    return new WriterAndPath(regionedits, w, entry.getKey().getSequenceId());
  }

  void filterCellByStore(Entry logEntry) {
    Map<byte[], Long> maxSeqIdInStores =
        spliter.regionMaxSeqIdInStores.get(Bytes.toString(logEntry.getKey().getEncodedRegionName()));
    // In KAFKA case bypass the filter logic
    if (this.spliter instanceof KafkaWALSplitter || maxSeqIdInStores == null || maxSeqIdInStores.isEmpty()) {
      return;
    }
    // Create the array list for the cells that aren't filtered.
    // We make the assumption that most cells will be kept.
    ArrayList<Cell> keptCells = new ArrayList<Cell>(logEntry.getEdit().getCells().size());
    for (Cell cell : logEntry.getEdit().getCells()) {
      if (WALEdit.isMetaEditFamily(cell)) {
        keptCells.add(cell);
      } else {
        byte[] family = CellUtil.cloneFamily(cell);
        Long maxSeqId = maxSeqIdInStores.get(family);
        // Do not skip cell even if maxSeqId is null. Maybe we are in a rolling upgrade,
        // or the master was crashed before and we can not get the information.
        if (maxSeqId == null || maxSeqId.longValue() < logEntry.getKey().getSequenceId()) {
          keptCells.add(cell);
        }
      }
    }

    // Anything in the keptCells array list is still live.
    // So rather than removing the cells from the array list
    // which would be an O(n^2) operation, we just replace the list
    logEntry.getEdit().setCells(keptCells);
  }

  @Override
  public void append(RegionEntryBuffer buffer) throws IOException {
    appendBuffer(buffer, true);
  }

  WriterAndPath appendBuffer(RegionEntryBuffer buffer, boolean reusable) throws IOException {
    List<Entry> entries = buffer.entryBuffer;
    if (entries.isEmpty()) {
      LOG.warn("got an empty buffer, skipping");
      return null;
    }
    WriterAndPath wap = null;

    long startTime = System.nanoTime();
    try {
      int editsCount = 0;

      for (Entry logEntry : entries) {
        if (wap == null) {
          wap = getWriterAndPath(logEntry, reusable);
          if (wap == null) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("getWriterAndPath decided we don't need to write edits for " + logEntry);
            }
            return null;
          }
        }
        filterCellByStore(logEntry);
        if (!logEntry.getEdit().isEmpty()) {
          wap.w.append(logEntry);
          this.updateRegionMaximumEditLogSeqNum(logEntry);
          editsCount++;
        } else {
          wap.incrementSkippedEdits(1);
        }
      }
      // Pass along summary statistics
      wap.incrementEdits(editsCount);
      wap.incrementNanoTime(System.nanoTime() - startTime);
    } catch (IOException e) {
      e = RemoteExceptionHandler.checkIOException(e);
      LOG.fatal(" Got while writing log entry to log", e);
      throw e;
    }
    return wap;
  }

  @Override
  public boolean keepRegionEvent(Entry entry) {
    ArrayList<Cell> cells = entry.getEdit().getCells();
    for (int i = 0; i < cells.size(); i++) {
      if (WALEdit.isCompactionMarker(cells.get(i))) {
        return true;
      }
    }
    return false;
  }

  /**
   * @return a map from encoded region ID to the number of edits written out for that region.
   */
  @Override
  public Map<byte[], Long> getOutputCounts() {
    TreeMap<byte[], Long> ret = new TreeMap<byte[], Long>(Bytes.BYTES_COMPARATOR);
    synchronized (writers) {
      for (Map.Entry<String, SinkWriter> entry : writers.entrySet()) {
        ret.put(Bytes.toBytes(entry.getKey()), entry.getValue().editsWritten);
      }
    }
    return ret;
  }

  @Override
  public int getNumberOfRecoveredRegions() {
    return writers.size();
  }

  /**
   * Get the completed recovered edits file path, renaming it to be by last edit
   * in the file from its first edit. Then we could use the name to skip
   * recovered edits when doing {@link HRegion#replayRecoveredEditsIfAny}.
   * @param srcPath
   * @param maximumEditLogSeqNum
   * @return dstPath take file's last edit log seq num as the name
   */
  private static Path getCompletedRecoveredEditsFilePath(Path srcPath,
      long maximumEditLogSeqNum) {
    String fileName = WALSplitter.formatRecoveredEditsFileName(maximumEditLogSeqNum);
    return new Path(srcPath.getParent(), fileName);
  }
}
