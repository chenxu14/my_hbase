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
package org.apache.hadoop.hbase.wal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.shaded.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.WALEntry;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutationProto.MutationType;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WAL.Reader;
import org.apache.hadoop.hbase.wal.WALProvider.Writer;

public class WALSplitter {
  protected static final Log LOG = LogFactory.getLog(WALSplitter.class);
  protected PipelineController controller;
  protected final Configuration conf;
  private final boolean splitWriterCreationBounded;
  protected EntryBuffers entryBuffers;
  protected OutputSink outputSink;
  protected boolean distributedLogReplay;
  protected MonitoredTask status;
  protected final FileSystem fs;
  protected final WALFactory walFactory;
  // Parameters for split process
  protected final Path rootDir;
  // Map encodedRegionName -> maxSeqIdInStores
  protected Map<String, Map<byte[], Long>> regionMaxSeqIdInStores =
     new ConcurrentHashMap<String, Map<byte[], Long>>();
  // the file being split currently
  protected String taskBeingSplit;

  // Number of writer threads
  final int numWriterThreads;
  public final static String SPLIT_WRITER_CREATION_BOUNDED = "hbase.split.writer.creation.bounded";
  public final static String SPLIT_REPORT_INTERVAL = "hbase.splitlog.report.interval.loglines";
  public final static int SPLIT_REPORT_INTERVAL_DEFAULT = 1024;

  /** By default we retry errors in splitting, rather than skipping. */
  public static final boolean SPLIT_SKIP_ERRORS_DEFAULT = false;
  private static final Pattern EDITFILES_NAME_PATTERN = Pattern.compile("-?[0-9]+");
  private static final String RECOVERED_LOG_TMPFILE_SUFFIX = ".temp";
  private static final String SEQUENCE_ID_FILE_SUFFIX = ".seqid";
  private static final String OLD_SEQUENCE_ID_FILE_SUFFIX = "_seqid";
  private static final int SEQUENCE_ID_FILE_SUFFIX_LENGTH = SEQUENCE_ID_FILE_SUFFIX.length();

  public WALSplitter(Configuration conf, FileSystem fs, WALFactory factory, Path rootDir) {
    this.conf = HBaseConfiguration.create(conf);
    this.fs = fs;
    this.walFactory = factory;
    this.rootDir = rootDir;
    this.controller = new PipelineController();
    this.splitWriterCreationBounded = conf.getBoolean(SPLIT_WRITER_CREATION_BOUNDED, false);
    entryBuffers = new EntryBuffers(controller,
      this.conf.getInt("hbase.regionserver.hlog.splitlog.buffersize",
          128*1024*1024), splitWriterCreationBounded);
    this.numWriterThreads = this.conf.getInt("hbase.regionserver.hlog.splitlog.writer.threads", 3);
    this.distributedLogReplay = false;
    if (splitWriterCreationBounded) {
      outputSink = new BoundedLogWriterCreationOutputSink(this);
    } else {
      outputSink = new LogRecoveredEditsOutputSink(this);
    }
  }

  /**
   * Class which accumulates edits and separates them into a buffer per region
   * while simultaneously accounting RAM usage. Blocks if the RAM usage crosses
   * a predefined threshold.
   *
   * Writer threads then pull region-specific buffers from this class.
   */
  public static class EntryBuffers {
    PipelineController controller;

    Map<byte[], RegionEntryBuffer> buffers =
      new TreeMap<byte[], RegionEntryBuffer>(Bytes.BYTES_COMPARATOR);

    /* Track which regions are currently in the middle of writing. We don't allow
       an IO thread to pick up bytes from a region if we're already writing
       data for that region in a different IO thread. */
    Set<byte[]> currentlyWriting = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);

    long totalBuffered = 0;
    final long maxHeapUsage;
    boolean splitWriterCreationBounded;

    public EntryBuffers(PipelineController controller, long maxHeapUsage) {
      this(controller, maxHeapUsage, false);
    }

    public EntryBuffers(PipelineController controller, long maxHeapUsage,
        boolean splitWriterCreationBounded) {
      this.controller = controller;
      this.maxHeapUsage = maxHeapUsage;
      this.splitWriterCreationBounded = splitWriterCreationBounded;
    }

    /**
     * Append a log entry into the corresponding region buffer.
     * Blocks if the total heap usage has crossed the specified threshold.
     *
     * @throws InterruptedException
     * @throws IOException
     */
    public void appendEntry(Entry entry) throws InterruptedException, IOException {
      WALKey key = entry.getKey();

      RegionEntryBuffer buffer;
      long incrHeap;
      synchronized (this) {
        buffer = buffers.get(key.getEncodedRegionName());
        if (buffer == null) {
          buffer = new RegionEntryBuffer(key.getTablename(), key.getEncodedRegionName());
          buffers.put(key.getEncodedRegionName(), buffer);
        }
        incrHeap= buffer.appendEntry(entry);
      }

      // If we crossed the chunk threshold, wait for more space to be available
      synchronized (controller.dataAvailable) {
        totalBuffered += incrHeap;
        while (totalBuffered > maxHeapUsage && controller.thrown.get() == null) {
          LOG.debug("Used " + totalBuffered + " bytes of buffered edits, waiting for IO threads...");
          controller.dataAvailable.wait(2000);
        }
        controller.dataAvailable.notifyAll();
      }
      controller.checkForErrors();
    }

    /**
     * @return RegionEntryBuffer a buffer of edits to be written or replayed.
     */
    synchronized RegionEntryBuffer getChunkToWrite() {
      // The core part of limiting opening writers is it doesn't return chunk only if the heap size
      // is over maxHeapUsage. Thus it doesn't need to create a writer for each region
      // during splitting. It will flush all the logs in the buffer after splitting through a
      // threadpool, which means the number of writers it created is under control
      if (splitWriterCreationBounded && totalBuffered < maxHeapUsage) {
        return null;
      }
      long biggestSize = 0;
      byte[] biggestBufferKey = null;

      for (Map.Entry<byte[], RegionEntryBuffer> entry : buffers.entrySet()) {
        long size = entry.getValue().heapSize();
        if (size > biggestSize && (!currentlyWriting.contains(entry.getKey()))) {
          biggestSize = size;
          biggestBufferKey = entry.getKey();
        }
      }
      if (biggestBufferKey == null) {
        return null;
      }

      RegionEntryBuffer buffer = buffers.remove(biggestBufferKey);
      currentlyWriting.add(biggestBufferKey);
      return buffer;
    }

    void doneWriting(RegionEntryBuffer buffer) {
      synchronized (this) {
        boolean removed = currentlyWriting.remove(buffer.encodedRegionName);
        assert removed;
      }
      long size = buffer.heapSize();

      synchronized (controller.dataAvailable) {
        totalBuffered -= size;
        // We may unblock writers
        controller.dataAvailable.notifyAll();
      }
    }

    synchronized boolean isRegionCurrentlyWriting(byte[] region) {
      return currentlyWriting.contains(region);
    }

    public void waitUntilDrained() {
      synchronized (controller.dataAvailable) {
        while (totalBuffered > 0) {
          try {
            controller.dataAvailable.wait(2000);
          } catch (InterruptedException e) {
            LOG.warn("Got intrerrupted while waiting for EntryBuffers is drained");
            Thread.interrupted();
            break;
          }
        }
      }
    }
  }

  /**
   * Contains some methods to control WAL-entries producer / consumer interactions
   */
  public static class PipelineController {
    // If an exception is thrown by one of the other threads, it will be
    // stored here.
    AtomicReference<Throwable> thrown = new AtomicReference<Throwable>();

    // Wait/notify for when data has been produced by the writer thread,
    // consumed by the reader thread, or an exception occurred
    public final Object dataAvailable = new Object();

    void writerThreadError(Throwable t) {
      thrown.compareAndSet(null, t);
    }

    /**
     * Check for errors in the writer threads. If any is found, rethrow it.
     */
    void checkForErrors() throws IOException {
      Throwable thrown = this.thrown.get();
      if (thrown == null) return;
      if (thrown instanceof IOException) {
        throw new IOException(thrown);
      } else {
        throw new RuntimeException(thrown);
      }
    }
  }

  /**
   * A buffer of some number of edits for a given region.
   * This accumulates edits and also provides a memory optimization in order to
   * share a single byte array instance for the table and region name.
   * Also tracks memory usage of the accumulated edits.
   */
  public static class RegionEntryBuffer implements HeapSize {
    long heapInBuffer = 0;
    List<Entry> entryBuffer;
    TableName tableName;
    byte[] encodedRegionName;

    RegionEntryBuffer(TableName tableName, byte[] region) {
      this.tableName = tableName;
      this.encodedRegionName = region;
      this.entryBuffer = new LinkedList<Entry>();
    }

    long appendEntry(Entry entry) {
      internify(entry);
      entryBuffer.add(entry);
      long incrHeap = entry.getEdit().heapSize() +
        ClassSize.align(2 * ClassSize.REFERENCE) + // WALKey pointers
        0; // TODO linkedlist entry
      heapInBuffer += incrHeap;
      return incrHeap;
    }

    private void internify(Entry entry) {
      WALKey k = entry.getKey();
      k.internTableName(this.tableName);
      k.internEncodedRegionName(this.encodedRegionName);
    }

    @Override
    public long heapSize() {
      return heapInBuffer;
    }

    public byte[] getEncodedRegionName() {
      return encodedRegionName;
    }

    public List<Entry> getEntryBuffer() {
      return entryBuffer;
    }

    public TableName getTableName() {
      return tableName;
    }
  }

  /**
   * Class wraps the actual writer which writes data out and related statistics
   */
  public abstract static class SinkWriter {
    /* Count of edits written to this path */
    long editsWritten = 0;
    /* Count of edits skipped to this path */
    long editsSkipped = 0;
    /* Number of nanos spent writing to this log */
    long nanosSpent = 0;

    void incrementEdits(int edits) {
      editsWritten += edits;
    }

    void incrementSkippedEdits(int skipped) {
      editsSkipped += skipped;
    }

    void incrementNanoTime(long nanos) {
      nanosSpent += nanos;
    }
  }

  /**
   * Private data structure that wraps a Writer and its Path, also collecting statistics about the
   * data written to this output.
   */
  final static class WriterAndPath extends SinkWriter {
    final Path p;
    final Writer w;
    final long minLogSeqNum;

    WriterAndPath(final Path p, final Writer w, final long minLogSeqNum) {
      this.p = p;
      this.w = w;
      this.minLogSeqNum = minLogSeqNum;
    }
  }

  /**
   * Move aside a bad edits file.
   *
   * @param fs
   * @param edits
   *          Edits file to move aside.
   * @return The name of the moved aside file.
   * @throws IOException
   */
  public static Path moveAsideBadEditsFile(final FileSystem fs, final Path edits)
      throws IOException {
    Path moveAsideName = new Path(edits.getParent(), edits.getName() + "."
        + System.currentTimeMillis());
    if (!fs.rename(edits, moveAsideName)) {
      LOG.warn("Rename failed from " + edits + " to " + moveAsideName);
    }
    return moveAsideName;
  }

  /**
   * @param regiondir
   *          This regions directory in the filesystem.
   * @return The directory that holds recovered edits files for the region
   *         <code>regiondir</code>
   */
  public static Path getRegionDirRecoveredEditsDir(final Path regiondir) {
    return new Path(regiondir, HConstants.RECOVERED_EDITS_DIR);
  }

  /**
   * Returns sorted set of edit files made by splitter, excluding files
   * with '.temp' suffix.
   *
   * @param fs
   * @param regiondir
   * @return Files in passed <code>regiondir</code> as a sorted set.
   * @throws IOException
   */
  public static NavigableSet<Path> getSplitEditFilesSorted(final FileSystem fs,
      final Path regiondir) throws IOException {
    NavigableSet<Path> filesSorted = new TreeSet<Path>();
    Path editsdir = getRegionDirRecoveredEditsDir(regiondir);
    if (!fs.exists(editsdir))
      return filesSorted;
    FileStatus[] files = FSUtils.listStatus(fs, editsdir, new PathFilter() {
      @Override
      public boolean accept(Path p) {
        boolean result = false;
        try {
          // Return files and only files that match the editfile names pattern.
          // There can be other files in this directory other than edit files.
          // In particular, on error, we'll move aside the bad edit file giving
          // it a timestamp suffix. See moveAsideBadEditsFile.
          Matcher m = EDITFILES_NAME_PATTERN.matcher(p.getName());
          result = fs.isFile(p) && m.matches();
          // Skip the file whose name ends with RECOVERED_LOG_TMPFILE_SUFFIX,
          // because it means splitwal thread is writting this file.
          if (p.getName().endsWith(RECOVERED_LOG_TMPFILE_SUFFIX)) {
            result = false;
          }
          // Skip SeqId Files
          if (isSequenceIdFile(p)) {
            result = false;
          }
        } catch (IOException e) {
          LOG.warn("Failed isFile check on " + p);
        }
        return result;
      }
    });
    if (files == null) {
      return filesSorted;
    }
    for (FileStatus status : files) {
      filesSorted.add(status.getPath());
    }
    return filesSorted;
  }

  /**
   * Is the given file a region open sequence id file.
   */
  @VisibleForTesting
  public static boolean isSequenceIdFile(final Path file) {
    return file.getName().endsWith(SEQUENCE_ID_FILE_SUFFIX)
        || file.getName().endsWith(OLD_SEQUENCE_ID_FILE_SUFFIX);
  }

  /**
   * Create a file with name as region open sequence id
   * @param fs
   * @param regiondir
   * @param newSeqId
   * @param saftyBumper
   * @return long new sequence Id value
   * @throws IOException
   */
  public static long writeRegionSequenceIdFile(final FileSystem fs, final Path regiondir,
      long newSeqId, long saftyBumper) throws IOException {

    Path editsdir = HdfsWALSplitter.getRegionDirRecoveredEditsDir(regiondir);
    long maxSeqId = 0;
    FileStatus[] files = null;
    if (fs.exists(editsdir)) {
      files = FSUtils.listStatus(fs, editsdir, new PathFilter() {
        @Override
        public boolean accept(Path p) {
          return isSequenceIdFile(p);
        }
      });
      if (files != null) {
        for (FileStatus status : files) {
          String fileName = status.getPath().getName();
          try {
            Long tmpSeqId = Long.parseLong(fileName.substring(0, fileName.length()
                - SEQUENCE_ID_FILE_SUFFIX_LENGTH));
            maxSeqId = Math.max(tmpSeqId, maxSeqId);
          } catch (NumberFormatException ex) {
            LOG.warn("Invalid SeqId File Name=" + fileName);
          }
        }
      }
    }
    if (maxSeqId > newSeqId) {
      newSeqId = maxSeqId;
    }
    newSeqId += saftyBumper; // bump up SeqId

    // write a new seqId file
    Path newSeqIdFile = new Path(editsdir, newSeqId + SEQUENCE_ID_FILE_SUFFIX);
    if (newSeqId != maxSeqId) {
      try {
        if (!fs.createNewFile(newSeqIdFile) && !fs.exists(newSeqIdFile)) {
          throw new IOException("Failed to create SeqId file:" + newSeqIdFile);
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Wrote region seqId=" + newSeqIdFile + " to file, newSeqId=" + newSeqId
              + ", maxSeqId=" + maxSeqId);
        }
      } catch (FileAlreadyExistsException ignored) {
        // latest hdfs throws this exception. it's all right if newSeqIdFile already exists
      }
    }
    // remove old ones
    if (files != null) {
      for (FileStatus status : files) {
        if (newSeqIdFile.equals(status.getPath())) {
          continue;
        }
        fs.delete(status.getPath(), false);
      }
    }
    return newSeqId;
  }

  /**
   * Path to a file under RECOVERED_EDITS_DIR directory of the region found in
   * <code>logEntry</code> named for the sequenceid in the passed
   * <code>logEntry</code>: e.g. /hbase/some_table/2323432434/recovered.edits/2332.
   * This method also ensures existence of RECOVERED_EDITS_DIR under the region
   * creating it if necessary.
   * @param fs
   * @param logEntry
   * @param rootDir HBase root dir.
   * @param fileNameBeingSplit the file being split currently. Used to generate tmp file name.
   * @return Path to file into which to dump split log edits.
   * @throws IOException
   */
  @SuppressWarnings("deprecation")
  @VisibleForTesting
  static Path getRegionSplitEditsPath(final FileSystem fs,
      final Entry logEntry, final Path rootDir, String fileNameBeingSplit)
  throws IOException {
    Path tableDir = FSUtils.getTableDir(rootDir, logEntry.getKey().getTablename());
    String encodedRegionName = Bytes.toString(logEntry.getKey().getEncodedRegionName());
    Path regiondir = HRegion.getRegionDir(tableDir, encodedRegionName);
    Path dir = getRegionDirRecoveredEditsDir(regiondir);

    if (!fs.exists(regiondir)) {
      LOG.info("This region's directory doesn't exist: "
          + regiondir.toString() + ". It is very likely that it was" +
          " already split so it's safe to discard those edits.");
      return null;
    }
    if (fs.exists(dir) && fs.isFile(dir)) {
      Path tmp = new Path("/tmp");
      if (!fs.exists(tmp)) {
        fs.mkdirs(tmp);
      }
      tmp = new Path(tmp,
        HConstants.RECOVERED_EDITS_DIR + "_" + encodedRegionName);
      LOG.warn("Found existing old file: " + dir + ". It could be some "
        + "leftover of an old installation. It should be a folder instead. "
        + "So moving it to " + tmp);
      if (!fs.rename(dir, tmp)) {
        LOG.warn("Failed to sideline old file " + dir);
      }
    }

    if (!fs.exists(dir) && !fs.mkdirs(dir)) {
      LOG.warn("mkdir failed on " + dir);
    }
    // Append fileBeingSplit to prevent name conflict since we may have duplicate wal entries now.
    // Append file name ends with RECOVERED_LOG_TMPFILE_SUFFIX to ensure
    // region's replayRecoveredEdits will not delete it
    String fileName = formatRecoveredEditsFileName(logEntry.getKey().getLogSeqNum());
    fileName = getTmpRecoveredEditsFileName(fileName + "-" + fileNameBeingSplit);
    return new Path(dir, fileName);
  }

  @VisibleForTesting
  static String formatRecoveredEditsFileName(final long seqid) {
    return String.format("%019d", seqid);
  }

  private static String getTmpRecoveredEditsFileName(String fileName) {
    return fileName + RECOVERED_LOG_TMPFILE_SUFFIX;
  }

  /** A struct used by getMutationsFromWALEntry */
  public static class MutationReplay implements Comparable<MutationReplay> {
    public MutationReplay(MutationType type, Mutation mutation, long nonceGroup, long nonce) {
      this.type = type;
      this.mutation = mutation;
      if(this.mutation.getDurability() != Durability.SKIP_WAL) {
        // using ASYNC_WAL for relay
        this.mutation.setDurability(Durability.ASYNC_WAL);
      }
      this.nonceGroup = nonceGroup;
      this.nonce = nonce;
    }

    public final MutationType type;
    public final Mutation mutation;
    public final long nonceGroup;
    public final long nonce;

    @Override
    public int compareTo(final MutationReplay d) {
      return this.mutation.compareTo(d.mutation);
    }

    @Override
    public boolean equals(Object obj) {
      if(!(obj instanceof MutationReplay)) {
        return false;
      } else {
        return this.compareTo((MutationReplay)obj) == 0;
      }
    }

    @Override
    public int hashCode() {
      return this.mutation.hashCode();
    }
  }

  /**
   * This function is used to construct mutations from a WALEntry. It also
   * reconstructs WALKey &amp; WALEdit from the passed in WALEntry
   * @param entry
   * @param cells
   * @param logEntry pair of WALKey and WALEdit instance stores WALKey and WALEdit instances
   *          extracted from the passed in WALEntry.
   * @param durability
   * @return list of Pair&lt;MutationType, Mutation&gt; to be replayed
   * @throws IOException
   */
  public static List<MutationReplay> getMutationsFromWALEntry(WALEntry entry, CellScanner cells,
      Pair<WALKey, WALEdit> logEntry, Durability durability)
          throws IOException {
    if (entry == null) {
      // return an empty array
      return new ArrayList<MutationReplay>();
    }

    long replaySeqId = (entry.getKey().hasOrigSequenceNumber()) ?
      entry.getKey().getOrigSequenceNumber() : entry.getKey().getLogSequenceNumber();
    int count = entry.getAssociatedCellCount();
    List<MutationReplay> mutations = new ArrayList<MutationReplay>();
    Cell previousCell = null;
    Mutation m = null;
    WALKey key = null;
    WALEdit val = null;
    if (logEntry != null) val = new WALEdit();

    for (int i = 0; i < count; i++) {
      // Throw index out of bounds if our cell count is off
      if (!cells.advance()) {
        throw new ArrayIndexOutOfBoundsException("Expected=" + count + ", index=" + i);
      }
      Cell cell = cells.current();
      if (val != null) val.add(cell);

      boolean isNewRowOrType =
          previousCell == null || previousCell.getTypeByte() != cell.getTypeByte()
              || !CellUtil.matchingRows(previousCell, cell);
      if (isNewRowOrType) {
        // Create new mutation
        if (CellUtil.isDelete(cell)) {
          m = new Delete(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
          // Deletes don't have nonces.
          mutations.add(new MutationReplay(
              MutationType.DELETE, m, HConstants.NO_NONCE, HConstants.NO_NONCE));
        } else {
          m = new Put(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
          // Puts might come from increment or append, thus we need nonces.
          long nonceGroup = entry.getKey().hasNonceGroup()
              ? entry.getKey().getNonceGroup() : HConstants.NO_NONCE;
          long nonce = entry.getKey().hasNonce() ? entry.getKey().getNonce() : HConstants.NO_NONCE;
          mutations.add(new MutationReplay(MutationType.PUT, m, nonceGroup, nonce));
        }
      }
      if (CellUtil.isDelete(cell)) {
        ((Delete) m).addDeleteMarker(cell);
      } else {
        ((Put) m).add(cell);
      }
      m.setDurability(durability);
      previousCell = cell;
    }

    // reconstruct WALKey
    if (logEntry != null) {
      org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.WALKey walKeyProto = entry.getKey();
      List<UUID> clusterIds = new ArrayList<UUID>(walKeyProto.getClusterIdsCount());
      for (HBaseProtos.UUID uuid : entry.getKey().getClusterIdsList()) {
        clusterIds.add(new UUID(uuid.getMostSigBits(), uuid.getLeastSigBits()));
      }
      // we use HLogKey here instead of WALKey directly to support legacy coprocessors.
      key = new HLogKey(walKeyProto.getEncodedRegionName().toByteArray(), TableName.valueOf(
              walKeyProto.getTableName().toByteArray()), replaySeqId, walKeyProto.getWriteTime(),
              clusterIds, walKeyProto.getNonceGroup(), walKeyProto.getNonce(), null);
      logEntry.setFirst(key);
      logEntry.setSecond(val);
    }

    return mutations;
  }

  /**
   * Create a new {@link Writer} for writing log splits.
   * @return a new Writer instance, caller should close
   */
  protected Writer createWriter(Path logfile)
      throws IOException {
    return walFactory.createRecoveredEditsWriter(fs, logfile);
  }

  /**
   * Create a new {@link Reader} for reading logs to split.
   * @return new Reader instance, caller should close
   */
  protected Reader getReader(Path curLogFile, CancelableProgressable reporter) throws IOException {
    return walFactory.createReader(fs, curLogFile, reporter);
  }
}
