/*
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Store flusher interface. Turns a snapshot of memstore into a set of store files (usually one).
 * Custom implementation can be provided.
 */
@InterfaceAudience.Private
abstract class StoreFlusher {
  private static final Log LOG = LogFactory.getLog(StoreFlusher.class);
  protected Configuration conf;
  protected Store store;
  private long mobCellValueSizeThreshold = 0;
  private Path targetPath;

  public StoreFlusher(Configuration conf, Store store) throws IOException {
    this.conf = conf;
    this.store = store;
    if(store.getFamily().isMobEnabled()) {
      this.mobCellValueSizeThreshold = store.getFamily().getMobThreshold();
      this.targetPath = MobUtils.getMobFamilyPath(conf, store.getTableName(),
          store.getColumnFamilyName());
      if (!this.store.getFileSystem().exists(targetPath)) {
        this.store.getFileSystem().mkdirs(targetPath);
      }
    }
  }

  /**
   * Turns a snapshot of memstore into a set of store files.
   * @param snapshot Memstore snapshot.
   * @param cacheFlushSeqNum Log cache flush sequence number.
   * @param status Task that represents the flush operation and may be updated with status.
   * @return List of files written. Can be empty; must not be null.
   */
  public abstract List<Path> flushSnapshot(MemStoreSnapshot snapshot, long cacheFlushSeqNum,
      MonitoredTask status) throws IOException;

  protected void finalizeWriter(StoreFile.Writer writer, long cacheFlushSeqNum,
      MonitoredTask status) throws IOException {
    // Write out the log sequence number that corresponds to this output
    // hfile. Also write current time in metadata as minFlushTime.
    // The hfile is current up to and including cacheFlushSeqNum.
    status.setStatus("Flushing " + store + ": appending metadata");
    writer.appendMetadata(cacheFlushSeqNum, false);
    status.setStatus("Flushing " + store + ": closing flushed file");
    writer.close();
  }


  /**
   * Creates the scanner for flushing snapshot. Also calls coprocessors.
   * @param snapshotScanners
   * @param smallestReadPoint
   * @return The scanner; null if coprocessor is canceling the flush.
   */
  protected InternalScanner createScanner(List<KeyValueScanner> snapshotScanners,
      long smallestReadPoint) throws IOException {
    InternalScanner scanner = null;
    if (store.getCoprocessorHost() != null) {
      scanner = store.getCoprocessorHost().preFlushScannerOpen(store, snapshotScanners,
          smallestReadPoint);
    }
    if (scanner == null) {
      Scan scan = new Scan();
      scan.setMaxVersions(store.getScanInfo().getMaxVersions());
      scanner = new StoreScanner(store, store.getScanInfo(), scan,
          snapshotScanners, ScanType.COMPACT_RETAIN_DELETES,
          smallestReadPoint, HConstants.OLDEST_TIMESTAMP);
    }
    assert scanner != null;
    if (store.getCoprocessorHost() != null) {
      try {
        return store.getCoprocessorHost().preFlush(store, scanner);
      } catch (IOException ioe) {
        scanner.close();
        throw ioe;
      }
    }
    return scanner;
  }

  /**
   * Performs memstore flush, writing data from scanner into sink.
   * @param scanner Scanner to get data from.
   * @param sink Sink to write data to. Could be StoreFile.Writer.
   * @param smallestReadPoint Smallest read point used for the flush.
   */
  protected void performFlush(InternalScanner scanner,
      CellSink sink, long smallestReadPoint) throws IOException {
    int compactionKVMax =
      conf.getInt(HConstants.COMPACTION_KV_MAX, HConstants.COMPACTION_KV_MAX_DEFAULT);

    ScannerContext scannerContext =
        ScannerContext.newBuilder().setBatchLimit(compactionKVMax).build();

    List<Cell> kvs = new ArrayList<Cell>();
    boolean hasMore;
    do {
      hasMore = scanner.next(kvs, scannerContext);
      if (!kvs.isEmpty()) {
        for (Cell c : kvs) {
          // If we know that this KV is going to be included always, then let us
          // set its memstoreTS to 0. This will help us save space when writing to
          // disk.
          sink.append(c);
        }
        kvs.clear();
      }
    } while (hasMore);
  }

  /**
   * Flushes the cells in the mob store.
   * <ol>In the mob store, the cells with PUT type might have or have no mob tags.
   * <li>If a cell does not have a mob tag, flushing the cell to different files depends
   * on the value length. If the length is larger than a threshold, it's flushed to a
   * mob file and the mob file is flushed to a store file in HBase. Otherwise, directly
   * flush the cell to a store file in HBase.</li>
   * <li>If a cell have a mob tag, its value is a mob file name, directly flush it
   * to a store file in HBase.</li>
   * </ol>
   * @param snapshot Memstore snapshot.
   * @param cacheFlushId Log cache flush sequence number.
   * @param scanner The scanner of memstore snapshot.
   * @param writer The store file writer.
   * @param status Task that represents the flush operation and may be updated with status.
   * @throws IOException
   */
  protected void performMobFlush(MemStoreSnapshot snapshot, long cacheFlushId,
      InternalScanner scanner, CellSink writer, MonitoredTask status) throws IOException {
    StoreFile.Writer mobFileWriter = null;
    int compactionKVMax = conf.getInt(HConstants.COMPACTION_KV_MAX,
        HConstants.COMPACTION_KV_MAX_DEFAULT);
    long mobCount = 0;
    long mobSize = 0;
    Date date = new Date(snapshot.getTimeRangeTracker().getMax());
    if (!(store instanceof HMobStore)) {
      throw new IllegalArgumentException("The store " + store + " is not a HMobStore");
    }

    mobFileWriter = ((HMobStore)store).createWriterInTmp(date, snapshot.getCellsCount(),
        store.getFamily().getCompression(), store.getRegionInfo().getStartKey());
    // the target path is {tableName}/.mob/{cfName}/mobFiles
    // the relative path is mobFiles
    byte[] fileName = Bytes.toBytes(mobFileWriter.getPath().getName());
    try {
      List<Cell> cells = new ArrayList<Cell>();
      boolean hasMore;
      ScannerContext scannerContext =
              ScannerContext.newBuilder().setBatchLimit(compactionKVMax).build();
      do {
        hasMore = scanner.next(cells, scannerContext);
        if (!cells.isEmpty()) {
          for (Cell c : cells) {
            // If we know that this KV is going to be included always, then let us
            // set its memstoreTS to 0. This will help us save space when writing to
            // disk.
            if (c.getValueLength() <= mobCellValueSizeThreshold || MobUtils.isMobReferenceCell(c)
                || c.getTypeByte() != KeyValue.Type.Put.getCode()) {
              writer.append(c);
            } else {
              // append the original keyValue in the mob file.
              mobFileWriter.append(c);
              mobSize += c.getValueLength();
              mobCount++;

              // append the tags to the KeyValue.
              // The key is same, the value is the filename of the mob file
              Cell reference = MobUtils.createMobRefCell(c, fileName,
                  ((HMobStore)store).getRefCellTags());
              writer.append(reference);
            }
          }
          cells.clear();
        }
      } while (hasMore);
    } finally {
      status.setStatus("Flushing mob file " + store + ": appending metadata");
      mobFileWriter.appendMetadata(cacheFlushId, false, mobCount);
      status.setStatus("Flushing mob file " + store + ": closing flushed file");
      mobFileWriter.close();
    }

    if (mobCount > 0) {
      // commit the mob file from temp folder to target folder.
      // If the mob file is committed successfully but the store file is not,
      // the committed mob file will be handled by the sweep tool as an unused
      // file.
      ((HMobStore)store).commitFile(mobFileWriter.getPath(), new Path(targetPath,
          MobUtils.formatDate(date)));
      ((HMobStore)store).updateMobFlushCount();
      ((HMobStore)store).updateMobFlushedCellsCount(mobCount);
      ((HMobStore)store).updateMobFlushedCellsSize(mobSize);
    } else {
      try {
        // If the mob file is empty, delete it instead of committing.
        store.getFileSystem().delete(mobFileWriter.getPath(), true);
      } catch (IOException e) {
        LOG.error("Failed to delete the temp mob file", e);
      }
    }
  }
}
