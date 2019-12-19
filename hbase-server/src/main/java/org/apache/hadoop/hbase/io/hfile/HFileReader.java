/*
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
package org.apache.hadoop.hbase.io.hfile;

import java.io.Closeable;
import java.io.DataInput;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.shaded.com.google.common.annotations.VisibleForTesting;

/**
 * An interface used by clients to open and iterate an {@link HFile}.
 */
@InterfaceAudience.Private
public interface HFileReader extends Closeable, CachingBlockReader {
  /**
   * Returns this reader's "name". Usually the last component of the path.
   * Needs to be constant as the file is being moved to support caching on
   * write.
   */
  String getName();

  CellComparator getComparator();

  HFileScanner getScanner(boolean cacheBlocks, final boolean pread, final boolean isCompaction);

  HFileBlock getMetaBlock(String metaBlockName, boolean cacheBlock) throws IOException;

  Cell getLastKey();

  Cell midkey() throws IOException;

  long length();

  long getEntries();

  Cell getFirstKey();

  long indexSize();

  byte[] getFirstRowKey();

  byte[] getLastRowKey();

  FixedFileTrailer getTrailer();

  void setDataBlockIndexReader(HFileBlockIndex.CellBasedKeyBlockIndexReader reader);
  HFileBlockIndex.CellBasedKeyBlockIndexReader getDataBlockIndexReader();

  void setMetaBlockIndexReader(HFileBlockIndex.ByteArrayKeyBlockIndexReader reader);
  HFileBlockIndex.ByteArrayKeyBlockIndexReader getMetaBlockIndexReader();

  HFileScanner getScanner(boolean cacheBlocks, boolean pread);

  /**
   * Retrieves general Bloom filter metadata as appropriate for each
   * {@link HFile} version.
   * Knows nothing about how that metadata is structured.
   */
  DataInput getGeneralBloomFilterMetadata() throws IOException;

  /**
   * Retrieves delete family Bloom filter metadata as appropriate for each
   * {@link HFile}  version.
   * Knows nothing about how that metadata is structured.
   */
  DataInput getDeleteBloomFilterMetadata() throws IOException;

  Path getPath();

  /** Close method with optional evictOnClose */
  void close(boolean evictOnClose) throws IOException;

  DataBlockEncoding getDataBlockEncoding();

  boolean hasMVCCInfo();

  /**
   * Return the file context of the HFile this reader belongs to
   */
  HFileContext getFileContext();

  DataBlockEncoding getEffectiveEncodingInCache(boolean isCompaction);

  @VisibleForTesting
  HFileBlock.FSReader getUncachedBlockReader();

  @VisibleForTesting
  boolean prefetchComplete();

  boolean isPrimaryReplicaReader();

  /**
   * To close the stream's socket. Note: This can be concurrently called from multiple threads and
   * implementation should take care of thread safety.
   */
  void unbufferStream();

  ReaderContext getContext();
  HFileInfo getHFileInfo();
  void setDataBlockEncoder(HFileDataBlockEncoder dataBlockEncoder);

  @SuppressWarnings("serial")
  public static class BlockIndexNotLoadedException extends IllegalStateException {
    public BlockIndexNotLoadedException() {
      // Add a message in case anyone relies on it as opposed to class name.
      super("Block index not loaded");
    }
  }

  /**
   * An exception thrown when an operation requiring a scanner to be seeked
   * is invoked on a scanner that is not seeked.
   */
  @SuppressWarnings("serial")
  public static class NotSeekedException extends IllegalStateException {
    public NotSeekedException() {
      super("Not seeked to a key/value");
    }
  }
}
