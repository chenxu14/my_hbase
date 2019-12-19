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
package org.apache.hadoop.hbase.io.hfile;

import java.io.DataInput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.shaded.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.IdLock;
import org.apache.htrace.core.TraceScope;

/**
 * Implementation that can handle all hfile versions of {@link HFileReader}.
 */
@InterfaceAudience.Private
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value="URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
public abstract class HFileReaderImpl implements HFileReader, Configurable {
  // This class is HFileReaderV3 + HFileReaderV2 + AbstractHFileReader all squashed together into
  // one file.  Ditto for all the HFileReader.ScannerV? implementations. I was running up against
  // the MaxInlineLevel limit because too many tiers involved reading from an hfile. Was also hard
  // to navigate the source code when so many classes participating in read.
  private static final Log LOG = LogFactory.getLog(HFileReaderImpl.class);

  /** Data block index reader keeping the root data index in memory */
  protected HFileBlockIndex.CellBasedKeyBlockIndexReader dataBlockIndexReader;

  /** Meta block index reader -- always single level */
  protected HFileBlockIndex.ByteArrayKeyBlockIndexReader metaBlockIndexReader;

  protected FixedFileTrailer trailer;

  private final boolean primaryReplicaReader;

  /**
   * What kind of data block encoding should be used while reading, writing,
   * and handling cache.
   */
  protected HFileDataBlockEncoder dataBlockEncoder = NoOpDataBlockEncoder.INSTANCE;

  /** Key comparator */
  protected CellComparator comparator = CellComparator.COMPARATOR;

  /** Block cache configuration. */
  protected final CacheConfig cacheConf;

  protected ReaderContext context;

  protected final HFileInfo fileInfo;

  /** Path of file */
  protected final Path path;

  /** File name to be used for block names */
  protected final String name;

  private Configuration conf;

  protected HFileContext hfileContext;

  /** Filesystem-level block reader. */
  protected HFileBlock.FSReader fsBlockReader;

  /**
   * A "sparse lock" implementation allowing to lock on a particular block
   * identified by offset. The purpose of this is to avoid two clients loading
   * the same block, and have all but one client wait to get the block from the
   * cache.
   */
  private IdLock offsetLock = new IdLock();

  /** Minimum minor version supported by this HFile format */
  static final int MIN_MINOR_VERSION = 0;

  /** Maximum minor version supported by this HFile format */
  // We went to version 2 when we moved to pb'ing fileinfo and the trailer on
  // the file. This version can read Writables version 1.
  static final int MAX_MINOR_VERSION = 3;

  /** Minor versions starting with this number have faked index key */
  static final int MINOR_VERSION_WITH_FAKED_KEY = 3;

  /**
   * Opens a HFile.
   * @param context Reader context info
   * @param fileInfo HFile info
   * @param cacheConf Cache configuration.
   * @param conf Configuration
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
  public HFileReaderImpl(ReaderContext context, HFileInfo fileInfo, CacheConfig cacheConf, Configuration conf)
      throws IOException {
    this.cacheConf = cacheConf;
    this.context = context;
    this.path = context.getFilePath();
    this.name = path.getName();
    this.conf = conf;
    this.primaryReplicaReader = context.isPrimaryReplicaReader();
    this.fileInfo = fileInfo;
    this.trailer = fileInfo.getTrailer();
    // Comparator class name is stored in the trailer in version 2.
    this.comparator = trailer.createComparator();
    this.hfileContext = fileInfo.getHFileContext();
    this.fsBlockReader = new HFileBlock.FSReaderImpl(context, hfileContext,
        cacheConf.getByteBuffAllocator());
    this.dataBlockEncoder = HFileDataBlockEncoderImpl.createFromFileInfo(fileInfo);
    fsBlockReader.setDataBlockEncoder(dataBlockEncoder);
    dataBlockIndexReader = fileInfo.getDataBlockIndexReader();
    metaBlockIndexReader = fileInfo.getMetaBlockIndexReader();
  }

  private String toStringFirstKey() {
    if(getFirstKey() == null)
      return null;
    return CellUtil.getCellKeyAsString(getFirstKey());
  }

  private String toStringLastKey() {
    return CellUtil.toString(getLastKey(), false);
  }

  @Override
  public String toString() {
    return "reader=" + path.toString() +
        (!isFileInfoLoaded()? "":
          ", compression=" + trailer.getCompressionCodec().getName() +
          ", cacheConf=" + cacheConf +
          ", firstKey=" + toStringFirstKey() +
          ", lastKey=" + toStringLastKey()) +
          ", avgKeyLen=" + fileInfo.getAvgKeyLen() +
          ", avgValueLen=" + fileInfo.getAvgValueLen() +
          ", entries=" + trailer.getEntryCount() +
          ", length=" + context.getFileSize();
  }

  @Override
  public long length() {
    return context.getFileSize();
  }

  /**
   * @return the first key in the file. May be null if file has no entries. Note
   *         that this is not the first row key, but rather the byte form of the
   *         first KeyValue.
   */
  @Override
  public Cell getFirstKey() {
    if (dataBlockIndexReader == null) {
      throw new BlockIndexNotLoadedException();
    }
    return dataBlockIndexReader.isEmpty() ? null
        : dataBlockIndexReader.getRootBlockKey(0);
  }

  /**
   * TODO left from {@link HFile} version 1: move this to StoreFile after Ryan's
   * patch goes in to eliminate {@link KeyValue} here.
   *
   * @return the first row key, or null if the file is empty.
   */
  @Override
  public byte[] getFirstRowKey() {
    Cell firstKey = getFirstKey();
    // We have to copy the row part to form the row key alone
    return firstKey == null? null: CellUtil.cloneRow(firstKey);
  }

  /**
   * TODO left from {@link HFile} version 1: move this to StoreFile after
   * Ryan's patch goes in to eliminate {@link KeyValue} here.
   *
   * @return the last row key, or null if the file is empty.
   */
  @Override
  public byte[] getLastRowKey() {
    Cell lastKey = getLastKey();
    return lastKey == null? null: CellUtil.cloneRow(lastKey);
  }

  /** @return number of KV entries in this HFile */
  @Override
  public long getEntries() {
    return trailer.getEntryCount();
  }

  /** @return comparator */
  @Override
  public CellComparator getComparator() {
    return comparator;
  }

  @VisibleForTesting
  public Compression.Algorithm getCompressionAlgorithm() {
    return trailer.getCompressionCodec();
  }

  /**
   * @return the total heap size of data and meta block indexes in bytes. Does
   *         not take into account non-root blocks of a multilevel data index.
   */
  public long indexSize() {
    return (dataBlockIndexReader != null ? dataBlockIndexReader.heapSize() : 0)
        + ((metaBlockIndexReader != null) ? metaBlockIndexReader.heapSize()
            : 0);
  }

  @Override
  public String getName() {
    return name;
  }

  public void setDataBlockEncoder(HFileDataBlockEncoder dataBlockEncoder) {
    this.dataBlockEncoder = dataBlockEncoder;
    this.fsBlockReader.setDataBlockEncoder(dataBlockEncoder);
  }

  @Override
  public void setDataBlockIndexReader(HFileBlockIndex.CellBasedKeyBlockIndexReader reader) {
    this.dataBlockIndexReader = reader;
  }

  @Override
  public HFileBlockIndex.CellBasedKeyBlockIndexReader getDataBlockIndexReader() {
    return dataBlockIndexReader;
  }

  @Override
  public void setMetaBlockIndexReader(HFileBlockIndex.ByteArrayKeyBlockIndexReader reader) {
    this.metaBlockIndexReader = reader;
  }

  @Override
  public HFileBlockIndex.ByteArrayKeyBlockIndexReader getMetaBlockIndexReader() {
    return metaBlockIndexReader;
  }

  @Override
  public FixedFileTrailer getTrailer() {
    return trailer;
  }

  @Override
  public ReaderContext getContext() {
    return this.context;
  }

  @Override
  public HFileInfo getHFileInfo() {
    return this.fileInfo;
  }

  @Override
  public boolean isPrimaryReplicaReader() {
    return primaryReplicaReader;
  }

  public Path getPath() {
    return path;
  }

  @Override
  public DataBlockEncoding getDataBlockEncoding() {
    return dataBlockEncoder.getDataBlockEncoding();
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  /** Minor versions in HFile starting with this number have hbase checksums */
  public static final int MINOR_VERSION_WITH_CHECKSUM = 1;
  /** In HFile minor version that does not support checksums */
  public static final int MINOR_VERSION_NO_CHECKSUM = 0;

  /** HFile minor version that introduced pbuf filetrailer */
  public static final int PBUF_TRAILER_MINOR_VERSION = 2;

  /**
   * Retrieve block from cache. Validates the retrieved block's type vs {@code expectedBlockType}
   * and its encoding vs. {@code expectedDataBlockEncoding}. Unpacks the block as necessary.
   */
  private HFileBlock getCachedBlock(BlockCacheKey cacheKey, boolean cacheBlock, boolean useLock,
      boolean isCompaction, boolean updateCacheMetrics, BlockType expectedBlockType,
      DataBlockEncoding expectedDataBlockEncoding) throws IOException {
    // Check cache for block. If found return.
    if (cacheConf.isBlockCacheEnabled()) {
      BlockCache cache = cacheConf.getBlockCache();
      HFileBlock cachedBlock = (HFileBlock) cache.getBlock(cacheKey, cacheBlock, useLock,
        updateCacheMetrics);
      if (cachedBlock != null) {
        if (cacheConf.shouldCacheCompressed(cachedBlock.getBlockType().getCategory())) {
          HFileBlock compressedBlock = cachedBlock;
          cachedBlock = compressedBlock.unpack(hfileContext, fsBlockReader);
          // In case of compressed block after unpacking we can release the compressed block
          if (compressedBlock != cachedBlock) {
            compressedBlock.release();
          }
        }
        try {
         validateBlockType(cachedBlock, expectedBlockType);
        } catch (IOException e) {
          returnAndEvictBlock(cache, cacheKey, cachedBlock);
          throw e;
        }

        if (expectedDataBlockEncoding == null) {
          return cachedBlock;
        }
        DataBlockEncoding actualDataBlockEncoding =
                cachedBlock.getDataBlockEncoding();
        // Block types other than data blocks always have
        // DataBlockEncoding.NONE. To avoid false negative cache misses, only
        // perform this check if cached block is a data block.
        if (cachedBlock.getBlockType().isData() &&
                !actualDataBlockEncoding.equals(expectedDataBlockEncoding)) {
          // This mismatch may happen if a Scanner, which is used for say a
          // compaction, tries to read an encoded block from the block cache.
          // The reverse might happen when an EncodedScanner tries to read
          // un-encoded blocks which were cached earlier.
          //
          // Because returning a data block with an implicit BlockType mismatch
          // will cause the requesting scanner to throw a disk read should be
          // forced here. This will potentially cause a significant number of
          // cache misses, so update so we should keep track of this as it might
          // justify the work on a CompoundScanner.
          if (!expectedDataBlockEncoding.equals(DataBlockEncoding.NONE) &&
                  !actualDataBlockEncoding.equals(DataBlockEncoding.NONE)) {
            // If the block is encoded but the encoding does not match the
            // expected encoding it is likely the encoding was changed but the
            // block was not yet evicted. Evictions on file close happen async
            // so blocks with the old encoding still linger in cache for some
            // period of time. This event should be rare as it only happens on
            // schema definition change.
            LOG.info("Evicting cached block with key " + cacheKey
               + " because of a data block encoding mismatch" + "; expected: "
               + expectedDataBlockEncoding + ", actual: " + actualDataBlockEncoding);
            // This is an error scenario. so here we need to release the block.
            returnAndEvictBlock(cache, cacheKey, cachedBlock);
          }
          return null;
        }
        return cachedBlock;
      }
    }
    return null;
  }

  private void returnAndEvictBlock(BlockCache cache, BlockCacheKey cacheKey, Cacheable block) {
    block.release();
    cache.evictBlock(cacheKey);
  }

  /**
   * @param metaBlockName
   * @param cacheBlock Add block to cache, if found
   * @return block wrapped in a ByteBuffer, with header skipped
   * @throws IOException
   */
  @Override
  public HFileBlock getMetaBlock(String metaBlockName, boolean cacheBlock)
      throws IOException {
    if (trailer.getMetaIndexCount() == 0) {
      return null; // there are no meta blocks
    }
    if (metaBlockIndexReader == null) {
      throw new IOException("Meta index not loaded");
    }

    byte[] mbname = Bytes.toBytes(metaBlockName);
    int block = metaBlockIndexReader.rootBlockContainingKey(mbname,
        0, mbname.length);
    if (block == -1)
      return null;
    long blockSize = metaBlockIndexReader.getRootBlockDataSize(block);

    // Per meta key from any given file, synchronize reads for said block. This
    // is OK to do for meta blocks because the meta block index is always
    // single-level.
    synchronized (metaBlockIndexReader.getRootBlockKey(block)) {
      // Check cache for block. If found return.
      long metaBlockOffset = metaBlockIndexReader.getRootBlockOffset(block);
      BlockCacheKey cacheKey = new BlockCacheKey(name, metaBlockOffset,
        this.isPrimaryReplicaReader());

      cacheBlock &= cacheConf.shouldCacheBlockOnRead(BlockType.META.getCategory());
      if (cacheConf.isBlockCacheEnabled()) {
        HFileBlock cachedBlock = getCachedBlock(cacheKey, cacheBlock, false, true, true,
          BlockType.META, null);
        if (cachedBlock != null) {
          assert cachedBlock.isUnpacked() : "Packed block leak.";
          // Return a distinct 'shallow copy' of the block,
          // so pos does not get messed by the scanner
          return cachedBlock;
        }
        // Cache Miss, please load.
      }

      HFileBlock compressedBlock =
          fsBlockReader.readBlockData(metaBlockOffset, blockSize, true, false, shouldUseHeap(BlockType.META));
      HFileBlock uncompressedBlock = compressedBlock.unpack(hfileContext, fsBlockReader);
      if (compressedBlock != uncompressedBlock) {
        compressedBlock.release();
      }

      // Cache the block
      if (cacheBlock) {
        cacheConf.getBlockCache().cacheBlock(cacheKey, uncompressedBlock,
            cacheConf.isInMemory(), this.cacheConf.isCacheDataInL1());
      }

      return uncompressedBlock;
    }
  }

  /**
   * If expected block is data block, we'll allocate the ByteBuff of block from
   * {@link org.apache.hadoop.hbase.io.ByteBuffAllocator} and it's usually an off-heap one,
   * otherwise it will allocate from heap.
   * @see org.apache.hadoop.hbase.io.hfile.HFileBlock.FSReader#readBlockData(long, long, boolean,
   *      boolean, boolean)
   */
  private boolean shouldUseHeap(BlockType expectedBlockType) {
    if (cacheConf.getBlockCache() == null || cacheConf.isCompositeBucketCache()) {
      return false;
    } else if (!cacheConf.isCombinedBlockCache()) {
      // Block to cache in LruBlockCache must be an heap one. So just allocate block memory from
      // heap for saving an extra off-heap to heap copying.
      return true;
    }
    return expectedBlockType != null && !expectedBlockType.isData();
  }

  @Override
  public HFileBlock readBlock(long dataBlockOffset, long onDiskBlockSize,
      final boolean cacheBlock, boolean pread, final boolean isCompaction,
      boolean updateCacheMetrics, BlockType expectedBlockType,
      DataBlockEncoding expectedDataBlockEncoding)
      throws IOException {
    if (dataBlockIndexReader == null) {
      throw new IOException("Block index not loaded");
    }
    long trailerOffset = trailer.getLoadOnOpenDataOffset();
    if (dataBlockOffset < 0 || dataBlockOffset >= trailerOffset) {
      throw new IOException("Requested block is out of range: " + dataBlockOffset +
        ", lastDataBlockOffset: " + trailer.getLastDataBlockOffset() +
        ", trailer.getLoadOnOpenDataOffset: " + trailerOffset);
    }
    // For any given block from any given file, synchronize reads for said
    // block.
    // Without a cache, this synchronizing is needless overhead, but really
    // the other choice is to duplicate work (which the cache would prevent you
    // from doing).

    BlockCacheKey cacheKey = new BlockCacheKey(name, dataBlockOffset,
      this.isPrimaryReplicaReader());

    boolean useLock = false;
    IdLock.Entry lockEntry = null;
    try(TraceScope traceScope = TraceUtil.createTrace("HFileReaderImpl.readBlock")) {
      while (true) {
        // Check cache for block. If found return.
        if (cacheConf.shouldReadBlockFromCache(expectedBlockType)) {
          if (useLock) {
            lockEntry = offsetLock.getLockEntry(dataBlockOffset);
          }
          // Try and get the block from the block cache. If the useLock variable is true then this
          // is the second time through the loop and it should not be counted as a block cache miss.
          HFileBlock cachedBlock = getCachedBlock(cacheKey, cacheBlock, useLock, isCompaction,
            updateCacheMetrics, expectedBlockType, expectedDataBlockEncoding);
          if (cachedBlock != null) {
            if (LOG.isTraceEnabled()) {
              LOG.trace("From Cache " + cachedBlock);
            }
            TraceUtil.addTimelineAnnotation("blockCacheHit");
            assert cachedBlock.isUnpacked() : "Packed block leak.";
            if (cachedBlock.getBlockType().isData()) {
              if (updateCacheMetrics) {
                HFile.DATABLOCK_READ_COUNT.increment();
              }
              // Validate encoding type for data blocks. We include encoding
              // type in the cache key, and we expect it to match on a cache hit.
              if (cachedBlock.getDataBlockEncoding() != dataBlockEncoder.getDataBlockEncoding()) {
                // Remember to release the block when in exceptional path.
                if (cacheConf.getBlockCache() != null) {
                  returnAndEvictBlock(cacheConf.getBlockCache(), cacheKey, cachedBlock);
                }
                throw new IOException("Cached block under key " + cacheKey + " "
                    + "has wrong encoding: " + cachedBlock.getDataBlockEncoding() + " (expected: "
                    + dataBlockEncoder.getDataBlockEncoding() + ")");
              }
            }
            // Cache-hit. Return!
            return cachedBlock;
          }
          if (!useLock && cacheBlock && cacheConf.shouldLockOnCacheMiss(expectedBlockType)) {
            // check cache again with lock
            useLock = true;
            continue;
          }
          // Carry on, please load.
        }
        TraceUtil.addTimelineAnnotation("blockCacheMiss");
        // Load block from filesystem.
        HFileBlock hfileBlock = fsBlockReader.readBlockData(dataBlockOffset, onDiskBlockSize,
            pread, !isCompaction, shouldUseHeap(expectedBlockType));
        validateBlockType(hfileBlock, expectedBlockType);
        HFileBlock unpacked = hfileBlock.unpack(hfileContext, fsBlockReader);
        BlockType.BlockCategory category = hfileBlock.getBlockType().getCategory();

        // Cache the block if necessary
        if (cacheBlock && cacheConf.shouldCacheBlockOnRead(category)) {
          cacheConf.getBlockCache().cacheBlock(cacheKey,
            cacheConf.shouldCacheCompressed(category) ? hfileBlock : unpacked,
            cacheConf.isInMemory(), this.cacheConf.isCacheDataInL1());
        }

        if (unpacked != hfileBlock) {
          // End of life here if hfileBlock is an independent block.
          hfileBlock.release();
        }

        if (updateCacheMetrics && hfileBlock.getBlockType().isData()) {
          HFile.DATABLOCK_READ_COUNT.increment();
        }

        return unpacked;
      }
    } finally {
      if (lockEntry != null) {
        offsetLock.releaseLockEntry(lockEntry);
      }
    }
  }

  @Override
  public boolean hasMVCCInfo() {
    return fileInfo.shouldIncludeMemStoreTS() && fileInfo.isDecodeMemstoreTS();
  }

  /**
   * Compares the actual type of a block retrieved from cache or disk with its
   * expected type and throws an exception in case of a mismatch. Expected
   * block type of {@link BlockType#DATA} is considered to match the actual
   * block type [@link {@link BlockType#ENCODED_DATA} as well.
   * @param block a block retrieved from cache or disk
   * @param expectedBlockType the expected block type, or null to skip the
   *          check
   */
  private void validateBlockType(HFileBlock block,
      BlockType expectedBlockType) throws IOException {
    if (expectedBlockType == null) {
      return;
    }
    BlockType actualBlockType = block.getBlockType();
    if (expectedBlockType.isData() && actualBlockType.isData()) {
      // We consider DATA to match ENCODED_DATA for the purpose of this
      // verification.
      return;
    }
    if (actualBlockType != expectedBlockType) {
      throw new IOException("Expected block type " + expectedBlockType + ", " +
          "but got " + actualBlockType + ": " + block);
    }
  }

  /**
   * @return Last key in the file. May be null if file has no entries. Note that
   *         this is not the last row key, but rather the byte form of the last
   *         KeyValue.
   */
  @Override
  public Cell getLastKey() {
    return dataBlockIndexReader.isEmpty() ? null : fileInfo.getLastKeyCell();
  }

  /**
   * @return Midkey for this file. We work with block boundaries only so
   *         returned midkey is an approximation only.
   * @throws IOException
   */
  @Override
  public Cell midkey() throws IOException {
    return dataBlockIndexReader.midkey(this);
  }

  @Override
  public void close() throws IOException {
    close(cacheConf.shouldEvictOnClose());
  }

  public DataBlockEncoding getEffectiveEncodingInCache(boolean isCompaction) {
    return dataBlockEncoder.getEffectiveEncodingInCache(isCompaction);
  }

  /** For testing */
  public HFileBlock.FSReader getUncachedBlockReader() {
    return fsBlockReader;
  }

  /**
   * Returns a buffer with the Bloom filter metadata. The caller takes
   * ownership of the buffer.
   */
  @Override
  public DataInput getGeneralBloomFilterMetadata() throws IOException {
    return this.getBloomFilterMetadata(BlockType.GENERAL_BLOOM_META);
  }

  @Override
  public DataInput getDeleteBloomFilterMetadata() throws IOException {
    return this.getBloomFilterMetadata(BlockType.DELETE_FAMILY_BLOOM_META);
  }

  private DataInput getBloomFilterMetadata(BlockType blockType)
  throws IOException {
    if (blockType != BlockType.GENERAL_BLOOM_META &&
        blockType != BlockType.DELETE_FAMILY_BLOOM_META) {
      throw new RuntimeException("Block Type: " + blockType.toString() +
          " is not supported") ;
    }

    for (HFileBlock b : fileInfo.getLoadOnOpenBlocks()) {
      if (b.getBlockType() == blockType) {
        return b.getByteStream();
      }
    }
    return null;
  }

  public boolean isFileInfoLoaded() {
    return true; // We load file info in constructor in version 2.
  }

  @Override
  public HFileContext getFileContext() {
    return hfileContext;
  }

  /**
   * Returns false if block prefetching was requested for this file and has
   * not completed, true otherwise
   */
  @VisibleForTesting
  public boolean prefetchComplete() {
    return PrefetchExecutor.isCompleted(path);
  }

  /**
   * Create a Scanner on this file. No seeks or reads are done on creation. Call
   * {@link HFileScanner#seekTo(Cell)} to position an start the read. There is
   * nothing to clean up in a Scanner. Letting go of your references to the
   * scanner is sufficient. NOTE: Do not use this overload of getScanner for
   * compactions. See {@link #getScanner(boolean, boolean, boolean)}
   *
   * @param cacheBlocks True if we should cache blocks read in by this scanner.
   * @param pread Use positional read rather than seek+read if true (pread is
   *          better for random reads, seek+read is better scanning).
   * @return Scanner on this file.
   */
  @Override
  @VisibleForTesting
  public HFileScanner getScanner(boolean cacheBlocks, final boolean pread) {
    return getScanner(cacheBlocks, pread, false);
  }

  /**
   * Create a Scanner on this file. No seeks or reads are done on creation. Call
   * {@link HFileScanner#seekTo(Cell)} to position an start the read. There is
   * nothing to clean up in a Scanner. Letting go of your references to the
   * scanner is sufficient.
   * @param cacheBlocks
   *          True if we should cache blocks read in by this scanner.
   * @param pread
   *          Use positional read rather than seek+read if true (pread is better
   *          for random reads, seek+read is better scanning).
   * @param isCompaction
   *          is scanner being used for a compaction?
   * @return Scanner on this file.
   */
  @Override
  public HFileScanner getScanner(boolean cacheBlocks, final boolean pread,
      final boolean isCompaction) {
    if (dataBlockEncoder.useEncodedScanner()) {
      return EncodedScanner.newInstance(this, cacheBlocks, pread, isCompaction, this.hfileContext);
    }
    return HFileScannerImpl.newInstance(this, cacheBlocks, pread, isCompaction);
  }

  public int getMajorVersion() {
    return 3;
  }

  @Override
  public void unbufferStream() {
    fsBlockReader.unbufferStream();
  }
}
