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
package org.apache.hadoop.hbase.io.hfile;

import static org.apache.hadoop.hbase.HConstants.BUCKET_CACHE_IOENGINE_KEY;
import static org.apache.hadoop.hbase.HConstants.BUCKET_CACHE_SIZE_KEY;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.util.concurrent.ForkJoinPool;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.io.hfile.BlockCache.CacheLevel;
import org.apache.hadoop.hbase.io.hfile.BlockType.BlockCategory;
import org.apache.hadoop.hbase.io.hfile.bucket.BucketCache;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

import org.apache.hadoop.hbase.shaded.com.google.common.annotations.VisibleForTesting;

/**
 * Stores all of the cache objects and configuration for a single HFile.
 */
@InterfaceAudience.Private
public class CacheConfig {
  private static final Log LOG = LogFactory.getLog(CacheConfig.class.getName());

  /**
   * Disabled cache configuration
   */
  public static final CacheConfig DISABLED = new CacheConfig();

  /**
   * Configuration key to cache data blocks on read. Bloom blocks and index blocks are always be
   * cached if the block cache is enabled.
   */
  public static final String CACHE_DATA_ON_READ_KEY = "hbase.block.data.cacheonread";

  /**
   * Configuration key to cache data blocks on write. There are separate
   * switches for bloom blocks and non-root index blocks.
   */
  public static final String CACHE_BLOCKS_ON_WRITE_KEY =
      "hbase.rs.cacheblocksonwrite";

  /**
   * Configuration key to cache leaf and intermediate-level index blocks on
   * write.
   */
  public static final String CACHE_INDEX_BLOCKS_ON_WRITE_KEY =
      "hfile.block.index.cacheonwrite";

  /**
   * Configuration key to cache compound bloom filter blocks on write.
   */
  public static final String CACHE_BLOOM_BLOCKS_ON_WRITE_KEY =
      "hfile.block.bloom.cacheonwrite";

  /**
   * Configuration key to cache data blocks in compressed and/or encrypted format.
   */
  public static final String CACHE_DATA_BLOCKS_COMPRESSED_KEY =
      "hbase.block.data.cachecompressed";

  /**
   * Configuration key to evict all blocks of a given file from the block cache
   * when the file is closed.
   */
  public static final String EVICT_BLOCKS_ON_CLOSE_KEY =
      "hbase.rs.evictblocksonclose";

  /**
   * Configuration key to cache block policy (Lru, TinyLfu).
   */
  public static final String BLOCKCACHE_POLICY_KEY = "hfile.block.cache.policy";
  public static final String BLOCKCACHE_POLICY_DEFAULT = "LRU";
  
  /**
   * If the chosen ioengine can persist its state across restarts, the path to the file to persist
   * to. This file is NOT the data file. It is a file into which we will serialize the map of
   * what is in the data file. For example, if you pass the following argument as
   * BUCKET_CACHE_IOENGINE_KEY ("hbase.bucketcache.ioengine"),
   * <code>file:/tmp/bucketcache.data </code>, then we will write the bucketcache data to the file
   * <code>/tmp/bucketcache.data</code> but the metadata on where the data is in the supplied file
   * is an in-memory map that needs to be persisted across restarts. Where to store this
   * in-memory state is what you supply here: e.g. <code>/tmp/bucketcache.map</code>.
   */
  public static final String BUCKET_CACHE_PERSISTENT_PATH_KEY = 
      "hbase.bucketcache.persistent.path";

  /**
   * If the bucket cache is used in league with the lru on-heap block cache (meta blocks such
   * as indices and blooms are kept in the lru blockcache and the data blocks in the
   * bucket cache).
   */
  public static final String BUCKET_CACHE_COMBINED_KEY = 
      "hbase.bucketcache.combinedcache.enabled";

  public static final String BUCKET_CACHE_COMPOSITE_KEY = 
      "hbase.bucketcache.composite.enabled";

  public static final String BUCKET_CACHE_WRITER_THREADS_KEY = "hbase.bucketcache.writer.threads";
  public static final String BUCKET_CACHE_WRITER_QUEUE_KEY = 
      "hbase.bucketcache.writer.queuelength";

  /**
   * A comma-delimited array of values for use as bucket sizes.
   */
  public static final String BUCKET_CACHE_BUCKETS_KEY = "hbase.bucketcache.bucket.sizes";

  /**
   * Defaults for Bucket cache
   */
  public static final boolean DEFAULT_BUCKET_CACHE_COMBINED = true;
  public static final int DEFAULT_BUCKET_CACHE_WRITER_THREADS = 3;
  public static final int DEFAULT_BUCKET_CACHE_WRITER_QUEUE = 64;

  /**
   * Configuration key to prefetch all blocks of a given file into the block cache
   * when the file is opened.
   */
  public static final String PREFETCH_BLOCKS_ON_OPEN_KEY = "hbase.rs.prefetchblocksonopen";

  /**
   * Configuration key to cache blocks when a compacted file is written, predicated on prefetching being enabled for
   * the column family.
   */
  public static final String PREFETCH_COMPACTED_BLOCKS_ON_WRITE_KEY =
      "hbase.rs.prefetchcompactedblocksonwrite";

  public static final String PREFETCH_HFILE_MAXSIZE = "hbase.rs.prefetch.hfile.maxsize";
  /**
   * from left to right is FLUSH,BULKLOAD,COMPACTION,LOAD
   */
  public static final String PREFETCH_EXCLUDE = "hbase.rs.prefetch.excludes";
  /**
   * exclude FLUSH & COMPACTION
   */
  public static final int PREFETCH_EXCLUDE_DEFAULT = 0b1010;

  /**
   * The target block size used by blockcache instances. Defaults to
   * {@link HConstants#DEFAULT_BLOCKSIZE}.
   * TODO: this config point is completely wrong, as it's used to determine the
   * target block size of BlockCache instances. Rename.
   */
  public static final String BLOCKCACHE_BLOCKSIZE_KEY = "hbase.offheapcache.minblocksize";

  private static final String EXTERNAL_BLOCKCACHE_KEY = "hbase.blockcache.use.external";
  private static final boolean EXTERNAL_BLOCKCACHE_DEFAULT = false;

  private static final String EXTERNAL_BLOCKCACHE_CLASS_KEY = "hbase.blockcache.external.class";
  private static final String DROP_BEHIND_CACHE_COMPACTION_KEY =
      "hbase.hfile.drop.behind.compaction";
  private static final boolean DROP_BEHIND_CACHE_COMPACTION_DEFAULT = true;

  /**
   * Enum of all built in external block caches.
   * This is used for config.
   */
  private static enum ExternalBlockCaches {
    memcached("org.apache.hadoop.hbase.io.hfile.MemcachedBlockCache");
    // TODO(eclark): Consider more. Redis, etc.
    Class<? extends BlockCache> clazz;
    ExternalBlockCaches(String clazzName) {
      try {
        clazz = (Class<? extends BlockCache>) Class.forName(clazzName);
      } catch (ClassNotFoundException cnef) {
        clazz = null;
      }
    }
    ExternalBlockCaches(Class<? extends BlockCache> clazz) {
      this.clazz = clazz;
    }
  }

  // Defaults
  public static final boolean DEFAULT_CACHE_DATA_ON_READ = true;
  public static final boolean DEFAULT_CACHE_DATA_ON_WRITE = false;
  public static final boolean DEFAULT_IN_MEMORY = false;
  public static final boolean DEFAULT_CACHE_INDEXES_ON_WRITE = false;
  public static final boolean DEFAULT_CACHE_BLOOMS_ON_WRITE = false;
  public static final boolean DEFAULT_EVICT_ON_CLOSE = false;
  public static final boolean DEFAULT_CACHE_DATA_COMPRESSED = false;
  public static final boolean DEFAULT_PREFETCH_ON_OPEN = false;
  public static final boolean DEFAULT_PREFETCH_COMPACTED_BLOCKS_ON_WRITE = false;

  /** Local reference to the block cache, null if completely disabled */
  private final BlockCache blockCache;

  private final ByteBuffAllocator byteBuffAllocator;

  /**
   * Whether blocks should be cached on read (default is on if there is a
   * cache but this can be turned off on a per-family or per-request basis).
   * If off we will STILL cache meta blocks; i.e. INDEX and BLOOM types.
   * This cannot be disabled.
   */
  private boolean cacheDataOnRead;

  /** Whether blocks should be flagged as in-memory when being cached */
  private final boolean inMemory;

  /** Whether data blocks should be cached when new files are written */
  private boolean cacheDataOnWrite;

  /** Whether index blocks should be cached when new files are written */
  private boolean cacheIndexesOnWrite;

  /** Whether compound bloom filter blocks should be cached on write */
  private boolean cacheBloomsOnWrite;

  /** Whether blocks of a file should be evicted when the file is closed */
  private boolean evictOnClose;

  /** Whether data blocks should be stored in compressed and/or encrypted form in the cache */
  private final boolean cacheDataCompressed;

  /** Whether data blocks should be prefetched into the cache */
  private final boolean prefetchOnOpen;

  /** Whether data blocks should be cached when compacted file is written for column families with prefetching */
  private final boolean prefetchCompactedDataOnWrite;

  private final long prefetchMaxsize;

  /**
   * If true and if more than one tier in this cache deploy -- e.g. CombinedBlockCache has an L1
   * and an L2 tier -- then cache data blocks up in the L1 tier (The meta blocks are likely being
   * cached up in L1 already.  At least this is the case if CombinedBlockCache).
   */
  private boolean cacheDataInL1;

  private final boolean dropBehindCompaction;

  private CacheConfig() {
    this(null, false, false, false, false, false,
        false, false, false, false, Long.MAX_VALUE, false, false, ByteBuffAllocator.HEAP);
  }

  @VisibleForTesting
  public CacheConfig(BlockCache blockCache) {
    this(blockCache, true, false, false, false, false,
        false, false, false, false, Long.MAX_VALUE, false, false, ByteBuffAllocator.HEAP);
  }

  public CacheConfig(Configuration conf, HColumnDescriptor family, ByteBuffAllocator allocator) {
    this(conf, family, CacheConfig.instantiateBlockCache(conf),allocator);
  }

  /**
   * Create a cache configuration using the specified configuration object and
   * family descriptor.
   * @param conf hbase configuration
   * @param family column family configuration
   */
  public CacheConfig(Configuration conf, HColumnDescriptor family, BlockCache blockCache, ByteBuffAllocator allocator) {
    this(
        blockCache,
        conf.getBoolean(CACHE_DATA_ON_READ_KEY, DEFAULT_CACHE_DATA_ON_READ) && (family == null ? true : family.isBlockCacheEnabled()),
        family == null ? DEFAULT_IN_MEMORY : family.isInMemory(),
        conf.getBoolean(CACHE_BLOCKS_ON_WRITE_KEY, DEFAULT_CACHE_DATA_ON_WRITE) || (family == null ? false : family.isCacheDataOnWrite()),
        conf.getBoolean(CACHE_INDEX_BLOCKS_ON_WRITE_KEY, DEFAULT_CACHE_INDEXES_ON_WRITE) || (family == null ? false : family.isCacheIndexesOnWrite()),
        conf.getBoolean(CACHE_BLOOM_BLOCKS_ON_WRITE_KEY, DEFAULT_CACHE_BLOOMS_ON_WRITE) || (family == null ? false : family.isCacheBloomsOnWrite()),
        conf.getBoolean(EVICT_BLOCKS_ON_CLOSE_KEY, DEFAULT_EVICT_ON_CLOSE) || (family == null ? false : family.isEvictBlocksOnClose()),
        conf.getBoolean(CACHE_DATA_BLOCKS_COMPRESSED_KEY, DEFAULT_CACHE_DATA_COMPRESSED),
        conf.getBoolean(PREFETCH_BLOCKS_ON_OPEN_KEY, DEFAULT_PREFETCH_ON_OPEN) || (family == null ? false : family.isPrefetchBlocksOnOpen()),
        conf.getBoolean(PREFETCH_COMPACTED_BLOCKS_ON_WRITE_KEY, DEFAULT_PREFETCH_COMPACTED_BLOCKS_ON_WRITE),
        family == null ? Long.MAX_VALUE : family.getPrefetchHFileMaxSize(),
        conf.getBoolean(HColumnDescriptor.CACHE_DATA_IN_L1, HColumnDescriptor.DEFAULT_CACHE_DATA_IN_L1) || (family == null ? false : family.isCacheDataInL1()),
        conf.getBoolean(DROP_BEHIND_CACHE_COMPACTION_KEY, DROP_BEHIND_CACHE_COMPACTION_DEFAULT),
        allocator
     );
    LOG.info("Created cacheConfig for " + (family == null ? "" : family.getNameAsString()) + ": " + this);
  }

  /**
   * Create a cache configuration using the specified configuration object and
   * defaults for family level settings. Only use if no column family context. Prefer
   * {@link CacheConfig#CacheConfig(Configuration, HColumnDescriptor)}
   * @see #CacheConfig(Configuration, HColumnDescriptor)
   * @param conf hbase configuration
   */
  public CacheConfig(Configuration conf) {
    this(conf, ByteBuffAllocator.HEAP);
  }

  public CacheConfig(Configuration conf, ByteBuffAllocator allocator) {
    this(CacheConfig.instantiateBlockCache(conf),
        conf.getBoolean(CACHE_DATA_ON_READ_KEY, DEFAULT_CACHE_DATA_ON_READ),
        DEFAULT_IN_MEMORY, // This is a family-level setting so can't be set
                           // strictly from conf
        conf.getBoolean(CACHE_BLOCKS_ON_WRITE_KEY, DEFAULT_CACHE_DATA_ON_WRITE),
        conf.getBoolean(CACHE_INDEX_BLOCKS_ON_WRITE_KEY, DEFAULT_CACHE_INDEXES_ON_WRITE),
        conf.getBoolean(CACHE_BLOOM_BLOCKS_ON_WRITE_KEY, DEFAULT_CACHE_BLOOMS_ON_WRITE),
        conf.getBoolean(EVICT_BLOCKS_ON_CLOSE_KEY, DEFAULT_EVICT_ON_CLOSE),
        conf.getBoolean(CACHE_DATA_BLOCKS_COMPRESSED_KEY, DEFAULT_CACHE_DATA_COMPRESSED),
        conf.getBoolean(PREFETCH_BLOCKS_ON_OPEN_KEY, DEFAULT_PREFETCH_ON_OPEN),
        conf.getBoolean(PREFETCH_COMPACTED_BLOCKS_ON_WRITE_KEY, DEFAULT_PREFETCH_COMPACTED_BLOCKS_ON_WRITE),
        conf.getLong(PREFETCH_HFILE_MAXSIZE, Long.MAX_VALUE),
        conf.getBoolean(HColumnDescriptor.CACHE_DATA_IN_L1, HColumnDescriptor.DEFAULT_CACHE_DATA_IN_L1),
        conf.getBoolean(DROP_BEHIND_CACHE_COMPACTION_KEY, DROP_BEHIND_CACHE_COMPACTION_DEFAULT),
        allocator
     );
    LOG.info("Created cacheConfig: " + this);
  }

  /**
   * Create a block cache configuration with the specified cache and configuration parameters.
   * @param blockCache reference to block cache, null if completely disabled
   * @param cacheDataOnRead whether DATA blocks should be cached on read (we always cache INDEX
   *          blocks and BLOOM blocks; this cannot be disabled).
   * @param inMemory whether blocks should be flagged as in-memory
   * @param cacheDataOnWrite whether data blocks should be cached on write
   * @param cacheIndexesOnWrite whether index blocks should be cached on write
   * @param cacheBloomsOnWrite whether blooms should be cached on write
   * @param evictOnClose whether blocks should be evicted when HFile is closed
   * @param cacheDataCompressed whether to store blocks as compressed in the cache
   * @param prefetchOnOpen whether to prefetch blocks upon open
   * @param cacheDataInL1 If more than one cache tier deployed, if true, cache this column families
   *          data blocks up in the L1 tier.
   * @param dropBehindCompaction indicate that we should set drop behind to true when open a store
   *          file reader for compaction
   */
  CacheConfig(final BlockCache blockCache,
      final boolean cacheDataOnRead, final boolean inMemory,
      final boolean cacheDataOnWrite, final boolean cacheIndexesOnWrite,
      final boolean cacheBloomsOnWrite, final boolean evictOnClose,
      final boolean cacheDataCompressed, final boolean prefetchOnOpen, final boolean prefetchCompactedDataOnWrite,
      final long prefetchMaxsize, final boolean cacheDataInL1, final boolean dropBehindCompaction,
      final ByteBuffAllocator byteBuffAllocator) {
    this.blockCache = blockCache;
    this.cacheDataOnRead = cacheDataOnRead;
    this.inMemory = inMemory;
    this.cacheDataOnWrite = cacheDataOnWrite;
    this.cacheIndexesOnWrite = cacheIndexesOnWrite;
    this.cacheBloomsOnWrite = cacheBloomsOnWrite;
    this.evictOnClose = evictOnClose;
    this.cacheDataCompressed = cacheDataCompressed;
    this.prefetchOnOpen = prefetchOnOpen;
    this.prefetchCompactedDataOnWrite = prefetchCompactedDataOnWrite;
    this.prefetchMaxsize = prefetchMaxsize;
    this.cacheDataInL1 = cacheDataInL1;
    this.dropBehindCompaction = dropBehindCompaction;
    this.byteBuffAllocator = byteBuffAllocator;
  }

  /**
   * Constructs a cache configuration copied from the specified configuration.
   * @param cacheConf
   */
  public CacheConfig(CacheConfig cacheConf) {
    this(cacheConf.blockCache, cacheConf.cacheDataOnRead, cacheConf.inMemory,
        cacheConf.cacheDataOnWrite, cacheConf.cacheIndexesOnWrite,
        cacheConf.cacheBloomsOnWrite, cacheConf.evictOnClose,
        cacheConf.cacheDataCompressed, cacheConf.prefetchOnOpen, cacheConf.prefetchCompactedDataOnWrite,
        cacheConf.prefetchMaxsize, cacheConf.cacheDataInL1, cacheConf.dropBehindCompaction,
        cacheConf.byteBuffAllocator);
  }

  /**
   * Checks whether the block cache is enabled.
   */
  public boolean isBlockCacheEnabled() {
    return this.blockCache != null;
  }

  /**
   * Returns the block cache.
   * @return the block cache, or null if caching is completely disabled
   */
  public BlockCache getBlockCache() {
    return this.blockCache;
  }

  /**
   * Returns whether the DATA blocks of this HFile should be cached on read or not (we always
   * cache the meta blocks, the INDEX and BLOOM blocks).
   * @return true if blocks should be cached on read, false if not
   */
  public boolean shouldCacheDataOnRead() {
    return isBlockCacheEnabled() && cacheDataOnRead;
  }

  public boolean shouldDropBehindCompaction() {
    return dropBehindCompaction;
  }

  /**
   * Should we cache a block of a particular category? We always cache
   * important blocks such as index blocks, as long as the block cache is
   * available.
   */
  public boolean shouldCacheBlockOnRead(BlockCategory category) {
    return isBlockCacheEnabled()
        && (cacheDataOnRead ||
            category == BlockCategory.INDEX ||
            category == BlockCategory.BLOOM ||
            (prefetchOnOpen &&
                (category != BlockCategory.META &&
                 category != BlockCategory.UNKNOWN)));
  }

  /**
   * @return true if blocks in this file should be flagged as in-memory
   */
  public boolean isInMemory() {
    return isBlockCacheEnabled() && this.inMemory;
  }

  /**
   * @return True if cache data blocks in L1 tier (if more than one tier in block cache deploy).
   */
  public boolean isCacheDataInL1() {
    return isBlockCacheEnabled() && this.cacheDataInL1;
  }

  /**
   * @return true if data blocks should be written to the cache when an HFile is
   *         written, false if not
   */
  public boolean shouldCacheDataOnWrite() {
    return isBlockCacheEnabled() && this.cacheDataOnWrite;
  }

  /**
   * @return true if blocks should be cached while writing during compaction, false if not
   */
  public boolean shouldCacheCompactedBlocksOnWrite() {
    return this.prefetchCompactedDataOnWrite && this.prefetchOnOpen;
  }

  /**
   * Only used for testing.
   * @param cacheDataOnWrite whether data blocks should be written to the cache
   *                         when an HFile is written
   */
  @VisibleForTesting
  public void setCacheDataOnWrite(boolean cacheDataOnWrite) {
    this.cacheDataOnWrite = cacheDataOnWrite;
  }

  public void disableCacheOnWrite() {
    this.cacheDataOnWrite = false;
    this.cacheIndexesOnWrite = false;
    this.cacheBloomsOnWrite = false;
  }

  public void enableCacheOnWrite() {
    this.cacheDataOnWrite = true;
    this.cacheIndexesOnWrite = true;
    this.cacheBloomsOnWrite = true;
  }

  /**
   * Only used for testing.
   * @param cacheDataInL1 Whether to cache data blocks up in l1 (if a multi-tier cache
   * implementation).
   */
  @VisibleForTesting
  public void setCacheDataInL1(boolean cacheDataInL1) {
    this.cacheDataInL1 = cacheDataInL1;
  }

  /**
   * @return true if index blocks should be written to the cache when an HFile
   *         is written, false if not
   */
  public boolean shouldCacheIndexesOnWrite() {
    return isBlockCacheEnabled() && this.cacheIndexesOnWrite;
  }

  /**
   * @return true if bloom blocks should be written to the cache when an HFile
   *         is written, false if not
   */
  public boolean shouldCacheBloomsOnWrite() {
    return isBlockCacheEnabled() && this.cacheBloomsOnWrite;
  }

  /**
   * @return true if blocks should be evicted from the cache when an HFile
   *         reader is closed, false if not
   */
  public boolean shouldEvictOnClose() {
    return isBlockCacheEnabled() && this.evictOnClose;
  }

  /**
   * Only used for testing.
   * @param evictOnClose whether blocks should be evicted from the cache when an
   *                     HFile reader is closed
   */
  public void setEvictOnClose(boolean evictOnClose) {
    this.evictOnClose = evictOnClose;
  }

  /**
   * @return true if data blocks should be compressed in the cache, false if not
   */
  public boolean shouldCacheDataCompressed() {
    return isBlockCacheEnabled() && this.cacheDataOnRead && this.cacheDataCompressed;
  }

  /**
   * @return true if this {@link BlockCategory} should be compressed in blockcache, false otherwise
   */
  public boolean shouldCacheCompressed(BlockCategory category) {
    if (!isBlockCacheEnabled()) return false;
    switch (category) {
      case DATA:
        return this.cacheDataOnRead && this.cacheDataCompressed;
      default:
        return false;
    }
  }

  /**
   * @return true if blocks should be prefetched into the cache on open, false if not
   */
  public boolean shouldPrefetchOnOpen(long fileSize) {
    return fileSize <= this.prefetchMaxsize && isBlockCacheEnabled() && this.prefetchOnOpen;
  }

  public boolean shouldPrefetchIndex() {
    return isBlockCacheEnabled() && this.cacheIndexesOnWrite;
  }

  public boolean shouldPrefetchBloom() {
    return isBlockCacheEnabled() && this.cacheBloomsOnWrite;
  }

  /**
   * Return true if we may find this type of block in block cache.
   * <p/>
   * TODO: today {@code family.isBlockCacheEnabled()} only means {@code cacheDataOnRead}, so here we
   * consider lots of other configurations such as {@code cacheDataOnWrite}. We should fix this in
   * the future, {@code cacheDataOnWrite} should honor the CF level {@code isBlockCacheEnabled}
   * configuration.
   */
  public boolean shouldReadBlockFromCache(BlockType blockType) {
    if (!isBlockCacheEnabled()) {
      return false;
    }
    if (cacheDataOnRead) {
      return true;
    }
    if (prefetchOnOpen) {
      return true;
    }
    if (cacheDataOnWrite) {
      return true;
    }
    if (blockType == null) {
      return true;
    }
    if (blockType.getCategory() == BlockCategory.BLOOM ||
            blockType.getCategory() == BlockCategory.INDEX) {
      return true;
    }
    return false;
  }

  /**
   * If we make sure the block could not be cached, we will not acquire the lock
   * otherwise we will acquire lock
   */
  public boolean shouldLockOnCacheMiss(BlockType blockType) {
    if (blockType == null) {
      return true;
    }
    return shouldCacheBlockOnRead(blockType.getCategory());
  }

  public boolean isCombinedBlockCache() {
    return blockCache instanceof CombinedBlockCache;
  }

  public boolean isCompositeBucketCache() {
    return blockCache instanceof CompositeBucketCache;
  }

  public ByteBuffAllocator getByteBuffAllocator() {
    return this.byteBuffAllocator;
  }

  @Override
  public String toString() {
    if (!isBlockCacheEnabled()) {
      return "CacheConfig:disabled";
    }
    return "blockCache=" + getBlockCache() +
      ", cacheDataOnRead=" + shouldCacheDataOnRead() +
      ", cacheDataOnWrite=" + shouldCacheDataOnWrite() +
      ", cacheIndexesOnWrite=" + shouldCacheIndexesOnWrite() +
      ", cacheBloomsOnWrite=" + shouldCacheBloomsOnWrite() +
      ", cacheEvictOnClose=" + shouldEvictOnClose() +
      ", cacheDataCompressed=" + shouldCacheDataCompressed() +
      ", prefetchOnOpen=" + shouldPrefetchOnOpen(0) +
      ", prefetchMaxsize= " + prefetchMaxsize;
  }

  // Static block cache reference and methods

  /**
   * Static reference to the block cache, or null if no caching should be used
   * at all.
   */
  // Clear this if in tests you'd make more than one block cache instance.
  @VisibleForTesting
  static BlockCache GLOBAL_BLOCK_CACHE_INSTANCE;

  /** Boolean whether we have disabled the block cache entirely. */
  @VisibleForTesting
  static boolean blockCacheDisabled = false;

  static long getL1CacheSize(final Configuration conf, final MemoryUsage mu) {
    float cachePercentage = conf.getFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY,
      HConstants.HFILE_BLOCK_CACHE_SIZE_DEFAULT);
    if (cachePercentage <= 0.0001f) {
      blockCacheDisabled = true;
      return -1;
    }
    if (cachePercentage > 1.0) {
      throw new IllegalArgumentException(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY +
        " must be between 0.0 and 1.0, and not > 1.0");
    }

    // Calculate the amount of heap to give the heap.
    return (long) (mu.getMax() * cachePercentage);
  }

  /**
   * @param c Configuration to use.
   * @param mu JMX Memory Bean
   * @return An L1 instance.  Currently an instance of LruBlockCache.
   */
  private static BlockCache getL1(final Configuration c, final MemoryUsage mu) {
    if (c.getBoolean(BUCKET_CACHE_COMPOSITE_KEY, false)) {
      return getBucketCache(c, mu, CacheLevel.L1);
    }
    long cacheSize = getL1CacheSize(c, mu);
    if (cacheSize < 0) return null;
    String policy = c.get(BLOCKCACHE_POLICY_KEY, BLOCKCACHE_POLICY_DEFAULT);
    int blockSize = c.getInt(BLOCKCACHE_BLOCKSIZE_KEY, HConstants.DEFAULT_BLOCKSIZE);
    LOG.info("Allocating BlockCache size=" +
        StringUtils.byteDesc(cacheSize) + ", blockSize=" + StringUtils.byteDesc(blockSize));
    if (policy.equalsIgnoreCase("LRU")) {
      return new LruBlockCache(cacheSize, blockSize, true, c);
    } else if (policy.equalsIgnoreCase("TinyLFU")) {
      return new TinyLfuBlockCache(cacheSize, blockSize, ForkJoinPool.commonPool(), c);
    } else {
      throw new IllegalArgumentException("Unknown policy: " + policy);
    }
  }

  /**
   * @param c Configuration to use.
   * @param mu JMX Memory Bean
   * @return Returns L2 block cache instance (for now it is BucketCache BlockCache all the time)
   * or null if not supposed to be a L2.
   */
  @VisibleForTesting
  static BlockCache getL2(final Configuration c, final MemoryUsage mu) {
    final boolean useExternal = c.getBoolean(EXTERNAL_BLOCKCACHE_KEY, EXTERNAL_BLOCKCACHE_DEFAULT);
    final boolean compositeBucketCache = c.getBoolean(BUCKET_CACHE_COMPOSITE_KEY, false);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Trying to use " + (useExternal?" External":" Internal") + " l2 cache");
    }

    // If we want to use an external block cache then create that.
    if (useExternal && !compositeBucketCache) {
      return getExternalBlockcache(c);
    }
    // otherwise use the bucket cache.
    return getBucketCache(c, mu , CacheLevel.L2);
  }

  private static BlockCache getExternalBlockcache(Configuration c) {
    Class klass = null;

    // Get the class, from the config. s
    try {
      klass = ExternalBlockCaches.valueOf(c.get(EXTERNAL_BLOCKCACHE_CLASS_KEY, "memcache")).clazz;
    } catch (IllegalArgumentException exception) {
      try {
        klass = c.getClass(EXTERNAL_BLOCKCACHE_CLASS_KEY, Class.forName(
            "org.apache.hadoop.hbase.io.hfile.MemcachedBlockCache"));
      } catch (ClassNotFoundException e) {
        return null;
      }
    }

    // Now try and create an instance of the block cache.
    try {
      LOG.info("Creating external block cache of type: " + klass);
      return (BlockCache) ReflectionUtils.newInstance(klass, c);
    } catch (Exception e) {
      LOG.warn("Error creating external block cache", e);
    }
    return null;

  }

  private static BlockCache getBucketCache(Configuration c, MemoryUsage mu, CacheLevel level) {
    // Check for ioengine name must be non-null.
    String bucketCacheIOEngineName;
    float bucketCachePercentage;
    int writerThreads;
    int writerQueueLen;
    String persistentPath;
    switch(level) {
    case L1:
      bucketCacheIOEngineName = c.get(CompositeBucketCache.IOENGINE_L1, null);
      bucketCachePercentage = c.getFloat(CompositeBucketCache.CACHESIZE_L1, 0F);
      writerThreads = c.getInt(CompositeBucketCache.WRITER_THREADS_L1, DEFAULT_BUCKET_CACHE_WRITER_THREADS);
      writerQueueLen = c.getInt(CompositeBucketCache.WRITER_QUEUE_LENGTH_L1, DEFAULT_BUCKET_CACHE_WRITER_QUEUE);
      persistentPath = c.get(CompositeBucketCache.PERSISTENT_PATH_L1);
      break;
    case L2:
    default:
      bucketCacheIOEngineName = c.get(CompositeBucketCache.IOENGINE_L2,
          c.get(BUCKET_CACHE_IOENGINE_KEY, null));
      bucketCachePercentage = c.getFloat(CompositeBucketCache.CACHESIZE_L2,
          c.getFloat(BUCKET_CACHE_SIZE_KEY, 0F));
      writerThreads = c.getInt(CompositeBucketCache.WRITER_THREADS_L2,
          c.getInt(BUCKET_CACHE_WRITER_THREADS_KEY, DEFAULT_BUCKET_CACHE_WRITER_THREADS));
      writerQueueLen = c.getInt(CompositeBucketCache.WRITER_QUEUE_LENGTH_L2,
          c.getInt(BUCKET_CACHE_WRITER_QUEUE_KEY, DEFAULT_BUCKET_CACHE_WRITER_QUEUE));
      persistentPath = c.get(CompositeBucketCache.PERSISTENT_PATH_L2, c.get(BUCKET_CACHE_PERSISTENT_PATH_KEY));
      break;
    }
    if (bucketCacheIOEngineName == null || bucketCacheIOEngineName.length() <= 0) return null;

    int blockSize = c.getInt(BLOCKCACHE_BLOCKSIZE_KEY, HConstants.DEFAULT_BLOCKSIZE);
    long bucketCacheSize = (long) (bucketCachePercentage < 1? mu.getMax() * bucketCachePercentage:
      bucketCachePercentage * 1024 * 1024);
    if (bucketCacheSize <= 0) {
      throw new IllegalStateException("bucketCacheSize <= 0; Check " +
        BUCKET_CACHE_SIZE_KEY + " setting and/or server java heap size");
    }
    if (c.get("hbase.bucketcache.percentage.in.combinedcache") != null) {
      LOG.warn("Configuration 'hbase.bucketcache.percentage.in.combinedcache' is no longer "
          + "respected. See comments in http://hbase.apache.org/book.html#_changes_of_note");
    }
    String[] configuredBucketSizes = c.getStrings(BUCKET_CACHE_BUCKETS_KEY);
    int [] bucketSizes = null;
    if (configuredBucketSizes != null) {
      bucketSizes = new int[configuredBucketSizes.length];
      for (int i = 0; i < configuredBucketSizes.length; i++) {
        int bucketSize = Integer.parseInt(configuredBucketSizes[i].trim());
        if (bucketSize % 256 != 0) {
          // We need all the bucket sizes to be multiples of 256. Having all the configured bucket
          // sizes to be multiples of 256 will ensure that the block offsets within buckets,
          // that are calculated, will also be multiples of 256.
          // See BucketEntry where offset to each block is represented using 5 bytes (instead of 8
          // bytes long). We would like to save heap overhead as less as possible.
          throw new IllegalArgumentException("Illegal value: " + bucketSize + " configured for '"
              + BUCKET_CACHE_BUCKETS_KEY + "'. All bucket sizes to be multiples of 256");
        }
        bucketSizes[i] = bucketSize;
      }
    }
    BucketCache bucketCache = null;
    try {
      int ioErrorsTolerationDuration = c.getInt(
        "hbase.bucketcache.ioengine.errors.tolerated.duration",
        BucketCache.DEFAULT_ERROR_TOLERATION_DURATION);
      // Bucket cache logs its stats on creation internal to the constructor.
      bucketCache = new BucketCache(bucketCacheIOEngineName,
        bucketCacheSize, blockSize, bucketSizes, writerThreads, writerQueueLen, persistentPath,
        ioErrorsTolerationDuration);
    } catch (IOException ioex) {
      LOG.error("Can't instantiate bucket cache", ioex); throw new RuntimeException(ioex);
    }
    return bucketCache;
  }

  /**
   * Returns the block cache or <code>null</code> in case none should be used.
   * Sets GLOBAL_BLOCK_CACHE_INSTANCE
   *
   * @param conf  The current configuration.
   * @return The block cache or <code>null</code>.
   */
  public static synchronized BlockCache instantiateBlockCache(Configuration conf) {
    if (GLOBAL_BLOCK_CACHE_INSTANCE != null) return GLOBAL_BLOCK_CACHE_INSTANCE;
    if (blockCacheDisabled) return null;
    MemoryUsage mu = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
    BlockCache l1 = getL1(conf, mu);
    BlockCache l2 = getL2(conf, mu);
    if (conf.getBoolean(BUCKET_CACHE_COMPOSITE_KEY, false)) {
      GLOBAL_BLOCK_CACHE_INSTANCE = new CompositeBucketCache((BucketCache)l1, (BucketCache)l2);
    } else {
      // blockCacheDisabled is set as a side-effect of getL1(), so check it again after the call.
      if (blockCacheDisabled) return null;
      if (l2 == null) {
        GLOBAL_BLOCK_CACHE_INSTANCE = l1;
      } else {
        boolean useExternal = conf.getBoolean(EXTERNAL_BLOCKCACHE_KEY, EXTERNAL_BLOCKCACHE_DEFAULT);
        boolean combinedWithLru = conf.getBoolean(BUCKET_CACHE_COMBINED_KEY,
          DEFAULT_BUCKET_CACHE_COMBINED);
        if (useExternal) {
          GLOBAL_BLOCK_CACHE_INSTANCE = new InclusiveCombinedBlockCache((FirstLevelBlockCache)l1, l2);
        } else {
          if (combinedWithLru) {
            GLOBAL_BLOCK_CACHE_INSTANCE = new CombinedBlockCache((FirstLevelBlockCache)l1, l2);
          } else {
            // L1 and L2 are not 'combined'.  They are connected via the LruBlockCache victimhandler
            // mechanism.  It is a little ugly but works according to the following: when the
            // background eviction thread runs, blocks evicted from L1 will go to L2 AND when we get
            // a block from the L1 cache, if not in L1, we will search L2.
            GLOBAL_BLOCK_CACHE_INSTANCE = l1;
          }
        }
        ((FirstLevelBlockCache)l1).setVictimCache(l2);
      }
    }
    return GLOBAL_BLOCK_CACHE_INSTANCE;
  }
}
