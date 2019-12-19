/**
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.io.hfile.bucket;

import static org.apache.hadoop.hbase.io.ByteBuffAllocator.HEAP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.io.hfile.CacheTestUtils;
import org.apache.hadoop.hbase.io.hfile.CacheTestUtils.HFileBlockPair;
import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.hadoop.hbase.io.hfile.HFileBlock;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.io.hfile.bucket.BucketAllocator.BucketSizeInfo;
import org.apache.hadoop.hbase.io.hfile.bucket.BucketAllocator.IndexStatistics;
import org.apache.hadoop.hbase.io.hfile.bucket.BucketCache.RAMCache;
import org.apache.hadoop.hbase.io.hfile.bucket.BucketCache.RAMQueueEntry;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

/**
 * Basic test of BucketCache.Puts and gets.
 * <p>
 * Tests will ensure that blocks' data correctness under several threads concurrency
 */
@RunWith(Parameterized.class)
@Category(SmallTests.class)
public class TestBucketCache {

  private static final Random RAND = new Random();

  @Parameterized.Parameters(name = "{index}: blockSize={0}, bucketSizes={1}")
  public static Iterable<Object[]> data() {
    return Arrays.asList(new Object[][] {
        { 8192, null }, // TODO: why is 8k the default blocksize for these tests?
        {
            16 * 1024,
            new int[] { 2 * 1024 + 1024, 4 * 1024 + 1024, 8 * 1024 + 1024, 16 * 1024 + 1024,
                28 * 1024 + 1024, 32 * 1024 + 1024, 64 * 1024 + 1024, 96 * 1024 + 1024,
                128 * 1024 + 1024 } } });
  }

  @Parameterized.Parameter(0)
  public int constructedBlockSize;

  @Parameterized.Parameter(1)
  public int[] constructedBlockSizes;

  BucketCache cache;
  final int CACHE_SIZE = 1000000;
  final int NUM_BLOCKS = 100;
  final int BLOCK_SIZE = CACHE_SIZE / NUM_BLOCKS;
  final int NUM_THREADS = 100;
  final int NUM_QUERIES = 10000;

  final long capacitySize = 32 * 1024 * 1024;
  final int writeThreads = BucketCache.DEFAULT_WRITER_THREADS;
  final int writerQLen = BucketCache.DEFAULT_WRITER_QUEUE_ITEMS;
  String ioEngineName = "heap";
  String persistencePath = null;

  private class MockedBucketCache extends BucketCache {

    public MockedBucketCache(String ioEngineName, long capacity, int blockSize, int[] bucketSizes,
        int writerThreads, int writerQLen, String persistencePath) throws FileNotFoundException,
        IOException {
      super(ioEngineName, capacity, blockSize, bucketSizes, writerThreads, writerQLen,
          persistencePath);
      super.wait_when_cache = true;
    }

    @Override
    public void cacheBlock(BlockCacheKey cacheKey, Cacheable buf, boolean inMemory,
        boolean cacheDataInL1) {
      super.cacheBlock(cacheKey, buf, inMemory, cacheDataInL1);
    }

    @Override
    public void cacheBlock(BlockCacheKey cacheKey, Cacheable buf) {
      super.cacheBlock(cacheKey, buf);
    }
  }

  @Before
  public void setup() throws FileNotFoundException, IOException {
    cache =
        new MockedBucketCache(ioEngineName, capacitySize, constructedBlockSize,
            constructedBlockSizes, writeThreads, writerQLen, persistencePath);
  }

  @After
  public void tearDown() {
    cache.shutdown();
  }

  /**
   * Return a random element from {@code a}.
   */
  private static <T> T randFrom(List<T> a) {
    return a.get(RAND.nextInt(a.size()));
  }

  @Test
  public void testBucketAllocator() throws BucketAllocatorException {
    BucketAllocator mAllocator = cache.getAllocator();
    /*
     * Test the allocator first
     */
    final List<Integer> BLOCKSIZES = Arrays.asList(4 * 1024, 8 * 1024, 64 * 1024, 96 * 1024);

    boolean full = false;
    ArrayList<Long> allocations = new ArrayList<Long>();
    // Fill the allocated extents by choosing a random blocksize. Continues selecting blocks until
    // the cache is completely filled.
    List<Integer> tmp = new ArrayList<Integer>(BLOCKSIZES);
    while (!full) {
      Integer blockSize = null;
      try {
        blockSize = randFrom(tmp);
        allocations.add(mAllocator.allocateBlock(blockSize));
      } catch (CacheFullException cfe) {
        tmp.remove(blockSize);
        if (tmp.isEmpty()) full = true;
      }
    }

    for (Integer blockSize : BLOCKSIZES) {
      BucketSizeInfo bucketSizeInfo = mAllocator.roundUpToBucketSizeInfo(blockSize);
      IndexStatistics indexStatistics = bucketSizeInfo.statistics();
      assertEquals("unexpected freeCount for " + bucketSizeInfo, 0, indexStatistics.freeCount());
    }

    for (long offset : allocations) {
      assertEquals(mAllocator.sizeOfAllocation(offset), mAllocator.freeBlock(offset));
    }
    assertEquals(0, mAllocator.getUsedSize());
  }

  @Test
  public void testCacheSimple() throws Exception {
    CacheTestUtils.testCacheSimple(cache, BLOCK_SIZE, NUM_QUERIES);
  }

  @Test
  public void testCacheMultiThreadedSingleKey() throws Exception {
    CacheTestUtils.hammerSingleKey(cache, 2 * NUM_THREADS, 2 * NUM_QUERIES);
  }

  @Test
  public void testHeapSizeChanges() throws Exception {
    cache.stopWriterThreads();
    CacheTestUtils.testHeapSizeChanges(cache, BLOCK_SIZE);
  }

  public static void waitUntilFlushedToBucket(BucketCache cache, BlockCacheKey cacheKey)
      throws InterruptedException {
    while (!cache.backingMap.containsKey(cacheKey) || cache.ramCache.containsKey(cacheKey)) {
      Thread.sleep(100);
    }
    Thread.sleep(1000);
  }

  public static void waitUntilAllFlushedToBucket(BucketCache cache) throws InterruptedException {
    while (!cache.ramCache.isEmpty()) {
      Thread.sleep(100);
    }
  }

  // BucketCache.cacheBlock is async, it first adds block to ramCache and writeQueue, then writer
  // threads will flush it to the bucket and put reference entry in backingMap.
  private void cacheAndWaitUntilFlushedToBucket(BucketCache cache, BlockCacheKey cacheKey,
      Cacheable block) throws InterruptedException {
    cache.cacheBlock(cacheKey, block);
    waitUntilFlushedToBucket(cache, cacheKey);
    Thread.sleep(1000);
  }

  @Test
  public void testMemoryLeak() throws Exception {
    final BlockCacheKey cacheKey = new BlockCacheKey("dummy", 1L);
    cacheAndWaitUntilFlushedToBucket(cache, cacheKey,
      new CacheTestUtils.ByteArrayCacheable(new byte[10]));
    long lockId = cache.backingMap.get(cacheKey).offset();
    ReentrantReadWriteLock lock = cache.offsetLock.getLock(lockId);
    lock.writeLock().lock();
    Thread evictThread = new Thread("evict-block") {

      @Override
      public void run() {
        cache.evictBlock(cacheKey);
      }

    };
    evictThread.start();
    cache.offsetLock.waitForWaiters(lockId, 1);
    cache.blockEvicted(cacheKey, cache.backingMap.remove(cacheKey), true);
    assertEquals(0, cache.getBlockCount());
    cacheAndWaitUntilFlushedToBucket(cache, cacheKey,
      new CacheTestUtils.ByteArrayCacheable(new byte[10]));
    assertEquals(1, cache.getBlockCount());
    lock.writeLock().unlock();
    evictThread.join();
    assertEquals(0, cache.getBlockCount());
    assertEquals(cache.getCurrentSize(), 0L);
  }

  @Test
  public void testRetrieveFromFile() throws Exception {
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    Path testDir = TEST_UTIL.getDataTestDir();
    TEST_UTIL.getTestFileSystem().mkdirs(testDir);

    String ioEngineName = "file:" + testDir + "/bucket.cache";
    String persistencePath = testDir + "/bucket.persistence";
    BucketCache bucketCache = new BucketCache(ioEngineName, capacitySize, constructedBlockSize,
        constructedBlockSizes, writeThreads, writerQLen, persistencePath);
    long usedSize = bucketCache.getAllocator().getUsedSize();
    assertEquals(0, usedSize);

    HFileBlockPair[] blocks = CacheTestUtils.generateHFileBlocks(constructedBlockSize, 1);
    // Add blocks
    for (HFileBlockPair block : blocks) {
      bucketCache.cacheBlock(block.getBlockName(), block.getBlock());
    }
    for (HFileBlockPair block : blocks) {
      cacheAndWaitUntilFlushedToBucket(bucketCache, block.getBlockName(), block.getBlock());
    }
    usedSize = bucketCache.getAllocator().getUsedSize();
    assertNotEquals(0, usedSize);
    // persist cache to file
    bucketCache.shutdown();
    assertTrue(new File(persistencePath).exists());

    // restore cache from file
    bucketCache = new BucketCache(ioEngineName, capacitySize, constructedBlockSize,
        constructedBlockSizes, writeThreads, writerQLen, persistencePath);
    assertFalse(new File(persistencePath).exists());
    assertEquals(usedSize, bucketCache.getAllocator().getUsedSize());
    // persist cache to file
    bucketCache.shutdown();
    assertTrue(new File(persistencePath).exists());

    // reconfig buckets sizes, the biggest bucket is small than constructedBlockSize (8k or 16k)
    // so it can't restore cache from file
    int[] smallBucketSizes = new int[] { 2 * 1024 + 1024, 4 * 1024 + 1024 };
    bucketCache = new BucketCache(ioEngineName, capacitySize, constructedBlockSize,
        smallBucketSizes, writeThreads, writerQLen, persistencePath);
    assertFalse(new File(persistencePath).exists());
    assertEquals(0, bucketCache.getAllocator().getUsedSize());
    assertEquals(0, bucketCache.backingMap.size());

    TEST_UTIL.cleanupTestDir();
  }

  @Test
  public void testFreeBlockWhenIOEngineWriteFailure() throws IOException {
    // initialize an block.
    int size = 100, offset = 20;
    int length = HConstants.HFILEBLOCK_HEADER_SIZE + size;
    ByteBuffer buf = ByteBuffer.allocate(length);
    HFileContext meta = new HFileContextBuilder().build();
    HFileBlock block = new HFileBlock(BlockType.DATA, size, size, -1, ByteBuff.wrap(buf),
        HFileBlock.FILL_HEADER, offset, 52, -1, meta, ByteBuffAllocator.HEAP);

    // initialize an mocked ioengine.
    IOEngine ioEngine = Mockito.mock(IOEngine.class);
    Mockito.when(ioEngine.usesSharedMemory()).thenReturn(false);
    // Mockito.doNothing().when(ioEngine).write(Mockito.any(ByteBuffer.class), Mockito.anyLong());
    Mockito.doThrow(RuntimeException.class).when(ioEngine).write(Mockito.any(ByteBuffer.class),
      Mockito.anyLong());
    Mockito.doThrow(RuntimeException.class).when(ioEngine).write(Mockito.any(ByteBuff.class),
      Mockito.anyLong());

    // create an bucket allocator.
    long availableSpace = 1024 * 1024 * 1024L;
    BucketAllocator allocator = new BucketAllocator(availableSpace, null);

    BlockCacheKey key = new BlockCacheKey("dummy", 1L);
    RAMQueueEntry re = new RAMQueueEntry(key, block, 1, true, ByteBuffAllocator.NONE);

    Assert.assertEquals(0, allocator.getUsedSize());
    try {
      re.writeToCache(ioEngine, allocator, null);
      Assert.fail();
    } catch (Exception e) {
    }
    Assert.assertEquals(0, allocator.getUsedSize());
  }

  @Test
  public void testRAMCache() {
    int size = 100;
    int length = HConstants.HFILEBLOCK_HEADER_SIZE + size;
    byte[] byteArr = new byte[length];
    ByteBuffer buf = ByteBuffer.wrap(byteArr, 0, size);
    HFileContext meta = new HFileContextBuilder().build();

    RAMCache cache = new RAMCache();
    BlockCacheKey key1 = new BlockCacheKey("file-1", 1);
    BlockCacheKey key2 = new BlockCacheKey("file-2", 2);
    HFileBlock blk1 = new HFileBlock(BlockType.DATA, size, size, -1, ByteBuff.wrap(buf),
        HFileBlock.FILL_HEADER, -1, 52, -1, meta, ByteBuffAllocator.HEAP);
    HFileBlock blk2 = new HFileBlock(BlockType.DATA, size, size, -1, ByteBuff.wrap(buf),
        HFileBlock.FILL_HEADER, -1, -1, -1, meta, ByteBuffAllocator.HEAP);
    RAMQueueEntry re1 = new RAMQueueEntry(key1, blk1, 1, false, ByteBuffAllocator.NONE);
    RAMQueueEntry re2 = new RAMQueueEntry(key1, blk2, 1, false, ByteBuffAllocator.NONE);

    assertFalse(cache.containsKey(key1));
    assertNull(cache.putIfAbsent(key1, re1));
    assertEquals(2, ((HFileBlock) re1.getData()).getBufferReadOnly().refCnt());

    assertNotNull(cache.putIfAbsent(key1, re2));
    assertEquals(2, ((HFileBlock) re1.getData()).getBufferReadOnly().refCnt());
    assertEquals(1, ((HFileBlock) re2.getData()).getBufferReadOnly().refCnt());

    assertNull(cache.putIfAbsent(key2, re2));
    assertEquals(2, ((HFileBlock) re1.getData()).getBufferReadOnly().refCnt());
    assertEquals(2, ((HFileBlock) re2.getData()).getBufferReadOnly().refCnt());

    cache.remove(key1);
    assertEquals(1, ((HFileBlock) re1.getData()).getBufferReadOnly().refCnt());
    assertEquals(2, ((HFileBlock) re2.getData()).getBufferReadOnly().refCnt());

    cache.clear();
    assertEquals(1, ((HFileBlock) re1.getData()).getBufferReadOnly().refCnt());
    assertEquals(1, ((HFileBlock) re2.getData()).getBufferReadOnly().refCnt());
  }

  @Test
  public void testCacheBlockNextBlockMetadataMissing() throws Exception {
    int size = 100;
    int length = HConstants.HFILEBLOCK_HEADER_SIZE + size;
    byte[] byteArr = new byte[length];
    ByteBuffer buf = ByteBuffer.wrap(byteArr, 0, size);
    HFileContext meta = new HFileContextBuilder().build();
    HFileBlock blockWithNextBlockMetadata = new HFileBlock(BlockType.DATA, size, size, -1, ByteBuff.wrap(buf),
        HFileBlock.FILL_HEADER, -1, 52, -1, meta, HEAP);
    HFileBlock blockWithoutNextBlockMetadata = new HFileBlock(BlockType.DATA, size, size, -1, ByteBuff.wrap(buf),
        HFileBlock.FILL_HEADER, -1, -1, -1, meta, HEAP);

    BlockCacheKey key = new BlockCacheKey("key1", 0);
    ByteBuffer actualBuffer = ByteBuffer.allocate(length);
    ByteBuffer block1Buffer = ByteBuffer.allocate(length);
    ByteBuffer block2Buffer = ByteBuffer.allocate(length);
    blockWithNextBlockMetadata.serialize(block1Buffer, true);
    blockWithoutNextBlockMetadata.serialize(block2Buffer, true);

    //Add blockWithNextBlockMetadata, expect blockWithNextBlockMetadata back.
    CacheTestUtils.getBlockAndAssertEquals(cache, key, blockWithNextBlockMetadata, actualBuffer,
        block1Buffer);

    //Add blockWithoutNextBlockMetada, expect blockWithNextBlockMetadata back.
    waitUntilFlushedToBucket(cache, key);
    // Add blockWithoutNextBlockMetada, expect blockWithNextBlockMetadata back.
    CacheTestUtils.getBlockAndAssertEquals(cache, key, blockWithoutNextBlockMetadata, actualBuffer,
        block1Buffer);

    //Clear and add blockWithoutNextBlockMetadata
    cache.evictBlock(key);
    assertNull(cache.getBlock(key, false, false, false));
    CacheTestUtils.getBlockAndAssertEquals(cache, key, blockWithoutNextBlockMetadata, actualBuffer,
        block2Buffer);

    waitUntilFlushedToBucket(cache, key);
    // Add blockWithNextBlockMetadata, expect blockWithNextBlockMetadata to replace.
    CacheTestUtils.getBlockAndAssertEquals(cache, key, blockWithNextBlockMetadata, actualBuffer,
        block1Buffer);
  }
}
