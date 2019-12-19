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
package org.apache.hadoop.hbase.mob.compactions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobFileName;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.mob.compactions.MobCompactionRequest.CompactionType;
import org.apache.hadoop.hbase.mob.compactions.PartitionedMobCompactionRequest.CompactionDelPartition;
import org.apache.hadoop.hbase.mob.compactions.PartitionedMobCompactionRequest.CompactionPartition;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.ScanInfo;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;
import org.apache.hadoop.hbase.regionserver.StoreScanner;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestPartitionedMobCompactor {
  private static final Log LOG = LogFactory.getLog(TestPartitionedMobCompactor.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final static String family = "family";
  private final static String qf = "qf";
  private static byte[] KEYS = Bytes.toBytes("012");
  private HColumnDescriptor hcd = new HColumnDescriptor(family);
  private Configuration conf = TEST_UTIL.getConfiguration();
  private CacheConfig cacheConf = new CacheConfig(conf);
  private FileSystem fs;
  private List<FileStatus> mobFiles = new ArrayList<>();
  private List<Path> delFiles = new ArrayList<>();
  private List<FileStatus> allFiles = new ArrayList<>();
  private Path basePath;
  private String mobSuffix;
  private String delSuffix;
  private static ExecutorService pool;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt("hbase.master.info.port", 0);
    TEST_UTIL.getConfiguration().setBoolean("hbase.regionserver.info.port.auto", true);
    TEST_UTIL.getConfiguration().setInt("hfile.format.version", 3);
    TEST_UTIL.startMiniCluster(1);
    pool = createThreadPool();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    pool.shutdown();
    TEST_UTIL.shutdownMiniCluster();
  }

  private void init(String tableName) throws Exception {
    fs = FileSystem.get(conf);
    Path testDir = FSUtils.getRootDir(conf);
    Path mobTestDir = new Path(testDir, MobConstants.MOB_DIR_NAME);
    basePath = new Path(new Path(mobTestDir, tableName), family);

    mobSuffix = UUID.randomUUID().toString().replaceAll("-", "");
    delSuffix = UUID.randomUUID().toString().replaceAll("-", "") + "_del";
    allFiles.clear();
    mobFiles.clear();
    delFiles.clear();
  }

  @Test
  public void testCompactionSelectWithAllFiles() throws Exception {
    String tableName = "testCompactionSelectWithAllFiles";
    testCompactionAtMergeSize(tableName, MobConstants.DEFAULT_MOB_COMPACTION_MERGEABLE_THRESHOLD,
        CompactionType.ALL_FILES, true);
  }

  @Test
  public void testCompactionSelectWithPartFiles() throws Exception {
    String tableName = "testCompactionSelectWithPartFiles";
    testCompactionAtMergeSize(tableName, 4000, CompactionType.PART_FILES, false);
  }

  @Test
  public void testCompactionSelectWithForceAllFiles() throws Exception {
    String tableName = "testCompactionSelectWithForceAllFiles";
    testCompactionAtMergeSize(tableName, Long.MAX_VALUE, CompactionType.ALL_FILES, true);
  }

  private void testCompactionAtMergeSize(final String tableName,
      final long mergeSize, final CompactionType type, final boolean isForceAllFiles)
      throws Exception {
    resetConf();
    init(tableName);
    int count = 10;
    // create 10 mob files.
    createStoreFiles(basePath, family, qf, count, Type.Put);
    // create 10 del files
    createStoreFiles(basePath, family, qf, count, Type.Delete);
    listFiles();
    List<String> expectedStartKeys = new ArrayList<>();
    for(FileStatus file : mobFiles) {
      if(file.getLen() < mergeSize) {
        String fileName = file.getPath().getName();
        String startKey = fileName.substring(0, 32);
        expectedStartKeys.add(startKey);
      }
    }
    // set the mob compaction mergeable threshold
    conf.setLong(MobConstants.MOB_COMPACTION_MERGEABLE_THRESHOLD, mergeSize);
    testSelectFiles(tableName, type, isForceAllFiles, expectedStartKeys);
  }

  @Test
  public void testCompactDelFilesWithDefaultBatchSize() throws Exception {
    String tableName = "testCompactDelFilesWithDefaultBatchSize";
    testCompactDelFilesAtBatchSize(tableName, MobConstants.DEFAULT_MOB_COMPACTION_BATCH_SIZE,
        MobConstants.DEFAULT_MOB_DELFILE_MAX_COUNT);
  }

  @Test
  public void testCompactDelFilesWithSmallBatchSize() throws Exception {
    String tableName = "testCompactDelFilesWithSmallBatchSize";
    testCompactDelFilesAtBatchSize(tableName, 4, MobConstants.DEFAULT_MOB_DELFILE_MAX_COUNT);
  }

  @Test
  public void testCompactDelFilesChangeMaxDelFileCount() throws Exception {
    String tableName = "testCompactDelFilesWithSmallBatchSize";
    testCompactDelFilesAtBatchSize(tableName, 4, 2);
  }

  /**
   * Create mulitple partition files
   */
  private void createMobFile(Path basePath) throws IOException {
    HFileContext meta = new HFileContextBuilder().withBlockSize(8 * 1024).build();
    MobFileName mobFileName = null;
    int ii = 0;
    Date today = new Date();
    for (byte k0 : KEYS) {
      byte[] startRow = Bytes.toBytes(ii++);

      mobFileName = MobFileName.create(startRow, MobUtils.formatDate(today), mobSuffix);

      HFile.Writer mobFileWriter = 
          HFile.getWriterFactory(conf, cacheConf).withFileContext(meta)
              .withPath(fs, new Path(basePath, mobFileName.getFileName())).create();

      long now = System.currentTimeMillis();
      try {
        for (int i = 0; i < 10; i++) {
          byte[] key = Bytes.add(Bytes.toBytes(k0), Bytes.toBytes(i));
          byte[] dummyData = new byte[5000];
          new Random().nextBytes(dummyData);
          mobFileWriter.append(
              new KeyValue(key, Bytes.toBytes(family), Bytes.toBytes(qf), now, Type.Put, dummyData));
        }
      } finally {
        mobFileWriter.close();
      }
    }
  }

  /**
   * Create mulitple partition delete files
   */
  private void createMobDelFile(Path basePath, int startKey) throws IOException {
    HFileContext meta = new HFileContextBuilder().withBlockSize(8 * 1024).build();
    MobFileName mobFileName = null;
    Date today = new Date();

    byte[] startRow = Bytes.toBytes(startKey);

    mobFileName = MobFileName.create(startRow, MobUtils.formatDate(today), delSuffix);

    HFile.Writer mobFileWriter =
        HFile.getWriterFactory(conf, cacheConf).withFileContext(meta)
            .withPath(fs, new Path(basePath, mobFileName.getFileName())).create();

    long now = System.currentTimeMillis();
    try {
      byte[] key = Bytes.add(Bytes.toBytes(KEYS[startKey]), Bytes.toBytes(0));
      byte[] dummyData = new byte[5000];
      new Random().nextBytes(dummyData);
      mobFileWriter.append(
          new KeyValue(key, Bytes.toBytes(family), Bytes.toBytes(qf), now, Type.Delete, dummyData));
      key = Bytes.add(Bytes.toBytes(KEYS[startKey]), Bytes.toBytes(2));
      mobFileWriter.append(
          new KeyValue(key, Bytes.toBytes(family), Bytes.toBytes(qf), now, Type.Delete, dummyData));
      key = Bytes.add(Bytes.toBytes(KEYS[startKey]), Bytes.toBytes(4));
      mobFileWriter.append(
          new KeyValue(key, Bytes.toBytes(family), Bytes.toBytes(qf), now, Type.Delete, dummyData));

    } finally {
      mobFileWriter.close();
    }
  }

  @Test
  public void testCompactFilesWithoutDelFile() throws Exception {
    String tableName = "testCompactFilesWithoutDelFile";
    resetConf();
    init(tableName);

    createMobFile(basePath);

    listFiles();

    PartitionedMobCompactor compactor = new PartitionedMobCompactor(conf, fs,
        TableName.valueOf(tableName), hcd, pool) {
      @Override
      public List<Path> compact(List<FileStatus> files, boolean isForceAllFiles)
          throws IOException {
        if (files == null || files.isEmpty()) {
          return null;
        }

        PartitionedMobCompactionRequest request = select(files, isForceAllFiles);

        // Make sure that there is no del Partitions
        Assert.assertTrue(request.getDelPartitions().size() == 0);

        // Make sure that when there is no startKey/endKey for partition.
        for (CompactionPartition p : request.getCompactionPartitions()) {
          Assert.assertTrue(p.getStartKey() == null);
          Assert.assertTrue(p.getEndKey() == null);
        }
        return null;
      }
    };

    compactor.compact(allFiles, true);
  }

  static class MyPartitionedMobCompactor extends PartitionedMobCompactor {
    int delPartitionSize = 0;
    int PartitionsIncludeDelFiles = 0;
    CacheConfig cacheConfig = null;

    MyPartitionedMobCompactor(Configuration conf, FileSystem fs, TableName tableName,
        HColumnDescriptor column, ExecutorService pool, final int delPartitionSize,
        final CacheConfig cacheConf, final int PartitionsIncludeDelFiles)
        throws IOException {
      super(conf, fs, tableName, column, pool);
      this.delPartitionSize = delPartitionSize;
      this.cacheConfig = cacheConf;
      this.PartitionsIncludeDelFiles = PartitionsIncludeDelFiles;
    }

    @Override public List<Path> compact(List<FileStatus> files, boolean isForceAllFiles)
        throws IOException {
      if (files == null || files.isEmpty()) {
        return null;
      }
      PartitionedMobCompactionRequest request = select(files, isForceAllFiles);

      Assert.assertTrue(request.getDelPartitions().size() == delPartitionSize);
      if (request.getDelPartitions().size() > 0) {
        for (CompactionPartition p : request.getCompactionPartitions()) {
          Assert.assertTrue(p.getStartKey() != null);
          Assert.assertTrue(p.getEndKey() != null);
        }
      }

      try {
        for (CompactionDelPartition delPartition : request.getDelPartitions()) {
          for (Path newDelPath : delPartition.listDelFiles()) {
            StoreFile sf = new StoreFile(fs, newDelPath, conf, this.cacheConfig, BloomType.NONE);
            // pre-create reader of a del file to avoid race condition when opening the reader in each
            // partition.
            sf.createReader();
            delPartition.addStoreFile(sf);
          }
        }

        // Make sure that CompactionDelPartitions does not overlap
        CompactionDelPartition prevDelP = null;
        for (CompactionDelPartition delP : request.getDelPartitions()) {
          Assert.assertTrue(
              Bytes.compareTo(delP.getId().getStartKey(), delP.getId().getEndKey()) <= 0);

          if (prevDelP != null) {
            Assert.assertTrue(
                Bytes.compareTo(prevDelP.getId().getEndKey(), delP.getId().getStartKey()) < 0);
          }
        }

        int affectedPartitions = 0;

        // Make sure that only del files within key range for a partition is included in compaction.
        // compact the mob files by partitions in parallel.
        for (CompactionPartition partition : request.getCompactionPartitions()) {
          List<StoreFile> delFiles = getListOfDelFilesForPartition(partition, request.getDelPartitions());
          if (!request.getDelPartitions().isEmpty()) {
            if (!((Bytes.compareTo(request.getDelPartitions().get(0).getId().getStartKey(),
                partition.getEndKey()) > 0) || (Bytes.compareTo(
                request.getDelPartitions().get(request.getDelPartitions().size() - 1).getId()
                    .getEndKey(), partition.getStartKey()) < 0))) {

              if (delFiles.size() > 0) {
                Assert.assertTrue(delFiles.size() == 1);
                affectedPartitions += delFiles.size();
//                Assert.assertTrue(Bytes.compareTo(partition.getStartKey(),
//                    delFiles.get(0).getLastKey()) <= 0);
                Assert.assertTrue(Bytes.compareTo(partition.getEndKey(),
                    delFiles.get(delFiles.size() - 1).getFirstKey()) >= 0);
              }
            }
          }
        }
        // The del file is only included in one partition
        Assert.assertTrue(affectedPartitions == PartitionsIncludeDelFiles);
      } finally {
        for (CompactionDelPartition delPartition : request.getDelPartitions()) {
          for (StoreFile storeFile : delPartition.getStoreFiles()) {
            try {
              storeFile.closeReader(true);
            } catch (IOException e) {
              LOG.warn("Failed to close the reader on store file " + storeFile.getPath(), e);
            }
          }
        }
      }

      return null;
    }
  }

  @Test
  public void testCompactFilesWithOneDelFile() throws Exception {
    String tableName = "testCompactFilesWithOneDelFile";
    resetConf();
    init(tableName);

    // Create only del file.
    createMobFile(basePath);
    createMobDelFile(basePath, 2);

    listFiles();

    MyPartitionedMobCompactor compactor = new MyPartitionedMobCompactor(conf, fs,
        TableName.valueOf(tableName), hcd, pool, 1, cacheConf, 1);

    compactor.compact(allFiles, true);
  }

  @Test
  public void testCompactFilesWithMultiDelFiles() throws Exception {
    String tableName = "testCompactFilesWithMultiDelFiles";
    resetConf();
    init(tableName);

    // Create only del file.
    createMobFile(basePath);
    createMobDelFile(basePath, 0);
    createMobDelFile(basePath, 1);
    createMobDelFile(basePath, 2);

    listFiles();

    MyPartitionedMobCompactor compactor = new MyPartitionedMobCompactor(conf, fs,
        TableName.valueOf(tableName), hcd, pool, 3, cacheConf, 3);
    compactor.compact(allFiles, true);
  }

  private void testCompactDelFilesAtBatchSize(String tableName, int batchSize,
      int delfileMaxCount)  throws Exception {
    resetConf();
    init(tableName);
    // create 20 mob files.
    createStoreFiles(basePath, family, qf, 20, Type.Put);
    // create 13 del files
    createStoreFiles(basePath, family, qf, 13, Type.Delete);
    listFiles();

    // set the max del file count
    conf.setInt(MobConstants.MOB_DELFILE_MAX_COUNT, delfileMaxCount);
    // set the mob compaction batch size
    conf.setInt(MobConstants.MOB_COMPACTION_BATCH_SIZE, batchSize);
    testCompactDelFiles(tableName, 1, 13, true);
  }

  /**
   * Tests the selectFiles
   * @param tableName the table name
   * @param type the expected compaction type
   * @param isForceAllFiles whether all the mob files are selected
   * @param expected the expected start keys
   */
  private void testSelectFiles(String tableName, final CompactionType type,
    final boolean isForceAllFiles, final List<String> expected) throws IOException {
    PartitionedMobCompactor compactor = new PartitionedMobCompactor(conf, fs,
      TableName.valueOf(tableName), hcd, pool) {
      @Override
      public List<Path> compact(List<FileStatus> files, boolean isForceAllFiles)
        throws IOException {
        if (files == null || files.isEmpty()) {
          return null;
        }
        PartitionedMobCompactionRequest request = select(files, isForceAllFiles);

        // Make sure that when there is no del files, there will be no startKey/endKey for partition.
        if (request.getDelPartitions().size() == 0) {
          for (CompactionPartition p : request.getCompactionPartitions()) {
            Assert.assertTrue(p.getStartKey() == null);
            Assert.assertTrue(p.getEndKey() == null);
          }
        }

        // Make sure that CompactionDelPartitions does not overlap
        CompactionDelPartition prevDelP = null;
        for (CompactionDelPartition delP : request.getDelPartitions()) {
          Assert.assertTrue(Bytes.compareTo(delP.getId().getStartKey(),
              delP.getId().getEndKey()) <= 0);
          if (prevDelP != null) {
            Assert.assertTrue(Bytes.compareTo(prevDelP.getId().getEndKey(),
                delP.getId().getStartKey()) < 0);
          }
        }

        // Make sure that only del files within key range for a partition is included in compaction.
        // compact the mob files by partitions in parallel.
        for (CompactionPartition partition : request.getCompactionPartitions()) {
          List<StoreFile> delFiles = getListOfDelFilesForPartition(partition, request.getDelPartitions());
          if (!request.getDelPartitions().isEmpty()) {
            if (!((Bytes.compareTo(request.getDelPartitions().get(0).getId().getStartKey(),
                partition.getEndKey()) > 0) || (Bytes.compareTo(
                request.getDelPartitions().get(request.getDelPartitions().size() - 1).getId()
                    .getEndKey(), partition.getStartKey()) < 0))) {
              if (delFiles.size() > 0) {
                Assert.assertTrue(Bytes
                    .compareTo(partition.getStartKey(), delFiles.get(0).getFirstKey()) >= 0);
                Assert.assertTrue(Bytes.compareTo(partition.getEndKey(),
                    delFiles.get(delFiles.size() - 1).getLastKey()) <= 0);
              }
            }
          }
        }
        // assert the compaction type
        Assert.assertEquals(type, request.type);
        // assert get the right partitions
        compareCompactedPartitions(expected, request.compactionPartitions);
        // assert get the right del files
        // compareDelFiles(request.getDelPartitions());
        return null;
      }
    };
    compactor.compact(allFiles, isForceAllFiles);
  }

  /**
   * Tests the compacteDelFile
   * @param tableName the table name
   * @param expectedFileCount the expected file count
   * @param expectedCellCount the expected cell count
   * @param isForceAllFiles whether all the mob files are selected
   */
  private void testCompactDelFiles(String tableName, final int expectedFileCount,
      final int expectedCellCount, boolean isForceAllFiles) throws IOException {
    PartitionedMobCompactor compactor = new PartitionedMobCompactor(conf, fs,
      TableName.valueOf(tableName), hcd, pool) {
      @Override
      protected List<Path> performCompaction(PartitionedMobCompactionRequest request)
          throws IOException {
        List<Path> delFilePaths = new ArrayList<Path>();
        for (CompactionDelPartition delPartition: request.getDelPartitions()) {
          for (Path p : delPartition.listDelFiles()) {
            delFilePaths.add(p);
          }
        }
        List<Path> newDelPaths = compactDelFiles(request, delFilePaths);
        // assert the del files are merged.
        Assert.assertEquals(expectedFileCount, newDelPaths.size());
        Assert.assertEquals(expectedCellCount, countDelCellsInDelFiles(newDelPaths));
        return null;
      }
    };
    compactor.compact(allFiles, isForceAllFiles);
  }

  /**
   * Lists the files in the path
   */
  private void listFiles() throws IOException {
    for (FileStatus file : fs.listStatus(basePath)) {
      String name = file.getPath().getName();
      if(file.isDirectory() && !MobConstants.MOB_DELFILE_DIR_NAME.equals(name)) {
        for(FileStatus f : fs.listStatus(file.getPath())){
          allFiles.add(f);
          mobFiles.add(f);
        }
      } else if (file.isDirectory() && MobConstants.MOB_DELFILE_DIR_NAME.equals(name)){
        for(FileStatus f : fs.listStatus(file.getPath())){
          allFiles.add(f);
          delFiles.add(f.getPath());
        }
      }
    }
  }

  /**
   * Compares the compacted partitions.
   * @param partitions the collection of CompactedPartitions
   */
  private void compareCompactedPartitions(List<String> expected,
      Collection<CompactionPartition> partitions) {
    List<String> actualKeys = new ArrayList<>();
    for (CompactionPartition partition : partitions) {
      actualKeys.add(partition.getPartitionId().getStartKey());
    }
    Collections.sort(expected);
    Collections.sort(actualKeys);
    Assert.assertEquals(expected.size(), actualKeys.size());
    for (int i = 0; i < expected.size(); i++) {
      Assert.assertEquals(expected.get(i), actualKeys.get(i));
    }
  }

  /**
   * Compares the del files.
   * @param delPartitions all del partitions
   */
  private void compareDelFiles(List<CompactionDelPartition> delPartitions) {
    Map<Path, Path> delMap = new HashMap<>();
    for (CompactionDelPartition delPartition : delPartitions) {
      for (Path f : delPartition.listDelFiles()) {
        delMap.put(f, f);
      }
    }
    for (Path f : delFiles) {
      Assert.assertTrue(delMap.containsKey(f));
    }
  }

  /**
   * Creates store files.
   * @param basePath the path to create file
   * @family the family name
   * @qualifier the column qualifier
   * @count the store file number
   * @type the key type
   */
  private void createStoreFiles(Path basePath, String family, String qualifier, int count,
      Type type) throws IOException {
    HFileContext meta = new HFileContextBuilder().withBlockSize(8 * 1024).build();
    String startKey = "row_";
    Path parentPath = null;
    if(type.equals(Type.Delete)) {
      parentPath = new Path(basePath, MobConstants.MOB_DELFILE_DIR_NAME);
    } else if(type.equals(Type.Put)){
      parentPath = new Path(basePath, MobUtils.formatDate(new Date()));
    }
    MobFileName mobFileName = null;
    for (int i = 0; i < count; i++) {
      byte[] startRow = Bytes.toBytes(startKey + i) ;
      if(type.equals(Type.Delete)) {
        mobFileName = MobFileName.create(startRow, MobUtils.formatDate(
            new Date()), delSuffix);
      }
      if(type.equals(Type.Put)){
        mobFileName = MobFileName.create(Bytes.toBytes(startKey + i), MobUtils.formatDate(
            new Date()), mobSuffix);
      }
      StoreFile.Writer mobFileWriter = new StoreFile.WriterBuilder(conf, cacheConf, fs)
      .withFileContext(meta).withFilePath(new Path(parentPath, mobFileName.getFileName())).build();
      writeStoreFile(mobFileWriter, startRow, Bytes.toBytes(family), Bytes.toBytes(qualifier),
          type, (i+1)*1000);
    }
  }

  /**
   * Writes data to store file.
   * @param writer the store file writer
   * @param row the row key
   * @param family the family name
   * @param qualifier the column qualifier
   * @param type the key type
   * @param size the size of value
   */
  private static void writeStoreFile(final StoreFile.Writer writer, byte[]row, byte[] family,
      byte[] qualifier, Type type, int size) throws IOException {
    long now = System.currentTimeMillis();
    try {
      byte[] dummyData = new byte[size];
      new Random().nextBytes(dummyData);
      writer.append(new KeyValue(row, family, qualifier, now, type, dummyData));
    } finally {
      writer.close();
    }
  }

  /**
   * Gets the number of del cell in the del files
   * @param paths the del file paths
   * @return the cell size
   */
  private int countDelCellsInDelFiles(List<Path> paths) throws IOException {
    List<StoreFile> sfs = new ArrayList<StoreFile>();
    int size = 0;
    for(Path path : paths) {
      StoreFile sf = new StoreFile(fs, path, conf, cacheConf, BloomType.NONE);
      sfs.add(sf);
    }
    List scanners = StoreFileScanner.getScannersForStoreFiles(sfs, false, true,
        false, false, HConstants.LATEST_TIMESTAMP);
    Scan scan = new Scan();
    scan.setMaxVersions(hcd.getMaxVersions());
    long timeToPurgeDeletes = Math.max(conf.getLong("hbase.hstore.time.to.purge.deletes", 0), 0);
    long ttl = HStore.determineTTLFromFamily(hcd);
    ScanInfo scanInfo = new ScanInfo(conf, hcd, ttl, timeToPurgeDeletes, KeyValue.COMPARATOR);
    StoreScanner scanner = new StoreScanner(scan, scanInfo, ScanType.COMPACT_RETAIN_DELETES, null,
        scanners, 0L, HConstants.LATEST_TIMESTAMP);
    List<Cell> results = new ArrayList<>();
    boolean hasMore = true;

    while (hasMore) {
      hasMore = scanner.next(results);
      size += results.size();
      results.clear();
    }
    scanner.close();
    return size;
  }

  private static ExecutorService createThreadPool() {
    int maxThreads = 10;
    long keepAliveTime = 60;
    final SynchronousQueue<Runnable> queue = new SynchronousQueue<Runnable>();
    ThreadPoolExecutor pool = new ThreadPoolExecutor(1, maxThreads, keepAliveTime,
      TimeUnit.SECONDS, queue, Threads.newDaemonThreadFactory("MobFileCompactionChore"),
      new RejectedExecutionHandler() {
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
          try {
            // waiting for a thread to pick up instead of throwing exceptions.
            queue.put(r);
          } catch (InterruptedException e) {
            throw new RejectedExecutionException(e);
          }
        }
      });
    ((ThreadPoolExecutor) pool).allowCoreThreadTimeOut(true);
    return pool;
  }

  /**
   * Resets the configuration.
   */
  private void resetConf() {
    conf.setLong(MobConstants.MOB_COMPACTION_MERGEABLE_THRESHOLD,
      MobConstants.DEFAULT_MOB_COMPACTION_MERGEABLE_THRESHOLD);
    conf.setInt(MobConstants.MOB_DELFILE_MAX_COUNT, MobConstants.DEFAULT_MOB_DELFILE_MAX_COUNT);
    conf.setInt(MobConstants.MOB_COMPACTION_BATCH_SIZE,
      MobConstants.DEFAULT_MOB_COMPACTION_BATCH_SIZE);
  }
}
