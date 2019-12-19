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

import static org.apache.hadoop.hbase.io.ByteBuffAllocator.BUFFER_SIZE_KEY;
import static org.apache.hadoop.hbase.io.ByteBuffAllocator.MAX_BUFFER_COUNT_KEY;
import static org.apache.hadoop.hbase.io.ByteBuffAllocator.MIN_ALLOCATE_SIZE_KEY;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ArrayBackedTag;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.hfile.HFile.Reader;
import org.apache.hadoop.hbase.io.hfile.HFile.Writer;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * test hfile features.
 * <p>
 * Copied from
 * <a href="https://issues.apache.org/jira/browse/HADOOP-3315">hadoop-3315 tfile</a>.
 * Remove after tfile is committed and use the tfile version of this class
 * instead.</p>
 */
@Category(SmallTests.class)
public class TestHFile extends HBaseTestCase {
  private static final Log LOG = LogFactory.getLog(TestHFile.class);

  private static final int NUM_VALID_KEY_TYPES = KeyValue.Type.values().length - 2;
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static String ROOT_DIR =
    TEST_UTIL.getDataTestDir("TestHFile").toString();
  private final int minBlockSize = 512;
  private static String localFormatter = "%010d";
  private static CacheConfig cacheConf = null;

  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }

  private ByteBuffAllocator initAllocator(boolean reservoirEnabled, int bufSize, int bufCount,
      int minAllocSize) {
    Configuration that = HBaseConfiguration.create(conf);
    that.setInt(BUFFER_SIZE_KEY, bufSize);
    that.setInt(MAX_BUFFER_COUNT_KEY, bufCount);
    // All ByteBuffers will be allocated from the buffers.
    that.setInt(MIN_ALLOCATE_SIZE_KEY, minAllocSize);
    return ByteBuffAllocator.create(that, reservoirEnabled);
  }

  private void fillByteBuffAllocator(ByteBuffAllocator alloc, int bufCount) {
    // Fill the allocator with bufCount ByteBuffer
    List<ByteBuff> buffs = new ArrayList<>();
    for (int i = 0; i < bufCount; i++) {
      buffs.add(alloc.allocateOneBuffer());
      Assert.assertEquals(alloc.getFreeBufferCount(), 0);
    }
    buffs.forEach(ByteBuff::release);
    Assert.assertEquals(alloc.getFreeBufferCount(), bufCount);
  }

  @Test
  public void testReaderWithoutBlockCache() throws Exception {
    int bufCount = 32;
    // AllByteBuffers will be allocated from the buffers.
    ByteBuffAllocator alloc = initAllocator(true, 64 * 1024, bufCount, 0);
    fillByteBuffAllocator(alloc, bufCount);
    // start write to store file.
    Path path = writeStoreFile();
    try {
      readStoreFile(path, conf, alloc);
    } catch (Exception e) {
      // fail test
      assertTrue(false);
    }
    Assert.assertEquals(bufCount, alloc.getFreeBufferCount());
    alloc.clean();
  }

  /**
   * Test case for HBASE-22127 in LruBlockCache.
   */
  @Test
  public void testReaderWithLRUBlockCache() throws Exception {
    int bufCount = 1024, blockSize = 64 * 1024;
    ByteBuffAllocator alloc = initAllocator(true, bufCount, blockSize, 0);
    fillByteBuffAllocator(alloc, bufCount);
    Path storeFilePath = writeStoreFile();
    // Open the file reader with LRUBlockCache
    BlockCache lru = new LruBlockCache(1024 * 1024 * 32, blockSize, true, conf);
    CacheConfig cacheConfig = new CacheConfig(conf, null, lru, alloc);
    HFile.Reader reader = HFile.createReader(fs, storeFilePath, cacheConfig, true, conf);
    long offset = 0;
    while (offset < reader.getTrailer().getLoadOnOpenDataOffset()) {
      BlockCacheKey key = new BlockCacheKey(storeFilePath.getName(), offset);
      HFileBlock block = reader.readBlock(offset, -1, true, true, false, true, null, null);
      offset += block.getOnDiskSizeWithHeader();
      // Ensure the block is an heap one.
      Cacheable cachedBlock = lru.getBlock(key, false, false, true);
      Assert.assertNotNull(cachedBlock);
      Assert.assertTrue(cachedBlock instanceof HFileBlock);
      Assert.assertFalse(((HFileBlock) cachedBlock).isSharedMem());
      // Should never allocate off-heap block from allocator because ensure that it's LRU.
      Assert.assertEquals(bufCount, alloc.getFreeBufferCount());
      block.release(); // return back the ByteBuffer back to allocator.
    }
    reader.close();
    Assert.assertEquals(bufCount, alloc.getFreeBufferCount());
    alloc.clean();
    lru.shutdown();
  }

  private void readStoreFile(Path storeFilePath, Configuration conf, ByteBuffAllocator alloc)
      throws Exception {
    // Open the file reader with block cache disabled.
    CacheConfig cache = new CacheConfig(conf, null, alloc);
    HFile.Reader reader = HFile.createReader(fs, storeFilePath, cache, true, conf);
    long offset = 0;
    while (offset < reader.getTrailer().getLoadOnOpenDataOffset()) {
      HFileBlock block = reader.readBlock(offset, -1, false, true, false, true, null, null);
      offset += block.getOnDiskSizeWithHeader();
      block.release(); // return back the ByteBuffer back to allocator.
    }
    reader.close();
  }

  private Path writeStoreFile() throws IOException {
    Path storeFileParentDir = new Path(TEST_UTIL.getDataTestDir(), "TestHFile");
    HFileContext meta = new HFileContextBuilder().withBlockSize(64 * 1024).build();
    StoreFile.Writer sfw =
        new StoreFile.WriterBuilder(conf, fs).withOutputDir(storeFileParentDir)
            .withComparator(CellComparator.COMPARATOR).withFileContext(meta).build();

    final int rowLen = 32;
    Random RNG = new Random();
    for (int i = 0; i < 1000; ++i) {
      byte[] k = RandomKeyValueUtil.randomOrderedKey(RNG, i);
      byte[] v = RandomKeyValueUtil.randomValue(RNG);
      int cfLen = RNG.nextInt(k.length - rowLen + 1);
      KeyValue kv =
          new KeyValue(k, 0, rowLen, k, rowLen, cfLen, k, rowLen + cfLen,
              k.length - rowLen - cfLen, RNG.nextLong(), generateKeyType(RNG), v, 0, v.length);
      sfw.append(kv);
    }

    sfw.close();
    return sfw.getPath();
  }

  public static KeyValue.Type generateKeyType(Random rand) {
    if (rand.nextBoolean()) {
      // Let's make half of KVs puts.
      return KeyValue.Type.Put;
    } else {
      KeyValue.Type keyType = KeyValue.Type.values()[1 + rand.nextInt(NUM_VALID_KEY_TYPES)];
      if (keyType == KeyValue.Type.Minimum || keyType == KeyValue.Type.Maximum) {
        throw new RuntimeException("Generated an invalid key type: " + keyType + ". "
            + "Probably the layout of KeyValue.Type has changed.");
      }
      return keyType;
    }
  }

  /**
   * Test empty HFile.
   * Test all features work reasonably when hfile is empty of entries.
   * @throws IOException
   */
  public void testEmptyHFile() throws IOException {
    if (cacheConf == null) cacheConf = new CacheConfig(conf);
    Path f = new Path(ROOT_DIR, getName());
    HFileContext context = new HFileContextBuilder().withIncludesTags(false).build();
    Writer w =
        HFile.getWriterFactory(conf, cacheConf).withPath(fs, f).withFileContext(context).create();
    w.close();
    Reader r = HFile.createReader(fs, f, cacheConf, true, conf);
    r.loadFileInfo();
    assertNull(r.getFirstKey());
    assertNull(r.getLastKey());
  }

  /**
   * Create 0-length hfile and show that it fails
   */
  public void testCorrupt0LengthHFile() throws IOException {
    if (cacheConf == null) cacheConf = new CacheConfig(conf);
    Path f = new Path(ROOT_DIR, getName());
    FSDataOutputStream fsos = fs.create(f);
    fsos.close();

    try {
      Reader r = HFile.createReader(fs, f, cacheConf, true, conf);
    } catch (CorruptHFileException che) {
      // Expected failure
      return;
    }
    fail("Should have thrown exception");
  }

  public static void truncateFile(FileSystem fs, Path src, Path dst) throws IOException {
    FileStatus fst = fs.getFileStatus(src);
    long len = fst.getLen();
    len = len / 2 ;

    // create a truncated hfile
    FSDataOutputStream fdos = fs.create(dst);
    byte[] buf = new byte[(int)len];
    FSDataInputStream fdis = fs.open(src);
    fdis.read(buf);
    fdos.write(buf);
    fdis.close();
    fdos.close();
  }

  /**
   * Create a truncated hfile and verify that exception thrown.
   */
  public void testCorruptTruncatedHFile() throws IOException {
    if (cacheConf == null) cacheConf = new CacheConfig(conf);
    Path f = new Path(ROOT_DIR, getName());
    HFileContext  context = new HFileContextBuilder().build();
    Writer w = HFile.getWriterFactory(conf, cacheConf).withPath(this.fs, f)
        .withFileContext(context).create();
    writeSomeRecords(w, 0, 100, false);
    w.close();

    Path trunc = new Path(f.getParent(), "trucated");
    truncateFile(fs, w.getPath(), trunc);

    try {
      Reader r = HFile.createReader(fs, trunc, cacheConf, true, conf);
    } catch (CorruptHFileException che) {
      // Expected failure
      return;
    }
    fail("Should have thrown exception");
  }

  // write some records into the tfile
  // write them twice
  private int writeSomeRecords(Writer writer, int start, int n, boolean useTags)
      throws IOException {
    String value = "value";
    KeyValue kv;
    for (int i = start; i < (start + n); i++) {
      String key = String.format(localFormatter, Integer.valueOf(i));
      if (useTags) {
        Tag t = new ArrayBackedTag((byte) 1, "myTag1");
        Tag[] tags = new Tag[1];
        tags[0] = t;
        kv = new KeyValue(Bytes.toBytes(key), Bytes.toBytes("family"), Bytes.toBytes("qual"),
            HConstants.LATEST_TIMESTAMP, Bytes.toBytes(value + key), tags);
        writer.append(kv);
      } else {
        kv = new KeyValue(Bytes.toBytes(key), Bytes.toBytes("family"), Bytes.toBytes("qual"),
            Bytes.toBytes(value + key));
        writer.append(kv);
      }
    }
    return (start + n);
  }

  private void readAllRecords(HFileScanner scanner) throws IOException {
    readAndCheckbytes(scanner, 0, 100);
  }

  // read the records and check
  private int readAndCheckbytes(HFileScanner scanner, int start, int n)
      throws IOException {
    String value = "value";
    int i = start;
    for (; i < (start + n); i++) {
      ByteBuffer key = ByteBuffer.wrap(((KeyValue)scanner.getKey()).getKey());
      ByteBuffer val = scanner.getValue();
      String keyStr = String.format(localFormatter, Integer.valueOf(i));
      String valStr = value + keyStr;
      KeyValue kv = new KeyValue(Bytes.toBytes(keyStr), Bytes.toBytes("family"),
          Bytes.toBytes("qual"), Bytes.toBytes(valStr));
      byte[] keyBytes = new KeyValue.KeyOnlyKeyValue(Bytes.toBytes(key), 0,
          Bytes.toBytes(key).length).getKey();
      assertTrue("bytes for keys do not match " + keyStr + " " +
        Bytes.toString(Bytes.toBytes(key)),
          Arrays.equals(kv.getKey(), keyBytes));
      byte [] valBytes = Bytes.toBytes(val);
      assertTrue("bytes for vals do not match " + valStr + " " +
        Bytes.toString(valBytes),
        Arrays.equals(Bytes.toBytes(valStr), valBytes));
      if (!scanner.next()) {
        break;
      }
    }
    assertEquals(i, start + n - 1);
    return (start + n);
  }

  private byte[] getSomeKey(int rowId) {
    KeyValue kv = new KeyValue(String.format(localFormatter, Integer.valueOf(rowId)).getBytes(),
        Bytes.toBytes("family"), Bytes.toBytes("qual"), HConstants.LATEST_TIMESTAMP, Type.Put);
    return kv.getKey();
  }

  private void writeRecords(Writer writer, boolean useTags) throws IOException {
    writeSomeRecords(writer, 0, 100, useTags);
    writer.close();
  }

  private FSDataOutputStream createFSOutput(Path name) throws IOException {
    //if (fs.exists(name)) fs.delete(name, true);
    FSDataOutputStream fout = fs.create(name);
    return fout;
  }

  /**
   * test none codecs
   * @param useTags 
   */
  void basicWithSomeCodec(String codec, boolean useTags) throws IOException {
    if (useTags) {
      conf.setInt("hfile.format.version", 3);
    }
    if (cacheConf == null) cacheConf = new CacheConfig(conf);
    Path ncTFile = new Path(ROOT_DIR, "basic.hfile." + codec.toString() + useTags);
    FSDataOutputStream fout = createFSOutput(ncTFile);
    HFileContext meta = new HFileContextBuilder()
                        .withBlockSize(minBlockSize)
                        .withCompression(HFileWriterImpl.compressionByName(codec))
                        .build();
    Writer writer = HFile.getWriterFactory(conf, cacheConf)
        .withOutputStream(fout)
        .withFileContext(meta)
        .withComparator(CellComparator.COMPARATOR)
        .create();
    LOG.info(writer);
    writeRecords(writer, useTags);
    fout.close();
    FSDataInputStream fin = fs.open(ncTFile);
    Reader reader = HFile.createReaderFromStream(ncTFile, fs.open(ncTFile),
      fs.getFileStatus(ncTFile).getLen(), cacheConf, conf);
    System.out.println(cacheConf.toString());
    // Load up the index.
    reader.loadFileInfo();
    // Get a scanner that caches and that does not use pread.
    HFileScanner scanner = reader.getScanner(true, false);
    // Align scanner at start of the file.
    scanner.seekTo();
    readAllRecords(scanner);
    int seekTo = scanner.seekTo(KeyValueUtil.createKeyValueFromKey(getSomeKey(50)));
    System.out.println(seekTo);
    assertTrue("location lookup failed",
        scanner.seekTo(KeyValueUtil.createKeyValueFromKey(getSomeKey(50))) == 0);
    // read the key and see if it matches
    ByteBuffer readKey = ByteBuffer.wrap(((KeyValue)scanner.getKey()).getKey());
    assertTrue("seeked key does not match", Arrays.equals(getSomeKey(50),
      Bytes.toBytes(readKey)));

    scanner.seekTo(KeyValueUtil.createKeyValueFromKey(getSomeKey(0)));
    ByteBuffer val1 = scanner.getValue();
    scanner.seekTo(KeyValueUtil.createKeyValueFromKey(getSomeKey(0)));
    ByteBuffer val2 = scanner.getValue();
    assertTrue(Arrays.equals(Bytes.toBytes(val1), Bytes.toBytes(val2)));

    reader.close();
    fin.close();
    fs.delete(ncTFile, true);
  }

  public void testTFileFeatures() throws IOException {
    testTFilefeaturesInternals(false);
    testTFilefeaturesInternals(true);
  }

  protected void testTFilefeaturesInternals(boolean useTags) throws IOException {
    basicWithSomeCodec("none", useTags);
    basicWithSomeCodec("gz", useTags);
  }

  private void writeNumMetablocks(Writer writer, int n) {
    for (int i = 0; i < n; i++) {
      writer.appendMetaBlock("HFileMeta" + i, new Writable() {
        private int val;
        public Writable setVal(int val) { this.val = val; return this; }
        
        @Override
        public void write(DataOutput out) throws IOException {
          out.write(("something to test" + val).getBytes());
        }
        
        @Override
        public void readFields(DataInput in) throws IOException { }
      }.setVal(i));
    }
  }

  private void someTestingWithMetaBlock(Writer writer) {
    writeNumMetablocks(writer, 10);
  }

  private void readNumMetablocks(Reader reader, int n) throws IOException {
    for (int i = 0; i < n; i++) {
      ByteBuff actual = reader.getMetaBlock("HFileMeta" + i, false).getBufferWithoutHeader();
      ByteBuffer expected = 
        ByteBuffer.wrap(("something to test" + i).getBytes());
      assertEquals(
          "failed to match metadata",
          Bytes.toStringBinary(expected),
          Bytes.toStringBinary(actual.array(), actual.arrayOffset() + actual.position(),
              actual.capacity()));
    }
  }

  private void someReadingWithMetaBlock(Reader reader) throws IOException {
    readNumMetablocks(reader, 10);
  }

  private void metablocks(final String compress) throws Exception {
    if (cacheConf == null) cacheConf = new CacheConfig(conf);
    Path mFile = new Path(ROOT_DIR, "meta.hfile");
    FSDataOutputStream fout = createFSOutput(mFile);
    HFileContext meta = new HFileContextBuilder()
                        .withCompression(HFileWriterImpl.compressionByName(compress))
                        .withBlockSize(minBlockSize).build();
    Writer writer = HFile.getWriterFactory(conf, cacheConf)
        .withOutputStream(fout)
        .withFileContext(meta)
        .create();
    someTestingWithMetaBlock(writer);
    writer.close();
    fout.close();
    FSDataInputStream fin = fs.open(mFile);
    Reader reader = HFile.createReaderFromStream(mFile, fs.open(mFile),
        this.fs.getFileStatus(mFile).getLen(), cacheConf, conf);
    reader.loadFileInfo();
    // No data -- this should return false.
    assertFalse(reader.getScanner(false, false).seekTo());
    someReadingWithMetaBlock(reader);
    fs.delete(mFile, true);
    reader.close();
    fin.close();
  }

  // test meta blocks for tfiles
  public void testMetaBlocks() throws Exception {
    metablocks("none");
    metablocks("gz");
  }

  public void testNullMetaBlocks() throws Exception {
    if (cacheConf == null) cacheConf = new CacheConfig(conf);
    for (Compression.Algorithm compressAlgo : 
        HBaseTestingUtility.COMPRESSION_ALGORITHMS) {
      Path mFile = new Path(ROOT_DIR, "nometa_" + compressAlgo + ".hfile");
      FSDataOutputStream fout = createFSOutput(mFile);
      HFileContext meta = new HFileContextBuilder().withCompression(compressAlgo)
                          .withBlockSize(minBlockSize).build();
      Writer writer = HFile.getWriterFactory(conf, cacheConf)
          .withOutputStream(fout)
          .withFileContext(meta)
          .create();
      KeyValue kv = new KeyValue("foo".getBytes(), "f1".getBytes(), null, "value".getBytes());
      writer.append(kv);
      writer.close();
      fout.close();
      Reader reader = HFile.createReader(fs, mFile, cacheConf, true, conf);
      reader.loadFileInfo();
      assertNull(reader.getMetaBlock("non-existant", false));
    }
  }

  /**
   * Make sure the ordinals for our compression algorithms do not change on us.
   */
  public void testCompressionOrdinance() {
    assertTrue(Compression.Algorithm.LZO.ordinal() == 0);
    assertTrue(Compression.Algorithm.GZ.ordinal() == 1);
    assertTrue(Compression.Algorithm.NONE.ordinal() == 2);
    assertTrue(Compression.Algorithm.SNAPPY.ordinal() == 3);
    assertTrue(Compression.Algorithm.LZ4.ordinal() == 4);
  }

  @Test
  public void testGetShortMidpoint() {
    Cell left = CellUtil.createCell(Bytes.toBytes("a"), Bytes.toBytes("a"), Bytes.toBytes("a"));
    Cell right = CellUtil.createCell(Bytes.toBytes("a"), Bytes.toBytes("a"), Bytes.toBytes("a"));
    Cell mid = HFileWriterImpl.getMidpoint(CellComparator.COMPARATOR, left, right);
    assertTrue(CellComparator.COMPARATOR.compareKeyIgnoresMvcc(left, mid) <= 0);
    assertTrue(CellComparator.COMPARATOR.compareKeyIgnoresMvcc(mid, right) <= 0);

    left = CellUtil.createCell(Bytes.toBytes("a"), Bytes.toBytes("a"), Bytes.toBytes("a"));
    right = CellUtil.createCell(Bytes.toBytes("b"), Bytes.toBytes("a"), Bytes.toBytes("a"));
    mid = HFileWriterImpl.getMidpoint(CellComparator.COMPARATOR, left, right);
    assertTrue(CellComparator.COMPARATOR.compareKeyIgnoresMvcc(left, mid) < 0);
    assertTrue(CellComparator.COMPARATOR.compareKeyIgnoresMvcc(mid, right) <= 0);

    left = CellUtil.createCell(Bytes.toBytes("g"), Bytes.toBytes("a"), Bytes.toBytes("a"));
    right = CellUtil.createCell(Bytes.toBytes("i"), Bytes.toBytes("a"), Bytes.toBytes("a"));
    mid = HFileWriterImpl.getMidpoint(CellComparator.COMPARATOR, left, right);
    assertTrue(CellComparator.COMPARATOR.compareKeyIgnoresMvcc(left, mid) < 0);
    assertTrue(CellComparator.COMPARATOR.compareKeyIgnoresMvcc(mid, right) <= 0);

    left = CellUtil.createCell(Bytes.toBytes("a"), Bytes.toBytes("a"), Bytes.toBytes("a"));
    right = CellUtil.createCell(Bytes.toBytes("bbbbbbb"), Bytes.toBytes("a"), Bytes.toBytes("a"));
    mid = HFileWriterImpl.getMidpoint(CellComparator.COMPARATOR, left, right);
    assertTrue(CellComparator.COMPARATOR.compareKeyIgnoresMvcc(left, mid) < 0);
    assertTrue(CellComparator.COMPARATOR.compareKeyIgnoresMvcc(mid, right) < 0);
    assertEquals(1, mid.getRowLength());

    left = CellUtil.createCell(Bytes.toBytes("a"), Bytes.toBytes("a"), Bytes.toBytes("a"));
    right = CellUtil.createCell(Bytes.toBytes("a"), Bytes.toBytes("b"), Bytes.toBytes("a"));
    mid = HFileWriterImpl.getMidpoint(CellComparator.COMPARATOR, left, right);
    assertTrue(CellComparator.COMPARATOR.compareKeyIgnoresMvcc(left, mid) < 0);
    assertTrue(CellComparator.COMPARATOR.compareKeyIgnoresMvcc(mid, right) <= 0);

    left = CellUtil.createCell(Bytes.toBytes("a"), Bytes.toBytes("a"), Bytes.toBytes("a"));
    right = CellUtil.createCell(Bytes.toBytes("a"), Bytes.toBytes("aaaaaaaa"), Bytes.toBytes("b"));
    mid = HFileWriterImpl.getMidpoint(CellComparator.COMPARATOR, left, right);
    assertTrue(CellComparator.COMPARATOR.compareKeyIgnoresMvcc(left, mid) < 0);
    assertTrue(CellComparator.COMPARATOR.compareKeyIgnoresMvcc(mid, right) < 0);
    assertEquals(2, mid.getFamilyLength());

    left = CellUtil.createCell(Bytes.toBytes("a"), Bytes.toBytes("a"), Bytes.toBytes("a"));
    right = CellUtil.createCell(Bytes.toBytes("a"), Bytes.toBytes("a"), Bytes.toBytes("aaaaaaaaa"));
    mid = HFileWriterImpl.getMidpoint(CellComparator.COMPARATOR, left, right);
    assertTrue(CellComparator.COMPARATOR.compareKeyIgnoresMvcc(left, mid) < 0);
    assertTrue(CellComparator.COMPARATOR.compareKeyIgnoresMvcc(mid, right) < 0);
    assertEquals(2, mid.getQualifierLength());

    left = CellUtil.createCell(Bytes.toBytes("a"), Bytes.toBytes("a"), Bytes.toBytes("a"));
    right = CellUtil.createCell(Bytes.toBytes("a"), Bytes.toBytes("a"), Bytes.toBytes("b"));
    mid = HFileWriterImpl.getMidpoint(CellComparator.COMPARATOR, left, right);
    assertTrue(CellComparator.COMPARATOR.compareKeyIgnoresMvcc(left, mid) < 0);
    assertTrue(CellComparator.COMPARATOR.compareKeyIgnoresMvcc(mid, right) <= 0);
    assertEquals(1, mid.getQualifierLength());

    // Assert that if meta comparator, it returns the right cell -- i.e. no
    // optimization done.
    left = CellUtil.createCell(Bytes.toBytes("g"), Bytes.toBytes("a"), Bytes.toBytes("a"));
    right = CellUtil.createCell(Bytes.toBytes("i"), Bytes.toBytes("a"), Bytes.toBytes("a"));
    mid = HFileWriterImpl.getMidpoint(CellComparator.META_COMPARATOR, left, right);
    assertTrue(CellComparator.META_COMPARATOR.compareKeyIgnoresMvcc(left, mid) < 0);
    assertTrue(CellComparator.META_COMPARATOR.compareKeyIgnoresMvcc(mid, right) == 0);

    /**
     * See HBASE-7845
     */
    byte[] rowA = Bytes.toBytes("rowA");
    byte[] rowB = Bytes.toBytes("rowB");

    byte[] family = Bytes.toBytes("family");
    byte[] qualA = Bytes.toBytes("qfA");
    byte[] qualB = Bytes.toBytes("qfB");
    final CellComparator keyComparator = CellComparator.COMPARATOR;
    // verify that faked shorter rowkey could be generated
    long ts = 5;
    KeyValue kv1 = new KeyValue(Bytes.toBytes("the quick brown fox"), family, qualA, ts, Type.Put);
    KeyValue kv2 = new KeyValue(Bytes.toBytes("the who test text"), family, qualA, ts, Type.Put);
    Cell newKey = HFileWriterImpl.getMidpoint(keyComparator, kv1, kv2);
    assertTrue(keyComparator.compare(kv1, newKey) < 0);
    assertTrue((keyComparator.compare(kv2, newKey)) > 0);
    byte[] expectedArray = Bytes.toBytes("the r");
    Bytes.equals(newKey.getRowArray(), newKey.getRowOffset(), newKey.getRowLength(), expectedArray,
        0, expectedArray.length);

    // verify: same with "row + family + qualifier", return rightKey directly
    kv1 = new KeyValue(Bytes.toBytes("ilovehbase"), family, qualA, 5, Type.Put);
    kv2 = new KeyValue(Bytes.toBytes("ilovehbase"), family, qualA, 0, Type.Put);
    assertTrue(keyComparator.compare(kv1, kv2) < 0);
    newKey = HFileWriterImpl.getMidpoint(keyComparator, kv1, kv2);
    assertTrue(keyComparator.compare(kv1, newKey) < 0);
    assertTrue((keyComparator.compare(kv2, newKey)) == 0);
    kv1 = new KeyValue(Bytes.toBytes("ilovehbase"), family, qualA, -5, Type.Put);
    kv2 = new KeyValue(Bytes.toBytes("ilovehbase"), family, qualA, -10, Type.Put);
    assertTrue(keyComparator.compare(kv1, kv2) < 0);
    newKey = HFileWriterImpl.getMidpoint(keyComparator, kv1, kv2);
    assertTrue(keyComparator.compare(kv1, newKey) < 0);
    assertTrue((keyComparator.compare(kv2, newKey)) == 0);

    // verify: same with row, different with qualifier
    kv1 = new KeyValue(Bytes.toBytes("ilovehbase"), family, qualA, 5, Type.Put);
    kv2 = new KeyValue(Bytes.toBytes("ilovehbase"), family, qualB, 5, Type.Put);
    assertTrue(keyComparator.compare(kv1, kv2) < 0);
    newKey = HFileWriterImpl.getMidpoint(keyComparator, kv1, kv2);
    assertTrue(keyComparator.compare(kv1, newKey) < 0);
    assertTrue((keyComparator.compare(kv2, newKey)) > 0);
    assertTrue(Arrays.equals(CellUtil.cloneFamily(newKey), family));
    assertTrue(Arrays.equals(CellUtil.cloneQualifier(newKey), qualB));
    assertTrue(newKey.getTimestamp() == HConstants.LATEST_TIMESTAMP);
    assertTrue(newKey.getTypeByte() == Type.Maximum.getCode());

    // verify metaKeyComparator's getShortMidpointKey output
    final CellComparator metaKeyComparator = CellComparator.META_COMPARATOR;
    kv1 = new KeyValue(Bytes.toBytes("ilovehbase123"), family, qualA, 5, Type.Put);
    kv2 = new KeyValue(Bytes.toBytes("ilovehbase234"), family, qualA, 0, Type.Put);
    newKey = HFileWriterImpl.getMidpoint(metaKeyComparator, kv1, kv2);
    assertTrue(metaKeyComparator.compare(kv1, newKey) < 0);
    assertTrue((metaKeyComparator.compare(kv2, newKey) == 0));

    // verify common fix scenario
    kv1 = new KeyValue(Bytes.toBytes("ilovehbase"), family, qualA, ts, Type.Put);
    kv2 = new KeyValue(Bytes.toBytes("ilovehbaseandhdfs"), family, qualA, ts, Type.Put);
    assertTrue(keyComparator.compare(kv1, kv2) < 0);
    newKey = HFileWriterImpl.getMidpoint(keyComparator, kv1, kv2);
    assertTrue(keyComparator.compare(kv1, newKey) < 0);
    assertTrue((keyComparator.compare(kv2, newKey)) > 0);
    expectedArray = Bytes.toBytes("ilovehbasea");
    Bytes.equals(newKey.getRowArray(), newKey.getRowOffset(), newKey.getRowLength(), expectedArray,
        0, expectedArray.length);
    // verify only 1 offset scenario
    kv1 = new KeyValue(Bytes.toBytes("100abcdefg"), family, qualA, ts, Type.Put);
    kv2 = new KeyValue(Bytes.toBytes("101abcdefg"), family, qualA, ts, Type.Put);
    assertTrue(keyComparator.compare(kv1, kv2) < 0);
    newKey = HFileWriterImpl.getMidpoint(keyComparator, kv1, kv2);
    assertTrue(keyComparator.compare(kv1, newKey) < 0);
    assertTrue((keyComparator.compare(kv2, newKey)) > 0);
    expectedArray = Bytes.toBytes("101");
    Bytes.equals(newKey.getRowArray(), newKey.getRowOffset(), newKey.getRowLength(), expectedArray,
        0, expectedArray.length);
  }
}

