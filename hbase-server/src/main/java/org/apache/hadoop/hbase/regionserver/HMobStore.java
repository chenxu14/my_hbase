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
package org.apache.hadoop.hbase.regionserver;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.NavigableSet;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ArrayBackedTag;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.TagType;
import org.apache.hadoop.hbase.TagUtil;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.CorruptHFileException;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.mob.MobCacheConfig;
import org.apache.hadoop.hbase.mob.MobCell;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobFile;
import org.apache.hadoop.hbase.mob.MobFileName;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;

/**
 * The store implementation to save MOBs (medium objects), it extends the HStore.
 * When a descriptor of a column family has the value "IS_MOB", it means this column family
 * is a mob one. When a HRegion instantiate a store for this column family, the HMobStore is
 * created.
 * HMobStore is almost the same with the HStore except using different types of scanners.
 * In the method of getScanner, the MobStoreScanner and MobReversedStoreScanner are returned.
 * In these scanners, a additional seeks in the mob files should be performed after the seek
 * to HBase is done.
 * The store implements how we save MOBs by extending HStore. When a descriptor
 * of a column family has the value "IS_MOB", it means this column family is a mob one. When a
 * HRegion instantiate a store for this column family, the HMobStore is created. HMobStore is
 * almost the same with the HStore except using different types of scanners. In the method of
 * getScanner, the MobStoreScanner and MobReversedStoreScanner are returned. In these scanners, a
 * additional seeks in the mob files should be performed after the seek in HBase is done.
 */
@InterfaceAudience.Private
public class HMobStore extends HStore {
  private static final Log LOG = LogFactory.getLog(HMobStore.class);
  private MobCacheConfig mobCacheConfig;
  private Path homePath;
  private Path mobFamilyPath;
  private volatile long cellsCountCompactedToMob = 0;
  private volatile long cellsCountCompactedFromMob = 0;
  private volatile long cellsSizeCompactedToMob = 0;
  private volatile long cellsSizeCompactedFromMob = 0;
  private volatile long mobFlushCount = 0;
  private volatile long mobFlushedCellsCount = 0;
  private volatile long mobFlushedCellsSize = 0;
  private volatile long mobScanCellsCount = 0;
  private volatile long mobScanCellsSize = 0;
  private HColumnDescriptor family;
  // When we add a MOB reference cell to the HFile, we will add 2 tags along with it
  // 1. A ref tag with type TagType.MOB_REFERENCE_TAG_TYPE. This just denote this this cell is not
  // original one but a ref to another MOB Cell.
  // 2. Table name tag. It's very useful in cloning the snapshot. When reading from the cloning
  // table, we need to find the original mob files by this table name. For details please see
  // cloning snapshot for mob files.
  private final byte[] refCellTags;

  public HMobStore(final HRegion region, final HColumnDescriptor family,
      final Configuration confParam) throws IOException {
    super(region, family, confParam);
    this.family = family;
    this.mobCacheConfig = (MobCacheConfig) cacheConf;
    this.homePath = MobUtils.getMobHome(conf);
    this.mobFamilyPath = MobUtils.getMobFamilyPath(conf, this.getTableName(),
        family.getNameAsString());
    List<Tag> tags = new ArrayList<>(2);
    tags.add(MobConstants.MOB_REF_TAG);
    Tag tableNameTag = new ArrayBackedTag(TagType.MOB_TABLE_NAME_TAG_TYPE,
        getTableName().getName());
    tags.add(tableNameTag);
    this.refCellTags = TagUtil.fromList(tags);
  }

  /**
   * Creates the mob cache config.
   */
  @Override
  protected void createCacheConf(HColumnDescriptor family) {
    cacheConf = new MobCacheConfig(conf, family, region.getRegionServicesForStores().getByteBuffAllocator());
  }

  /**
   * Gets current config.
   */
  public Configuration getConfiguration() {
    return this.conf;
  }

  /**
   * Gets the MobStoreScanner or MobReversedStoreScanner. In these scanners, a additional seeks in
   * the mob files should be performed after the seek in HBase is done.
   */
  @Override
  protected KeyValueScanner createScanner(Scan scan, final NavigableSet<byte[]> targetCols,
      long readPt, KeyValueScanner scanner) throws IOException {
    if (scanner == null) {
      if (MobUtils.isRefOnlyScan(scan)) {
        Filter refOnlyFilter = new MobReferenceOnlyFilter();
        Filter filter = scan.getFilter();
        if (filter != null) {
          scan.setFilter(new FilterList(filter, refOnlyFilter));
        } else {
          scan.setFilter(refOnlyFilter);
        }
      }
      scanner = scan.isReversed() ? new ReversedMobStoreScanner(this, getScanInfo(), scan,
          targetCols, readPt) : new MobStoreScanner(this, getScanInfo(), scan, targetCols, readPt);
    }
    return scanner;
  }

  /**
   * Gets the temp directory.
   * @return The temp directory.
   */
  private Path getTempDir() {
    Path tempParent = new Path(homePath, MobConstants.TEMP_DIR_NAME);
    return new Path(tempParent, this.getTableName().getNameAsString().replaceAll(":", "_"));
  }

  /**
   * Creates the writer for the mob file in temp directory.
   * @param date The latest date of written cells.
   * @param maxKeyCount The key count.
   * @param compression The compression algorithm.
   * @param startKey The start key.
   * @return The writer for the mob file.
   * @throws IOException
   */
  public StoreFile.Writer createWriterInTmp(Date date, long maxKeyCount,
      Compression.Algorithm compression, byte[] startKey) throws IOException {
    if (startKey == null) {
      startKey = HConstants.EMPTY_START_ROW;
    }
    Path path = getTempDir();
    return createWriterInTmp(MobUtils.formatDate(date), path, maxKeyCount, compression, startKey);
  }

  /**
   * Creates the writer for the del file in temp directory.
   * The del file keeps tracking the delete markers. Its name has a suffix _del,
   * the format is [0-9a-f]+(_del)?.
   * @param date The latest date of written cells.
   * @param maxKeyCount The key count.
   * @param compression The compression algorithm.
   * @param startKey The start key.
   * @return The writer for the del file.
   * @throws IOException
   */
  public StoreFile.Writer createDelFileWriterInTmp(Date date, long maxKeyCount,
      Compression.Algorithm compression, byte[] startKey) throws IOException {
    if (startKey == null) {
      startKey = HConstants.EMPTY_START_ROW;
    }
    Path path = getTempDir();
    String suffix = UUID
        .randomUUID().toString().replaceAll("-", "") + "_del";
    MobFileName mobFileName = MobFileName.create(startKey, MobUtils.formatDate(date), suffix);
    return createWriterInTmp(mobFileName, path, maxKeyCount, compression);
  }

  /**
   * Creates the writer for the mob file in temp directory.
   * @param date The date string, its format is yyyymmmdd.
   * @param basePath The basic path for a temp directory.
   * @param maxKeyCount The key count.
   * @param compression The compression algorithm.
   * @param startKey The start key.
   * @return The writer for the mob file.
   * @throws IOException
   */
  public StoreFile.Writer createWriterInTmp(String date, Path basePath, long maxKeyCount,
      Compression.Algorithm compression, byte[] startKey) throws IOException {
    MobFileName mobFileName = MobFileName.create(startKey, date, UUID.randomUUID()
        .toString().replaceAll("-", ""));
    return createWriterInTmp(mobFileName, basePath, maxKeyCount, compression);
  }

  /**
   * Creates the writer for the mob file in temp directory.
   * @param mobFileName The mob file name.
   * @param basePath The basic path for a temp directory.
   * @param maxKeyCount The key count.
   * @param compression The compression algorithm.
   * @return The writer for the mob file.
   * @throws IOException
   */
  public StoreFile.Writer createWriterInTmp(MobFileName mobFileName, Path basePath,
      long maxKeyCount, Compression.Algorithm compression) throws IOException {
    final CacheConfig writerCacheConf = mobCacheConfig;
    HFileContext hFileContext = new HFileContextBuilder().withCompression(compression)
        .withIncludesMvcc(true).withIncludesTags(true)
        .withCompressTags(family.isCompressTags())
        .withChecksumType(checksumType)
        .withBytesPerCheckSum(bytesPerChecksum)
        .withBlockSize(blocksize)
        .withHBaseCheckSum(true).withDataBlockEncoding(getFamily().getDataBlockEncoding())
        .withEncryptionContext(cryptoContext)
        .withCreateTime(EnvironmentEdgeManager.currentTime()).build();

    StoreFile.Writer w = new StoreFile.WriterBuilder(conf, writerCacheConf, region.getFilesystem())
        .withFilePath(new Path(basePath, mobFileName.getFileName()))
        .withComparator(CellComparator.COMPARATOR).withBloomType(BloomType.NONE)
        .withMaxKeyCount(maxKeyCount).withFileContext(hFileContext).build();
    return w;
  }

  /**
   * Commits the mob file.
   * @param sourceFile The source file.
   * @param targetPath The directory path where the source file is renamed to.
   * @throws IOException
   */
  public void commitFile(final Path sourceFile, Path targetPath) throws IOException {
    if (sourceFile == null) {
      return;
    }
    Path dstPath = new Path(targetPath, sourceFile.getName());
    validateMobFile(sourceFile);
    String msg = "Renaming flushed file from " + sourceFile + " to " + dstPath;
    LOG.info(msg);
    Path parent = dstPath.getParent();
    if (!region.getFilesystem().exists(parent)) {
      region.getFilesystem().mkdirs(parent);
    }
    if (!region.getFilesystem().rename(sourceFile, dstPath)) {
      throw new IOException("Failed rename of " + sourceFile + " to " + dstPath);
    }
  }

  /**
   * Validates a mob file by opening and closing it.
   *
   * @param path the path to the mob file
   */
  private void validateMobFile(Path path) throws IOException {
    StoreFile storeFile = null;
    try {
      storeFile = new StoreFile(region.getFilesystem(), path, conf, this.mobCacheConfig,
          BloomType.NONE, isPrimaryReplicaStore());
      storeFile.initReader();
    } catch (IOException e) {
      LOG.error("Fail to open mob file[" + path + "], keep it in temp directory.", e);
      throw e;
    } finally {
      if (storeFile != null) {
        storeFile.closeReader(false);
      }
    }
  }

  /**
   * Reads the cell from the mob file, and the read point does not count. This is used for
   * DefaultMobStoreCompactor where we can read empty value for the missing cell.
   * @param reference The cell found in the HBase, its value is a path to a mob file.
   * @param cacheBlocks Whether the scanner should cache blocks.
   * @return The cell found in the mob file.
   * @throws IOException
   */
  public MobCell resolve(Cell reference, boolean cacheBlocks) throws IOException {
    return resolve(reference, cacheBlocks, -1, true);
  }

  /**
   * Reads the cell from the mob file.
   * @param reference The cell found in the HBase, its value is a path to a mob file.
   * @param cacheBlocks Whether the scanner should cache blocks.
   * @param readPt the read point.
   * @param readEmptyValueOnMobCellMiss Whether return null value when the mob file is missing or
   *          corrupt.
   * @return The cell found in the mob file.
   * @throws IOException
   */
  public MobCell resolve(Cell reference, boolean cacheBlocks, long readPt,
    boolean readEmptyValueOnMobCellMiss) throws IOException {
    MobCell mobCell = null;
    if (MobUtils.hasValidMobRefCellValue(reference)) {
      String fileName = MobUtils.getMobFileName(reference);
      Tag tableNameTag = MobUtils.getTableNameTag(reference);
      if (tableNameTag != null) {
        String tableNameString = TagUtil.getValueAsString(tableNameTag);
        TableName tn = TableName.valueOf(tableNameString);
        Path familyPath = MobUtils.getMobFamilyPath(conf, tn, family.getNameAsString());
        String date = MobFileName.getDateFromName(fileName);
        mobCell = readCell(new Path(familyPath, date), fileName, reference, cacheBlocks, readPt);
        if(mobCell == null){
          mobCell = readCell(familyPath, fileName, reference, cacheBlocks, readPt);
        }
        if(mobCell == null){
          Path archPath = HFileArchiveUtil.getStoreArchivePath(conf, tn, MobUtils
            .getMobRegionInfo(tn).getEncodedName(), family.getNameAsString());
          mobCell = readCell(archPath, fileName, reference, cacheBlocks, readPt);
        }
      }
    }
    if (mobCell != null) {
      return mobCell;
    } else if (readEmptyValueOnMobCellMiss) {
      LOG.warn("The KeyValue result is null, assemble a new KeyValue with the same row,family,"
          + "qualifier,timestamp,type and tags but with an empty value to return.");
      Cell cell = new KeyValue(reference.getRowArray(), reference.getRowOffset(),
          reference.getRowLength(), reference.getFamilyArray(), reference.getFamilyOffset(),
          reference.getFamilyLength(), reference.getQualifierArray(),
          reference.getQualifierOffset(), reference.getQualifierLength(), reference.getTimestamp(),
          Type.codeToType(reference.getTypeByte()), HConstants.EMPTY_BYTE_ARRAY,
          0, 0, reference.getTagsArray(), reference.getTagsOffset(),
          reference.getTagsLength());
      mobCell = new MobCell(cell);
    } else {
      throw new IOException("target mobCell not find!");
    }
    return mobCell;
  }

  /**
   * Reads the cell from a mob file.
   * The mob file might be located in different directories.
   * 1. The working directory.
   * 2. The archive directory.
   * Reads the cell from the files located in both of the above directories.
   * @param locations The possible locations where the mob files are saved.
   * @param fileName The file to be read.
   * @param search The cell to be searched.
   * @param cacheMobBlocks Whether the scanner should cache blocks.
   * @param readPt the read point.
   * @param readEmptyValueOnMobCellMiss Whether return null value when the mob file is
   *        missing or corrupt.
   * @return The found cell. Null if there's no such a cell.
   * @throws IOException
   */
  private MobCell readCell(Path location, String fileName, Cell search, boolean cacheMobBlocks, long readPt) {
    FileSystem fs = getFileSystem();
    MobFile file = null;
    Path path = new Path(location, fileName);
    try {
      file = mobCacheConfig.getMobFileCache().openFile(fs, path, mobCacheConfig);
      return readPt != -1 ? file.readCell(search, cacheMobBlocks, readPt)
          : file.readCell(search, cacheMobBlocks);
    } catch (IOException e) {
      mobCacheConfig.getMobFileCache().evictFile(fileName);
      if ((e instanceof FileNotFoundException) ||
          (e.getCause() instanceof FileNotFoundException)) {
        LOG.warn("Fail to read the cell, the mob file " + path + " doesn't exist", e);
      } else if (e instanceof CorruptHFileException) {
        LOG.error("The mob file " + path + " is corrupt", e);
      } else {
        LOG.error(e.getMessage(), e);
      }
    } catch (NullPointerException e) { // HDFS 1.x - DFSInputStream.getBlockAt()
      mobCacheConfig.getMobFileCache().evictFile(fileName);
      LOG.warn("Fail to read the cell", e);
    } catch (AssertionError e) { // assert in HDFS 1.x - DFSInputStream.getBlockAt()
      mobCacheConfig.getMobFileCache().evictFile(fileName);
      LOG.warn("Fail to read the cell", e);
    } finally {
      if (file != null) {
        mobCacheConfig.getMobFileCache().closeFile(file);
      }
    }
    LOG.error("The mob file " + fileName + " could not be found in the path " + path
        + " or it is corrupt");
    return null;
  }

  /**
   * Gets the mob file path.
   * @return The mob file path.
   */
  public Path getPath() {
    return mobFamilyPath;
  }

  public void updateCellsCountCompactedToMob(long count) {
    cellsCountCompactedToMob += count;
  }

  public long getCellsCountCompactedToMob() {
    return cellsCountCompactedToMob;
  }

  public void updateCellsCountCompactedFromMob(long count) {
    cellsCountCompactedFromMob += count;
  }

  public long getCellsCountCompactedFromMob() {
    return cellsCountCompactedFromMob;
  }

  public void updateCellsSizeCompactedToMob(long size) {
    cellsSizeCompactedToMob += size;
  }

  public long getCellsSizeCompactedToMob() {
    return cellsSizeCompactedToMob;
  }

  public void updateCellsSizeCompactedFromMob(long size) {
    cellsSizeCompactedFromMob += size;
  }

  public long getCellsSizeCompactedFromMob() {
    return cellsSizeCompactedFromMob;
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "VO_VOLATILE_INCREMENT")
  public void updateMobFlushCount() {
    mobFlushCount++;
  }

  public long getMobFlushCount() {
    return mobFlushCount;
  }

  public void updateMobFlushedCellsCount(long count) {
    mobFlushedCellsCount += count;
  }

  public long getMobFlushedCellsCount() {
    return mobFlushedCellsCount;
  }

  public void updateMobFlushedCellsSize(long size) {
    mobFlushedCellsSize += size;
  }

  public long getMobFlushedCellsSize() {
    return mobFlushedCellsSize;
  }

  public void updateMobScanCellsCount(long count) {
    mobScanCellsCount += count;
  }

  public long getMobScanCellsCount() {
    return mobScanCellsCount;
  }

  public void updateMobScanCellsSize(long size) {
    mobScanCellsSize += size;
  }

  public long getMobScanCellsSize() {
    return mobScanCellsSize;
  }

  public byte[] getRefCellTags() {
    return this.refCellTags;
  }
}
