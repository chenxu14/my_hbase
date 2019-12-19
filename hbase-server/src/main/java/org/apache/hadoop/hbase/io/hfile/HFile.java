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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CanUnbuffer;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.MetricsIO;
import org.apache.hadoop.hbase.io.MetricsIOWrapperImpl;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.hfile.ReaderContext.ReaderType;
import org.apache.hadoop.hbase.shaded.com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.util.Counter;
import org.apache.hadoop.hbase.util.FSUtils;

/**
 * File format for hbase.
 * A file of sorted key/value pairs. Both keys and values are byte arrays.
 * <p>
 * The memory footprint of a HFile includes the following (below is taken from the
 * <a
 * href=https://issues.apache.org/jira/browse/HADOOP-3315>TFile</a> documentation
 * but applies also to HFile):
 * <ul>
 * <li>Some constant overhead of reading or writing a compressed block.
 * <ul>
 * <li>Each compressed block requires one compression/decompression codec for
 * I/O.
 * <li>Temporary space to buffer the key.
 * <li>Temporary space to buffer the value.
 * </ul>
 * <li>HFile index, which is proportional to the total number of Data Blocks.
 * The total amount of memory needed to hold the index can be estimated as
 * (56+AvgKeySize)*NumBlocks.
 * </ul>
 * Suggestions on performance optimization.
 * <ul>
 * <li>Minimum block size. We recommend a setting of minimum block size between
 * 8KB to 1MB for general usage. Larger block size is preferred if files are
 * primarily for sequential access. However, it would lead to inefficient random
 * access (because there are more data to decompress). Smaller blocks are good
 * for random access, but require more memory to hold the block index, and may
 * be slower to create (because we must flush the compressor stream at the
 * conclusion of each data block, which leads to an FS I/O flush). Further, due
 * to the internal caching in Compression codec, the smallest possible block
 * size would be around 20KB-30KB.
 * <li>The current implementation does not offer true multi-threading for
 * reading. The implementation uses FSDataInputStream seek()+read(), which is
 * shown to be much faster than positioned-read call in single thread mode.
 * However, it also means that if multiple threads attempt to access the same
 * HFile (using multiple scanners) simultaneously, the actual I/O is carried out
 * sequentially even if they access different DFS blocks (Reexamine! pread seems
 * to be 10% faster than seek+read in my testing -- stack).
 * <li>Compression codec. Use "none" if the data is not very compressable (by
 * compressable, I mean a compression ratio at least 2:1). Generally, use "lzo"
 * as the starting point for experimenting. "gz" overs slightly better
 * compression ratio over "lzo" but requires 4x CPU to compress and 2x CPU to
 * decompress, comparing to "lzo".
 * </ul>
 *
 * For more on the background behind HFile, see <a
 * href=https://issues.apache.org/jira/browse/HBASE-61>HBASE-61</a>.
 * <p>
 * File is made of data blocks followed by meta data blocks (if any), a fileinfo
 * block, data block index, meta data block index, and a fixed size trailer
 * which records the offsets at which file changes content type.
 * <pre>&lt;data blocks&gt;&lt;meta blocks&gt;&lt;fileinfo&gt;&lt;
 * data index&gt;&lt;meta index&gt;&lt;trailer&gt;</pre>
 * Each block has a bit of magic at its start.  Block are comprised of
 * key/values.  In data blocks, they are both byte arrays.  Metadata blocks are
 * a String key and a byte array value.  An empty file looks like this:
 * <pre>&lt;fileinfo&gt;&lt;trailer&gt;</pre>.  That is, there are not data nor meta
 * blocks present.
 * <p>
 * TODO: Do scanners need to be able to take a start and end row?
 * TODO: Should BlockIndex know the name of its file?  Should it have a Path
 * that points at its file say for the case where an index lives apart from
 * an HFile instance?
 */
@InterfaceAudience.Private
public class HFile {
  // LOG is being used in HFileBlock and CheckSumUtil
  static final Log LOG = LogFactory.getLog(HFile.class);

  /**
   * Maximum length of key in HFile.
   */
  public final static int MAXIMUM_KEY_LENGTH = Integer.MAX_VALUE;

  /**
   * Default compression: none.
   */
  public final static Compression.Algorithm DEFAULT_COMPRESSION_ALGORITHM =
    Compression.Algorithm.NONE;

  /** Minimum supported HFile format version */
  public static final int MIN_FORMAT_VERSION = 2;

  /** Maximum supported HFile format version
   */
  public static final int MAX_FORMAT_VERSION = 3;

  /**
   * Minimum HFile format version with support for persisting cell tags
   */
  public static final int MIN_FORMAT_VERSION_WITH_TAGS = 3;

  /** Default compression name: none. */
  public final static String DEFAULT_COMPRESSION =
    DEFAULT_COMPRESSION_ALGORITHM.getName();

  /** Meta data block name for bloom filter bits. */
  public static final String BLOOM_FILTER_DATA_KEY = "BLOOM_FILTER_DATA";

  /**
   * We assume that HFile path ends with
   * ROOT_DIR/TABLE_NAME/REGION_NAME/CF_NAME/HFILE, so it has at least this
   * many levels of nesting. This is needed for identifying table and CF name
   * from an HFile path.
   */
  public final static int MIN_NUM_HFILE_PATH_LEVELS = 5;

  /**
   * The number of bytes per checksum.
   */
  public static final int DEFAULT_BYTES_PER_CHECKSUM = 16 * 1024;

  /** Static instance for the metrics so that HFileReaders access the same instance */
  static final MetricsIO metrics = new MetricsIO(new MetricsIOWrapperImpl());

  // For measuring number of checksum failures
  static final Counter CHECKSUM_FAILURES = new Counter();

  // For tests. Gets incremented when we read a block whether from HDFS or from Cache.
  public static final Counter DATABLOCK_READ_COUNT = new Counter();

  /**
   * Number of checksum verification failures. It also
   * clears the counter.
   */
  public static final long getAndResetChecksumFailuresCount() {
    long count = CHECKSUM_FAILURES.get();
    CHECKSUM_FAILURES.set(0);
    return count;
  }

  /**
   * Number of checksum verification failures. It also
   * clears the counter.
   */
  public static final long getChecksumFailuresCount() {
    return CHECKSUM_FAILURES.get();
  }

  public static final void updateReadLatency(long latencyMillis, boolean pread) {
    if (pread) {
      metrics.updateFsPreadTime(latencyMillis);
    } else {
      metrics.updateFsReadTime(latencyMillis);
    }
  }

  public static final void updateWriteLatency(long latencyMillis) {
    metrics.updateFsWriteTime(latencyMillis);
  }

  /**
   * The configuration key for HFile version to use for new files
   */
  public static final String FORMAT_VERSION_KEY = "hfile.format.version";

  public static int getFormatVersion(Configuration conf) {
    int version = conf.getInt(FORMAT_VERSION_KEY, MAX_FORMAT_VERSION);
    checkFormatVersion(version);
    return version;
  }

  /**
   * Returns the factory to be used to create {@link HFile} writers.
   * Disables block cache access for all writers created through the
   * returned factory.
   */
  public static final HFileWriterFactory getWriterFactoryNoCache(Configuration conf) {
    return getWriterFactory(conf, CacheConfig.DISABLED);
  }

  /**
   * Returns the factory to be used to create {@link HFile} writers
   */
  public static final HFileWriterFactory getWriterFactory(Configuration conf,
      CacheConfig cacheConf) {
    int version = getFormatVersion(conf);
    switch (version) {
    case 2:
      throw new IllegalArgumentException("This should never happen. " +
        "Did you change hfile.format.version to read v2? This version of the software writes v3" +
        " hfiles only (but it can read v2 files without having to update hfile.format.version " +
        "in hbase-site.xml)");
    case 3:
      return new HFileWriterFactory(conf, cacheConf);
    default:
      throw new IllegalArgumentException("Cannot create writer for HFile " +
          "format version " + version);
    }
  }

  /**
   * Method returns the reader given the specified arguments.
   * TODO This is a bad abstraction.  See HBASE-6635.
   *
   * @param context Reader context info
   * @param fileInfo HFile info
   * @param cacheConf Cache configuation values, cannot be null.
   * @param conf Configuration
   * @return an appropriate instance of HFileReader
   * @throws IOException If file is invalid, will throw CorruptHFileException flavored IOException
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="SF_SWITCH_FALLTHROUGH",
      justification="Intentional")
  public static HFileReader createReader(ReaderContext context, HFileInfo fileInfo,
      CacheConfig cacheConf, Configuration conf) throws IOException {
    try {
      if (context.getReaderType() == ReaderType.STREAM) {
        // stream reader will share trailer with pread reader, see HFileStreamReader#copyFields
        return new HFileStreamReader(context, fileInfo, cacheConf, conf);
      }
      FixedFileTrailer trailer = fileInfo.getTrailer();
      switch (trailer.getMajorVersion()) {
        case 2:
          LOG.debug("Opening HFile v2 with v3 reader");
          // Fall through. FindBugs: SF_SWITCH_FALLTHROUGH
        case 3:
          return new HFilePreadReader(context, fileInfo, cacheConf, conf);
        default:
          throw new IllegalArgumentException("Invalid HFile version " + trailer.getMajorVersion());
      }
    } catch (Throwable t) {
      IOUtils.closeQuietly(context.getInputStreamWrapper());
      throw new CorruptHFileException("Problem reading HFile Trailer from file "
          + context.getFilePath(), t);
    } finally {
      unbufferStream(context.getInputStreamWrapper());
    }
  }

  private static void unbufferStream(FSDataInputStreamWrapper fsdis) {
    boolean useHBaseChecksum = fsdis.shouldUseHBaseChecksum();
    final FSDataInputStream stream = fsdis.getStream(useHBaseChecksum);
    if (stream != null && stream.getWrappedStream() instanceof CanUnbuffer) {
      // Enclosing unbuffer() in try-catch just to be on defensive side.
      try {
        stream.unbuffer();
      } catch (Throwable e) {
        LOG.error("Failed to unbuffer the stream so possibly there may be a TCP socket connection "
            + "left open in CLOSE_WAIT state.", e);
      }
    }
  }

  /**
   * Creates reader with cache configuration disabled
   * @param fs filesystem
   * @param path Path to file to read
   * @return an active Reader instance
   * @throws IOException Will throw a CorruptHFileException
   * (DoNotRetryIOException subtype) if hfile is corrupt/invalid.
   */
  public static HFileReader createReader(
      FileSystem fs, Path path,  Configuration conf) throws IOException {
    // The primaryReplicaReader is mainly used for constructing block cache key, so if we do not use
    // block cache then it is OK to set it as any value. We use true here.
    return createReader(fs, path, CacheConfig.DISABLED, true, conf);
  }

  /**
   * @param fs filesystem
   * @param path Path to file to read
   * @param cacheConf This must not be null.  @see {@link org.apache.hadoop.hbase.io.hfile.CacheConfig#CacheConfig(Configuration)}
   * @return an active Reader instance
   * @throws IOException Will throw a CorruptHFileException (DoNotRetryIOException subtype) if hfile is corrupt/invalid.
   */
  public static HFileReader createReader(
      FileSystem fs, Path path, CacheConfig cacheConf, boolean primaryReplicaReader,
      Configuration conf) throws IOException {
    Preconditions.checkNotNull(cacheConf, "Cannot create Reader with null CacheConf");
    FSDataInputStreamWrapper stream = new FSDataInputStreamWrapper(fs, path);
    ReaderContext context = new ReaderContextBuilder()
        .withFilePath(path)
        .withInputStreamWrapper(stream)
        .withFileSize(fs.getFileStatus(path).getLen())
        .withFileSystem(stream.getHfs())
        .withPrimaryReplicaReader(primaryReplicaReader)
        .withReaderType(ReaderType.PREAD)
        .build();
    HFileInfo fileInfo = new HFileInfo(context, conf);
    HFileReader reader = createReader(context, fileInfo, cacheConf, conf);
    fileInfo.initMetaAndIndex(reader);
    return reader;
  }

  /**
   * Returns true if the specified file has a valid HFile Trailer.
   * @param fs filesystem
   * @param path Path to file to verify
   * @return true if the file has a valid HFile Trailer, otherwise false
   * @throws IOException if failed to read from the underlying stream
   */
  public static boolean isHFileFormat(final FileSystem fs, final Path path) throws IOException {
    return isHFileFormat(fs, fs.getFileStatus(path));
  }

  /**
   * Returns true if the specified file has a valid HFile Trailer.
   * @param fs filesystem
   * @param fileStatus the file to verify
   * @return true if the file has a valid HFile Trailer, otherwise false
   * @throws IOException if failed to read from the underlying stream
   */
  public static boolean isHFileFormat(final FileSystem fs, final FileStatus fileStatus)
      throws IOException {
    final Path path = fileStatus.getPath();
    final long size = fileStatus.getLen();
    try (FSDataInputStreamWrapper fsdis = new FSDataInputStreamWrapper(fs, path)) {
      boolean isHBaseChecksum = fsdis.shouldUseHBaseChecksum();
      assert !isHBaseChecksum; // Initially we must read with FS checksum.
      FixedFileTrailer.readFromStream(fsdis.getStream(isHBaseChecksum), size);
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  /**
   * Get names of supported compression algorithms. The names are acceptable by
   * HFile.Writer.
   *
   * @return Array of strings, each represents a supported compression
   *         algorithm. Currently, the following compression algorithms are
   *         supported.
   *         <ul>
   *         <li>"none" - No compression.
   *         <li>"gz" - GZIP compression.
   *         </ul>
   */
  public static String[] getSupportedCompressionAlgorithms() {
    return Compression.getSupportedAlgorithms();
  }

  // Utility methods.
  /*
   * @param l Long to convert to an int.
   * @return <code>l</code> cast as an int.
   */
  static int longToInt(final long l) {
    // Expecting the size() of a block not exceeding 4GB. Assuming the
    // size() will wrap to negative integer if it exceeds 2GB (From tfile).
    return (int)(l & 0x00000000ffffffffL);
  }

  /**
   * Returns all HFiles belonging to the given region directory. Could return an
   * empty list.
   *
   * @param fs  The file system reference.
   * @param regionDir  The region directory to scan.
   * @return The list of files found.
   * @throws IOException When scanning the files fails.
   */
  static List<Path> getStoreFiles(FileSystem fs, Path regionDir)
      throws IOException {
    List<Path> regionHFiles = new ArrayList<Path>();
    PathFilter dirFilter = new FSUtils.DirFilter(fs);
    FileStatus[] familyDirs = fs.listStatus(regionDir, dirFilter);
    for(FileStatus dir : familyDirs) {
      FileStatus[] files = fs.listStatus(dir.getPath());
      for (FileStatus file : files) {
        if (!file.isDirectory() &&
            (!file.getPath().toString().contains(HConstants.HREGION_OLDLOGDIR_NAME)) &&
            (!file.getPath().toString().contains(HConstants.RECOVERED_EDITS_DIR))) {
          regionHFiles.add(file.getPath());
        }
      }
    }
    return regionHFiles;
  }

  /**
   * Checks the given {@link HFile} format version, and throws an exception if
   * invalid. Note that if the version number comes from an input file and has
   * not been verified, the caller needs to re-throw an {@link IOException} to
   * indicate that this is not a software error, but corrupted input.
   *
   * @param version an HFile version
   * @throws IllegalArgumentException if the version is invalid
   */
  public static void checkFormatVersion(int version)
      throws IllegalArgumentException {
    if (version < MIN_FORMAT_VERSION || version > MAX_FORMAT_VERSION) {
      throw new IllegalArgumentException("Invalid HFile version: " + version
          + " (expected to be " + "between " + MIN_FORMAT_VERSION + " and "
          + MAX_FORMAT_VERSION + ")");
    }
  }

  public static void checkHFileVersion(final Configuration c) {
    int version = c.getInt(FORMAT_VERSION_KEY, MAX_FORMAT_VERSION);
    if (version < MAX_FORMAT_VERSION || version > MAX_FORMAT_VERSION) {
      throw new IllegalArgumentException("The setting for " + FORMAT_VERSION_KEY +
        " (in your hbase-*.xml files) is " + version + " which does not match " +
        MAX_FORMAT_VERSION +
        "; are you running with a configuration from an older or newer hbase install (an " +
        "incompatible hbase-default.xml or hbase-site.xml on your CLASSPATH)?");
    }
  }

  public static void main(String[] args) throws Exception {
    // delegate to preserve old behavior
    HFilePrettyPrinter.main(args);
  }
}
