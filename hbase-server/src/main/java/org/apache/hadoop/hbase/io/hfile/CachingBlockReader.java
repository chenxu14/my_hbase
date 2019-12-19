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

import java.io.IOException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;

/**
 * An abstraction used by the block index.
 * Implementations will check cache for any asked-for block and return cached block if found.
 * Otherwise, after reading from fs, will try and put block into cache before returning.
 */
@InterfaceAudience.Private
public interface CachingBlockReader {
  /**
   * Read in a file block.
   * @param offset offset to read.
   * @param onDiskBlockSize size of the block
   * @param cacheBlock
   * @param pread
   * @param isCompaction is this block being read as part of a compaction
   * @param expectedBlockType the block type we are expecting to read with this read operation,
   *  or null to read whatever block type is available and avoid checking (that might reduce
   *  caching efficiency of encoded data blocks)
   * @param expectedDataBlockEncoding the data block encoding the caller is expecting data blocks
   *  to be in, or null to not perform this check and return the block irrespective of the
   *  encoding. This check only applies to data blocks and can be set to null when the caller is
   *  expecting to read a non-data block and has set expectedBlockType accordingly.
   * @return Block wrapped in a ByteBuffer.
   * @throws IOException
   */
  HFileBlock readBlock(long offset, long onDiskBlockSize,
      boolean cacheBlock, final boolean pread, final boolean isCompaction,
      final boolean updateCacheMetrics, BlockType expectedBlockType,
      DataBlockEncoding expectedDataBlockEncoding)
      throws IOException;
}
