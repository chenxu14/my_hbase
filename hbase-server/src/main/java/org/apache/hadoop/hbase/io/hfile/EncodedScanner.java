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
import java.nio.ByteBuffer;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoder;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.encoding.HFileBlockDecodingContext;
import org.apache.hadoop.hbase.io.hfile.HFileReader.NotSeekedException;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;

/**
 * Scanner that operates on encoded data blocks.
 */
@InterfaceAudience.Private
public class EncodedScanner extends HFileScannerImpl {
  private HFileBlockDecodingContext decodingCtx;
  private DataBlockEncoder.EncodedSeeker seeker;
  private DataBlockEncoder dataBlockEncoder;
  private Handle<EncodedScanner> encodedHandle;

  private static final Recycler<EncodedScanner> RECYCLER = new Recycler<EncodedScanner>() {
    @Override
    protected EncodedScanner newObject(Handle<EncodedScanner> handle) {
      return new EncodedScanner(handle);
    }
  };

  private EncodedScanner(Handle<EncodedScanner> encodedHandle) {
    super(null);
    this.encodedHandle = encodedHandle;
  }

  public static EncodedScanner newInstance(HFileReader reader, boolean cacheBlocks,
      boolean pread, boolean isCompaction, HFileContext meta) {
    EncodedScanner scanner = RECYCLER.get();
    scanner.reader = reader;
    scanner.cacheBlocks = cacheBlocks;
    scanner.pread = pread;
    scanner.isCompaction = isCompaction;
    DataBlockEncoding encoding = reader.getDataBlockEncoding();
    scanner.dataBlockEncoder = encoding.getEncoder();
    scanner.decodingCtx = scanner.dataBlockEncoder.newDataBlockDecodingContext(meta);
    scanner.seeker = scanner.dataBlockEncoder.createSeeker(
        reader.getComparator(), scanner.decodingCtx);
    return scanner;
  }

  @Override
  public boolean isSeeked(){
    return curBlock != null;
  }

  public void setNonSeekedState() {
    reset();
  }

  /**
   * Updates the current block to be the given {@link HFileBlock}. Seeks to the the first
   * key/value pair.
   * @param newBlock the block to make current, and read by {@link HFileReaderImpl#readBlock},
   *          it's a totally new block with new allocated {@link ByteBuff}, so if no further
   *          reference to this block, we should release it carefully.
   * @throws CorruptHFileException
   */
  @Override
  protected void updateCurrentBlock(HFileBlock newBlock) throws CorruptHFileException {
    try {
      // sanity checks
      if (newBlock.getBlockType() != BlockType.ENCODED_DATA) {
        throw new IllegalStateException("EncodedScanner works only on encoded data blocks");
      }
      short dataBlockEncoderId = newBlock.getDataBlockEncodingId();
      if (!DataBlockEncoding.isCorrectEncoder(dataBlockEncoder, dataBlockEncoderId)) {
        String encoderCls = dataBlockEncoder.getClass().getName();
        throw new CorruptHFileException(
            "Encoder " + encoderCls + " doesn't support data block encoding "
                + DataBlockEncoding.getNameFromId(dataBlockEncoderId));
      }
      updateCurrBlockRef(newBlock);
      ByteBuff encodedBuffer = getEncodedBuffer(newBlock);
      seeker.setCurrentBuffer(encodedBuffer);
    } finally {
      releaseIfNotCurBlock(newBlock);
    }
    // Reset the next indexed key
    this.nextIndexedKey = null;
  }

  private ByteBuff getEncodedBuffer(HFileBlock newBlock) {
    ByteBuff origBlock = newBlock.getBufferReadOnly();
    int pos = newBlock.headerSize() + DataBlockEncoding.ID_SIZE;
    origBlock.position(pos);
    origBlock
        .limit(pos + newBlock.getUncompressedSizeWithoutHeader() - DataBlockEncoding.ID_SIZE);
    return origBlock.slice();
  }

  @Override
  protected boolean processFirstDataBlock() throws IOException {
    seeker.rewind();
    return true;
  }

  @Override
  public boolean next() throws IOException {
    boolean isValid = seeker.next();
    if (!isValid) {
      HFileBlock newBlock = readNextDataBlock();
      isValid = newBlock != null;
      if (isValid) {
        updateCurrentBlock(newBlock);
      } else {
        setNonSeekedState();
      }
    }
    return isValid;
  }

  @Override
  public Cell getKey() {
    assertValidSeek();
    return seeker.getKey();
  }

  @Override
  public ByteBuffer getValue() {
    assertValidSeek();
    return seeker.getValueShallowCopy();
  }

  @Override
  public Cell getCell() {
    if (this.curBlock == null) {
      return null;
    }
    return seeker.getCell();
  }

  @Override
  public String getKeyString() {
    return CellUtil.toString(getKey(), true);
  }

  @Override
  public String getValueString() {
    ByteBuffer valueBuffer = getValue();
    return ByteBufferUtils.toStringBinary(valueBuffer);
  }

  private void assertValidSeek() {
    if (this.curBlock == null) {
      throw new NotSeekedException();
    }
  }

  protected Cell getFirstKeyCellInBlock(HFileBlock curBlock) {
    return dataBlockEncoder.getFirstKeyCellInBlock(getEncodedBuffer(curBlock));
  }

  @Override
  protected int loadBlockAndSeekToKey(HFileBlock seekToBlock, Cell nextIndexedKey,
      boolean rewind, Cell key, boolean seekBefore) throws IOException {
    if (this.curBlock == null || this.curBlock.getOffset() != seekToBlock.getOffset()) {
      updateCurrentBlock(seekToBlock);
    } else if (rewind) {
      seeker.rewind();
    }
    this.nextIndexedKey = nextIndexedKey;
    return seeker.seekToKeyInBlock(key, seekBefore);
  }

  public int compareKey(CellComparator comparator, Cell key) {
    return seeker.compareKey(comparator, key);
  }

  protected void recycle() {
    if (encodedHandle != null) {
      clear();
      encodedHandle.recycle(this);
    }
  }
}
