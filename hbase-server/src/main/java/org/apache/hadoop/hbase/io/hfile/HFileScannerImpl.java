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
import java.util.ArrayList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.ByteBufferKeyOnlyKeyValue;
import org.apache.hadoop.hbase.ByteBufferKeyValue;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.NoTagsByteBufferKeyValue;
import org.apache.hadoop.hbase.SizeCachedKeyValue;
import org.apache.hadoop.hbase.SizeCachedNoTagsKeyValue;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.HFileReader.NotSeekedException;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ObjectIntPair;
import org.apache.hadoop.io.WritableUtils;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;

@InterfaceAudience.Private
public class HFileScannerImpl implements HFileScanner {
  private static final Log LOG = LogFactory.getLog(HFileReaderImpl.class);
  /**
   * The size of a (key length, value length) tuple that prefixes each entry in
   * a data block.
   */
  public final static int KEY_VALUE_LEN_SIZE = 2 * Bytes.SIZEOF_INT;
  private static final Recycler<HFileScannerImpl> RECYCLER = new Recycler<HFileScannerImpl>() {
    @Override
    protected HFileScannerImpl newObject(Handle<HFileScannerImpl> handle) {
      return new HFileScannerImpl(handle);
    }
  };
  private ByteBuff blockBuffer;
  protected boolean cacheBlocks;
  protected boolean pread;
  protected boolean isCompaction;
  private int currKeyLen;
  private int currValueLen;
  private int currMemstoreTSLen;
  private long currMemstoreTS;
  protected HFileReader reader;
  private int currTagsLen;
  // buffer backed keyonlyKV
  private ByteBufferKeyOnlyKeyValue bufBackedKeyOnlyKv = new ByteBufferKeyOnlyKeyValue();
  // A pair for reusing in blockSeek() so that we don't garbage lot of objects
  final ObjectIntPair<ByteBuffer> pair = new ObjectIntPair<ByteBuffer>();

  /**
   * The next indexed key is to keep track of the indexed key of the next data block.
   * If the nextIndexedKey is HConstants.NO_NEXT_INDEXED_KEY, it means that the
   * current data block is the last data block.
   *
   * If the nextIndexedKey is null, it means the nextIndexedKey has not been loaded yet.
   */
  protected Cell nextIndexedKey;
  // Current block being used. NOTICE: DON't release curBlock separately except in shipped() or
  // close() methods. Because the shipped() or close() will do the release finally, even if any
  // exception occur the curBlock will be released by the close() method (see
  // RegionScannerImpl#handleException). Call the releaseIfNotCurBlock() to release the
  // unreferenced block please.
  protected HFileBlock curBlock;
  // Previous blocks that were used in the course of the read
  protected final ArrayList<HFileBlock> prevBlocks = new ArrayList<HFileBlock>();
  private Handle<HFileScannerImpl> handle;

  public static HFileScannerImpl newInstance(final HFileReader reader, final boolean cacheBlocks,
      final boolean pread, final boolean isCompaction) {
    HFileScannerImpl scanner = RECYCLER.get();
    scanner.reader = reader;
    scanner.cacheBlocks = cacheBlocks;
    scanner.pread = pread;
    scanner.isCompaction = isCompaction;
    return scanner;
  }

  HFileScannerImpl(Handle<HFileScannerImpl> handle) {
    this.handle = handle;
  }

  protected void recycle() {
    if (handle != null) {
      clear();
      handle.recycle(this);
    }
  }

  protected void clear() {
    blockBuffer = null;
    currKeyLen = 0;
    currValueLen = 0;
    currMemstoreTSLen = 0;
    currMemstoreTS = 0;
    reader = null;
    currTagsLen = 0;
    nextIndexedKey = null;
    curBlock = null;
    if (!prevBlocks.isEmpty()) {
      prevBlocks.clear();
    }
  }

  void updateCurrBlockRef(HFileBlock block) {
//      if (block != null && curBlock != null && block.getOffset() == curBlock.getOffset()) {
//        return;
//      }
    if (this.curBlock != null && this.curBlock.isSharedMem()) {
      prevBlocks.add(this.curBlock);
    }
    this.curBlock = block;
  }

  void reset() {
    // We don't have to keep ref to heap block
    if (this.curBlock != null && this.curBlock.isSharedMem()) {
      this.prevBlocks.add(this.curBlock);
    }
    this.curBlock = null;
  }

  private void returnBlocks(boolean returnAll) {
    this.prevBlocks.forEach(HFileBlock::release);
    this.prevBlocks.clear();
    if (returnAll && this.curBlock != null) {
      this.curBlock.release();
      this.curBlock = null;
    }
  }

  @Override
  public boolean isSeeked(){
    return blockBuffer != null;
  }

  @Override
  public String toString() {
    return "HFileScanner for reader " + String.valueOf(getReader());
  }

  protected void assertSeeked() {
    if (!isSeeked())
      throw new NotSeekedException();
  }

  @Override
  public HFileReader getReader() {
    return reader;
  }

  // From non encoded HFiles, we always read back KeyValue or its descendant.(Note: When HFile
  // block is in DBB, it will be OffheapKV). So all parts of the Cell is in a contiguous
  // array/buffer. How many bytes we should wrap to make the KV is what this method returns.
  private int getKVBufSize() {
    int kvBufSize = KEY_VALUE_LEN_SIZE + currKeyLen + currValueLen;
    if (currTagsLen > 0) {
      kvBufSize += Bytes.SIZEOF_SHORT + currTagsLen;
    }
    return kvBufSize;
  }

  @Override
  public void close() {
    if (!pread) {
      // For seek + pread stream socket should be closed when the scanner is closed. HBASE-9393
      reader.unbufferStream();
    }
    this.returnBlocks(true);
    this.recycle();
  }

  // Returns the #bytes in HFile for the current cell. Used to skip these many bytes in current
  // HFile block's buffer so as to position to the next cell.
  private int getCurCellSerializedSize() {
    int curCellSize =  KEY_VALUE_LEN_SIZE + currKeyLen + currValueLen
        + currMemstoreTSLen;
    if (this.reader.getFileContext().isIncludesTags()) {
      curCellSize += Bytes.SIZEOF_SHORT + currTagsLen;
    }
    return curCellSize;
  }

  protected void readKeyValueLen() {
    // This is a hot method. We go out of our way to make this method short so it can be
    // inlined and is not too big to compile. We also manage position in ByteBuffer ourselves
    // because it is faster than going via range-checked ByteBuffer methods or going through a
    // byte buffer array a byte at a time.
    // Get a long at a time rather than read two individual ints. In micro-benchmarking, even
    // with the extra bit-fiddling, this is order-of-magnitude faster than getting two ints.
    // Trying to imitate what was done - need to profile if this is better or
    // earlier way is better by doing mark and reset?
    // But ensure that you read long instead of two ints
    long ll = blockBuffer.getLongAfterPosition(0);
    // Read top half as an int of key length and bottom int as value length
    this.currKeyLen = (int)(ll >> Integer.SIZE);
    this.currValueLen = (int)(Bytes.MASK_FOR_LOWER_INT_IN_LONG ^ ll);
    checkKeyValueLen();
    // Move position past the key and value lengths and then beyond the key and value
    int p = (Bytes.SIZEOF_LONG + currKeyLen + currValueLen);
    if (reader.getFileContext().isIncludesTags()) {
      // Tags length is a short.
      this.currTagsLen = blockBuffer.getShortAfterPosition(p);
      checkTagsLen();
      p += (Bytes.SIZEOF_SHORT + currTagsLen);
    }
    readMvccVersion(p);
  }

  private final void checkTagsLen() {
    if (checkLen(this.currTagsLen)) {
      throw new IllegalStateException("Invalid currTagsLen " + this.currTagsLen
          + ". Block offset: " + curBlock.getOffset() + ", block length: "
          + this.blockBuffer.limit()
          + ", position: " + this.blockBuffer.position() + " (without header).");
    }
  }

  /**
   * Read mvcc. Does checks to see if we even need to read the mvcc at all.
   * @param offsetFromPos
   */
  protected void readMvccVersion(final int offsetFromPos) {
    // See if we even need to decode mvcc.
    if (!this.reader.getHFileInfo().shouldIncludeMemStoreTS()) {
      return;
    }
    if (!this.reader.getHFileInfo().isDecodeMemstoreTS()) {
      currMemstoreTS = 0;
      currMemstoreTSLen = 1;
      return;
    }
    _readMvccVersion(offsetFromPos);
  }

  /**
   * Actually do the mvcc read. Does no checks.
   * @param offsetFromPos
   */
  private void _readMvccVersion(int offsetFromPos) {
    // This is Bytes#bytesToVint inlined so can save a few instructions in this hot method; i.e.
    // previous if one-byte vint, we'd redo the vint call to find int size.
    // Also the method is kept small so can be inlined.
    byte firstByte = blockBuffer.getByteAfterPosition(offsetFromPos);
    int len = WritableUtils.decodeVIntSize(firstByte);
    if (len == 1) {
      this.currMemstoreTS = firstByte;
    } else {
      int remaining = len -1;
      long i = 0;
      offsetFromPos++;
      if (remaining >= Bytes.SIZEOF_INT) {
        // The int read has to be converted to unsigned long so the & op
        i = (blockBuffer.getIntAfterPosition(offsetFromPos) & 0x00000000ffffffffL);
        remaining -= Bytes.SIZEOF_INT;
        offsetFromPos += Bytes.SIZEOF_INT;
      }
      if (remaining >= Bytes.SIZEOF_SHORT) {
        short s = blockBuffer.getShortAfterPosition(offsetFromPos);
        i = i << 16;
        i = i | (s & 0xFFFF);
        remaining -= Bytes.SIZEOF_SHORT;
        offsetFromPos += Bytes.SIZEOF_SHORT;
      }
      for (int idx = 0; idx < remaining; idx++) {
        byte b = blockBuffer.getByteAfterPosition(offsetFromPos + idx);
        i = i << 8;
        i = i | (b & 0xFF);
      }
      currMemstoreTS = (WritableUtils.isNegativeVInt(firstByte) ? ~i : i);
    }
    this.currMemstoreTSLen = len;
  }

  /**
   * Within a loaded block, seek looking for the last key that is smaller than
   * (or equal to?) the key we are interested in.
   * A note on the seekBefore: if you have seekBefore = true, AND the first
   * key in the block = key, then you'll get thrown exceptions. The caller has
   * to check for that case and load the previous block as appropriate.
   * @param key
   *          the key to find
   * @param seekBefore
   *          find the key before the given key in case of exact match.
   * @return 0 in case of an exact key match, 1 in case of an inexact match,
   *         -2 in case of an inexact match and furthermore, the input key
   *         less than the first key of current block(e.g. using a faked index
   *         key)
   */
  protected int blockSeek(Cell key, boolean seekBefore) {
    int klen, vlen, tlen = 0;
    int lastKeyValueSize = -1;
    int offsetFromPos;
    do {
      offsetFromPos = 0;
      // Better to ensure that we use the BB Utils here
      long ll = blockBuffer.getLongAfterPosition(offsetFromPos);
      klen = (int)(ll >> Integer.SIZE);
      vlen = (int)(Bytes.MASK_FOR_LOWER_INT_IN_LONG ^ ll);
      if (checkKeyLen(klen) || checkLen(vlen)) {
        throw new IllegalStateException("Invalid klen " + klen + " or vlen "
            + vlen + ". Block offset: "
            + curBlock.getOffset() + ", block length: " + blockBuffer.limit() + ", position: "
            + blockBuffer.position() + " (without header).");
      }
      offsetFromPos += Bytes.SIZEOF_LONG;
      blockBuffer.asSubByteBuffer(blockBuffer.position() + offsetFromPos, klen, pair);
      bufBackedKeyOnlyKv.setKey(pair.getFirst(), pair.getSecond(), klen);
      int comp = reader.getComparator().compareKeyIgnoresMvcc(key, bufBackedKeyOnlyKv);
      offsetFromPos += klen + vlen;
      if (this.reader.getFileContext().isIncludesTags()) {
        // Read short as unsigned, high byte first
        tlen = ((blockBuffer.getByteAfterPosition(offsetFromPos) & 0xff) << 8)
            ^ (blockBuffer.getByteAfterPosition(offsetFromPos + 1) & 0xff);
        if (checkLen(tlen)) {
          throw new IllegalStateException("Invalid tlen " + tlen + ". Block offset: "
              + curBlock.getOffset() + ", block length: " + blockBuffer.limit() + ", position: "
              + blockBuffer.position() + " (without header).");
        }
        // add the two bytes read for the tags.
        offsetFromPos += tlen + (Bytes.SIZEOF_SHORT);
      }
      if (this.reader.getHFileInfo().shouldIncludeMemStoreTS()) {
        // Directly read the mvcc based on current position
        readMvccVersion(offsetFromPos);
      }

      if (comp == 0) {
        if (seekBefore) {
          if (lastKeyValueSize < 0) {
            throw new IllegalStateException("blockSeek with seekBefore "
                + "at the first key of the block: key=" + CellUtil.getCellKeyAsString(key)
                + ", blockOffset=" + curBlock.getOffset() + ", onDiskSize="
                + curBlock.getOnDiskSizeWithHeader());
          }
          blockBuffer.moveBack(lastKeyValueSize);
          readKeyValueLen();
          return 1; // non exact match.
        }
        currKeyLen = klen;
        currValueLen = vlen;
        currTagsLen = tlen;
        return 0; // indicate exact match
      } else if (comp < 0) {
        if (lastKeyValueSize > 0) {
          blockBuffer.moveBack(lastKeyValueSize);
        }
        readKeyValueLen();
        if (lastKeyValueSize == -1 && blockBuffer.position() == 0) {
          return HConstants.INDEX_KEY_MAGIC;
        }
        return 1;
      }
      // The size of this key/value tuple, including key/value length fields.
      lastKeyValueSize = klen + vlen + currMemstoreTSLen + KEY_VALUE_LEN_SIZE;
      // include tag length also if tags included with KV
      if (reader.getFileContext().isIncludesTags()) {
        lastKeyValueSize += tlen + Bytes.SIZEOF_SHORT;
      }
      blockBuffer.skip(lastKeyValueSize);
    } while (blockBuffer.hasRemaining());

    // Seek to the last key we successfully read. This will happen if this is
    // the last key/value pair in the file, in which case the following call
    // to next() has to return false.
    blockBuffer.moveBack(lastKeyValueSize);
    readKeyValueLen();
    return 1; // didn't exactly find it.
  }

  @Override
  public Cell getNextIndexedKey() {
    return nextIndexedKey;
  }

  @Override
  public int seekTo(Cell key) throws IOException {
    return seekTo(key, true);
  }

  @Override
  public int reseekTo(Cell key) throws IOException {
    int compared;
    if (isSeeked()) {
      compared = compareKey(reader.getComparator(), key);
      if (compared < 1) {
        // If the required key is less than or equal to current key, then
        // don't do anything.
        return compared;
      } else {
        // The comparison with no_next_index_key has to be checked
        if (this.nextIndexedKey != null &&
            (this.nextIndexedKey == KeyValueScanner.NO_NEXT_INDEXED_KEY || reader
            .getComparator().compareKeyIgnoresMvcc(key, nextIndexedKey) < 0)) {
          // The reader shall continue to scan the current data block instead
          // of querying the
          // block index as long as it knows the target key is strictly
          // smaller than
          // the next indexed key or the current data block is the last data
          // block.
          return loadBlockAndSeekToKey(this.curBlock, nextIndexedKey, false, key,
              false);
        }
      }
    }
    // Don't rewind on a reseek operation, because reseek implies that we are
    // always going forward in the file.
    return seekTo(key, false);
  }

  /**
   * An internal API function. Seek to the given key, optionally rewinding to
   * the first key of the block before doing the seek.
   *
   * @param key - a cell representing the key that we need to fetch
   * @param rewind whether to rewind to the first key of the block before
   *        doing the seek. If this is false, we are assuming we never go
   *        back, otherwise the result is undefined.
   * @return -1 if the key is earlier than the first key of the file,
   *         0 if we are at the given key, 1 if we are past the given key
   *         -2 if the key is earlier than the first key of the file while
   *         using a faked index key
   * @throws IOException
   */
  public int seekTo(Cell key, boolean rewind) throws IOException {
    HFileBlockIndex.BlockIndexReader indexReader = reader.getDataBlockIndexReader();
    BlockWithScanInfo blockWithScanInfo = indexReader.loadDataBlockWithScanInfo(key, curBlock,
        cacheBlocks, pread, isCompaction, getEffectiveDataBlockEncoding(), reader);
    if (blockWithScanInfo == null || blockWithScanInfo.getHFileBlock() == null) {
      // This happens if the key e.g. falls before the beginning of the file.
      return -1;
    }
    return loadBlockAndSeekToKey(blockWithScanInfo.getHFileBlock(),
        blockWithScanInfo.getNextIndexedKey(), rewind, key, false);
  }

  @Override
  public boolean seekBefore(Cell key) throws IOException {
    HFileBlock seekToBlock = reader.getDataBlockIndexReader().seekToDataBlock(key, curBlock,
        cacheBlocks, pread, isCompaction, reader.getEffectiveEncodingInCache(isCompaction),
        reader);
    if (seekToBlock == null) {
      return false;
    }
    Cell firstKey = getFirstKeyCellInBlock(seekToBlock);
    if (reader.getComparator().compareKeyIgnoresMvcc(firstKey, key) >= 0) {
      long previousBlockOffset = seekToBlock.getPrevBlockOffset();
      // The key we are interested in
      if (previousBlockOffset == -1) {
        // we have a 'problem', the key we want is the first of the file.
        releaseIfNotCurBlock(seekToBlock);
        return false;
      }

      // The first key in the current block 'seekToBlock' is greater than the given 
      // seekBefore key. We will go ahead by reading the next block that satisfies the
      // given key. Return the current block before reading the next one.
      releaseIfNotCurBlock(seekToBlock);
      // It is important that we compute and pass onDiskSize to the block
      // reader so that it does not have to read the header separately to
      // figure out the size.  Currently, we do not have a way to do this
      // correctly in the general case however.
      // TODO: See https://issues.apache.org/jira/browse/HBASE-14576
      int prevBlockSize = -1;
      seekToBlock = reader.readBlock(previousBlockOffset, prevBlockSize, cacheBlocks, pread,
        isCompaction, true, BlockType.DATA, getEffectiveDataBlockEncoding());
      // TODO shortcut: seek forward in this block to the last key of the
      // block.
    }
    loadBlockAndSeekToKey(seekToBlock, firstKey, true, key, true);
    return true;
  }

  /**
   * The curBlock will be released by shipping or close method, so only need to consider releasing
   * the block, which was read from HFile before and not referenced by curBlock.
   */
  protected void releaseIfNotCurBlock(HFileBlock block) {
    if (curBlock != block) {
      block.release();
    }
  }

  /**
   * Scans blocks in the "scanned" section of the {@link HFile} until the next
   * data block is found.
   *
   * @return the next block, or null if there are no more data blocks
   * @throws IOException
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="NP_NULL_ON_SOME_PATH",
      justification="Yeah, unnecessary null check; could do w/ clean up")
  protected HFileBlock readNextDataBlock() throws IOException {
    long lastDataBlockOffset = reader.getTrailer().getLastDataBlockOffset();
    if (curBlock == null) {
      return null;
    }
    HFileBlock block = this.curBlock;
    do {
      if (block.getOffset() >= lastDataBlockOffset) {
        releaseIfNotCurBlock(block);
        return null;
      }
      if (block.getOffset() < 0) {
        releaseIfNotCurBlock(block);
        throw new IOException("Invalid block file offset: " + block);
      }
      // We are reading the next block without block type validation, because
      // it might turn out to be a non-data block.
      block = reader.readBlock(block.getOffset() + block.getOnDiskSizeWithHeader(),
          block.getNextBlockOnDiskSize(), cacheBlocks, pread, isCompaction, true, null,
          getEffectiveDataBlockEncoding());
      if (block != null && !block.getBlockType().isData()) {
        // Whatever block we read we will be returning it unless
        // it is a datablock. Just in case the blocks are non data blocks
        block.release();
      }
    } while (!block.getBlockType().isData());
    return block;
  }

  public DataBlockEncoding getEffectiveDataBlockEncoding() {
    return this.reader.getEffectiveEncodingInCache(isCompaction);
  }

  @Override
  public Cell getCell() {
    if (!isSeeked())
      return null;

    Cell ret;
    int cellBufSize = getKVBufSize();
    long seqId = 0l;
    if (this.reader.getHFileInfo().shouldIncludeMemStoreTS()) {
      seqId = currMemstoreTS;
    }
    if (blockBuffer.hasArray()) {
      // TODO : reduce the varieties of KV here. Check if based on a boolean
      // we can handle the 'no tags' case.
      if (currTagsLen > 0) {
        ret = new SizeCachedKeyValue(blockBuffer.array(),
            blockBuffer.arrayOffset() + blockBuffer.position(), cellBufSize, seqId);
      } else {
        ret = new SizeCachedNoTagsKeyValue(blockBuffer.array(),
            blockBuffer.arrayOffset() + blockBuffer.position(), cellBufSize, seqId);
      }
    } else {
      ByteBuffer buf = blockBuffer.asSubByteBuffer(cellBufSize);
      if (buf.isDirect()) {
        ret = currTagsLen > 0 ? new ByteBufferKeyValue(buf, buf.position(), cellBufSize, seqId)
            : new NoTagsByteBufferKeyValue(buf, buf.position(), cellBufSize, seqId);
      } else {
        if (currTagsLen > 0) {
          ret = new SizeCachedKeyValue(buf.array(), buf.arrayOffset() + buf.position(),
              cellBufSize, seqId);
        } else {
          ret = new SizeCachedNoTagsKeyValue(buf.array(), buf.arrayOffset() + buf.position(),
              cellBufSize, seqId);
        }
      }
    }
    return ret;
  }

  @Override
  public Cell getKey() {
    assertSeeked();
    // Create a new object so that this getKey is cached as firstKey, lastKey
    ObjectIntPair<ByteBuffer> keyPair = new ObjectIntPair<ByteBuffer>();
    blockBuffer.asSubByteBuffer(blockBuffer.position() + KEY_VALUE_LEN_SIZE, currKeyLen, keyPair);
    ByteBuffer keyBuf = keyPair.getFirst();
    if (keyBuf.hasArray()) {
      return new KeyValue.KeyOnlyKeyValue(keyBuf.array(), keyBuf.arrayOffset()
          + keyPair.getSecond(), currKeyLen);
    } else {
      // Better to do a copy here instead of holding on to this BB so that
      // we could release the blocks referring to this key. This key is specifically used 
      // in HalfStoreFileReader to get the firstkey and lastkey by creating a new scanner
      // every time. So holding onto the BB (incase of DBB) is not advised here.
      byte[] key = new byte[currKeyLen];
      ByteBufferUtils.copyFromBufferToArray(key, keyBuf, keyPair.getSecond(), 0, currKeyLen);
      return new KeyValue.KeyOnlyKeyValue(key, 0, currKeyLen);
    }      
  }

  @Override
  public ByteBuffer getValue() {
    assertSeeked();
    // Okie to create new Pair. Not used in hot path
    ObjectIntPair<ByteBuffer> valuePair = new ObjectIntPair<ByteBuffer>();
    this.blockBuffer.asSubByteBuffer(blockBuffer.position() + KEY_VALUE_LEN_SIZE + currKeyLen,
        currValueLen, valuePair);
    ByteBuffer valBuf = valuePair.getFirst().duplicate();
    valBuf.position(valuePair.getSecond());
    valBuf.limit(currValueLen + valuePair.getSecond());
    return valBuf.slice();
  }

  protected void setNonSeekedState() {
    reset();
    blockBuffer = null;
    currKeyLen = 0;
    currValueLen = 0;
    currMemstoreTS = 0;
    currMemstoreTSLen = 0;
    currTagsLen = 0;
  }

  /**
   * Set the position on current backing blockBuffer.
   */
  private void positionThisBlockBuffer() {
    try {
      blockBuffer.skip(getCurCellSerializedSize());
    } catch (IllegalArgumentException e) {
      LOG.error("Current pos = " + blockBuffer.position()
          + "; currKeyLen = " + currKeyLen + "; currValLen = "
          + currValueLen + "; block limit = " + blockBuffer.limit()
          + "; HFile name = " + reader.getName()
          + "; currBlock currBlockOffset = " + this.curBlock.getOffset());
      throw e;
    }
  }

  /**
   * Set our selves up for the next 'next' invocation, set up next block.
   * @return True is more to read else false if at the end.
   * @throws IOException
   */
  private boolean positionForNextBlock() throws IOException {
    // Methods are small so they get inlined because they are 'hot'.
    long lastDataBlockOffset = reader.getTrailer().getLastDataBlockOffset();
    if (this.curBlock.getOffset() >= lastDataBlockOffset) {
      setNonSeekedState();
      return false;
    }
    return isNextBlock();
  }

  private boolean isNextBlock() throws IOException {
    // Methods are small so they get inlined because they are 'hot'.
    HFileBlock nextBlock = readNextDataBlock();
    if (nextBlock == null) {
      setNonSeekedState();
      return false;
    }
    updateCurrentBlock(nextBlock);
    return true;
  }

  private final boolean _next() throws IOException {
    // Small method so can be inlined. It is a hot one.
    if (blockBuffer.remaining() <= 0) {
      return positionForNextBlock();
    }

    // We are still in the same block.
    readKeyValueLen();
    return true;
  }

  /**
   * Go to the next key/value in the block section. Loads the next block if
   * necessary. If successful, {@link #getKey()} and {@link #getValue()} can
   * be called.
   *
   * @return true if successfully navigated to the next key/value
   */
  @Override
  public boolean next() throws IOException {
    // This is a hot method so extreme measures taken to ensure it is small and inlineable.
    // Checked by setting: -XX:+UnlockDiagnosticVMOptions -XX:+PrintInlining -XX:+PrintCompilation
    assertSeeked();
    positionThisBlockBuffer();
    return _next();
  }

  /**
   * Positions this scanner at the start of the file.
   *
   * @return false if empty file; i.e. a call to next would return false and
   *         the current key and value are undefined.
   * @throws IOException
   */
  @Override
  public boolean seekTo() throws IOException {
    if (reader == null) {
      return false;
    }

    if (reader.getTrailer().getEntryCount() == 0) {
      // No data blocks.
      return false;
    }

    long firstDataBlockOffset = reader.getTrailer().getFirstDataBlockOffset();
    if (curBlock != null && curBlock.getOffset() == firstDataBlockOffset) {
      return processFirstDataBlock();
    }

    readAndUpdateNewBlock(firstDataBlockOffset);
    return true;
  }

  protected boolean processFirstDataBlock() throws IOException{
    blockBuffer.rewind();
    readKeyValueLen();
    return true;
  }

  protected void readAndUpdateNewBlock(long firstDataBlockOffset) throws IOException {
    HFileBlock newBlock = reader.readBlock(firstDataBlockOffset, -1, cacheBlocks, pread,
        isCompaction, true, BlockType.DATA, getEffectiveDataBlockEncoding());
    if (newBlock.getOffset() < 0) {
      releaseIfNotCurBlock(newBlock);
      throw new IOException("Invalid block offset: " + newBlock.getOffset());
    }
    updateCurrentBlock(newBlock);
  }

  protected int loadBlockAndSeekToKey(HFileBlock seekToBlock, Cell nextIndexedKey, boolean rewind,
      Cell key, boolean seekBefore) throws IOException {
    if (this.curBlock == null || this.curBlock.getOffset() != seekToBlock.getOffset()) {
      updateCurrentBlock(seekToBlock);
    } else if (rewind) {
      blockBuffer.rewind();
    }

    // Update the nextIndexedKey
    this.nextIndexedKey = nextIndexedKey;
    return blockSeek(key, seekBefore);
  }

  /**
   * @param v
   * @return True if v &lt;= 0 or v &gt; current block buffer limit.
   */
  protected final boolean checkKeyLen(final int v) {
    return v <= 0 || v > this.blockBuffer.limit();
  }

  /**
   * @param v
   * @return True if v < 0 or v > current block buffer limit.
   */
  protected final boolean checkLen(final int v) {
    return v < 0 || v > this.blockBuffer.limit();
  }

  /**
   * Check key and value lengths are wholesome.
   */
  protected final void checkKeyValueLen() {
    if (checkKeyLen(this.currKeyLen) || checkLen(this.currValueLen)) {
      throw new IllegalStateException("Invalid currKeyLen " + this.currKeyLen
          + " or currValueLen " + this.currValueLen + ". Block offset: "
          + this.curBlock.getOffset() + ", block length: "
          + this.blockBuffer.limit() + ", position: " + this.blockBuffer.position()
          + " (without header).");
    }
  }

  /**
   * Updates the current block to be the given {@link HFileBlock}. Seeks to the the first
   * key/value pair.
   * @param newBlock the block read by {@link HFileReaderImpl#readBlock}, it's a totally new block
   *          with new allocated {@link ByteBuff}, so if no further reference to this block, we
   *          should release it carefully.
   */
  protected void updateCurrentBlock(HFileBlock newBlock) throws IOException {
    try {
      if (newBlock.getBlockType() != BlockType.DATA) {
        throw new IllegalStateException(
            "ScannerV2 works only on data blocks, got " + newBlock.getBlockType()
                + "; fileName=" + reader.getName() + ", dataBlockEncoder="
                + reader.getDataBlockEncoding() + ", isCompaction=" + isCompaction);
      }
      updateCurrBlockRef(newBlock);
      blockBuffer = curBlock.getBufferWithoutHeader();
      readKeyValueLen();
    } finally {
      releaseIfNotCurBlock(newBlock);
    }
    // Reset the next indexed key
    this.nextIndexedKey = null;
  }

  protected Cell getFirstKeyCellInBlock(HFileBlock curBlock) {
    ByteBuff buffer = curBlock.getBufferWithoutHeader();
    // It is safe to manipulate this buffer because we own the buffer object.
    buffer.rewind();
    int klen = buffer.getInt();
    buffer.skip(Bytes.SIZEOF_INT);// Skip value len part
    ByteBuffer keyBuff = buffer.asSubByteBuffer(klen);
    if (keyBuff.hasArray()) {
      return new KeyValue.KeyOnlyKeyValue(keyBuff.array(), keyBuff.arrayOffset()
          + keyBuff.position(), klen);
    } else {
      return new ByteBufferKeyOnlyKeyValue(keyBuff, keyBuff.position(), klen);
    }
  }

  @Override
  public String getKeyString() {
    return CellUtil.toString(getKey(), false);
  }

  @Override
  public String getValueString() {
    return ByteBufferUtils.toStringBinary(getValue());
  }

  public int compareKey(CellComparator comparator, Cell key) {
    blockBuffer.asSubByteBuffer(blockBuffer.position() + KEY_VALUE_LEN_SIZE, currKeyLen, pair);
    this.bufBackedKeyOnlyKv.setKey(pair.getFirst(), pair.getSecond(), currKeyLen);
    return comparator.compareKeyIgnoresMvcc(key, this.bufBackedKeyOnlyKv);
  }

  @Override
  public void shipped() throws IOException {
    this.returnBlocks(false);
  }
}
