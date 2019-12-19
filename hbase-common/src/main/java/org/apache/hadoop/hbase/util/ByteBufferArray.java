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
package org.apache.hadoop.hbase.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.shaded.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.util.StringUtils;

/**
 * This class manages an array of ByteBuffers with a default size 4MB. These
 * buffers are sequential and could be considered as a large buffer.It supports
 * reading/writing data from this large buffer with a position and offset
 */
@InterfaceAudience.Private
public class ByteBufferArray {
  private static final Log LOG = LogFactory.getLog(ByteBufferArray.class);

  public static final int DEFAULT_BUFFER_SIZE = 4 * 1024 * 1024;
  @VisibleForTesting
  ByteBuffer buffers[];
  private int bufferSize;
  @VisibleForTesting
  int bufferCount;

  /**
   * We allocate a number of byte buffers as the capacity. In order not to out
   * of the array bounds for the last byte(see {@link ByteBufferArray#multiple}),
   * we will allocate one additional buffer with capacity 0;
   * @param capacity total size of the byte buffer array
   * @param directByteBuffer true if we allocate direct buffer
   * @param allocator the ByteBufferAllocator that will create the buffers
   * @throws IOException throws IOException if there is an exception thrown by the allocator
   */
  public ByteBufferArray(long capacity, boolean directByteBuffer, ByteBufferAllocator allocator)
      throws IOException {
    this.bufferSize = DEFAULT_BUFFER_SIZE;
    if (this.bufferSize > (capacity / 16))
      this.bufferSize = (int) roundUp(capacity / 16, 32768);
    this.bufferCount = (int) (roundUp(capacity, bufferSize) / bufferSize);
    LOG.info("Allocating buffers total=" + StringUtils.byteDesc(capacity)
        + ", sizePerBuffer=" + StringUtils.byteDesc(bufferSize) + ", count="
        + bufferCount + ", direct=" + directByteBuffer);
    buffers = new ByteBuffer[bufferCount + 1];
    createBuffers(directByteBuffer, allocator);
  }

  @VisibleForTesting
  void createBuffers(boolean directByteBuffer, ByteBufferAllocator allocator)
      throws IOException {
    int threadCount = getThreadCount();
    ExecutorService service = new ThreadPoolExecutor(threadCount, threadCount, 0L,
        TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
    int perThreadCount = (int)Math.floor((double) (bufferCount) / threadCount);
    int lastThreadCount = bufferCount - (perThreadCount * (threadCount - 1));
    Future<ByteBuffer[]>[] futures = new Future[threadCount];
    try {
      for (int i = 0; i < threadCount; i++) {
        // Last thread will have to deal with a different number of buffers
        int buffersToCreate = (i == threadCount - 1) ? lastThreadCount : perThreadCount;
        futures[i] = service.submit(
          new BufferCreatorCallable(bufferSize, directByteBuffer, buffersToCreate, allocator));
      }
      int bufferIndex = 0;
      for (Future<ByteBuffer[]> future : futures) {
        try {
          ByteBuffer[] buffers = future.get();
          for (ByteBuffer buffer : buffers) {
            this.buffers[bufferIndex++] = buffer;
          }
        } catch (InterruptedException | ExecutionException e) {
          LOG.error("Buffer creation interrupted", e);
          throw new IOException(e);
        }
      }
    } finally {
      service.shutdownNow();
    }
    // always create on heap empty dummy buffer at last
    this.buffers[bufferCount] = ByteBuffer.allocate(0);
  }

  @VisibleForTesting
  int getThreadCount() {
    return Runtime.getRuntime().availableProcessors();
  }

  /**
   * A callable that creates buffers of the specified length either onheap/offheap using the
   * {@link ByteBufferAllocator}
   */
  private static class BufferCreatorCallable implements Callable<ByteBuffer[]> {
    private final int bufferCapacity;
    private final boolean directByteBuffer;
    private final int bufferCount;
    private final ByteBufferAllocator allocator;

    BufferCreatorCallable(int bufferCapacity, boolean directByteBuffer, int bufferCount,
        ByteBufferAllocator allocator) {
      this.bufferCapacity = bufferCapacity;
      this.directByteBuffer = directByteBuffer;
      this.bufferCount = bufferCount;
      this.allocator = allocator;
    }

    @Override
    public ByteBuffer[] call() throws Exception {
      ByteBuffer[] buffers = new ByteBuffer[this.bufferCount];
      for (int i = 0; i < this.bufferCount; i++) {
        buffers[i] = allocator.allocate(this.bufferCapacity, this.directByteBuffer);
      }
      return buffers;
    }
  }

  private long roundUp(long n, long to) {
    return ((n + to - 1) / to) * to;
  }

  /**
   * Transfers bytes from this buffers array into the given destination {@link ByteBuff}
   * @param offset start position in this big logical array.
   * @param dst the destination ByteBuff. Notice that its position will be advanced.
   * @return number of bytes read
   */
  public int read(long offset, ByteBuff dst) {
    return internalTransfer(offset, dst, READER);
  }

  /**
   * Transfers bytes from the given source {@link ByteBuff} into this buffer array
   * @param offset start offset of this big logical array.
   * @param src the source ByteBuff. Notice that its position will be advanced.
   * @return number of bytes write
   */
  public int write(long offset, ByteBuff src) {
    return internalTransfer(offset, src, WRITER);
  }

  /**
   * Transfer bytes from source {@link ByteBuff} to destination {@link ByteBuffer}. Position of both
   * source and destination will be advanced.
   */
  private static final BiConsumer<ByteBuffer, ByteBuff> WRITER = (dst, src) -> {
    int off = src.position(), len = dst.remaining();
    src.get(dst, off, len);
    src.position(off + len);
  };

  /**
   * Transfer bytes from source {@link ByteBuffer} to destination {@link ByteBuff}, Position of both
   * source and destination will be advanced.
   */
  private static final BiConsumer<ByteBuffer, ByteBuff> READER = (src, dst) -> {
    int off = dst.position(), len = src.remaining(), srcOff = src.position();
    dst.put(off, ByteBuff.wrap(src), srcOff, len);
    src.position(srcOff + len);
    dst.position(off + len);
  };

  /**
   * Transferring all remaining bytes from b to the buffers array starting at offset, or
   * transferring bytes from the buffers array at offset to b until b is filled. Notice that
   * position of ByteBuff b will be advanced.
   * @param offset where we start in the big logical array.
   * @param b the ByteBuff to transfer from or to
   * @param transfer the transfer interface.
   * @return the length of bytes we transferred.
   */
  private int internalTransfer(long offset, ByteBuff b, BiConsumer<ByteBuffer, ByteBuff> transfer) {
    int expectedTransferLen = b.remaining();
    if (expectedTransferLen == 0) {
      return 0;
    }
    BufferIterator it = new BufferIterator(offset, expectedTransferLen);
    while (it.hasNext()) {
      ByteBuffer a = it.next();
      transfer.accept(a, b);
      assert !a.hasRemaining();
    }
    assert expectedTransferLen == it.getSum() : "Expected transfer length (=" + expectedTransferLen
        + ") don't match the actual transfer length(=" + it.getSum() + ")";
    return expectedTransferLen;
  }

  /**
   * Creates a ByteBuff from a given array of ByteBuffers from the given offset to the length
   * specified. For eg, if there are 4 buffers forming an array each with length 10 and if we call
   * asSubBuffer(5, 10) then we will create an MBB consisting of two BBs and the first one be a BB
   * from 'position' 5 to a 'length' 5 and the 2nd BB will be from 'position' 0 to 'length' 5.
   * @param offset the position in the whole array which is composited by multiple byte buffers.
   * @param len the length of bytes
   * @return a ByteBuff formed from the underlying ByteBuffers
   */
  public ByteBuffer[] asSubByteBuffers(long offset, final int len) {
    BufferIterator it = new BufferIterator(offset, len);
    ByteBuffer[] mbb = new ByteBuffer[it.getBufferCount()];
    for (int i = 0; i < mbb.length; i++) {
      assert it.hasNext();
      mbb[i] = it.next();
    }
    assert it.getSum() == len;
    return mbb;
  }

  /**
   * Iterator to fetch ByteBuffers from offset with given length in this big logical array.
   */
  private class BufferIterator implements Iterator<ByteBuffer> {
    private final int len;
    private int startBuffer, startOffset, endBuffer, endOffset;
    private int curIndex, sum = 0;

    private int index(long pos) {
      return (int) (pos / bufferSize);
    }

    private int offset(long pos) {
      return (int) (pos % bufferSize);
    }

    public BufferIterator(long offset, int len) {
      assert len >= 0 && offset >= 0;
      this.len = len;

      this.startBuffer = index(offset);
      this.startOffset = offset(offset);

      this.endBuffer = index(offset + len);
      this.endOffset = offset(offset + len);
      if (startBuffer < endBuffer && endOffset == 0) {
        endBuffer--;
        endOffset = bufferSize;
      }
      assert startBuffer >= 0 && startBuffer < bufferCount;
      assert endBuffer >= 0 && endBuffer < bufferCount;

      // initialize the index to the first buffer index.
      this.curIndex = startBuffer;
    }

    @Override
    public boolean hasNext() {
      return this.curIndex <= endBuffer;
    }

    /**
     * The returned ByteBuffer is an sliced one, it won't affect the position or limit of the
     * original one.
     */
    @Override
    public ByteBuffer next() {
      ByteBuffer bb = buffers[curIndex].duplicate();
      if (curIndex == startBuffer) {
        bb.position(startOffset).limit(Math.min(bufferSize, startOffset + len));
      } else if (curIndex == endBuffer) {
        bb.position(0).limit(endOffset);
      } else {
        bb.position(0).limit(bufferSize);
      }
      curIndex++;
      sum += bb.remaining();
      // Make sure that its pos is zero, it's important because MBB will count from zero for all nio
      // ByteBuffers.
      return bb.slice();
    }

    int getSum() {
      return sum;
    }

    int getBufferCount() {
      return this.endBuffer - this.startBuffer + 1;
    }
  }
}
