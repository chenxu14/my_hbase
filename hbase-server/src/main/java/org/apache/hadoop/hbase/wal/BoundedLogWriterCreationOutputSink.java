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
package org.apache.hadoop.hbase.wal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALSplitter.RegionEntryBuffer;
import org.apache.hadoop.hbase.wal.WALSplitter.WriterAndPath;
import org.apache.hadoop.io.MultipleIOException;

/**
 * Class that will limit the number of hdfs writers we create to split the logs
 */
@InterfaceAudience.Private
public class BoundedLogWriterCreationOutputSink extends LogRecoveredEditsOutputSink {
  private static final Log LOG = LogFactory.getLog(LogRecoveredEditsOutputSink.class);
  ConcurrentHashMap<String, Long> regionRecoverStatMap = new ConcurrentHashMap<>();

  public BoundedLogWriterCreationOutputSink(WALSplitter spliter){
    super(spliter);
  }

  @Override
  public List<Path> finishWritingAndClose() throws IOException {
    boolean isSuccessful;
    List<Path> result;
    try {
      isSuccessful = finishWriting(false);
    } finally {
      result = close();
    }
    if (isSuccessful) {
      splits = result;
    }
    return splits;
  }

  @Override
  boolean executeCloseTask(CompletionService<Void> closeCompletionService,
      final List<IOException> thrown, final List<Path> paths)
      throws InterruptedException, ExecutionException {
    for (final Map.Entry<byte[], RegionEntryBuffer> buffer : entryBuffers.buffers.entrySet()) {
      LOG.info("Submitting write then close of " +
          Bytes.toString(buffer.getValue().encodedRegionName));
      closeCompletionService.submit(new Callable<Void>() {
        public Void call() throws Exception {
          Path dst = writeThenClose(buffer.getValue());
          paths.add(dst);
          return null;
        }
      });
    }

    boolean progress_failed = false;
    for (int i = 0, n = entryBuffers.buffers.size(); i < n; i++) {
      Future<Void> future = closeCompletionService.take();
      future.get();
      if (!progress_failed && reporter != null && !reporter.progress()) {
        progress_failed = true;
      }
    }
    return progress_failed;
  }

  @Override
  public Map<byte[], Long> getOutputCounts() {
    Map<byte[], Long> regionRecoverStatMapResult = new HashMap<>();
    for (Map.Entry<String, Long> entry: regionRecoverStatMap.entrySet()) {
      regionRecoverStatMapResult.put(Bytes.toBytes(entry.getKey()), entry.getValue());
    }
    return regionRecoverStatMapResult;
  }

  @Override
  public int getNumberOfRecoveredRegions() {
    return regionRecoverStatMap.size();
  }

  @Override
  public void append(RegionEntryBuffer buffer) throws IOException {
    writeThenClose(buffer);
  }

  private Path writeThenClose(RegionEntryBuffer buffer) throws IOException {
    WriterAndPath wap = appendBuffer(buffer, false);
    Path dst = null;
    if (wap != null) {
      String encodedRegionName = Bytes.toString(buffer.encodedRegionName);
      Long value = regionRecoverStatMap.putIfAbsent(encodedRegionName, wap.editsWritten);
      if (value != null) {
        Long newValue = regionRecoverStatMap.get(encodedRegionName) + wap.editsWritten;
        regionRecoverStatMap.put(encodedRegionName, newValue);
      }
    }

    List<IOException> thrown = new ArrayList<>();
    if (wap != null) {
      dst = closeWriter(Bytes.toString(buffer.encodedRegionName), wap, thrown);
    }

    if (!thrown.isEmpty()) {
      throw MultipleIOException.createIOException(thrown);
    }
    return dst;
  }
}
