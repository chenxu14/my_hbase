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
package org.apache.hadoop.hbase.wal;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.wal.WALSplitter.EntryBuffers;
import org.apache.hadoop.hbase.wal.WALSplitter.PipelineController;
import org.apache.hadoop.hbase.wal.WALSplitter.RegionEntryBuffer;

@InterfaceAudience.Private
public class WriterThread extends Thread {
  private static final Log LOG = LogFactory.getLog(WriterThread.class);
  volatile boolean shouldStop = false;
  private PipelineController controller;
  private EntryBuffers entryBuffers;
  private OutputSink outputSink = null;

  WriterThread(PipelineController controller, EntryBuffers entryBuffers, OutputSink sink, int i){
    super(Thread.currentThread().getName() + "-Writer-" + i);
    this.controller = controller;
    this.entryBuffers = entryBuffers;
    outputSink = sink;
  }

  @Override
  public void run()  {
    try {
      doRun();
    } catch (Throwable t) {
      LOG.error("Exiting thread", t);
      controller.writerThreadError(t);
    }
  }

  private void doRun() throws IOException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Writer thread starting");
    }
    while (true) {
      RegionEntryBuffer buffer = entryBuffers.getChunkToWrite();
      if (buffer == null) {
        // No data currently available, wait on some more to show up
        synchronized (controller.dataAvailable) {
          if (shouldStop && !this.outputSink.flush()) {
            return;
          }
          try {
            controller.dataAvailable.wait(500);
          } catch (InterruptedException ie) {
            if (!shouldStop) {
              throw new RuntimeException(ie);
            }
          }
        }
        continue;
      }
      assert buffer != null;
      try {
        writeBuffer(buffer);
      } finally {
        entryBuffers.doneWriting(buffer);
      }
    }
  }

  private void writeBuffer(RegionEntryBuffer buffer) throws IOException {
    outputSink.append(buffer);
  }

  void finish() {
    synchronized (controller.dataAvailable) {
      shouldStop = true;
      controller.dataAvailable.notifyAll();
    }
  }
}
