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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.shaded.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ZooKeeperProtos.SplitLogTask.RecoveryMode;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.wal.KafkaWALSplitter;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.kafka.common.KafkaException;

/**
 * Do Log split task base on KAFKA
 */
@InterfaceAudience.Private
public class KafkaSplitLogWorker extends SplitLogWorker {
  private static final Log LOG = LogFactory.getLog(KafkaSplitLogWorker.class);

  @VisibleForTesting
  public KafkaSplitLogWorker(Server hserver, Configuration conf, RegionServerServices server,
      TaskExecutor splitTaskExecutor) {
    super(hserver, conf, server, splitTaskExecutor);
  }

  public KafkaSplitLogWorker(Server hserver, Configuration conf, RegionServerServices server,
      final LastSequenceId sequenceIdChecker, final WALFactory factory) {
    super(hserver, conf, server, new TaskExecutor() {
      @Override
      public Status exec(String name, RecoveryMode mode, CancelableProgressable p) {
        LOG.info("grap one task : " + name);
        Path rootdir;
        FileSystem fs;
        try {
          rootdir = FSUtils.getRootDir(conf);
          fs = rootdir.getFileSystem(conf);
        } catch (IOException e) {
          LOG.warn("could not find root dir or fs", e);
          return Status.RESIGNED;
        }
        try {
          if (!new KafkaWALSplitter(conf, fs, factory, rootdir).splitLogFile(name, p)) {
            return Status.PREEMPTED;
          }
        } catch (InterruptedIOException iioe) {
          LOG.warn("log splitting of " + name + " interrupted, resigning", iioe);
          return Status.RESIGNED;
        } catch (KafkaException e) {
          LOG.warn("log splitting of " + name + " has kafka exception, resigning", e);
          return Status.RESIGNED;
        } catch (Exception e) {
          Throwable cause = e.getCause();
          if (e instanceof RetriesExhaustedException && (cause instanceof NotServingRegionException
              || cause instanceof ConnectException
              || cause instanceof SocketTimeoutException)) {
            LOG.warn("log replaying of " + name + " can't connect to the target regionserver, "
                + "resigning", e);
            return Status.RESIGNED;
          } else if (cause instanceof InterruptedException) {
            LOG.warn("log splitting of " + name + " interrupted, resigning", e);
            return Status.RESIGNED;
          }
          LOG.warn("log splitting of " + name + " failed, returning error", e);
          return Status.ERR;
        }
        return Status.DONE;
      }
    });
  }

  @Override
  public boolean isMetaTask(String taskName) {
    return false;
  }
}
