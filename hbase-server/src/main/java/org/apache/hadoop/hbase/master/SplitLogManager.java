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
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.SplitLogCounters;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.coordination.BaseCoordinatedStateManager;
import org.apache.hadoop.hbase.coordination.SplitLogManagerCoordination;
import org.apache.hadoop.hbase.coordination.ZKSplitLogManagerCoordination.TaskFinisher;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.shaded.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ZooKeeperProtos.SplitLogTask.RecoveryMode;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hbase.wal.HdfsWALSplitter;

/**
 * Distributes the task of log splitting to the available region servers.
 * Coordination happens via coordination engine. For every log file that has to be split a
 * task is created. SplitLogWorkers race to grab a task.
 *
 * <p>SplitLogManager monitors the tasks that it creates using the
 * timeoutMonitor thread. If a task's progress is slow then
 * {@link SplitLogManagerCoordination#checkTasks} will take away the
 * task from the owner {@link org.apache.hadoop.hbase.regionserver.SplitLogWorker} 
 * and the task will be up for grabs again. When the task is done then it is deleted 
 * by SplitLogManager.
 *
 * <p>Clients call {@link #splitLogDistributed(Path)} to split a region server's
 * log files. The caller thread waits in this method until all the log files
 * have been split.
 *
 * <p>All the coordination calls made by this class are asynchronous. This is mainly
 * to help reduce response time seen by the callers.
 *
 * <p>There is race in this design between the SplitLogManager and the
 * SplitLogWorker. SplitLogManager might re-queue a task that has in reality
 * already been completed by a SplitLogWorker. We rely on the idempotency of
 * the log splitting task for correctness.
 *
 * <p>It is also assumed that every log splitting task is unique and once
 * completed (either with success or with error) it will be not be submitted
 * again. If a task is resubmitted then there is a risk that old "delete task"
 * can delete the re-submission.
 */
@InterfaceAudience.Private
public class SplitLogManager extends LogRecoveryManager {
  private static final Log LOG = LogFactory.getLog(SplitLogManager.class);

  /**
   * In distributedLogReplay mode, we need touch both splitlog and recovering-regions znodes in one
   * operation. So the lock is used to guard such cases.
   */
  protected final ReentrantLock recoveringRegionLock = new ReentrantLock();

  /**
   * Its OK to construct this object even when region-servers are not online. It does lookup the
   * orphan tasks in coordination engine but it doesn't block waiting for them to be done.
   * @param server the server instance
   * @param conf the HBase configuration
   * @param stopper the stoppable in case anything is wrong
   * @param master the master services
   * @param serverName the master server name
   * @throws IOException
   */
  public SplitLogManager(Server server, Configuration conf, Stoppable stopper,
      MasterServices master, ServerName serverName) throws IOException {
    super(server, conf, stopper, master, serverName);
  }

  @Override
  protected TaskFinisher getTaskFinisher() {
    return new TaskFinisher() {
      @Override
      public Status finish(ServerName workerName, String logfile) {
        try {
          HdfsWALSplitter.finishSplitLogFile(logfile, conf);
        } catch (IOException e) {
          LOG.warn("Could not finish splitting of log file " + logfile, e);
          return Status.ERR;
        }
        return Status.DONE;
      }
    };
  }

  private FileStatus[] getFileList(List<Path> logDirs, PathFilter filter) throws IOException {
    return getFileList(conf, logDirs, filter);
  }

  /**
   * Get a list of paths that need to be split given a set of server-specific directories and
   * optionally  a filter.
   *
   * See {@link AbstractFSWALProvider#getServerNameFromWALDirectoryName} for more info on directory
   * layout.
   *
   * Should be package-private, but is needed by
   * {@link org.apache.hadoop.hbase.wal.HdfsWALSplitter#split(Path, Path, Path, FileSystem,
   *     Configuration, org.apache.hadoop.hbase.wal.WALFactory)} for tests.
   */
  @VisibleForTesting
  public static FileStatus[] getFileList(final Configuration conf, final List<Path> logDirs,
      final PathFilter filter)
      throws IOException {
    List<FileStatus> fileStatus = new ArrayList<FileStatus>();
    for (Path logDir : logDirs) {
      final FileSystem fs = logDir.getFileSystem(conf);
      if (!fs.exists(logDir)) {
        LOG.warn(logDir + " doesn't exist. Nothing to do!");
        continue;
      }
      FileStatus[] logfiles = FSUtils.listStatus(fs, logDir, filter);
      if (logfiles == null || logfiles.length == 0) {
        LOG.info(logDir + " is empty dir, no logs to split");
      } else {
        Collections.addAll(fileStatus, logfiles);
      }
    }
    FileStatus[] a = new FileStatus[fileStatus.size()];
    return fileStatus.toArray(a);
  }

  /**
   * @param logDir one region sever wal dir path in .logs
   * @throws IOException if there was an error while splitting any log file
   * @return cumulative size of the logfiles split
   * @throws IOException
   */
  public long splitLogDistributed(final Path logDir) throws IOException {
    List<Path> logDirs = new ArrayList<Path>();
    logDirs.add(logDir);
    return splitLogDistributed(logDirs);
  }

  /**
   * The caller will block until all the log files of the given region server have been processed -
   * successfully split or an error is encountered - by an available worker region server. This
   * method must only be called after the region servers have been brought online.
   * @param logDirs List of log dirs to split
   * @throws IOException If there was an error while splitting any log file
   * @return cumulative size of the logfiles split
   */
  public long splitLogDistributed(final List<Path> logDirs) throws IOException {
    if (logDirs.isEmpty()) {
      return 0;
    }
    Set<ServerName> serverNames = new HashSet<ServerName>();
    for (Path logDir : logDirs) {
      try {
        ServerName serverName = AbstractFSWALProvider.getServerNameFromWALDirectoryName(logDir);
        if (serverName != null) {
          serverNames.add(serverName);
        }
      } catch (IllegalArgumentException e) {
        // ignore invalid format error.
        LOG.warn("Cannot parse server name from " + logDir);
      }
    }
    return splitLogDistributed(serverNames, logDirs, null);
  }

  /**
   * The caller will block until all the hbase:meta log files of the given region server have been
   * processed - successfully split or an error is encountered - by an available worker region
   * server. This method must only be called after the region servers have been brought online.
   * @param logDirs List of log dirs to split
   * @param filter the Path filter to select specific files for considering
   * @throws IOException If there was an error while splitting any log file
   * @return cumulative size of the logfiles split
   */
  public long splitLogDistributed(final Set<ServerName> serverNames, final List<Path> logDirs,
      PathFilter filter) throws IOException {
    if (logDirs == null || logDirs.size() == 0) {
      LOG.info("No need to do log split, since there is no log file.");
      return 0;
    }
    MonitoredTask status = TaskMonitor.get().createStatus("Doing distributed log split in " +
      logDirs + " for serverName=" + serverNames);
    FileStatus[] logfiles = getFileList(logDirs, filter);
    status.setStatus("Checking directory contents...");
    SplitLogCounters.tot_mgr_log_split_batch_start.incrementAndGet();
    LOG.info("Started splitting " + logfiles.length + " logs in " + logDirs +
      " for " + serverNames);
    long t = EnvironmentEdgeManager.currentTime();
    long totalSize = 0;
    TaskBatch batch = new TaskBatch();
    for (FileStatus lf : logfiles) {
      // TODO If the log file is still being written to - which is most likely
      // the case for the last log file - then its length will show up here
      // as zero. The size of such a file can only be retrieved after
      // recover-lease is done. totalSize will be under in most cases and the
      // metrics that it drives will also be under-reported.
      totalSize += lf.getLen();
      String pathToLog = FSUtils.removeRootPath(lf.getPath(), conf);
      if (!enqueueSplitTask(pathToLog, batch)) {
        throw new IOException("duplicate log split scheduled for " + lf.getPath());
      }
    }
    waitForSplittingCompletion(batch, status);

    if (batch.done != batch.installed) {
      batch.isDead = true;
      SplitLogCounters.tot_mgr_log_split_batch_err.incrementAndGet();
      LOG.warn("error while splitting logs in " + logDirs + " installed = " + batch.installed
          + " but only " + batch.done + " done");
      String msg = "error or interrupted while splitting logs in " + logDirs + " Task = " + batch;
      status.abort(msg);
      throw new IOException(msg);
    }
    for (Path logDir : logDirs) {
      status.setStatus("Cleaning up log directory...");
      final FileSystem fs = logDir.getFileSystem(conf);
      try {
        if (fs.exists(logDir) && !fs.delete(logDir, false)) {
          LOG.warn("Unable to delete log src dir. Ignoring. " + logDir);
        }
      } catch (IOException ioe) {
        FileStatus[] files = fs.listStatus(logDir);
        if (files != null && files.length > 0) {
          LOG.warn("Returning success without actually splitting and "
              + "deleting all the log files in path " + logDir + ": "
              + Arrays.toString(files), ioe);
        } else {
          LOG.warn("Unable to delete log src dir. Ignoring. " + logDir, ioe);
        }
      }
      SplitLogCounters.tot_mgr_log_split_batch_success.incrementAndGet();
    }
    String msg =
        "finished splitting (more than or equal to) " + totalSize + " bytes in " + batch.installed
            + " log files in " + logDirs + " in "
            + (EnvironmentEdgeManager.currentTime() - t) + "ms";
    status.markComplete(msg);
    LOG.info(msg);
    return totalSize;
  }

  /**
   * It removes stale recovering regions under /hbase/recovering-regions/[encoded region name]
   * during master initialization phase.
   * @param failedServers A set of known failed servers
   * @throws IOException
   */
  @VisibleForTesting
  void removeStaleRecoveringRegions(final Set<ServerName> failedServers) throws IOException,
      InterruptedIOException {
    Set<String> knownFailedServers = new HashSet<String>();
    if (failedServers != null) {
      for (ServerName tmpServerName : failedServers) {
        knownFailedServers.add(tmpServerName.getServerName());
      }
    }

    this.recoveringRegionLock.lock();
    try {
      ((BaseCoordinatedStateManager) server.getCoordinatedStateManager())
          .getSplitLogManagerCoordination().removeStaleRecoveringRegions(knownFailedServers);
    } finally {
      this.recoveringRegionLock.unlock();
    }
  }

  /**
   * This function is to set recovery mode from outstanding split log tasks from before or current
   * configuration setting
   * @param isForInitialization
   * @throws IOException throws if it's impossible to set recovery mode
   */
  public void setRecoveryMode(boolean isForInitialization) throws IOException {
    ((BaseCoordinatedStateManager) server.getCoordinatedStateManager())
        .getSplitLogManagerCoordination().setRecoveryMode(isForInitialization);

  }

  public void markRegionsRecovering(ServerName server, Set<HRegionInfo> userRegions)
      throws InterruptedIOException, IOException {
    if (userRegions == null || (!isLogReplaying())) {
      return;
    }
    try {
      this.recoveringRegionLock.lock();
      // mark that we're creating recovering regions
      ((BaseCoordinatedStateManager) this.server.getCoordinatedStateManager())
          .getSplitLogManagerCoordination().markRegionsRecovering(server, userRegions);
    } finally {
      this.recoveringRegionLock.unlock();
    }

  }

  /**
   * @return whether log is replaying
   */
  public boolean isLogReplaying() {
    if (server.getCoordinatedStateManager() == null) return false;
    return ((BaseCoordinatedStateManager) server.getCoordinatedStateManager())
        .getSplitLogManagerCoordination().isReplaying();
  }

  /**
   * @return whether log is splitting
   */
  @VisibleForTesting
  public boolean isLogSplitting() {
    if (server.getCoordinatedStateManager() == null) return false;
    return ((BaseCoordinatedStateManager) server.getCoordinatedStateManager())
        .getSplitLogManagerCoordination().isSplitting();
  }

  /**
   * @return the current log recovery mode
   */
  public RecoveryMode getRecoveryMode() {
    return ((BaseCoordinatedStateManager) server.getCoordinatedStateManager())
        .getSplitLogManagerCoordination().getRecoveryMode();
  }
}
