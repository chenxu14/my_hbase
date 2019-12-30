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

import static org.apache.hadoop.hbase.master.LogRecoveryManager.ResubmitDirective.CHECK;
import static org.apache.hadoop.hbase.master.LogRecoveryManager.ResubmitDirective.FORCE;
import static org.apache.hadoop.hbase.master.LogRecoveryManager.TerminationStatus.DELETED;
import static org.apache.hadoop.hbase.master.LogRecoveryManager.TerminationStatus.FAILURE;
import static org.apache.hadoop.hbase.master.LogRecoveryManager.TerminationStatus.IN_PROGRESS;
import static org.apache.hadoop.hbase.master.LogRecoveryManager.TerminationStatus.SUCCESS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.SplitLogCounters;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.coordination.BaseCoordinatedStateManager;
import org.apache.hadoop.hbase.coordination.SplitLogManagerCoordination;
import org.apache.hadoop.hbase.coordination.SplitLogManagerCoordination.SplitLogManagerDetails;
import org.apache.hadoop.hbase.coordination.ZKSplitLogManagerCoordination.TaskFinisher;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.shaded.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * Abstract class for log recovery
 */
@InterfaceAudience.Private
public abstract class LogRecoveryManager {
  private static final Log LOG = LogFactory.getLog(LogRecoveryManager.class);
  protected Server server;
  protected final Configuration conf;
  protected final Stoppable stopper;
  protected ChoreService choreService;

  protected final ConcurrentMap<String, Task> tasks = new ConcurrentHashMap<String, Task>();
  public static final int DEFAULT_UNASSIGNED_TIMEOUT = (3 * 60 * 1000); // 3 min

  private TimeoutMonitor timeoutMonitor;
  private long unassignedTimeout;
  private final Object deadWorkersLock = new Object();
  private volatile Set<ServerName> deadWorkers = null;
  private long lastTaskCreateTime = Long.MAX_VALUE;

  /**
   * Its OK to construct this object even when region-servers are not online. It does lookup the
   * orphan tasks in coordination engine but it doesn't block waiting for them to be done.
   * @param server the server instance
   * @param conf the HBase configuration
   * @param stopper the stoppable in case anything is wrong
   * @param master the master services
   * @param serverName the master server name
   * @param initCoordination KafkaRecoverManager no need to init SplitLogManagerCoordination
   * @throws IOException
   */
  public LogRecoveryManager(Server server, Configuration conf, Stoppable stopper,
      MasterServices master, ServerName serverName, boolean initCoordination) throws IOException {
    this.server = server;
    this.conf = conf;
    this.stopper = stopper;
    if (server.getCoordinatedStateManager() != null) {
      SplitLogManagerCoordination coordination =
          ((BaseCoordinatedStateManager) server.getCoordinatedStateManager())
              .getSplitLogManagerCoordination();
      Set<String> failedDeletions = Collections.synchronizedSet(new HashSet<String>());
      TaskFinisher taskFinisher = getTaskFinisher();
      coordination.addTaskFinisher(taskFinisher);
      SplitLogManagerDetails details =
          new SplitLogManagerDetails(tasks, master, failedDeletions, serverName);
      coordination.addDetails(taskFinisher.getTaskType(), details);
      if (initCoordination) { // only SplitLogManager do this 
        coordination.init();
      }
    }
    if (initCoordination) {
      this.choreService = new ChoreService(serverName.toString() + "_LogRecoveryManager_");
      this.unassignedTimeout =
          conf.getInt("hbase.splitlog.manager.unassigned.timeout", DEFAULT_UNASSIGNED_TIMEOUT);
      this.timeoutMonitor =
          new TimeoutMonitor(conf.getInt("hbase.splitlog.manager.timeoutmonitor.period", 1000), stopper);
      choreService.scheduleChore(timeoutMonitor);
      LOG.info("TimeoutMonitor schedule echa " + unassignedTimeout);
    }
  }

  protected abstract TaskFinisher getTaskFinisher();

  void handleDeadWorker(ServerName workerName) {
    // resubmit the tasks on the TimeoutMonitor thread. Makes it easier
    // to reason about concurrency. Makes it easier to retry.
    synchronized (deadWorkersLock) {
      if (deadWorkers == null) {
        deadWorkers = new HashSet<ServerName>(100);
      }
      deadWorkers.add(workerName);
    }
    LOG.info("dead splitlog worker " + workerName);
  }

  void handleDeadWorkers(Set<ServerName> serverNames) {
    synchronized (deadWorkersLock) {
      if (deadWorkers == null) {
        deadWorkers = new HashSet<ServerName>(100);
      }
      deadWorkers.addAll(serverNames);
    }
    LOG.info("dead splitlog workers " + serverNames);
  }

  public void stop() {
    if (choreService != null) {
      choreService.shutdown();
    }
    if (timeoutMonitor != null) {
      timeoutMonitor.cancel(true);
    }
  }

  /**
   * Add a task entry to coordination if it is not already there.
   * @param taskname the path of the log to be split
   * @param batch the batch this task belongs to
   * @return true if a new entry is created, false if it is already there.
   */
  boolean enqueueSplitTask(String taskname, TaskBatch batch) {
    lastTaskCreateTime = EnvironmentEdgeManager.currentTime();
    String task =
        ((BaseCoordinatedStateManager) server.getCoordinatedStateManager())
            .getSplitLogManagerCoordination().prepareTask(taskname);
    Task oldtask = createTaskIfAbsent(task, batch);
    if (oldtask == null) {
      // publish the task in the coordination engine
      ((BaseCoordinatedStateManager) server.getCoordinatedStateManager())
          .getSplitLogManagerCoordination().submitTask(task);
      return true;
    }
    return false;
  }

  protected void waitForSplittingCompletion(TaskBatch batch, MonitoredTask status) {
    synchronized (batch) {
      while ((batch.done + batch.error) != batch.installed) {
        try {
          status.setStatus("Waiting for distributed tasks to finish. " + " scheduled="
              + batch.installed + " done=" + batch.done + " error=" + batch.error);
          int remaining = batch.installed - (batch.done + batch.error);
          int actual = activeTasks(batch);
          if (remaining != actual) {
            LOG.warn("Expected " + remaining + " active tasks, but actually there are " + actual);
          }
          int remainingTasks =
              ((BaseCoordinatedStateManager) server.getCoordinatedStateManager())
                  .getSplitLogManagerCoordination().remainingTasksInCoordination();
          if (remainingTasks >= 0 && actual > remainingTasks) {
            LOG.warn("Expected at least " + actual + " tasks remaining, but actually there are "
                + remainingTasks);
          }
          if (remainingTasks == 0 || actual == 0) {
            LOG.warn("No more task remaining, splitting "
                + "should have completed. Remaining tasks is " + remainingTasks
                + ", active tasks in map " + actual);
            if (remainingTasks == 0 && actual == 0) {
              return;
            }
          }
          batch.wait(100);
          if (stopper.isStopped()) {
            LOG.warn("Stopped while waiting for log splits to be completed");
            return;
          }
        } catch (InterruptedException e) {
          LOG.warn("Interrupted while waiting for log splits to be completed");
          Thread.currentThread().interrupt();
          return;
        }
      }
    }
  }

  private int activeTasks(final TaskBatch batch) {
    int count = 0;
    for (Task t : tasks.values()) {
      if (t.batch == batch && t.status == TerminationStatus.IN_PROGRESS) {
        count++;
      }
    }
    return count;
  }

  /**
   * @param path
   * @param batch
   * @return null on success, existing task on error
   */
  private Task createTaskIfAbsent(String path, TaskBatch batch) {
    Task oldtask;
    // batch.installed is only changed via this function and
    // a single thread touches batch.installed.
    Task newtask = new Task();
    newtask.batch = batch;
    oldtask = tasks.putIfAbsent(path, newtask);
    if (oldtask == null) {
      batch.installed++;
      return null;
    }
    // new task was not used.
    synchronized (oldtask) {
      if (oldtask.isOrphan()) {
        if (oldtask.status == SUCCESS) {
          // The task is already done. Do not install the batch for this
          // task because it might be too late for setDone() to update
          // batch.done. There is no need for the batch creator to wait for
          // this task to complete.
          return (null);
        }
        if (oldtask.status == IN_PROGRESS) {
          oldtask.batch = batch;
          batch.installed++;
          LOG.debug("Previously orphan task " + path + " is now being waited upon");
          return null;
        }
        while (oldtask.status == FAILURE) {
          LOG.debug("wait for status of task " + path + " to change to DELETED");
          SplitLogCounters.tot_mgr_wait_for_zk_delete.incrementAndGet();
          try {
            oldtask.wait();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Interrupted when waiting for znode delete callback");
            // fall through to return failure
            break;
          }
        }
        if (oldtask.status != DELETED) {
          LOG.warn("Failure because previously failed task"
              + " state still present. Waiting for znode delete callback" + " path=" + path);
          return oldtask;
        }
        // reinsert the newTask and it must succeed this time
        Task t = tasks.putIfAbsent(path, newtask);
        if (t == null) {
          batch.installed++;
          return null;
        }
        LOG.fatal("Logic error. Deleted task still present in tasks map");
        assert false : "Deleted task still present in tasks map";
        return t;
      }
      LOG.warn("Failure because two threads can't wait for the same task; path=" + path);
      return oldtask;
    }
  }

  /**
   * Periodically checks all active tasks and resubmits the ones that have timed out
   */
  private class TimeoutMonitor extends ScheduledChore {
    private long lastLog = 0;

    public TimeoutMonitor(final int period, Stoppable stopper) {
      super("SplitLogManager Timeout Monitor", stopper, period);
    }

    @Override
    protected void chore() {
      int resubmitted = 0;
      int unassigned = 0;
      int tot = 0;
      boolean found_assigned_task = false;
      Set<ServerName> localDeadWorkers;

      synchronized (deadWorkersLock) {
        localDeadWorkers = deadWorkers;
        deadWorkers = null;
      }

      for (Map.Entry<String, Task> e : tasks.entrySet()) {
        String taskName = e.getKey();
        Task task = e.getValue();
        ServerName cur_worker = task.cur_worker_name;
        tot++;
        // don't easily resubmit a task which hasn't been picked up yet. It
        // might be a long while before a SplitLogWorker is free to pick up a
        // task. This is because a SplitLogWorker picks up a task one at a
        // time. If we want progress when there are no region servers then we
        // will have to run a SplitLogWorker thread in the Master.
        if (task.isUnassigned()) {
          unassigned++;
          continue;
        }
        found_assigned_task = true;
        if (localDeadWorkers != null && localDeadWorkers.contains(cur_worker)) {
          SplitLogCounters.tot_mgr_resubmit_dead_server_task.incrementAndGet();
          if (((BaseCoordinatedStateManager) server.getCoordinatedStateManager())
              .getSplitLogManagerCoordination().resubmitTask(taskName, task, FORCE)) {
            resubmitted++;
          } else {
            handleDeadWorker(cur_worker);
            LOG.warn("Failed to resubmit task " + taskName + " owned by dead " + cur_worker
                + ", will retry.");
          }
        } else if (((BaseCoordinatedStateManager) server.getCoordinatedStateManager())
            .getSplitLogManagerCoordination().resubmitTask(taskName, task, CHECK)) {
          resubmitted++;
        }
      }
      if (tot > 0) {
        long now = EnvironmentEdgeManager.currentTime();
        if (now > lastLog + 5000) {
          lastLog = now;
          LOG.info("total tasks = " + tot + " unassigned = " + unassigned + " tasks=" + tasks);
        }
      }
      if (resubmitted > 0) {
        LOG.info("resubmitted " + resubmitted + " out of " + tot + " tasks");
      }
      // If there are pending tasks and all of them have been unassigned for
      // some time then put up a RESCAN node to ping the workers.
      // ZKSplitlog.DEFAULT_UNASSIGNED_TIMEOUT is of the order of minutes
      // because a. it is very unlikely that every worker had a
      // transient error when trying to grab the task b. if there are no
      // workers then all tasks wills stay unassigned indefinitely and the
      // manager will be indefinitely creating RESCAN nodes. TODO may be the
      // master should spawn both a manager and a worker thread to guarantee
      // that there is always one worker in the system
      if (tot > 0
          && !found_assigned_task
          && ((EnvironmentEdgeManager.currentTime() - lastTaskCreateTime) > unassignedTimeout)) {
        for (Map.Entry<String, Task> e : tasks.entrySet()) {
          String taskName = e.getKey();
          Task task = e.getValue();
          // we have to do task.isUnassigned() check again because tasks might
          // have been asynchronously assigned. There is no locking required
          // for these checks ... it is OK even if tryGetDataSetWatch() is
          // called unnecessarily for a taskpath
          if (task.isUnassigned() && (task.status != FAILURE)) {
            // We just touch the znode to make sure its still there
            ((BaseCoordinatedStateManager) server.getCoordinatedStateManager())
                .getSplitLogManagerCoordination().checkTaskStillAvailable(taskName);
          }
        }
        ((BaseCoordinatedStateManager) server.getCoordinatedStateManager())
            .getSplitLogManagerCoordination().checkTasks();
        SplitLogCounters.tot_mgr_resubmit_unassigned.incrementAndGet();
        LOG.debug("resubmitting unassigned task(s) after timeout");
      }
      Set<String> failedDeletions = new HashSet<>();
      for (SplitLogManagerDetails detail : ((BaseCoordinatedStateManager) server.getCoordinatedStateManager())
          .getSplitLogManagerCoordination().getDetails()) {
        failedDeletions.addAll(detail.getFailedDeletions());
      }
      // Retry previously failed deletes
      if (failedDeletions.size() > 0) {
        List<String> tmpPaths = new ArrayList<String>(failedDeletions);
        for (String tmpPath : tmpPaths) {
          // deleteNode is an async call
          ((BaseCoordinatedStateManager) server.getCoordinatedStateManager())
              .getSplitLogManagerCoordination().deleteTask(tmpPath);
        }
        failedDeletions.removeAll(tmpPaths);
      }
    }
  }

  /**
   * in memory state of an active task.
   */
  @InterfaceAudience.Private
  public static class Task {
    public volatile long last_update;
    public volatile int last_version;
    public volatile ServerName cur_worker_name;
    public volatile TaskBatch batch;
    public volatile TerminationStatus status;
    public volatile AtomicInteger incarnation = new AtomicInteger(0);
    public final AtomicInteger unforcedResubmits = new AtomicInteger();
    public volatile boolean resubmitThresholdReached;

    @Override
    public String toString() {
      return ("last_update = " + last_update + " last_version = " + last_version
          + " cur_worker_name = " + cur_worker_name + " status = " + status + " incarnation = "
          + incarnation + " resubmits = " + unforcedResubmits.get() + " batch = " + batch);
    }

    public Task() {
      last_version = -1;
      status = IN_PROGRESS;
      setUnassigned();
    }

    public boolean isOrphan() {
      return (batch == null || batch.isDead);
    }

    public boolean isUnassigned() {
      return (cur_worker_name == null);
    }

    public void heartbeatNoDetails(long time) {
      last_update = time;
    }

    public void heartbeat(long time, int version, ServerName worker) {
      last_version = version;
      last_update = time;
      cur_worker_name = worker;
    }

    public void setUnassigned() {
      cur_worker_name = null;
      last_update = -1;
    }
  }

  /**
   * Keeps track of the batch of tasks submitted together by a caller in splitLogDistributed().
   * Clients threads use this object to wait for all their tasks to be done.
   * <p>
   * All access is synchronized.
   */
  @InterfaceAudience.Private
  public static class TaskBatch {
    public int installed = 0;
    public int done = 0;
    public int error = 0;
    public volatile boolean isDead = false;

    @Override
    public String toString() {
      return ("installed = " + installed + " done = " + done + " error = " + error);
    }
  }

  public enum TerminationStatus {
    IN_PROGRESS("in_progress"), SUCCESS("success"), FAILURE("failure"), DELETED("deleted");

    String statusMsg;

    TerminationStatus(String msg) {
      statusMsg = msg;
    }

    @Override
    public String toString() {
      return statusMsg;
    }
  }

  public enum ResubmitDirective {
    CHECK(), FORCE();
  }

  @VisibleForTesting
  Task findOrCreateOrphanTask(String path) {
    Task orphanTask = new Task();
    Task task;
    task = tasks.putIfAbsent(path, orphanTask);
    if (task == null) {
      LOG.info("creating orphan task " + path);
      SplitLogCounters.tot_mgr_orphan_task_acquired.incrementAndGet();
      task = orphanTask;
    }
    return task;
  }

  @VisibleForTesting
  ConcurrentMap<String, Task> getTasks() {
    return tasks;
  }
}
