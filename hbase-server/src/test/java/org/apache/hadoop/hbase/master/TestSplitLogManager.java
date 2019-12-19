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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.SplitLogCounters;
import org.apache.hadoop.hbase.SplitLogTask;
import org.apache.hadoop.hbase.master.LogRecoveryManager.Task;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos.SplitLogTask.RecoveryMode;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.zookeeper.ZKSplitLog;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestSplitLogManager extends TestLogRecoveryManager{
  private static final Log LOG = LogFactory.getLog(TestSplitLogManager.class);

  @Test (timeout=180000)
  public void testEmptyLogDir() throws Exception {
    LOG.info("testEmptyLogDir");
    slm = new SplitLogManager(ds, conf, ds, master, DUMMY_MASTER);
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Path emptyLogDirPath = new Path(fs.getWorkingDirectory(),
        UUID.randomUUID().toString());
    fs.mkdirs(emptyLogDirPath);
    ((SplitLogManager)slm).splitLogDistributed(emptyLogDirPath);
    assertFalse(fs.exists(emptyLogDirPath));
  }

  @Test (timeout = 60000)
  public void testLogFilesAreArchived() throws Exception {
    LOG.info("testLogFilesAreArchived");
    final SplitLogManager slm = new SplitLogManager(ds, conf, ds, master, DUMMY_MASTER);
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Path dir = TEST_UTIL.getDataTestDirOnTestFS("testLogFilesAreArchived");
    conf.set(HConstants.HBASE_DIR, dir.toString());
    Path logDirPath = new Path(dir, UUID.randomUUID().toString());
    fs.mkdirs(logDirPath);
    // create an empty log file
    String logFile = ServerName.valueOf("foo", 1, 1).toString();
    fs.create(new Path(logDirPath, logFile)).close();

    // spin up a thread mocking split done.
    new Thread() {
      @Override
      public void run() {
        boolean done = false;
        while (!done) {
          for (Map.Entry<String, Task> entry : slm.getTasks().entrySet()) {
            final ServerName worker1 = ServerName.valueOf("worker1,1,1");
            SplitLogTask slt = new SplitLogTask.Done(worker1, RecoveryMode.LOG_SPLITTING);
            boolean encounteredZKException = false;
            try {
              ZKUtil.setData(zkw, entry.getKey(), slt.toByteArray());
            } catch (KeeperException e) {
              LOG.warn(e);
              encounteredZKException = true;
            }
            if (!encounteredZKException) {
              done = true;
            }
          }
        }
      };
    }.start();
    slm.splitLogDistributed(logDirPath);
    assertFalse(fs.exists(logDirPath));
  }

  /**
   * The following test case is aiming to test the situation when distributedLogReplay is turned off
   * and restart a cluster there should no recovery regions in ZK left.
   * @throws Exception
   */
  @Test(timeout = 300000)
  public void testRecoveryRegionRemovedFromZK() throws Exception {
    LOG.info("testRecoveryRegionRemovedFromZK");
    conf.setBoolean(HConstants.DISTRIBUTED_LOG_REPLAY_KEY, false);
    String nodePath =
        ZKUtil.joinZNode(zkw.recoveringRegionsZNode,
          HRegionInfo.FIRST_META_REGIONINFO.getEncodedName());
    ZKUtil.createSetData(zkw, nodePath, ZKUtil.positionToByteArray(0L));
    slm = new SplitLogManager(ds, conf, ds, master, DUMMY_MASTER);
    ((SplitLogManager)slm).removeStaleRecoveringRegions(null);
    List<String> recoveringRegions =
        zkw.getRecoverableZooKeeper().getChildren(zkw.recoveringRegionsZNode, false);

    assertTrue("Recovery regions isn't cleaned", recoveringRegions.isEmpty());
  }

  @Ignore("DLR is broken by HBASE-12751") @Test(timeout=60000)
  public void testGetPreviousRecoveryMode() throws Exception {
    LOG.info("testGetPreviousRecoveryMode");
    SplitLogCounters.resetCounters();
    // Not actually enabling DLR for the cluster, just for the ZkCoordinatedStateManager to use.
    // The test is just manipulating ZK manually anyways.
    conf.setBoolean(HConstants.DISTRIBUTED_LOG_REPLAY_KEY, true);

    zkw.getRecoverableZooKeeper().create(ZKSplitLog.getEncodedNodeName(zkw, "testRecovery"),
      new SplitLogTask.Unassigned(
        ServerName.valueOf("mgr,1,1"), RecoveryMode.LOG_SPLITTING).toByteArray(),
        Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

    slm = new SplitLogManager(ds, conf, ds, master, DUMMY_MASTER);
    LOG.info("Mode1=" + ((SplitLogManager)slm).getRecoveryMode());
    assertTrue(((SplitLogManager)slm).isLogSplitting());
    zkw.getRecoverableZooKeeper().delete(ZKSplitLog.getEncodedNodeName(zkw, "testRecovery"), -1);
    LOG.info("Mode2=" + ((SplitLogManager)slm).getRecoveryMode());
    ((SplitLogManager)slm).setRecoveryMode(false);
    LOG.info("Mode3=" + ((SplitLogManager)slm).getRecoveryMode());
    assertTrue("Mode4=" + ((SplitLogManager)slm).getRecoveryMode(), ((SplitLogManager)slm).isLogReplaying());
  }

  @Override
  protected LogRecoveryManager getLogRecoveryManager() throws IOException {
    return new SplitLogManager(ds, conf, ds, master, DUMMY_MASTER);
  }
}
