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

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ZooKeeperProtos;

/**
 * The Class MobCompactChore for running compaction regularly to merge small mob files.
 */
@InterfaceAudience.Private
public class MobCompactionChore extends ScheduledChore {

  private static final Log LOG = LogFactory.getLog(MobCompactionChore.class);
  private HMaster master;
  private TableLockManager tableLockManager;
  private ExecutorService pool;

  public MobCompactionChore(HMaster master, int period) {
    // use the period as initial delay.
    super(master.getServerName() + "-MobCompactionChore", master, period, period, TimeUnit.SECONDS);
    this.master = master;
    this.tableLockManager = master.getTableLockManager();
    this.pool = MobUtils.createMobCompactorThreadPool(master.getConfiguration());
  }

  @Override
  protected void chore() {
    try {
      TableDescriptors htds = master.getTableDescriptors();
      Map<String, HTableDescriptor> map = htds.getAll();
      for (HTableDescriptor htd : map.values()) {
        if (!master.getAssignmentManager().getTableStateManager().isTableState(htd.getTableName(),
          ZooKeeperProtos.Table.State.ENABLED)) {
          continue;
        }
        boolean reported = false;
        try {
          for (HColumnDescriptor hcd : htd.getColumnFamilies()) {
            if (!hcd.isMobEnabled()) {
              continue;
            }
            if (!reported) {
              master.reportMobCompactionStart(htd.getTableName());
              reported = true;
            }
            MobUtils.doMobCompaction(master.getConfiguration(), master.getFileSystem(),
                htd.getTableName(), hcd, pool, false, null, tableLockManager);
          }
        } finally {
          if (reported) {
            master.reportMobCompactionEnd(htd.getTableName());
          }
        }
      }
    } catch (Exception e) {
      LOG.error("Failed to compact mob files", e);
    }
  }

  @Override
  protected void cleanup() {
    super.cleanup();
    pool.shutdown();
  }
}
