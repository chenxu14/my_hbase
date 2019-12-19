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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.regionserver.wal.FSHLog;
import org.apache.hadoop.hbase.regionserver.wal.KafkaClientWAL;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.shaded.com.google.common.collect.Lists;
import org.apache.hadoop.hbase.util.FSUtils;

/**
 * A WAL provider that use {@link KafkaClientWAL}.
 */
@InterfaceAudience.Private
public class KafkaClientWALProvider implements WALProvider {
  private static final Log LOG = LogFactory.getLog(KafkaClientWALProvider.class);
  private AtomicBoolean initialized = new AtomicBoolean(false);
  private KafkaClientWAL kafkaWal;
  private Configuration conf;
  private WALFactory factory;
  private String providerId;
  private List<WALActionsListener> listeners = null;
  private final ReadWriteLock walCreateLock = new ReentrantReadWriteLock();

  @Override
  public void init(WALFactory factory, Configuration conf, List<WALActionsListener> listeners, String providerId)
      throws IOException {
    if (!initialized.compareAndSet(false, true)) {
      throw new IllegalStateException("WALProvider.init should only be called once.");
    }
    this.conf = conf;
    this.factory = factory;
    this.providerId = providerId;
    this.listeners = listeners;
  }

  @Override
  public WAL getWAL(byte[] identifier, byte[] namespace) throws IOException {
    KafkaClientWAL walCopy = kafkaWal;
    if (walCopy != null) {
      return walCopy;
    }
    walCreateLock.writeLock().lock();
    try {
      walCopy = kafkaWal;
      if (walCopy != null) {
        return walCopy;
      }
      LOG.info("should create new WAL, because current WAL is null.");
      walCopy = createWAL();
      LOG.info("created new WAL " + walCopy.toString());
      kafkaWal = walCopy;
      return walCopy;
    } finally {
      walCreateLock.writeLock().unlock();
    }
  }

  private KafkaClientWAL createWAL() throws IOException {
    // get log prefix
    StringBuilder sb = new StringBuilder().append(factory.factoryId);
    if (providerId != null) {
      if (providerId.startsWith(AbstractFSWALProvider.WAL_FILE_NAME_DELIMITER)) {
        sb.append(providerId);
      } else {
        sb.append(AbstractFSWALProvider.WAL_FILE_NAME_DELIMITER).append(providerId);
      }
    }
    String logPrefix = sb.toString();
    FSHLog hdfsWal = new FSHLog(FileSystem.get(conf), FSUtils.getRootDir(conf),
        AbstractFSWALProvider.getWALDirectoryName(factory.factoryId),
        HConstants.HREGION_OLDLOGDIR_NAME, conf, listeners, true, logPrefix,
        AbstractFSWALProvider.META_WAL_PROVIDER_ID.equals(providerId) ?
            AbstractFSWALProvider.META_WAL_PROVIDER_ID : null);
    return new KafkaClientWAL(conf, hdfsWal);
  }

  @Override
  public List<WAL> getWALs() {
    if (kafkaWal != null) {
      return Lists.newArrayList(kafkaWal);
    }
    walCreateLock.readLock().lock();
    try {
      if (kafkaWal == null) {
        return Collections.emptyList();
      } else {
        return Lists.newArrayList(kafkaWal);
      }
    } finally {
      walCreateLock.readLock().unlock();
    }
  }

  @Override
  public void shutdown() throws IOException {
    KafkaClientWAL log = this.kafkaWal;
    if (log != null) {
      log.shutdown();
    }
  }

  @Override
  public void close() throws IOException {
    KafkaClientWAL log = this.kafkaWal;
    if (log != null) {
      log.close();
    }
  }

  @Override
  public long getNumLogFiles() {
    return 0;
  }

  @Override
  public long getLogFileSize() {
    return 0;
  }
}
