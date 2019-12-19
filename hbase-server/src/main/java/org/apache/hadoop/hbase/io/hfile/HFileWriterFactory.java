/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.io.hfile;

import java.io.IOException;
import java.net.InetSocketAddress;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.shaded.com.google.common.base.Preconditions;

/**
 * This variety of ways to construct writers is used throughout the code, and we
 * want to be able to swap writer implementations.
 */
@InterfaceAudience.Private
public class HFileWriterFactory {
  static final Log LOG = LogFactory.getLog(HFileWriterFactory.class);
  protected final Configuration conf;
  protected final CacheConfig cacheConf;
  protected FileSystem fs;
  protected Path path;
  protected FSDataOutputStream ostream;
  protected CellComparator comparator = CellComparator.COMPARATOR;
  protected InetSocketAddress[] favoredNodes;
  private HFileContext fileContext;
  protected boolean shouldDropBehind = false;

  HFileWriterFactory(Configuration conf, CacheConfig cacheConf) {
    this.conf = conf;
    this.cacheConf = cacheConf;
  }

  public HFileWriterFactory withPath(FileSystem fs, Path path) {
    Preconditions.checkNotNull(fs);
    Preconditions.checkNotNull(path);
    this.fs = fs;
    this.path = path;
    return this;
  }

  public HFileWriterFactory withOutputStream(FSDataOutputStream ostream) {
    Preconditions.checkNotNull(ostream);
    this.ostream = ostream;
    return this;
  }

  public HFileWriterFactory withComparator(CellComparator comparator) {
    Preconditions.checkNotNull(comparator);
    this.comparator = comparator;
    return this;
  }

  public HFileWriterFactory withFavoredNodes(InetSocketAddress[] favoredNodes) {
    // Deliberately not checking for null here.
    this.favoredNodes = favoredNodes;
    return this;
  }

  public HFileWriterFactory withFileContext(HFileContext fileContext) {
    this.fileContext = fileContext;
    return this;
  }

  public HFileWriterFactory withShouldDropCacheBehind(boolean shouldDropBehind) {
    this.shouldDropBehind = shouldDropBehind;
    return this;
  }

  public HFileWriter create() throws IOException {
    if ((path != null ? 1 : 0) + (ostream != null ? 1 : 0) != 1) {
      throw new AssertionError("Please specify exactly one of " + "filesystem/path or path");
    }
    if (path != null) {
      ostream = HFileWriterImpl.createOutputStream(conf, fs, path, favoredNodes);
      try {
        ostream.setDropBehind(shouldDropBehind && cacheConf.shouldDropBehindCompaction());
      } catch (UnsupportedOperationException uoe) {
        if (LOG.isTraceEnabled())
          LOG.trace("Unable to set drop behind on " + path, uoe);
        else if (LOG.isDebugEnabled())
          LOG.debug("Unable to set drop behind on " + path);
      }
    }
    return new HFileWriterImpl(conf, cacheConf, path, ostream, comparator, fileContext);
  }
}
