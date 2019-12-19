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
package org.apache.hadoop.hbase.mob.compactions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.util.FSUtils;

import com.google.common.annotations.VisibleForTesting;

/**
 * A mob compactor to directly compact the mob files.
 */
@InterfaceAudience.Private
public abstract class MobCompactor {
  private static final Log LOG = LogFactory.getLog(MobCompactor.class);
  protected FileSystem fs;
  protected Configuration conf;
  protected TableName tableName;
  protected HColumnDescriptor column;

  protected Path mobTableDir;
  protected Path mobFamilyDir;
  protected ExecutorService pool;

  public MobCompactor(Configuration conf, FileSystem fs, TableName tableName,
    HColumnDescriptor column, ExecutorService pool) {
    this.conf = conf;
    this.fs = fs;
    this.tableName = tableName;
    this.column = column;
    this.pool = pool;
    mobTableDir = FSUtils.getTableDir(MobUtils.getMobHome(conf), tableName);
    mobFamilyDir = MobUtils.getMobFamilyPath(conf, tableName, column.getNameAsString());
  }

  /**
   * Compacts the mob files for the current column family.
   * @return The paths of new mob files generated in the compaction.
   * @throws IOException
   */
  public List<Path> compact() throws IOException {
    return compact(false);
  }

  /**
   * Compacts the mob files by compaction type for the current column family.
   * @param allFiles Whether add all mob files into the compaction.
   * @return The paths of new mob files generated in the compaction.
   * @throws IOException
   */
  public List<Path> compact(boolean allFiles) throws IOException {
    Date yesterday = new Date(System.currentTimeMillis() - 24 * 60 * 60 * 1000);
    final String date = MobUtils.formatDate(yesterday);
    return compact(allFiles, date);
  }

  /**
   * Compacts the mob files by compaction type for the current column family.
   * @param allFiles Whether add all mob files into the compaction.
   * @return The paths of new mob files generated in the compaction.
   * @throws IOException
   */
  @VisibleForTesting
  public List<Path> compact(boolean allFiles, final String date) throws IOException {
    List<FileStatus> files = new ArrayList<FileStatus>();
    FileStatus[] folders = null;
    if(allFiles){
      folders = fs.listStatus(mobFamilyDir, new FSUtils.DirFilter(fs));
    } else {
      // compact current day or yesterday ?
      folders = fs.listStatus(mobFamilyDir, new PathFilter() {
        @Override
        public boolean accept(Path path) {
          return path.getName().equals(date) ? true : false;
        }
      });
    }
    if(folders != null) {
      StringBuilder sb = new StringBuilder("Compact date partitions: ");
      for(FileStatus folder : folders){
        sb.append(folder.getPath().getName()).append(" ");
        files.addAll(Arrays.asList(fs.listStatus(folder.getPath())));
      }
      LOG.info(sb.toString());
    }
    return compact(files, allFiles);
  }

  /**
   * Compacts the candidate mob files.
   * @param files The candidate mob files.
   * @param allFiles Whether add all mob files into the compaction.
   * @return The paths of new mob files generated in the compaction.
   * @throws IOException
   */
  public abstract List<Path> compact(List<FileStatus> files, boolean allFiles)
    throws IOException;
}
