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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;

/**
 * It's provided to have a way for coprocessors to observe, rewrite,
 * or skip WALEdits as they are being written to the WAL.
 *
 * Note that implementers of WALObserver will not see WALEdits that report themselves
 * as empty via {@link WALEdit#isEmpty()}.
 *
 * {@link org.apache.hadoop.hbase.coprocessor.RegionObserver} provides
 * hooks for adding logic for WALEdits in the region context during reconstruction.
 *
 * Defines coprocessor hooks for interacting with operations on the
 * {@link org.apache.hadoop.hbase.wal.WAL}.
 * 
 * Since most implementations will be interested in only a subset of hooks, this class uses
 * 'default' functions to avoid having to add unnecessary overrides. When the functions are
 * non-empty, it's simply to satisfy the compiler by returning value of expected (non-void) type.
 * It is done in a way that these default definitions act as no-op. So our suggestion to
 * implementation would be to not call these 'default' methods from overrides.
 * <br><br>
 *
 * <h3>Exception Handling</h3>
 * For all functions, exception handling is done as follows:
 * <ul>
 *   <li>Exceptions of type {@link IOException} are reported back to client.</li>
 *   <li>For any other kind of exception:
 *     <ul>
 *       <li>If the configuration {@link CoprocessorHost#ABORT_ON_ERROR_KEY} is set to true, then
 *         the server aborts.</li>
 *       <li>Otherwise, coprocessor is removed from the server and
 *         {@link org.apache.hadoop.hbase.DoNotRetryIOException} is returned to the client.</li>
 *     </ul>
 *   </li>
 * </ul>
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public interface WALObserver extends Coprocessor {

  /**
   * Called before a {@link org.apache.hadoop.hbase.regionserver.wal.WALEdit}
   * is writen to WAL.
   *
   * @return true if default behavior should be bypassed, false otherwise
   * @deprecated Since hbase-2.0.0. To be replaced with an alternative that does not expose
   * InterfaceAudience classes such as WALKey and WALEdit. Will be removed in hbase-3.0.0.
   */
  // TODO: return value is not used
  @Deprecated
  default boolean preWALWrite(ObserverContext<? extends WALCoprocessorEnvironment> ctx,
      HRegionInfo info, WALKey logKey, WALEdit logEdit) throws IOException {
    return false;
  }

  /**
   * Called before a {@link org.apache.hadoop.hbase.regionserver.wal.WALEdit}
   * is writen to WAL.
   *
   * This method is left in place to maintain binary compatibility with older
   * {@link WALObserver}s. If an implementation directly overrides
   * {@link #preWALWrite(ObserverContext, HRegionInfo, WALKey, WALEdit)} then this version
   * won't be called at all, barring problems with the Security Manager. To work correctly
   * in the presence of a strict Security Manager, or in the case of an implementation that
   * relies on a parent class to implement preWALWrite, you should implement this method
   * as a call to the non-deprecated version.
   *
   * Users of this method will see all edits that can be treated as HLogKey. If there are
   * edits that can't be treated as HLogKey they won't be offered to coprocessors that rely
   * on this method. If a coprocessor gets skipped because of this mechanism, a log message
   * at ERROR will be generated per coprocessor on the logger for {@link CoprocessorHost} once per
   * classloader.
   *
   * @return true if default behavior should be bypassed, false otherwise
   * @deprecated use {@link #preWALWrite(ObserverContext, HRegionInfo, WALKey, WALEdit)}
   */
  @Deprecated
  default boolean preWALWrite(ObserverContext<WALCoprocessorEnvironment> ctx,
      HRegionInfo info, HLogKey logKey, WALEdit logEdit) throws IOException {
    return false;
  }

  /**
   * Called after a {@link org.apache.hadoop.hbase.regionserver.wal.WALEdit}
   * is writen to WAL.
   * @deprecated Since hbase-2.0.0. To be replaced with an alternative that does not expose
   * InterfaceAudience classes such as WALKey and WALEdit. Will be removed in hbase-3.0.0.
   */
  @Deprecated
  default void postWALWrite(ObserverContext<? extends WALCoprocessorEnvironment> ctx,
      HRegionInfo info, WALKey logKey, WALEdit logEdit) throws IOException {}

  /**
   * Called after a {@link org.apache.hadoop.hbase.regionserver.wal.WALEdit}
   * is writen to WAL.
   *
   * This method is left in place to maintain binary compatibility with older
   * {@link WALObserver}s. If an implementation directly overrides
   * {@link #postWALWrite(ObserverContext, HRegionInfo, WALKey, WALEdit)} then this version
   * won't be called at all, barring problems with the Security Manager. To work correctly
   * in the presence of a strict Security Manager, or in the case of an implementation that
   * relies on a parent class to implement preWALWrite, you should implement this method
   * as a call to the non-deprecated version.
   *
   * Users of this method will see all edits that can be treated as HLogKey. If there are
   * edits that can't be treated as HLogKey they won't be offered to coprocessors that rely
   * on this method. If a coprocessor gets skipped because of this mechanism, a log message
   * at ERROR will be generated per coprocessor on the logger for {@link CoprocessorHost} once per
   * classloader.
   *
   * @deprecated use {@link #postWALWrite(ObserverContext, HRegionInfo, WALKey, WALEdit)}
   */
  @Deprecated
  default void postWALWrite(ObserverContext<WALCoprocessorEnvironment> ctx,
      HRegionInfo info, HLogKey logKey, WALEdit logEdit) throws IOException {}

  /**
   * Called before rolling the current WAL
   * @param oldPath the path of the current wal that we are replacing
   * @param newPath the path of the wal we are going to create
   */
  default void preWALRoll(ObserverContext<? extends WALCoprocessorEnvironment> ctx,
      Path oldPath, Path newPath) throws IOException {}

  /**
   * Called after rolling the current WAL
   * @param oldPath the path of the wal that we replaced
   * @param newPath the path of the wal we have created and now is the current
   */
  default void postWALRoll(ObserverContext<? extends WALCoprocessorEnvironment> ctx,
      Path oldPath, Path newPath) throws IOException {}
}
