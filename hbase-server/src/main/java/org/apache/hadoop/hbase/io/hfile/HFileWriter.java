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

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.regionserver.CellSink;
import org.apache.hadoop.hbase.regionserver.ShipperListener;
import org.apache.hadoop.hbase.util.BloomFilterWriter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

/**
 * API required to write an {@link HFile}
 */
@InterfaceAudience.Private
public interface HFileWriter extends Closeable, CellSink, ShipperListener {
  /** Max memstore (mvcc) timestamp in FileInfo */
  public static final byte [] MAX_MEMSTORE_TS_KEY = Bytes.toBytes("MAX_MEMSTORE_TS_KEY");

  /** Add an element to the file info map. */
  void appendFileInfo(byte[] key, byte[] value) throws IOException;

  /** @return the path to this {@link HFile} */
  Path getPath();

  /**
   * Adds an inline block writer such as a multi-level block index writer or
   * a compound Bloom filter writer.
   */
  void addInlineBlockWriter(InlineBlockWriter bloomWriter);

  // The below three methods take Writables.  We'd like to undo Writables but undoing the below would be pretty
  // painful.  Could take a byte [] or a Message but we want to be backward compatible around hfiles so would need
  // to map between Message and Writable or byte [] and current Writable serialization.  This would be a bit of work
  // to little gain.  Thats my thinking at moment.  St.Ack 20121129

  void appendMetaBlock(String bloomFilterMetaKey, Writable metaWriter);

  /**
   * Store general Bloom filter in the file. This does not deal with Bloom filter
   * internals but is necessary, since Bloom filters are stored differently
   * in HFile version 1 and version 2.
   */
  void addGeneralBloomFilter(BloomFilterWriter bfw);

  /**
   * Store delete family Bloom filter in the file, which is only supported in
   * HFile V2.
   */
  void addDeleteFamilyBloomFilter(BloomFilterWriter bfw) throws IOException;

  /**
   * Return the file context for the HFile this writer belongs to
   */
  HFileContext getFileContext();
}
