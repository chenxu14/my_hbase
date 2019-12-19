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
package org.apache.hadoop.hbase.io.hfile;

import static org.apache.hadoop.hbase.HConstants.BUCKET_CACHE_IOENGINE_KEY;
import static org.apache.hadoop.hbase.HConstants.BUCKET_CACHE_SIZE_KEY;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({SmallTests.class})
public class TestCombinedBlockCache extends TestCompositeBlockCache {

  @Test
  public void testMultiThreadGetAndEvictBlock() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.set(BUCKET_CACHE_IOENGINE_KEY, "offheap");
    conf.setInt(BUCKET_CACHE_SIZE_KEY, 32);
    BlockCache blockCache = CacheConfig.instantiateBlockCache(conf);
    Assert.assertTrue(blockCache instanceof CombinedBlockCache);
    TestLruBlockCache.testMultiThreadGetAndEvictBlockInternal(blockCache);
  }
}
