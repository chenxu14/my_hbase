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

import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.Counter;
import org.apache.hadoop.hbase.util.FastLongHistogram;

/**
 * Class that implements cache metrics.
 */
@InterfaceAudience.Private
public class CacheStats {

  /** Sliding window statistics. The number of metric periods to include in
   * sliding window hit ratio calculations.
   */
  static final int DEFAULT_WINDOW_PERIODS = 5;

  /** The number of getBlock requests that were cache hits */
  private final Counter hitCount = new Counter();

  /** The number of getBlock requests that were cache hits from primary replica */
  private final Counter primaryHitCount = new Counter();
  
  /**
   * The number of getBlock requests that were cache hits, but only from
   * requests that were set to use the block cache.  This is because all reads
   * attempt to read from the block cache even if they will not put new blocks
   * into the block cache.  See HBASE-2253 for more information.
   */
  private final Counter hitCachingCount = new Counter();

  /** The number of getBlock requests that were cache misses */
  private final Counter missCount = new Counter();

  /** The number of getBlock requests for primary replica that were cache misses */
  private final Counter primaryMissCount = new Counter();
  /**
   * The number of getBlock requests that were cache misses, but only from
   * requests that were set to use the block cache.
   */
  private final Counter missCachingCount = new Counter();

  /** The number of times an eviction has occurred */
  private final Counter evictionCount = new Counter();

  /** The total number of blocks that have been evicted */
  private final Counter evictedBlockCount = new Counter();

  /** The total number of blocks for primary replica that have been evicted */
  private final Counter primaryEvictedBlockCount = new Counter();

  /** The total number of blocks that were not inserted. */
  private final AtomicLong failedInserts = new AtomicLong(0);

  /** The number of metrics periods to include in window */
  private final int numPeriodsInWindow;
  /** Hit counts for each period in window */
  private final long [] hitCounts;
  /** Caching hit counts for each period in window */
  private final long [] hitCachingCounts;
  /** Access counts for each period in window */
  private final long [] requestCounts;
  /** Caching access counts for each period in window */
  private final long [] requestCachingCounts;
  /** Last hit count read */
  private long lastHitCount = 0;
  /** Last hit caching count read */
  private long lastHitCachingCount = 0;
  /** Last request count read */
  private long lastRequestCount = 0;
  /** Last request caching count read */
  private long lastRequestCachingCount = 0;
  /** Current window index (next to be updated) */
  private int windowIndex = 0;
  /**
   * Keep running age at eviction time
   */
  private FastLongHistogram ageAtEviction;
  private long startTime = System.nanoTime();

  public CacheStats(final String name) {
    this(name, DEFAULT_WINDOW_PERIODS);
  }

  public CacheStats(final String name, int numPeriodsInWindow) {
    this.numPeriodsInWindow = numPeriodsInWindow;
    this.hitCounts = initializeZeros(numPeriodsInWindow);
    this.hitCachingCounts = initializeZeros(numPeriodsInWindow);
    this.requestCounts = initializeZeros(numPeriodsInWindow);
    this.requestCachingCounts = initializeZeros(numPeriodsInWindow);
    this.ageAtEviction = new FastLongHistogram();
  }

  @Override
  public String toString() {
    AgeSnapshot snapshot = getAgeAtEvictionSnapshot();
    return "hitCount=" + getHitCount() + ", hitCachingCount=" + getHitCachingCount() +
      ", missCount=" + getMissCount() + ", missCachingCount=" + getMissCachingCount() +
      ", evictionCount=" + getEvictionCount() +
      ", evictedBlockCount=" + getEvictedCount() +
      ", primaryMissCount=" + getPrimaryMissCount() +
      ", primaryHitCount=" + getPrimaryHitCount() +
      ", evictedAgeMean=" + snapshot.getMean();
  }

  public void miss(boolean caching, boolean primary) {
    missCount.increment();
    if (primary) primaryMissCount.increment();
    if (caching) missCachingCount.increment();
  }

  public void hit(boolean caching) {
    hit(caching, true);
  }

  public void hit(boolean caching, boolean primary) {
    hitCount.increment();
    if (primary) primaryHitCount.increment();
    if (caching) hitCachingCount.increment();
  }

  public void evict() {
    evictionCount.increment();
  }

  public void evicted(final long t, boolean primary) {
    if (t > this.startTime) this.ageAtEviction.add(t - this.startTime,1);
    this.evictedBlockCount.increment();
    if (primary) {
      primaryEvictedBlockCount.increment();
    }
  }

  public long failInsert() {
    return failedInserts.incrementAndGet();
  }

  public long getRequestCount() {
    return getHitCount() + getMissCount();
  }

  public long getRequestCachingCount() {
    return getHitCachingCount() + getMissCachingCount();
  }

  public long getMissCount() {
    return missCount.get();
  }

  public long getPrimaryMissCount() {
    return primaryMissCount.get();
  }

  public long getMissCachingCount() {
    return missCachingCount.get();
  }

  public long getHitCount() {
    return hitCount.get();
  }

  public long getPrimaryHitCount() {
    return primaryHitCount.get();
  }

  public long getHitCachingCount() {
    return hitCachingCount.get();
  }

  public long getEvictionCount() {
    return evictionCount.get();
  }

  public long getEvictedCount() {
    return this.evictedBlockCount.get();
  }

  public long getPrimaryEvictedCount() {
    return primaryEvictedBlockCount.get();
  }

  public double getHitRatio() {
    return ((float)getHitCount()/(float)getRequestCount());
  }

  public double getHitCachingRatio() {
    return ((float)getHitCachingCount()/(float)getRequestCachingCount());
  }

  public double getMissRatio() {
    return ((float)getMissCount()/(float)getRequestCount());
  }

  public double getMissCachingRatio() {
    return ((float)getMissCachingCount()/(float)getRequestCachingCount());
  }

  public double evictedPerEviction() {
    return ((float)getEvictedCount()/(float)getEvictionCount());
  }

  public long getFailedInserts() {
    return failedInserts.get();
  }

  public void rollMetricsPeriod() {
    hitCounts[windowIndex] = getHitCount() - lastHitCount;
    lastHitCount = getHitCount();
    hitCachingCounts[windowIndex] =
      getHitCachingCount() - lastHitCachingCount;
    lastHitCachingCount = getHitCachingCount();
    requestCounts[windowIndex] = getRequestCount() - lastRequestCount;
    lastRequestCount = getRequestCount();
    requestCachingCounts[windowIndex] =
      getRequestCachingCount() - lastRequestCachingCount;
    lastRequestCachingCount = getRequestCachingCount();
    windowIndex = (windowIndex + 1) % numPeriodsInWindow;
  }

  public long getSumHitCountsPastNPeriods() {
    return sum(hitCounts);
  }

  public long getSumRequestCountsPastNPeriods() {
    return sum(requestCounts);
  }

  public long getSumHitCachingCountsPastNPeriods() {
    return sum(hitCachingCounts);
  }

  public long getSumRequestCachingCountsPastNPeriods() {
    return sum(requestCachingCounts);
  }

  public double getHitRatioPastNPeriods() {
    double ratio = ((double)getSumHitCountsPastNPeriods() /
        (double)getSumRequestCountsPastNPeriods());
    return Double.isNaN(ratio) ? 0 : ratio;
  }

  public double getHitCachingRatioPastNPeriods() {
    double ratio = ((double)getSumHitCachingCountsPastNPeriods() /
        (double)getSumRequestCachingCountsPastNPeriods());
    return Double.isNaN(ratio) ? 0 : ratio;
  }

  public AgeSnapshot getAgeAtEvictionSnapshot() {
    return new AgeSnapshot(this.ageAtEviction);
  }

  private static long sum(long [] counts) {
    long sum = 0;
    for (long count : counts) sum += count;
    return sum;
  }

  private static long [] initializeZeros(int n) {
    long [] zeros = new long [n];
    for (int i=0; i<n; i++) {
      zeros[i] = 0L;
    }
    return zeros;
  }
}
