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

package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.metrics.BaseSourceImpl;
import org.apache.hadoop.hbase.metrics.Interns;
import org.apache.hadoop.metrics2.MetricHistogram;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.MutableFastCounter;

/**
 * Hadoop2 implementation of MetricsRegionServerSource.
 *
 * Implements BaseSource through BaseSourceImpl, following the pattern
 */
@InterfaceAudience.Private
public class MetricsRegionServerSourceImpl
    extends BaseSourceImpl implements MetricsRegionServerSource {


  final MetricsRegionServerWrapper rsWrap;
  private final MetricHistogram putHisto;
  private final MetricHistogram putBatchHisto;
  private final MetricHistogram deleteHisto;
  private final MetricHistogram deleteBatchHisto;
  private final MetricHistogram checkAndDeleteHisto;
  private final MetricHistogram checkAndPutHisto;
  private final MetricHistogram getHisto;
  private final MetricHistogram incrementHisto;
  private final MetricHistogram appendHisto;
  private final MetricHistogram replayHisto;
  private final MetricHistogram scanSizeHisto;
  private final MetricHistogram scanTimeHisto;

  private final MutableFastCounter jvmPause;
  private final MutableFastCounter slowPut;
  private final MutableFastCounter slowDelete;
  private final MutableFastCounter slowGet;
  private final MutableFastCounter slowIncrement;
  private final MutableFastCounter slowAppend;
  private final MutableFastCounter splitRequest;
  private final MutableFastCounter splitSuccess;

  private final MetricHistogram splitTimeHisto;
  private final MetricHistogram flushTimeHisto;

  public MetricsRegionServerSourceImpl(MetricsRegionServerWrapper rsWrap) {
    this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT, METRICS_JMX_CONTEXT, rsWrap);
  }

  public MetricsRegionServerSourceImpl(String metricsName,
                                       String metricsDescription,
                                       String metricsContext,
                                       String metricsJmxContext,
                                       MetricsRegionServerWrapper rsWrap) {
    super(metricsName, metricsDescription, metricsContext, metricsJmxContext);
    this.rsWrap = rsWrap;

    putHisto = getMetricsRegistry().newTimeHistogram(PUT_KEY);
    putBatchHisto = getMetricsRegistry().newTimeHistogram(PUT_BATCH_KEY);
    slowPut = getMetricsRegistry().newCounter(SLOW_PUT_KEY, SLOW_PUT_DESC, 0L);
    jvmPause = getMetricsRegistry().newCounter(JVM_PAUSE_KEY, JVM_PAUSE_DESC, 0l);

    deleteHisto = getMetricsRegistry().newTimeHistogram(DELETE_KEY);
    slowDelete = getMetricsRegistry().newCounter(SLOW_DELETE_KEY, SLOW_DELETE_DESC, 0L);

    deleteBatchHisto = getMetricsRegistry().newTimeHistogram(DELETE_BATCH_KEY);
    checkAndDeleteHisto = getMetricsRegistry().newTimeHistogram(CHECK_AND_DELETE_KEY);
    checkAndPutHisto = getMetricsRegistry().newTimeHistogram(CHECK_AND_PUT_KEY);

    getHisto = getMetricsRegistry().newTimeHistogram(GET_KEY);
    slowGet = getMetricsRegistry().newCounter(SLOW_GET_KEY, SLOW_GET_DESC, 0L);

    incrementHisto = getMetricsRegistry().newTimeHistogram(INCREMENT_KEY);
    slowIncrement = getMetricsRegistry().newCounter(SLOW_INCREMENT_KEY, SLOW_INCREMENT_DESC, 0L);

    appendHisto = getMetricsRegistry().newTimeHistogram(APPEND_KEY);
    slowAppend = getMetricsRegistry().newCounter(SLOW_APPEND_KEY, SLOW_APPEND_DESC, 0L);
    
    replayHisto = getMetricsRegistry().newTimeHistogram(REPLAY_KEY);
    scanSizeHisto = getMetricsRegistry().newSizeHistogram(SCAN_SIZE_KEY);
    scanTimeHisto = getMetricsRegistry().newTimeHistogram(SCAN_TIME_KEY);

    splitTimeHisto = getMetricsRegistry().newTimeHistogram(SPLIT_KEY);
    flushTimeHisto = getMetricsRegistry().newTimeHistogram(FLUSH_KEY);

    splitRequest = getMetricsRegistry().newCounter(SPLIT_REQUEST_KEY, SPLIT_REQUEST_DESC, 0L);
    splitSuccess = getMetricsRegistry().newCounter(SPLIT_SUCCESS_KEY, SPLIT_SUCCESS_DESC, 0L);
  }

  @Override
  public void updatePut(long t) {
    putHisto.add(t);
  }

  @Override
  public void updateDelete(long t) {
    deleteHisto.add(t);
  }

  @Override
  public void updateGet(long t) {
    getHisto.add(t);
  }

  @Override
  public void updateIncrement(long t) {
    incrementHisto.add(t);
  }

  @Override
  public void updateAppend(long t) {
    appendHisto.add(t);
  }

  @Override
  public void updateReplay(long t) {
    replayHisto.add(t);
  }

  @Override
  public void updateScanSize(long scanSize) {
    scanSizeHisto.add(scanSize);
  }

  @Override
  public void updateScanTime(long t) {
    scanTimeHisto.add(t);
  }

  @Override
  public void incrSlowPut() {
   slowPut.incr();
  }

  public void incrJvmPause(){
    jvmPause.incr();
  }

  @Override
  public void incrSlowDelete() {
    slowDelete.incr();
  }

  @Override
  public void incrSlowGet() {
    slowGet.incr();
  }

  @Override
  public void incrSlowIncrement() {
    slowIncrement.incr();
  }

  @Override
  public void incrSlowAppend() {
    slowAppend.incr();
  }

  @Override
  public void incrSplitRequest() {
    splitRequest.incr();
  }

  @Override
  public void incrSplitSuccess() {
    splitSuccess.incr();
  }

  @Override
  public void updateSplitTime(long t) {
    splitTimeHisto.add(t);
  }

  @Override
  public void updateFlushTime(long t) {
    flushTimeHisto.add(t);
  }

  @Override
  public void updateDeleteBatch(long t) {
    deleteBatchHisto.add(t);
  }

  @Override
  public void updateCheckAndDelete(long t) {
    checkAndDeleteHisto.add(t);
  }

  @Override
  public void updateCheckAndPut(long t) {
    checkAndPutHisto.add(t);
  }

  @Override
  public void updatePutBatch(long t) {
    putBatchHisto.add(t);
  }

  /**
   * Yes this is a get function that doesn't return anything.  Thanks Hadoop for breaking all
   * expectations of java programmers.  Instead of returning anything Hadoop metrics expects
   * getMetrics to push the metrics into the collector.
   *
   * @param metricsCollector Collector to accept metrics
   * @param all              push all or only changed?
   */
  @Override
  public void getMetrics(MetricsCollector metricsCollector, boolean all) {

    MetricsRecordBuilder mrb = metricsCollector.addRecord(metricsName);

    // rsWrap can be null because this function is called inside of init.
    if (rsWrap != null) {
      mrb.addGauge(Interns.info(REGION_COUNT, REGION_COUNT_DESC), rsWrap.getNumOnlineRegions())
          .addGauge(Interns.info(STORE_COUNT, STORE_COUNT_DESC), rsWrap.getNumStores())
          .addGauge(Interns.info(WALFILE_COUNT, WALFILE_COUNT_DESC), rsWrap.getNumWALFiles())
          .addGauge(Interns.info(WALFILE_SIZE, WALFILE_SIZE_DESC), rsWrap.getWALFileSize())
          .addGauge(Interns.info(STOREFILE_COUNT, STOREFILE_COUNT_DESC), rsWrap.getNumStoreFiles())
          .addGauge(Interns.info(MEMSTORE_SIZE, MEMSTORE_SIZE_DESC), rsWrap.getMemstoreSize())
          .addGauge(Interns.info(STOREFILE_SIZE, STOREFILE_SIZE_DESC), rsWrap.getStoreFileSize())
          .addGauge(Interns.info(RS_START_TIME_NAME, RS_START_TIME_DESC),
              rsWrap.getStartCode())
          .addCounter(Interns.info(TOTAL_REQUEST_COUNT, TOTAL_REQUEST_COUNT_DESC),
              rsWrap.getTotalRequestCount())
          .addCounter(Interns.info(READ_REQUEST_COUNT, READ_REQUEST_COUNT_DESC),
              rsWrap.getReadRequestsCount())
          .addCounter(Interns.info(CP_REQUEST_COUNT, CP_REQUEST_COUNT_DESC),
              rsWrap.getCpRequestsCount())
          .addCounter(Interns.info(WRITE_REQUEST_COUNT, WRITE_REQUEST_COUNT_DESC),
              rsWrap.getWriteRequestsCount())
          .addCounter(Interns.info(CHECK_MUTATE_FAILED_COUNT, CHECK_MUTATE_FAILED_COUNT_DESC),
              rsWrap.getCheckAndMutateChecksFailed())
          .addCounter(Interns.info(CHECK_MUTATE_PASSED_COUNT, CHECK_MUTATE_PASSED_COUNT_DESC),
              rsWrap.getCheckAndMutateChecksPassed())
          .addGauge(Interns.info(STOREFILE_INDEX_SIZE, STOREFILE_INDEX_SIZE_DESC),
              rsWrap.getStoreFileIndexSize())
          .addGauge(Interns.info(STATIC_INDEX_SIZE, STATIC_INDEX_SIZE_DESC),
              rsWrap.getTotalStaticIndexSize())
          .addGauge(Interns.info(STATIC_BLOOM_SIZE, STATIC_BLOOM_SIZE_DESC),
            rsWrap.getTotalStaticBloomSize())
          .addGauge(
            Interns.info(NUMBER_OF_MUTATIONS_WITHOUT_WAL, NUMBER_OF_MUTATIONS_WITHOUT_WAL_DESC),
              rsWrap.getNumMutationsWithoutWAL())
          .addGauge(Interns.info(DATA_SIZE_WITHOUT_WAL, DATA_SIZE_WITHOUT_WAL_DESC),
              rsWrap.getDataInMemoryWithoutWAL())
          .addGauge(Interns.info(PERCENT_FILES_LOCAL, PERCENT_FILES_LOCAL_DESC),
              rsWrap.getPercentFileLocal())
          .addGauge(Interns.info(PERCENT_FILES_LOCAL_SECONDARY_REGIONS,
              PERCENT_FILES_LOCAL_SECONDARY_REGIONS_DESC),
              rsWrap.getPercentFileLocalSecondaryRegions())
          .addGauge(Interns.info(SPLIT_QUEUE_LENGTH, SPLIT_QUEUE_LENGTH_DESC),
              rsWrap.getSplitQueueSize())
          .addGauge(Interns.info(COMPACTION_QUEUE_LENGTH, COMPACTION_QUEUE_LENGTH_DESC),
              rsWrap.getCompactionQueueSize())
          .addGauge(Interns.info(FLUSH_QUEUE_LENGTH, FLUSH_QUEUE_LENGTH_DESC),
              rsWrap.getFlushQueueSize())
          .addGauge(Interns.info(BLOCK_CACHE_FREE_SIZE, BLOCK_CACHE_FREE_DESC),
              rsWrap.getBlockCacheFreeSize())
          .addGauge(Interns.info(BLOCK_CACHE_COUNT, BLOCK_CACHE_COUNT_DESC),
              rsWrap.getBlockCacheCount())
          .addGauge(Interns.info(BLOCK_CACHE_SIZE, BLOCK_CACHE_SIZE_DESC),
              rsWrap.getBlockCacheSize())
          .addCounter(Interns.info(BLOCK_CACHE_HIT_COUNT, BLOCK_CACHE_HIT_COUNT_DESC),
              rsWrap.getBlockCacheHitCount())
          .addCounter(Interns.info(BLOCK_CACHE_PRIMARY_HIT_COUNT,
            BLOCK_CACHE_PRIMARY_HIT_COUNT_DESC), rsWrap.getBlockCachePrimaryHitCount())
          .addCounter(Interns.info(BLOCK_CACHE_MISS_COUNT, BLOCK_COUNT_MISS_COUNT_DESC),
              rsWrap.getBlockCacheMissCount())
          .addCounter(Interns.info(BLOCK_CACHE_PRIMARY_MISS_COUNT,
            BLOCK_COUNT_PRIMARY_MISS_COUNT_DESC), rsWrap.getBlockCachePrimaryMissCount())
          .addCounter(Interns.info(BLOCK_CACHE_EVICTION_COUNT, BLOCK_CACHE_EVICTION_COUNT_DESC),
              rsWrap.getBlockCacheEvictedCount())
          .addCounter(Interns.info(BLOCK_CACHE_PRIMARY_EVICTION_COUNT,
            BLOCK_CACHE_PRIMARY_EVICTION_COUNT_DESC), rsWrap.getBlockCachePrimaryEvictedCount())
          .addGauge(Interns.info(BLOCK_CACHE_HIT_PERCENT, BLOCK_CACHE_HIT_PERCENT_DESC),
              rsWrap.getBlockCacheHitPercent())
          .addGauge(Interns.info(BLOCK_CACHE_EXPRESS_HIT_PERCENT,
              BLOCK_CACHE_EXPRESS_HIT_PERCENT_DESC), rsWrap.getBlockCacheHitCachingPercent())
          .addCounter(Interns.info(BLOCK_CACHE_FAILED_INSERTION_COUNT,
              BLOCK_CACHE_FAILED_INSERTION_COUNT_DESC),rsWrap.getBlockCacheFailedInsertions())
          .addCounter(Interns.info(UPDATES_BLOCKED_TIME, UPDATES_BLOCKED_DESC),
              rsWrap.getUpdatesBlockedTime())
          .addCounter(Interns.info(FLUSHED_CELLS, FLUSHED_CELLS_DESC),
              rsWrap.getFlushedCellsCount())
          .addCounter(Interns.info(COMPACTED_CELLS, COMPACTED_CELLS_DESC),
              rsWrap.getCompactedCellsCount())
          .addCounter(Interns.info(MAJOR_COMPACTED_CELLS, MAJOR_COMPACTED_CELLS_DESC),
              rsWrap.getMajorCompactedCellsCount())
          .addCounter(Interns.info(FLUSHED_CELLS_SIZE, FLUSHED_CELLS_SIZE_DESC),
              rsWrap.getFlushedCellsSize())
          .addCounter(Interns.info(COMPACTED_CELLS_SIZE, COMPACTED_CELLS_SIZE_DESC),
              rsWrap.getCompactedCellsSize())
          .addCounter(Interns.info(MAJOR_COMPACTED_CELLS_SIZE, MAJOR_COMPACTED_CELLS_SIZE_DESC),
              rsWrap.getMajorCompactedCellsSize())

          .addCounter(
              Interns.info(CELLS_COUNT_COMPACTED_FROM_MOB, CELLS_COUNT_COMPACTED_FROM_MOB_DESC),
              rsWrap.getCellsCountCompactedFromMob())
          .addCounter(Interns.info(CELLS_COUNT_COMPACTED_TO_MOB, CELLS_COUNT_COMPACTED_TO_MOB_DESC),
              rsWrap.getCellsCountCompactedToMob())
          .addCounter(
              Interns.info(CELLS_SIZE_COMPACTED_FROM_MOB, CELLS_SIZE_COMPACTED_FROM_MOB_DESC),
              rsWrap.getCellsSizeCompactedFromMob())
          .addCounter(Interns.info(CELLS_SIZE_COMPACTED_TO_MOB, CELLS_SIZE_COMPACTED_TO_MOB_DESC),
              rsWrap.getCellsSizeCompactedToMob())
          .addCounter(Interns.info(MOB_FLUSH_COUNT, MOB_FLUSH_COUNT_DESC),
              rsWrap.getMobFlushCount())
          .addCounter(Interns.info(MOB_FLUSHED_CELLS_COUNT, MOB_FLUSHED_CELLS_COUNT_DESC),
              rsWrap.getMobFlushedCellsCount())
          .addCounter(Interns.info(MOB_FLUSHED_CELLS_SIZE, MOB_FLUSHED_CELLS_SIZE_DESC),
              rsWrap.getMobFlushedCellsSize())
          .addCounter(Interns.info(MOB_SCAN_CELLS_COUNT, MOB_SCAN_CELLS_COUNT_DESC),
              rsWrap.getMobScanCellsCount())
          .addCounter(Interns.info(MOB_SCAN_CELLS_SIZE, MOB_SCAN_CELLS_SIZE_DESC),
              rsWrap.getMobScanCellsSize())
          .addGauge(Interns.info(MOB_FILE_CACHE_COUNT, MOB_FILE_CACHE_COUNT_DESC),
              rsWrap.getMobFileCacheCount())
          .addCounter(Interns.info(MOB_FILE_CACHE_ACCESS_COUNT, MOB_FILE_CACHE_ACCESS_COUNT_DESC),
              rsWrap.getMobFileCacheAccessCount())
          .addCounter(Interns.info(MOB_FILE_CACHE_MISS_COUNT, MOB_FILE_CACHE_MISS_COUNT_DESC),
              rsWrap.getMobFileCacheMissCount())
          .addCounter(
              Interns.info(MOB_FILE_CACHE_EVICTED_COUNT, MOB_FILE_CACHE_EVICTED_COUNT_DESC),
              rsWrap.getMobFileCacheEvictedCount())
          .addGauge(Interns.info(MOB_FILE_CACHE_HIT_PERCENT, MOB_FILE_CACHE_HIT_PERCENT_DESC),
              rsWrap.getMobFileCacheHitPercent())

          .addCounter(Interns.info(BLOCKED_REQUESTS_COUNT, BLOCKED_REQUESTS_COUNT_DESC),
            rsWrap.getBlockedRequestsCount())

          .tag(Interns.info(ZOOKEEPER_QUORUM_NAME, ZOOKEEPER_QUORUM_DESC),
              rsWrap.getZookeeperQuorum())
          .tag(Interns.info(SERVER_NAME_NAME, SERVER_NAME_DESC), rsWrap.getServerName())
          .tag(Interns.info(CLUSTER_ID_NAME, CLUSTER_ID_DESC), rsWrap.getClusterId())
          .addGauge(Interns.info(BYTE_BUFF_ALLOCATOR_HEAP_ALLOCATION_BYTES,
                  BYTE_BUFF_ALLOCATOR_HEAP_ALLOCATION_BYTES_DESC),
                rsWrap.getByteBuffAllocatorHeapAllocationBytes())
            .addGauge(Interns.info(BYTE_BUFF_ALLOCATOR_POOL_ALLOCATION_BYTES,
                  BYTE_BUFF_ALLOCATOR_POOL_ALLOCATION_BYTES_DESC),
                rsWrap.getByteBuffAllocatorPoolAllocationBytes())
            .addGauge(Interns.info(BYTE_BUFF_ALLOCATOR_HEAP_ALLOCATION_RATIO,
                  BYTE_BUFF_ALLOCATOR_HEAP_ALLOCATION_RATIO_DESC),
                rsWrap.getByteBuffAllocatorHeapAllocRatio())
            .addGauge(Interns.info(BYTE_BUFF_ALLOCATOR_TOTAL_BUFFER_COUNT,
                BYTE_BUFF_ALLOCATOR_TOTAL_BUFFER_COUNT_DESC),
                rsWrap.getByteBuffAllocatorTotalBufferCount())
            .addGauge(Interns.info(BYTE_BUFF_ALLOCATOR_USED_BUFFER_COUNT,
                BYTE_BUFF_ALLOCATOR_USED_BUFFER_COUNT_DESC),
                rsWrap.getByteBuffAllocatorUsedBufferCount());
    }

    metricsRegistry.snapshot(mrb, all);
  }
}
