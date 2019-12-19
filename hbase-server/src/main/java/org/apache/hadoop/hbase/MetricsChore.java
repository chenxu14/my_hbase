package org.apache.hadoop.hbase;

import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.executor.ExecutorService.ExecutorStatus;
import org.apache.hadoop.hbase.regionserver.MetricsRegionServerSource;
import org.apache.hadoop.metrics2.lib.DynamicMetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.hbase.regionserver.MetricsRegionServerSourceImpl;
import org.apache.hadoop.hbase.util.DirectMemoryUtils;

public class MetricsChore extends ScheduledChore {
  private static Log LOG = LogFactory.getLog(MetricsChore.class);
  private ExecutorService service;
  private DynamicMetricsRegistry metricsRegistry;

  public MetricsChore(int sleepTime, Stoppable stopper, ExecutorService service,
      MetricsRegionServerSource metrics) {
    super("MetricsChore", stopper, sleepTime);
    LOG.info("MetricsChore runs every " + StringUtils.formatTime(sleepTime));
    this.service = service;
    this.metricsRegistry = ((MetricsRegionServerSourceImpl) metrics).getMetricsRegistry();
  }

  @Override
  protected void chore() {
    try{
      // thread pool monitor
      Map<String, ExecutorStatus> statuses = service.getAllExecutorStatuses();
      for (Map.Entry<String, ExecutorStatus> statusEntry : statuses.entrySet()) {
        String name = statusEntry.getKey();
        String poolName = name.split("-")[0];
        ExecutorStatus status = statusEntry.getValue();
        MutableGaugeLong queued = metricsRegistry.getGauge(poolName + "_queued", 0L);
        MutableGaugeLong running = metricsRegistry.getGauge(poolName + "_running", 0L);
        int queueSize = status.getQueuedEvents().size();
        int runningSize = status.getRunning().size();
        if(queueSize > 0){
          LOG.warn(poolName + "'s size info, queued: " + queueSize + ", running:" + runningSize);
        }
        queued.set(queueSize);
        running.set(runningSize);
      }
      // direct memory monitor
      long usage = DirectMemoryUtils.getDirectMemoryUsage();
      MutableGaugeLong usedMetric = metricsRegistry.getGauge("DirectMemoryUsed", 0L);
      usedMetric.set(usage);
    } catch(Throwable e) {
      LOG.error(e.getMessage(), e);
    }
  }
}