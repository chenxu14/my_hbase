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
package org.apache.hadoop.hbase.client;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;

/**
 * Helper class for custom client scanners.
 */
@InterfaceAudience.Private
public abstract class AbstractClientScanner implements ResultScanner {
  protected ScanMetrics scanMetrics;

  /**
   * Check and initialize if application wants to collect scan metrics
   */
  protected void initScanMetrics(Scan scan) {
    // check if application wants to collect scan metrics
    if (scan.isScanMetricsEnabled()) {
      scanMetrics = new ScanMetrics();
    }
  }

  /**
   * Used internally accumulating metrics on scan. To
   * enable collection of metrics on a Scanner, call {@link Scan#setScanMetricsEnabled(boolean)}.
   * These metrics are cleared at key transition points. Metrics are accumulated in the
   * {@link Scan} object itself.
   * @see Scan#getScanMetrics()
   * @return Returns the running {@link ScanMetrics} instance or null if scan metrics not enabled.
   */
  public ScanMetrics getScanMetrics() {
    return scanMetrics;
  }

  /**
   * Allow the client to renew the scanner's lease on the server.
   * @return true if the lease was successfully renewed, false otherwise.
   */
  // Note that this method should be on ResultScanner, but that is marked stable.
  // Callers have to cast their instance of ResultScanner to AbstractClientScanner to use this.
  public abstract boolean renewLease();
}
