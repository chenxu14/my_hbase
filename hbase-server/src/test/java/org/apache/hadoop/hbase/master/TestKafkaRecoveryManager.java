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
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import org.apache.hadoop.hbase.KafkaSplitTask;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestKafkaRecoveryManager extends TestLogRecoveryManager {
  @Override
  protected LogRecoveryManager getLogRecoveryManager() throws IOException {
    return new KafkaRecoveryManager(ds, master, true);
  }

  @Override
  protected String getTaskName() {
    return new KafkaSplitTask("topic","partition","0","10","regionName","1","2").getTaskName();
  }
}
