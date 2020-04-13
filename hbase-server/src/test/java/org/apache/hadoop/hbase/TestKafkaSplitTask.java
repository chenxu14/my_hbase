/**
 * Copyright The Apache Software Foundation
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
package org.apache.hadoop.hbase;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestKafkaSplitTask {
  @Test
  public void testTaskBuild() {
    String topic = "topic";
    String partitions = "1-2";
    String startOffsets = "123-124";
    String endOffsets = "456-457";
    String regionName = "regionName";
    String startKey = "startKey";
    String endkey = "endkey";
    assertEquals("KAFKASPLITTASK_5_topic_3_1-2_7_123-124_7_456-457_10_regionName_8_startKey_6_endkey",
        new KafkaSplitTask(topic, partitions, startOffsets, endOffsets, regionName, startKey, endkey)
            .getTaskName());
  }

  @Test
  public void testTaskParse() {
    String name = "KAFKASPLITTASK_5_topic_3_1-2_7_123-124_7_456-457_10_regionName_8_startKey_6_endkey";
    KafkaSplitTask task = KafkaSplitTask.parseFrom(name);
    assertEquals("topic", task.getTopic());
    assertEquals("1-2", task.getPartitions());
    assertEquals("123-124", task.getStartOffsets());
    assertEquals("456-457", task.getEndOffsets());
    assertEquals("regionName", task.getRegionName());
    assertEquals("startKey", task.getStartKey());
    assertEquals("endkey", task.getEndkey());
  }
}
