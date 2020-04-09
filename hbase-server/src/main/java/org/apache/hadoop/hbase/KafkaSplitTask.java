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
package org.apache.hadoop.hbase;

import org.apache.hadoop.hbase.classification.InterfaceAudience;

@InterfaceAudience.Private
public class KafkaSplitTask {
  public static final String LEN_SPLITER = "_";
  public static final String FIELD_INNER_SPLITER = "-";
  public static final String KAFKA_TASK_PREFIX = "KAFKASPLITTASK";
  private String topic;
  private String partitions;
  private String startOffsets;
  private String endOffsets;
  private String regionName;
  private String startKey;
  private String endkey;

  public KafkaSplitTask() { }

  public KafkaSplitTask(String topic, String partitions, String startOffsets, String endOffsets,
      String regionName, String startKey, String endkey) {
    this.topic = topic;
    this.partitions = partitions;
    this.startOffsets = startOffsets;
    this.endOffsets = endOffsets;
    this.regionName = regionName;
    this.startKey = startKey;
    this.endkey = endkey;
  }

  public String getTopic() {
    return topic;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public String getPartitions() {
    return partitions;
  }

  public void setPartitions(String partitions) {
    this.partitions = partitions;
  }

  public String getStartOffsets() {
    return startOffsets;
  }

  public void setStartOffsets(String startOffsets) {
    this.startOffsets = startOffsets;
  }

  public String getEndOffsets() {
    return endOffsets;
  }

  public void setEndOffsets(String endOffsets) {
    this.endOffsets = endOffsets;
  }

  public String getRegionName() {
    return regionName;
  }

  public void setRegionName(String regionName) {
    this.regionName = regionName;
  }

  public String getStartKey() {
    return startKey;
  }

  public void setStartKey(String startKey) {
    this.startKey = startKey;
  }

  public String getEndkey() {
    return endkey;
  }

  public void setEndkey(String endkey) {
    this.endkey = endkey;
  }

  public String getTaskName() {
    StringBuilder task = new StringBuilder(KAFKA_TASK_PREFIX)
      .append(LEN_SPLITER).append(topic.length()).append(LEN_SPLITER).append(topic)
      .append(LEN_SPLITER).append(partitions.length()).append(LEN_SPLITER).append(partitions)
      .append(LEN_SPLITER).append(startOffsets.length()).append(LEN_SPLITER).append(startOffsets)
      .append(LEN_SPLITER).append(endOffsets.length()).append(LEN_SPLITER).append(endOffsets)
      .append(LEN_SPLITER).append(regionName.length()).append(LEN_SPLITER).append(regionName)
      .append(LEN_SPLITER).append(startKey.length()).append(LEN_SPLITER).append(startKey)
      .append(LEN_SPLITER).append(endkey.length()).append(LEN_SPLITER).append(endkey);
    return task.toString();
  }

  public static KafkaSplitTask parseFrom(String taskName) {
    KafkaSplitTask task = new KafkaSplitTask();
    int index = KafkaSplitTask.KAFKA_TASK_PREFIX.length() + 1;
    int i = 0;
    while (index < taskName.length() - 1) {
      int endIndex = taskName.indexOf("_", index);
      int len = Integer.parseInt(taskName.substring(index, endIndex));
      String value = taskName.substring(endIndex + 1, endIndex + 1 + len);
      switch(i) {
      case 0 :
        task.setTopic(value);
        break;
      case 1 :
        task.setPartitions(value);
        break;
      case 2 :
        task.setStartOffsets(value);
        break;
      case 3 :
        task.setEndOffsets(value);
        break;
      case 4 :
        task.setRegionName(value);
        break;
      case 5 :
        task.setStartKey(value);
        break;
      case 6 :
        task.setEndkey(value);
        break;
      default :
        break;
      }
      index = endIndex + len + 2;
      i++;
    }
    return task;
  }
}
