/*
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
package org.apache.hadoop.hbase.coprocessor.example;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.coprocessor.CoprocessorJudger;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.protobuf.generated.AggregateProtos.AggregateRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Scan;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Coprocessor judger to decide if the client call to AggregateService
 * involes large query
 */
@InterfaceAudience.Private
public class AggregateServiceJudger implements CoprocessorJudger {
  private static final Log LOG = LogFactory.getLog(AggregateServiceJudger.class.getName());

  @Override
  public boolean isLargeQuery(ByteString msg) {
    try {
      AggregateRequest request = AggregateRequest.parseFrom(msg.toByteArray());
      Scan scan = request.getScan();
      String startRow = scan.getStartRow().toStringUtf8();
      String stopRow = scan.getStopRow().toStringUtf8();
      if ("".equals(startRow) || "".equals(stopRow)) {
        return true;
      }
    } catch (InvalidProtocolBufferException e) {
      LOG.info(e.getMessage(), e);
    }
    return false;
  }

}
