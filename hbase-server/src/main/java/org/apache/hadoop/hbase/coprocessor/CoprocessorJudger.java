package org.apache.hadoop.hbase.coprocessor;

import org.apache.hadoop.hbase.shaded.com.google.protobuf.ByteString;

public interface CoprocessorJudger {
  public boolean isLargeQuery(ByteString request);
}
