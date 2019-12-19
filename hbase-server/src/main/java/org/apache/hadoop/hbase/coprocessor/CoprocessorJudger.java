package org.apache.hadoop.hbase.coprocessor;

import com.google.protobuf.ByteString;

public interface CoprocessorJudger {
  public boolean isLargeQuery(ByteString request);
}
