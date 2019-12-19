/*
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
package org.apache.hadoop.hbase.io.hfile.bucket;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.BlockPriority;
import org.apache.hadoop.hbase.io.hfile.CacheableDeserializerIdManager;
import org.apache.hadoop.hbase.io.hfile.HFileBlock;
import org.apache.hadoop.hbase.shaded.protobuf.generated.BucketCacheProtos;

@InterfaceAudience.Private
final class BucketProtoUtils {
  private BucketProtoUtils() {

  }

  static BucketCacheProtos.BucketCacheEntry toPB(BucketCache cache) {
    return BucketCacheProtos.BucketCacheEntry.newBuilder()
        .setCacheCapacity(cache.getMaxSize())
        .setIoClass(cache.ioEngine.getClass().getName())
        .setMapClass(cache.backingMap.getClass().getName())
        .putAllDeserializers(CacheableDeserializerIdManager.save())
        .setBackingMap(BucketProtoUtils.toPB(cache.backingMap))
        .build();
  }

  private static BucketCacheProtos.BackingMap toPB(
      Map<BlockCacheKey, BucketEntry> backingMap) {
    BucketCacheProtos.BackingMap.Builder builder = BucketCacheProtos.BackingMap.newBuilder();
    for (Map.Entry<BlockCacheKey, BucketEntry> entry : backingMap.entrySet()) {
      builder.addEntry(BucketCacheProtos.BackingMapEntry.newBuilder()
          .setKey(toPB(entry.getKey()))
          .setValue(toPB(entry.getValue()))
          .build());
    }
    return builder.build();
  }

  private static BucketCacheProtos.BlockCacheKey toPB(BlockCacheKey key) {
    return BucketCacheProtos.BlockCacheKey.newBuilder()
        .setHfilename(key.getHfileName())
        .setOffset(key.getOffset())
        .setPrimaryReplicaBlock(key.isPrimary())
        .build();
  }

  private static BucketCacheProtos.BucketEntry toPB(BucketEntry entry) {
    return BucketCacheProtos.BucketEntry.newBuilder()
        .setOffset(entry.offset())
        .setLength(entry.getLength())
        .setDeserialiserIndex(entry.deserializerIndex)
        .setAccessCounter(entry.getAccessCounter())
        .setPriority(toPB(entry.getPriority()))
        .build();
  }

  private static BucketCacheProtos.BlockPriority toPB(BlockPriority p) {
    switch (p) {
      case MULTI:
        return BucketCacheProtos.BlockPriority.multi;
      case MEMORY:
        return BucketCacheProtos.BlockPriority.memory;
      case SINGLE:
        return BucketCacheProtos.BlockPriority.single;
      default:
        throw new Error("Unrecognized BlockPriority.");
    }
  }

  static ConcurrentHashMap<BlockCacheKey, BucketEntry> fromPB(
      Map<Integer, String> deserializers, BucketCacheProtos.BackingMap backingMap)
      throws IOException {
    ConcurrentHashMap<BlockCacheKey, BucketEntry> result = new ConcurrentHashMap<>();
    for (BucketCacheProtos.BackingMapEntry entry : backingMap.getEntryList()) {
      BucketCacheProtos.BlockCacheKey protoKey = entry.getKey();
      BlockCacheKey key = new BlockCacheKey(protoKey.getHfilename(), protoKey.getOffset(),
          protoKey.getPrimaryReplicaBlock());
      BucketCacheProtos.BucketEntry protoValue = entry.getValue();
      BucketEntry value = new BucketEntry(
          protoValue.getOffset(),
          protoValue.getLength(),
          protoValue.getAccessCounter(),
          protoValue.getPriority() == BucketCacheProtos.BlockPriority.memory);
      // This is the deserializer that we stored
      int oldIndex = protoValue.getDeserialiserIndex();
      String deserializerClass = deserializers.get(oldIndex);
      if (deserializerClass == null) {
        throw new IOException("Found deserializer index without matching entry.");
      }
      // Convert it to the identifier for the deserializer that we have in this runtime
      if (deserializerClass.equals(HFileBlock.BlockDeserializer.class.getName())) {
        int actualIndex = HFileBlock.BLOCK_DESERIALIZER.getDeserializerIdentifier();
        value.deserializerIndex = (byte) actualIndex;
      } else {
        // We could make this more plugable, but right now HFileBlock is the only implementation
        // of Cacheable outside of tests, so this might not ever matter.
        throw new IOException("Unknown deserializer class found: " + deserializerClass);
      }
      result.put(key, value);
    }
    return result;
  }
}
