/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.catalog.hive;

import com.datastrato.graviton.meta.rel.BaseBucket;

public class HiveBucket extends BaseBucket {

  public static class Builder extends BaseBucket.BaseBucketBuilder<Builder, HiveBucket> {
    @Override
    public HiveBucket build() {
      HiveBucket bucket = new HiveBucket();
      bucket.transforms = transforms;
      bucket.bucketNum = bucketNum;
      bucket.bucketMethod = bucketMethod;
      return bucket;
    }
  }
}
