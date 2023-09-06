/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.rel;

public interface Bucket {

  Transform[] transforms();

  int bucketNum();

  BucketMethod bucketMethod();

  enum BucketMethod {
    HASH,
    RANGE,
    EVEN
  }
}
