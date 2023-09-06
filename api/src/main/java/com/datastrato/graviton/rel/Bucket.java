/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.rel;

public interface Bucket {

  /**
   * Bucket transforms. These are the transforms that are used to create the bucket. Say we have a
   * table with 3 columns: a, b, c. We want to bucket on a and b. Then the bucket transforms are [a,
   * b].
   *
   * @return
   */
  Transform[] transforms();

  /**
   * Number of buckets. Default is 0.
   *
   * @return
   */
  int bucketNum();

  /**
   * Bucket method. Default is HASH.
   *
   * @return
   */
  BucketMethod bucketMethod();

  enum BucketMethod {
    HASH,
    RANGE,
    EVEN
  }
}
