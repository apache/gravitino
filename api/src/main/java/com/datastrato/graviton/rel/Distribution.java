/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.rel;

public interface Distribution {

  /**
   * Distribution transforms. These are the transforms that are used to create the bucket. Say we
   * have a table with three columns: a, b, c. We want to bucket on a and b. Then the bucket
   * transforms are [a, b].
   */
  Transform[] transforms();

  /** Amount of distribution. Default is 0. */
  int distNum();

  /** Distribution method. Default is HASH. */
  DistributionMethod distMethod();

  enum DistributionMethod {
    HASH,
    RANGE,
    EVEN
  }
}
