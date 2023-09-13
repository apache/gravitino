/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.rel;

import com.datastrato.graviton.rel.transforms.Transform;
import lombok.Builder;

@Builder
public class Distribution {
  public enum DistributionMethod {
    HASH,
    RANGE,
    EVEN
  }

  /**
   * Distriubtion transform. This is the transform that is used to distribute the data. Say we have
   * a table with 3 columns: a, b, c. We want to distribute on a. Then the distribution transform is
   * a.
   */
  private final Transform[] transforms;

  /** Number of bucket/distribution. */
  private final int distNum;

  /** Distribution method. */
  private final DistributionMethod distMethod;

  public Transform[] transforms() {
    return transforms;
  }

  public int distNum() {
    return distNum;
  }

  public DistributionMethod distMethod() {
    return distMethod;
  }
}
