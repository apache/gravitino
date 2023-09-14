/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.rel;

import com.datastrato.graviton.rel.transforms.Transform;

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
  private final int distributionNumber;

  /** Distribution method. */
  private final DistributionMethod distributionMethod;

  public Transform[] transforms() {
    return transforms;
  }

  public int distributionNumber() {
    return distributionNumber;
  }

  public DistributionMethod distMethod() {
    return distributionMethod;
  }

  private Distribution(
      Transform[] transforms, int distributionNumber, DistributionMethod distributionMethod) {
    this.transforms = transforms;
    this.distributionNumber = distributionNumber;
    this.distributionMethod = distributionMethod;
  }

  public static DistributionBuilder builder() {
    return new DistributionBuilder();
  }

  public static class DistributionBuilder {

    private Transform[] transforms;
    private int distributionNumber;
    private DistributionMethod distributionMethod;

    public DistributionBuilder withTransforms(Transform[] transforms) {
      this.transforms = transforms;
      return this;
    }

    public DistributionBuilder withDistributionNumber(int distributionNumber) {
      this.distributionNumber = distributionNumber;
      return this;
    }

    public DistributionBuilder withdistributionMethod(DistributionMethod distributionMethod) {
      this.distributionMethod = distributionMethod;
      return this;
    }

    public Distribution build() {
      return new Distribution(transforms, distributionNumber, distributionMethod);
    }
  }
}
