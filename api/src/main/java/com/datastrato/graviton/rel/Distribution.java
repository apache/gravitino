/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.rel;

import com.datastrato.graviton.rel.transforms.Transform;
import com.google.common.base.Objects;

public class Distribution {

  // NONE is used to indicate that there is no distribution.
  public static final Distribution NONE = new Distribution(new Transform[0], 0, Strategy.HASH);

  public enum Strategy {
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
  private final int number;

  /** Distribution strategy/method. */
  private final Strategy strategy;

  public Transform[] transforms() {
    return transforms;
  }

  public int number() {
    return number;
  }

  public Strategy strategy() {
    return strategy;
  }

  private Distribution(Transform[] transforms, int number, Strategy strategy) {
    this.transforms = transforms;
    this.number = number;
    this.strategy = strategy;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private Transform[] transforms;
    private int number;
    private Strategy strategy;

    public Builder withTransforms(Transform[] transforms) {
      this.transforms = transforms;
      return this;
    }

    public Builder withNumber(int number) {
      this.number = number;
      return this;
    }

    public Builder withStrategy(Strategy strategy) {
      this.strategy = strategy;
      return this;
    }

    public Distribution build() {
      return new Distribution(transforms, number, strategy);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Distribution that = (Distribution) o;
    return number == that.number
        && Objects.equal(transforms, that.transforms)
        && strategy == that.strategy;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(transforms, number, strategy);
  }
}
