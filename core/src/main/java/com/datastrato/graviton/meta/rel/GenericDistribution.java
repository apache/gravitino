/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.meta.rel;

import com.datastrato.graviton.rel.Distribution;
import com.datastrato.graviton.rel.Transform;
import lombok.Builder;

@Builder
public class GenericDistribution implements Distribution {
  private final Transform[] transforms;
  private final int distNum;
  private final DistributionMethod distMethod;

  @Override
  public Transform[] transforms() {
    return transforms;
  }

  @Override
  public int distNum() {
    return distNum;
  }

  @Override
  public DistributionMethod distMethod() {
    return distMethod;
  }
}
