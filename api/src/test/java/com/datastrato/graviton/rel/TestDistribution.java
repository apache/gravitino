/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.rel;

import com.datastrato.graviton.rel.Distribution.Strategy;
import com.datastrato.graviton.rel.transforms.Transform;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestDistribution {

  @Test
  void testDistribution() {
    Distribution.DistributionBuilder builder = new Distribution.DistributionBuilder();
    builder.withStrategy(Strategy.HASH);
    builder.withNumber(1);
    builder.withTransforms(new Transform[] {});
    Distribution bucket = builder.build();

    Assertions.assertEquals(Strategy.HASH, bucket.strategy());
    Assertions.assertEquals(1, bucket.number());
    Assertions.assertArrayEquals(new Transform[] {}, bucket.transforms());

    builder.withStrategy(Strategy.EVEN);
    builder.withNumber(11111);
    builder.withTransforms(new Transform[] {});
    bucket = builder.build();

    Assertions.assertEquals(Strategy.EVEN, bucket.strategy());
    Assertions.assertEquals(11111, bucket.number());
    Assertions.assertArrayEquals(new Transform[] {}, bucket.transforms());
  }
}
