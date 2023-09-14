/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.rel;

import com.datastrato.graviton.rel.Distribution.Strategy;
import com.datastrato.graviton.rel.transforms.Transform;
import com.datastrato.graviton.rel.transforms.Transforms;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestDistribution {

  @Test
  void testDistribution() {
    Distribution.DistributionBuilder builder = new Distribution.DistributionBuilder();
    builder.withStrategy(Strategy.HASH);
    builder.withNumber(1);
    builder.withTransforms(new Transform[] {Transforms.field(new String[] {"field1"})});

    Distribution bucket = builder.build();

    Assertions.assertEquals(Strategy.HASH, bucket.strategy());
    Assertions.assertEquals(1, bucket.number());
    Assertions.assertArrayEquals(
        new Transform[] {Transforms.field(new String[] {"field1"})}, bucket.transforms());

    builder.withStrategy(Strategy.EVEN);
    builder.withNumber(11111);
    Transform[] transforms =
        new Transform[] {
          Transforms.field(new String[] {"field1"}), Transforms.function("now", new Transform[0])
        };

    builder.withTransforms(transforms);
    bucket = builder.build();

    Assertions.assertEquals(Strategy.EVEN, bucket.strategy());
    Assertions.assertEquals(11111, bucket.number());
    Assertions.assertArrayEquals(transforms, bucket.transforms());
  }
}
