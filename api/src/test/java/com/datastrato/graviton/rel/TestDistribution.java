/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.rel;

import com.datastrato.graviton.rel.Distribution.DistributionMethod;
import com.datastrato.graviton.rel.transforms.Transform;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestDistribution {

  @Test
  void testDistribution() {
    Distribution.DistributionBuilder builder = new Distribution.DistributionBuilder();
    builder.distMethod(DistributionMethod.HASH);
    builder.distNum(1);
    builder.transforms(new Transform[] {});
    Distribution bucket = builder.build();

    Assertions.assertEquals(DistributionMethod.HASH, bucket.distMethod());
    Assertions.assertEquals(1, bucket.distNum());
    Assertions.assertArrayEquals(new Transform[] {}, bucket.transforms());

    builder.distMethod(DistributionMethod.EVEN);
    builder.distNum(11111);
    builder.transforms(new Transform[] {});
    bucket = builder.build();

    Assertions.assertEquals(DistributionMethod.EVEN, bucket.distMethod());
    Assertions.assertEquals(11111, bucket.distNum());
    Assertions.assertArrayEquals(new Transform[] {}, bucket.transforms());
  }
}
