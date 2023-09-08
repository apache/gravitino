/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.meta.rel;

import com.datastrato.graviton.rel.Distribution.DistributionMethod;
import com.datastrato.graviton.rel.transforms.Transform;
import com.datastrato.graviton.rel.transforms.Transforms;
import com.datastrato.graviton.rel.transforms.Transforms.NamedReference;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class DistributionTest {

  @Test
  void testDistribution() {
    GenericDistribution.GenericDistributionBuilder builder =
        new GenericDistribution.GenericDistributionBuilder();
    builder.distMethod(DistributionMethod.HASH);
    builder.distNum(1);
    builder.transforms(new Transform[] {Transforms.field(new String[] {"a"})});
    GenericDistribution bucket = builder.build();

    Assertions.assertEquals(bucket.distMethod(), DistributionMethod.HASH);
    Assertions.assertEquals(bucket.distNum(), 1);
    Assertions.assertArrayEquals(
        ((NamedReference) bucket.transforms()[0]).value(), new String[] {"a"});

    builder.distMethod(DistributionMethod.EVEN);
    builder.distNum(11111);
    builder.transforms(new Transform[] {Transforms.function("Random", new Transform[] {})});
    bucket = builder.build();

    Assertions.assertEquals(bucket.distMethod(), DistributionMethod.EVEN);
    Assertions.assertEquals(bucket.distNum(), 11111);
    Assertions.assertEquals(bucket.transforms()[0].name(), "Random");
    Assertions.assertArrayEquals(bucket.transforms()[0].arguments(), new Transform[] {});
  }
}
