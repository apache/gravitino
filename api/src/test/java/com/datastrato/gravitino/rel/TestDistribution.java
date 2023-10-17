/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.rel;

import com.datastrato.gravitino.rel.Distribution.Builder;
import com.datastrato.gravitino.rel.Distribution.Strategy;
import com.datastrato.gravitino.rel.transforms.Transform;
import com.datastrato.gravitino.rel.transforms.Transforms;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestDistribution {

  @Test
  void testDistribution() {
    Builder builder = new Builder();
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

  @Test
  void testUtils() {
    Distribution distribution =
        Distribution.nameReferenceDistribution(Strategy.HASH, 1, new String[] {"a"});
    Assertions.assertEquals(Strategy.HASH, distribution.strategy());
    Assertions.assertEquals(1, distribution.number());
    Assertions.assertTrue(distribution.transforms()[0] instanceof Transforms.NamedReference);
    Assertions.assertArrayEquals(
        new String[] {"a"}, ((Transforms.NamedReference) distribution.transforms()[0]).value());

    distribution =
        Distribution.nameReferenceDistribution(
            Strategy.HASH, 2, new String[] {"a"}, new String[] {"b"});

    Assertions.assertEquals(Strategy.HASH, distribution.strategy());
    Assertions.assertEquals(2, distribution.number());
    Assertions.assertTrue(distribution.transforms()[0] instanceof Transforms.NamedReference);
    Assertions.assertArrayEquals(
        new String[] {"a"}, ((Transforms.NamedReference) distribution.transforms()[0]).value());
    Assertions.assertTrue(distribution.transforms()[1] instanceof Transforms.NamedReference);
    Assertions.assertArrayEquals(
        new String[] {"b"}, ((Transforms.NamedReference) distribution.transforms()[1]).value());
  }
}
