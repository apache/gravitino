/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.catalog.hive;

import com.datastrato.graviton.meta.rel.transforms.Transforms;
import com.datastrato.graviton.meta.rel.transforms.Transforms.NamedReference;
import com.datastrato.graviton.rel.Bucket.BucketMethod;
import com.datastrato.graviton.rel.Transform;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class HiveBucketTest {

  @Test
  void testHiveBucket() {
    HiveBucket.Builder builder = new HiveBucket.Builder();
    builder.withBucketMethod(BucketMethod.HASH);
    builder.withBucketNum(1);
    builder.withTransforms(new Transform[] {Transforms.field(new String[] {"a"})});
    HiveBucket bucket = builder.build();

    Assertions.assertEquals(bucket.bucketMethod(), BucketMethod.HASH);
    Assertions.assertEquals(bucket.bucketNum(), 1);
    Assertions.assertArrayEquals(
        ((NamedReference) bucket.transforms()[0]).value(), new String[] {"a"});

    builder.withBucketMethod(BucketMethod.EVEN);
    builder.withBucketNum(11111);
    builder.withTransforms(new Transform[] {Transforms.function("Random", new Transform[] {})});
    bucket = builder.build();

    Assertions.assertEquals(bucket.bucketMethod(), BucketMethod.EVEN);
    Assertions.assertEquals(bucket.bucketNum(), 11111);
    Assertions.assertEquals(bucket.transforms()[0].name(), "Random");
    Assertions.assertArrayEquals(bucket.transforms()[0].arguments(), new Transform[] {});
  }
}
