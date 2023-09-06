/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.meta.rel;

import com.datastrato.graviton.rel.Bucket;
import com.datastrato.graviton.rel.Transform;

public class BaseBucket implements Bucket {
  protected Transform[] transforms;
  protected int bucketNum;
  protected BucketMethod bucketMethod;

  @Override
  public Transform[] transforms() {
    return transforms;
  }

  @Override
  public int bucketNum() {
    return bucketNum;
  }

  @Override
  public BucketMethod bucketMethod() {
    return bucketMethod;
  }

  interface Builder<SELF extends BaseBucket.Builder<SELF, T>, T extends BaseBucket> {

    SELF withTransforms(Transform[] transforms);

    SELF withBucketNum(int bucketNum);

    SELF withBucketMethod(BucketMethod bucketMethod);

    T build();
  }

  // Need to learn this constructor syntax
  public abstract static class BaseBucketBuilder<
          SELF extends BaseBucket.Builder<SELF, T>, T extends BaseBucket>
      implements BaseBucket.Builder<SELF, T> {
    protected Transform[] transforms;
    protected int bucketNum;
    protected BucketMethod bucketMethod;

    public SELF withTransforms(Transform[] transforms) {
      this.transforms = transforms;
      return (SELF) this;
    }

    public SELF withBucketNum(int bucketNum) {
      this.bucketNum = bucketNum;
      return (SELF) this;
    }

    public SELF withBucketMethod(BucketMethod bucketMethod) {
      this.bucketMethod = bucketMethod;
      return (SELF) this;
    }
  }

  public static class BucketBuilder extends BaseBucketBuilder<BucketBuilder, BaseBucket> {
    public BaseBucket build() {
      BaseBucket baseBucket = new BaseBucket();
      baseBucket.bucketMethod = bucketMethod;
      baseBucket.transforms = transforms;
      baseBucket.bucketNum = bucketNum;
      return baseBucket;
    }
  }
}
