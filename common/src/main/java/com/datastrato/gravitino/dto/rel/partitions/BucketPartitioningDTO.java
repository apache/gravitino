/*
 * Copyright 2023 DATASTRATO Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.rel.partitions;

import static com.datastrato.gravitino.dto.rel.PartitionUtils.validateFieldExistence;
import static com.datastrato.gravitino.rel.expressions.transforms.Transforms.bucket;

import com.datastrato.gravitino.dto.rel.ColumnDTO;
import com.datastrato.gravitino.rel.expressions.Expression;
import java.util.Arrays;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode
public final class BucketPartitioningDTO implements Partitioning {

  public static BucketPartitioningDTO of(int numBuckets, String[]... fieldNames) {
    return new BucketPartitioningDTO(numBuckets, fieldNames);
  }

  private final int numBuckets;
  private final String[][] fieldNames;

  private BucketPartitioningDTO(int numBuckets, String[][] fieldNames) {
    this.numBuckets = numBuckets;
    this.fieldNames = fieldNames;
  }

  public int numBuckets() {
    return numBuckets;
  }

  public String[][] fieldNames() {
    return fieldNames;
  }

  @Override
  public Strategy strategy() {
    return Strategy.BUCKET;
  }

  @Override
  public void validate(ColumnDTO[] columns) throws IllegalArgumentException {
    Arrays.stream(fieldNames).forEach(fieldName -> validateFieldExistence(columns, fieldName));
  }

  @Override
  public String name() {
    return strategy().name().toLowerCase();
  }

  @Override
  public Expression[] arguments() {
    return bucket(numBuckets, fieldNames).arguments();
  }
}
