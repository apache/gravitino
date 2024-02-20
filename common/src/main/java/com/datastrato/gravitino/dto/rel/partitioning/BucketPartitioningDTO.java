/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.rel.partitioning;

import static com.datastrato.gravitino.dto.rel.PartitionUtils.validateFieldExistence;
import static com.datastrato.gravitino.rel.expressions.transforms.Transforms.bucket;

import com.datastrato.gravitino.dto.rel.ColumnDTO;
import com.datastrato.gravitino.rel.expressions.Expression;
import java.util.Arrays;
import lombok.EqualsAndHashCode;

/** Data transfer object representing bucket partitioning. */
@EqualsAndHashCode
public final class BucketPartitioningDTO implements Partitioning {

  /**
   * Creates a new instance of {@link BucketPartitioningDTO}.
   *
   * @param numBuckets The number of buckets.
   * @param fieldNames The field names.
   * @return The new instance.
   */
  public static BucketPartitioningDTO of(int numBuckets, String[]... fieldNames) {
    return new BucketPartitioningDTO(numBuckets, fieldNames);
  }

  private final int numBuckets;
  private final String[][] fieldNames;

  private BucketPartitioningDTO(int numBuckets, String[][] fieldNames) {
    this.numBuckets = numBuckets;
    this.fieldNames = fieldNames;
  }

  /**
   * Returns the number of buckets.
   *
   * @return The number of buckets.
   */
  public int numBuckets() {
    return numBuckets;
  }

  /**
   * Returns the field names.
   *
   * @return The field names.
   */
  public String[][] fieldNames() {
    return fieldNames;
  }

  /** @return The strategy of the partitioning. */
  @Override
  public Strategy strategy() {
    return Strategy.BUCKET;
  }

  /**
   * Validates the partitioning columns.
   *
   * @param columns The columns to be validated.
   * @throws IllegalArgumentException If the columns are invalid, this exception is thrown.
   */
  @Override
  public void validate(ColumnDTO[] columns) throws IllegalArgumentException {
    Arrays.stream(fieldNames).forEach(fieldName -> validateFieldExistence(columns, fieldName));
  }

  /** @return The name of the partitioning strategy. */
  @Override
  public String name() {
    return strategy().name().toLowerCase();
  }

  /** @return The arguments of the partitioning strategy. */
  @Override
  public Expression[] arguments() {
    return bucket(numBuckets, fieldNames).arguments();
  }
}
