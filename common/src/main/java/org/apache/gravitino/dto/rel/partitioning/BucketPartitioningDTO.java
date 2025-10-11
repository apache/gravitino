/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.dto.rel.partitioning;

import static org.apache.gravitino.rel.expressions.transforms.Transforms.bucket;

import java.util.Arrays;
import lombok.EqualsAndHashCode;
import org.apache.gravitino.dto.rel.ColumnDTO;
import org.apache.gravitino.dto.rel.PartitionUtils;
import org.apache.gravitino.rel.expressions.Expression;

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

  /**
   * @return The strategy of the partitioning.
   */
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
    Arrays.stream(fieldNames)
        .forEach(fieldName -> PartitionUtils.validateFieldExistence(columns, fieldName));
  }

  /**
   * @return The name of the partitioning strategy.
   */
  @Override
  public String name() {
    return strategy().name().toLowerCase();
  }

  /**
   * @return The arguments of the partitioning strategy.
   */
  @Override
  public Expression[] arguments() {
    return bucket(numBuckets, fieldNames).arguments();
  }
}
