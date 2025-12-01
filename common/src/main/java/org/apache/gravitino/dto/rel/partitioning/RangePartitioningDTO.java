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

import static org.apache.gravitino.rel.expressions.NamedReference.field;

import lombok.EqualsAndHashCode;
import org.apache.gravitino.dto.rel.ColumnDTO;
import org.apache.gravitino.dto.rel.PartitionUtils;
import org.apache.gravitino.dto.rel.partitions.RangePartitionDTO;
import org.apache.gravitino.rel.expressions.Expression;

/** Represents the range partitioning. */
@EqualsAndHashCode
public final class RangePartitioningDTO implements Partitioning {

  /**
   * Creates a new RangePartitioningDTO with no pre-assigned partitions.
   *
   * @param fieldName The name of the field to partition.
   * @return The new RangePartitioningDTO.
   */
  public static RangePartitioningDTO of(String[] fieldName) {
    return of(fieldName, new RangePartitionDTO[0]);
  }

  /**
   * Creates a new RangePartitioningDTO.
   *
   * @param fieldName The name of the field to partition.
   * @param assignments The pre-assigned range partitions.
   * @return The new RangePartitioningDTO.
   */
  public static RangePartitioningDTO of(String[] fieldName, RangePartitionDTO[] assignments) {
    return new RangePartitioningDTO(fieldName, assignments);
  }

  private final String[] fieldName;
  private final RangePartitionDTO[] assignments;

  private RangePartitioningDTO(String[] fieldName, RangePartitionDTO[] assignments) {
    this.fieldName = fieldName;
    this.assignments = assignments;
  }

  /**
   * @return The name of the field to partition.
   */
  public String[] fieldName() {
    return fieldName;
  }

  /**
   * @return The name of the partitioning strategy.
   */
  @Override
  public String name() {
    return strategy().name().toLowerCase();
  }

  @Override
  public RangePartitionDTO[] assignments() {
    return assignments;
  }

  /**
   * @return The arguments of the partitioning.
   */
  @Override
  public Expression[] arguments() {
    return new Expression[] {field(fieldName)};
  }

  /**
   * @return The strategy of the partitioning.
   */
  @Override
  public Strategy strategy() {
    return Strategy.RANGE;
  }

  /**
   * Validates the partitioning columns.
   *
   * @param columns The columns to be validated.
   * @throws IllegalArgumentException If the columns are invalid, this exception is thrown.
   */
  @Override
  public void validate(ColumnDTO[] columns) throws IllegalArgumentException {
    PartitionUtils.validateFieldExistence(columns, fieldName);
  }
}
