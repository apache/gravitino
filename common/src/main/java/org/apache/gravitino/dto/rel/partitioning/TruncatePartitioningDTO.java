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

import static org.apache.gravitino.rel.expressions.transforms.Transforms.truncate;

import lombok.EqualsAndHashCode;
import org.apache.gravitino.dto.rel.ColumnDTO;
import org.apache.gravitino.dto.rel.PartitionUtils;
import org.apache.gravitino.rel.expressions.Expression;

/** Represents the truncate partitioning. */
@EqualsAndHashCode
public final class TruncatePartitioningDTO implements Partitioning {

  /**
   * Constructs a truncate partitioning.
   *
   * @param width The width of the truncate partitioning.
   * @param fieldName The name of the field to partition.
   * @return The truncate partitioning.
   */
  public static TruncatePartitioningDTO of(int width, String[] fieldName) {
    return new TruncatePartitioningDTO(width, fieldName);
  }

  private final int width;
  private final String[] fieldName;

  private TruncatePartitioningDTO(int width, String[] fieldName) {
    this.width = width;
    this.fieldName = fieldName;
  }

  /**
   * @return The width of the partitioning.
   */
  public int width() {
    return width;
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

  /**
   * @return The arguments of the partitioning.
   */
  @Override
  public Expression[] arguments() {
    return truncate(width, fieldName).arguments();
  }

  /**
   * @return The strategy of the partitioning.
   */
  @Override
  public Strategy strategy() {
    return Strategy.TRUNCATE;
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
