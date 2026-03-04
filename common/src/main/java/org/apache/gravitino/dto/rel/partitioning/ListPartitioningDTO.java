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

import java.util.Arrays;
import lombok.EqualsAndHashCode;
import org.apache.gravitino.dto.rel.ColumnDTO;
import org.apache.gravitino.dto.rel.PartitionUtils;
import org.apache.gravitino.dto.rel.partitions.ListPartitionDTO;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.NamedReference;

/** Data transfer object representing a list partitioning. */
@EqualsAndHashCode
public final class ListPartitioningDTO implements Partitioning {

  /**
   * Creates a new ListPartitioningDTO with no pre-assigned partitions.
   *
   * @param fieldNames The names of the fields to partition.
   * @return The new ListPartitioningDTO.
   */
  public static ListPartitioningDTO of(String[][] fieldNames) {
    return of(fieldNames, new ListPartitionDTO[0]);
  }

  /**
   * Creates a new ListPartitioningDTO.
   *
   * @param fieldNames The names of the fields to partition.
   * @param assignments The pre-assigned list partitions.
   * @return The new ListPartitioningDTO.
   */
  public static ListPartitioningDTO of(String[][] fieldNames, ListPartitionDTO[] assignments) {
    return new ListPartitioningDTO(fieldNames, assignments);
  }

  private final String[][] fieldNames;
  private final ListPartitionDTO[] assignments;

  private ListPartitioningDTO(String[][] fieldNames, ListPartitionDTO[] assignments) {
    this.fieldNames = fieldNames;
    this.assignments = assignments;
  }

  /**
   * @return The names of the fields to partition.
   */
  public String[][] fieldNames() {
    return fieldNames;
  }

  @Override
  public ListPartitionDTO[] assignments() {
    return assignments;
  }

  /**
   * @return The strategy of the partitioning.
   */
  @Override
  public Strategy strategy() {
    return Strategy.LIST;
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
    return Arrays.stream(fieldNames).map(NamedReference::field).toArray(Expression[]::new);
  }
}
