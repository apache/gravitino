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
import org.apache.gravitino.dto.rel.expressions.FunctionArg;
import org.apache.gravitino.rel.expressions.Expression;

/** Data transfer object for function partitioning. */
@EqualsAndHashCode
public final class FunctionPartitioningDTO implements Partitioning {

  /**
   * Creates a new function partitioning DTO.
   *
   * @param functionName The name of the function.
   * @param args The arguments of the function.
   * @return The function partitioning DTO.
   */
  public static FunctionPartitioningDTO of(String functionName, FunctionArg... args) {
    return new FunctionPartitioningDTO(functionName, args);
  }

  String functionName;
  FunctionArg[] args;

  private FunctionPartitioningDTO(String functionName, FunctionArg[] args) {
    this.functionName = functionName;
    this.args = args;
  }

  /**
   * Returns the name of the function.
   *
   * @return The name of the function.
   */
  public String functionName() {
    return functionName;
  }

  /**
   * Returns the arguments of the function.
   *
   * @return The arguments of the function.
   */
  public FunctionArg[] args() {
    return args;
  }

  /**
   * Returns the name of the function.
   *
   * @return The name of the function.
   */
  @Override
  public String name() {
    return functionName;
  }

  /**
   * Returns the arguments of the function.
   *
   * @return The arguments of the function.
   */
  @Override
  public Expression[] arguments() {
    return args;
  }

  /**
   * Returns the strategy of the partitioning.
   *
   * @return The strategy of the partitioning.
   */
  @Override
  public Strategy strategy() {
    return Strategy.FUNCTION;
  }

  /**
   * Validates the function partitioning.
   *
   * @param columns The columns to be validated.
   * @throws IllegalArgumentException If the function partitioning is invalid.
   */
  @Override
  public void validate(ColumnDTO[] columns) throws IllegalArgumentException {
    Arrays.stream(references())
        .forEach(ref -> PartitionUtils.validateFieldExistence(columns, ref.fieldName()));
  }
}
