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
package org.apache.gravitino.dto.rel.expressions;

import java.util.Arrays;
import org.apache.gravitino.dto.rel.ColumnDTO;
import org.apache.gravitino.dto.rel.PartitionUtils;
import org.apache.gravitino.rel.expressions.Expression;

/** An argument of a function. */
public interface FunctionArg extends Expression {

  /** Constant for an empty array of function arguments. */
  FunctionArg[] EMPTY_ARGS = new FunctionArg[0];

  /**
   * Arguments type of the function.
   *
   * @return The type of the argument.
   */
  ArgType argType();

  /**
   * Validates the function argument.
   *
   * @param columns The columns of the table.
   * @throws IllegalArgumentException If the function argument is invalid.
   */
  default void validate(ColumnDTO[] columns) throws IllegalArgumentException {
    Arrays.stream(references())
        .forEach(ref -> PartitionUtils.validateFieldExistence(columns, ref.fieldName()));
  }

  /** The type of the argument. */
  enum ArgType {
    /** A literal argument. */
    LITERAL,

    /** * A field argument. */
    FIELD,

    /** A function argument. */
    FUNCTION,

    /** An argument that cannot be parsed. */
    UNPARSED
  }
}
