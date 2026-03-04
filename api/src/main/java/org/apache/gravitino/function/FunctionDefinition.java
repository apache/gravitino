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
package org.apache.gravitino.function;

import javax.annotation.Nullable;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.rel.types.Type;

/**
 * A function definition that pairs a specific parameter list with its implementations and return
 * type. A single function can include multiple definitions (overloads), each with distinct
 * parameters and implementations. Overload resolution is based on parameters only; return type or
 * return columns are metadata and do not participate in definition matching.
 *
 * <p>For scalar or aggregate functions, use {@link #returnType()} to specify the return type. For
 * table-valued functions, use {@link #returnColumns()} to specify the output columns.
 */
@Evolving
public interface FunctionDefinition {

  /** An empty array of {@link FunctionColumn}. */
  FunctionColumn[] EMPTY_COLUMNS = new FunctionColumn[0];

  /**
   * @return The parameters for this definition. Maybe an empty array for a no-arg definition.
   */
  FunctionParam[] parameters();

  /**
   * The return type for scalar or aggregate function definitions.
   *
   * @return The return type, or null if this is a table-valued function definition.
   */
  @Nullable
  default Type returnType() {
    return null;
  }

  /**
   * The output columns for a table-valued function definition.
   *
   * <p>A table-valued function is a function that returns a table instead of a scalar value or an
   * aggregate result. The returned table has a fixed schema defined by the columns returned from
   * this method.
   *
   * @return The output columns that define the schema of the table returned by this definition, or
   *     an empty array if this is a scalar or aggregate function definition.
   */
  default FunctionColumn[] returnColumns() {
    return EMPTY_COLUMNS;
  }

  /**
   * @return The implementations associated with this definition.
   */
  FunctionImpl[] impls();
}
