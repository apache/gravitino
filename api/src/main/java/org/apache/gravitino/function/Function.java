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
import org.apache.gravitino.Auditable;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.rel.types.Type;

/** Represents a user-defined function registered in Gravitino. */
@Evolving
public interface Function extends Auditable {

  /**
   * @return The function signature.
   */
  FunctionSignature signature();

  /**
   * @return The function type.
   */
  FunctionType functionType();

  /**
   * @return Whether the function is deterministic.
   */
  boolean deterministic();

  /**
   * @return The optional comment of the function.
   */
  @Nullable
  default String comment() {
    return null;
  }

  /**
   * The return type for scalar or aggregate functions.
   *
   * @return The return type, null if this is a table-valued function.
   */
  @Nullable
  default Type returnType() {
    return null;
  }

  /**
   * The output columns for a table-valued function.
   *
   * @return The output columns of a table-valued function, or an empty array for scalar or
   *     aggregate functions.
   */
  default FunctionColumn[] returnColumns() {
    return new FunctionColumn[0];
  }

  /**
   * @return The implementations of the function.
   */
  FunctionImpl[] impls();

  /**
   * @return The version of the function.
   */
  int version();
}
