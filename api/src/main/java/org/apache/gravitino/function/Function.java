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
import org.apache.gravitino.Namespace;
import org.apache.gravitino.annotation.Evolving;

/**
 * An interface representing a user-defined function under a schema {@link Namespace}. A function is
 * a reusable computational unit that can be invoked within queries across different compute
 * engines. Users can register a function in Gravitino to manage the function metadata and enable
 * cross-engine function sharing. The typical use case is to define custom business logic once and
 * reuse it across multiple compute engines like Spark, Trino, and AI engines.
 *
 * <p>A function is characterized by its name, type (scalar for row-by-row operations, aggregate for
 * group operations, or table-valued for set-returning operations), whether it is deterministic, and
 * its definitions that contain parameters, return type or columns (for table function), and
 * implementations for different runtime engines.
 */
@Evolving
public interface Function extends Auditable {

  /**
   * @return The function name.
   */
  String name();

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
   * @return The definitions of the function.
   */
  FunctionDefinition[] definitions();
}
