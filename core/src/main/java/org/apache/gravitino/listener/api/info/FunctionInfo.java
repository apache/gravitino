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

package org.apache.gravitino.listener.api.info;

import java.util.Arrays;
import javax.annotation.Nullable;
import org.apache.gravitino.Audit;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.function.Function;
import org.apache.gravitino.function.FunctionDefinition;
import org.apache.gravitino.function.FunctionType;

/**
 * FunctionInfo exposes function information for event listener, it's supposed to be read only. Most
 * of the fields are shallow copied internally not deep copies for performance.
 */
@DeveloperApi
public final class FunctionInfo {
  private final String name;
  private final FunctionType functionType;
  private final boolean deterministic;
  @Nullable private final String comment;
  private final FunctionDefinition[] definitions;
  @Nullable private final Audit auditInfo;

  /**
   * Constructs a FunctionInfo object from a Function instance.
   *
   * @param function The source Function instance.
   */
  public FunctionInfo(Function function) {
    this(
        function.name(),
        function.functionType(),
        function.deterministic(),
        function.comment(),
        function.definitions(),
        function.auditInfo());
  }

  /**
   * Constructs a FunctionInfo object with specified details.
   *
   * @param name Name of the function.
   * @param functionType The function type (SCALAR, AGGREGATE, or TABLE).
   * @param deterministic Whether the function is deterministic.
   * @param comment Optional comment about the function.
   * @param definitions The function definitions.
   * @param auditInfo Optional audit information.
   */
  public FunctionInfo(
      String name,
      FunctionType functionType,
      boolean deterministic,
      String comment,
      FunctionDefinition[] definitions,
      Audit auditInfo) {
    this.name = name;
    this.functionType = functionType;
    this.deterministic = deterministic;
    this.comment = comment;
    this.definitions =
        definitions == null
            ? new FunctionDefinition[0]
            : Arrays.copyOf(definitions, definitions.length);
    this.auditInfo = auditInfo;
  }

  /**
   * Returns the name of the function.
   *
   * @return Function name.
   */
  public String name() {
    return name;
  }

  /**
   * Returns the function type.
   *
   * @return The function type.
   */
  public FunctionType functionType() {
    return functionType;
  }

  /**
   * Returns whether the function is deterministic.
   *
   * @return True if the function is deterministic.
   */
  public boolean deterministic() {
    return deterministic;
  }

  /**
   * Returns the comment of the function.
   *
   * @return Function comment, or {@code null} if not available.
   */
  @Nullable
  public String comment() {
    return comment;
  }

  /**
   * Returns the definitions of the function.
   *
   * @return Array of function definitions.
   */
  public FunctionDefinition[] definitions() {
    return definitions;
  }

  /**
   * Returns the audit information for the function.
   *
   * @return Audit information, or {@code null} if not available.
   */
  @Nullable
  public Audit auditInfo() {
    return auditInfo;
  }
}
