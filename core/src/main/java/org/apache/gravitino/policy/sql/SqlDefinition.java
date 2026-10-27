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
package org.apache.gravitino.policy.sql;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/** A validated definition; parameter names are local and have no physical field IDs yet. */
public final class SqlDefinition {
  private final Map<String, SqlType> parameters;
  private final SqlType resultType;
  private final SqlExpression body;
  private final Set<String> quotedParameters;
  private final boolean columnMask;

  SqlDefinition(
      Map<String, SqlType> parameters,
      SqlType resultType,
      SqlExpression body,
      Set<String> quotedParameters,
      boolean columnMask) {
    this.parameters = Collections.unmodifiableMap(new LinkedHashMap<>(parameters));
    this.resultType = resultType;
    this.body = body;
    this.quotedParameters = Collections.unmodifiableSet(new LinkedHashSet<>(quotedParameters));
    this.columnMask = columnMask;
  }

  /**
   * Returns the immutable declared parameter types keyed by normalized local name.
   *
   * @return the immutable declared parameter types keyed by normalized local name
   */
  public Map<String, SqlType> parameters() {
    return parameters;
  }

  /**
   * Returns the declared result type.
   *
   * @return the declared result type
   */
  public SqlType resultType() {
    return resultType;
  }

  /**
   * Returns the validated unresolved expression body.
   *
   * @return the validated unresolved expression body
   */
  public SqlExpression body() {
    return body;
  }

  /**
   * Checks whether a parameter was declared with exact SQL quoting.
   *
   * @param name normalized local parameter name
   * @return whether physical column matching must be case sensitive
   */
  public boolean isQuotedParameter(String name) {
    return quotedParameters.contains(name);
  }
  /**
   * @return whether this definition was validated as a column mask
   */
  public boolean isColumnMask() {
    return columnMask;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return "SqlDefinition{parameters="
        + parameters
        + ", resultType="
        + resultType
        + ", body="
        + body
        + ", quotedParameters="
        + quotedParameters
        + ", columnMask="
        + columnMask
        + "}";
  }
}
