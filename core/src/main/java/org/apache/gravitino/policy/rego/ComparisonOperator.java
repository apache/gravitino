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
package org.apache.gravitino.policy.rego;

import com.google.common.base.Preconditions;

/** Comparison operators supported by {@code restricted-rego-v1}. */
enum ComparisonOperator {
  /** Equality comparison. */
  EQ("=="),
  /** Inequality comparison. */
  NEQ("!="),
  /** Less-than comparison. */
  LT("<"),
  /** Less-than-or-equal comparison. */
  LTE("<="),
  /** Greater-than comparison. */
  GT(">"),
  /** Greater-than-or-equal comparison. */
  GTE(">="),
  /** Literal-array membership comparison. */
  IN("in");

  private final String sourceToken;

  ComparisonOperator(String sourceToken) {
    this.sourceToken = sourceToken;
  }

  static ComparisonOperator fromSourceToken(String token) {
    Preconditions.checkArgument(token != null && !token.isEmpty(), "operator cannot be empty");
    switch (token) {
      case "==":
        return EQ;
      case "!=":
        return NEQ;
      case "<":
        return LT;
      case "<=":
        return LTE;
      case ">":
        return GT;
      case ">=":
        return GTE;
      case "in":
        return IN;
      default:
        throw new IllegalArgumentException("Unsupported restricted-rego-v1 operator: " + token);
    }
  }

  String sourceToken() {
    return sourceToken;
  }
}
