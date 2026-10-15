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

/**
 * A node in an unresolved {@code restricted-rego-v1} expression tree.
 *
 * <p>The tree contains only the source constructs allowlisted by the read-restriction contract.
 * Column and context references remain symbolic until the effective-policy resolver binds them to a
 * table schema and request context.
 */
interface CanonicalExpression {

  /**
   * Validates this expression node and all of its children.
   *
   * @throws IllegalArgumentException if the expression is malformed or outside the allowlisted
   *     source profile
   */
  void validate() throws IllegalArgumentException;

  /**
   * Returns the operation depth defined by {@code restricted-rego-v1}.
   *
   * <p>Value leaves such as a column, string, or number have depth 0. Atomic predicates such as a
   * comparison, Boolean literal, or group-membership check have depth 1. Negation and a logical
   * expression add one to their deepest operand. A consecutive n-ary logical chain counts as one
   * operation, so {@code true and false and true} has depth 2.
   *
   * @return expression operation depth
   */
  int depth();
}
