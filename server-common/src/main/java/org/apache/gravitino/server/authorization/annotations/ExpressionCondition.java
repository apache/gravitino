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
package org.apache.gravitino.server.authorization.annotations;

/**
 * Defines the conditions under which an authorization expression is evaluated in {@link
 * AuthorizationExpression}.
 */
public enum ExpressionCondition {
  /**
   * Not configured — the expression is <em>not</em> evaluated. Default for the secondary and third
   * expressions so that they are only triggered when a condition is explicitly set.
   */
  NONE,

  /**
   * The expression is always evaluated unconditionally. Default for the primary ({@code
   * firstExpressionCondition}) so that the primary check runs on every request unless overridden by
   * a more specific condition.
   */
  ALWAYS,

  /**
   * The expression is evaluated when the request does <em>not</em> contain explicit required
   * privileges (e.g. the {@code ?privileges=} query parameter is absent or blank). Used on the
   * primary expression to restrict it to the no-privileges path, making the expression selection
   * symmetric with {@link #CONTAIN_REQUIRED_PRIVILEGES}.
   */
  NOT_CONTAIN_REQUIRED_PRIVILEGES,

  /**
   * The expression is evaluated when the request contains explicit required privileges (e.g. via a
   * {@code ?privileges=} query parameter). Used to apply stricter authorization when callers
   * declare the privileges they intend to exercise.
   */
  CONTAIN_REQUIRED_PRIVILEGES,

  /**
   * The expression is evaluated when the preceding primary (or secondary) expression was denied.
   * Used to apply a fallback check — for example, verifying object existence for users who hold
   * creation or view-only privileges but not full read access.
   */
  PREVIOUS_EXPRESSION_FORBIDDEN,

  /**
   * The expression is evaluated when the rename operation moves a table across namespace boundaries
   * (i.e. source schema ≠ destination schema). Used to enforce stricter checks such as table
   * ownership on the source and {@code CREATE_TABLE} on the destination schema.
   */
  RENAMING_CROSSING_NAMESPACE
}
