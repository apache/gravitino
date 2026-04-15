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

package org.apache.gravitino.maintenance.optimizer.recommender.util;

import java.util.Map;

/**
 * Evaluates rule expressions against a provided context map.
 *
 * <p>Implementations must treat {@code context} keys as variable names and resolve them when
 * computing boolean or numeric results. Callers are expected to supply any required variables in
 * the context map.
 */
public interface ExpressionEvaluator {
  /**
   * Evaluates an expression that returns a boolean value.
   *
   * @param expression expression to evaluate
   * @param context variable bindings for the expression
   * @return evaluation result
   */
  boolean evaluateBool(String expression, Map<String, Object> context);

  /**
   * Evaluates an expression that returns a numeric value and coerces it to a {@code long}.
   *
   * @param expression expression to evaluate
   * @param context variable bindings for the expression
   * @return evaluation result as a {@code long}
   */
  long evaluateLong(String expression, Map<String, Object> context);
}
