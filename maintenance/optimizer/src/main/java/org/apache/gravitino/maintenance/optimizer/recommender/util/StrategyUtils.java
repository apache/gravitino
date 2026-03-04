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

import org.apache.gravitino.maintenance.optimizer.api.common.Strategy;

/** Utility methods and rule keys for interpreting optimizer strategies. */
public class StrategyUtils {

  /** Prefix for job option keys exposed as rules. */
  public static final String JOB_ROLE_PREFIX = "job.";
  /** Rule key for the trigger expression. */
  public static final String TRIGGER_EXPR = "trigger-expr";
  /** Rule key for the score expression. */
  public static final String SCORE_EXPR = "score-expr";

  private static final String DEFAULT_TRIGGER_EXPR = "false";
  private static final String DEFAULT_SCORE_EXPR = "-1";
  /**
   * Resolve the trigger expression for a strategy.
   *
   * @param strategy strategy definition
   * @return trigger expression or {@code false} by default
   */
  public static String getTriggerExpression(Strategy strategy) {
    Object value = strategy.rules().get(TRIGGER_EXPR);
    if (value == null) {
      return DEFAULT_TRIGGER_EXPR;
    }
    String expression = value.toString();
    return expression.trim().isEmpty() ? DEFAULT_TRIGGER_EXPR : expression;
  }

  /**
   * Resolve the score expression for a strategy.
   *
   * @param strategy strategy definition
   * @return score expression or {@code -1} by default
   */
  public static String getScoreExpression(Strategy strategy) {
    Object value = strategy.rules().get(SCORE_EXPR);
    if (value == null) {
      return DEFAULT_SCORE_EXPR;
    }
    String expression = value.toString();
    return expression.trim().isEmpty() ? DEFAULT_SCORE_EXPR : expression;
  }
}
