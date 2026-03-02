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

package org.apache.gravitino.maintenance.optimizer.api.recommender;

import java.util.Set;
import org.apache.gravitino.annotation.DeveloperApi;

/**
 * Executes a single recommender strategy: declares required inputs, initializes with those inputs,
 * scores a potential action, and optionally submits a job description for execution.
 */
@DeveloperApi
public interface StrategyHandler {

  /**
   * Declares the optional data the handler needs before it can be initialized. The {@link
   * StrategyHandlerContext} handed to {@link #initialize(StrategyHandlerContext)} will include only
   * the requested items.
   */
  enum DataRequirement {
    TABLE_METADATA,
    TABLE_STATISTICS,
    PARTITION_STATISTICS
  }

  /**
   * Declares which pieces of data this handler needs before it can be initialized.
   *
   * @return set of requested data items; empty means no additional data needed
   */
  default Set<DataRequirement> dataRequirements() {
    return Set.of();
  }

  /**
   * Initialize the handler with the supplied context.
   *
   * @param context immutable view of the target identifier, strategy, and any requested metadata or
   *     statistics
   */
  void initialize(StrategyHandlerContext context);

  /**
   * Stable identifier for the strategy type this handler supports (for example, {@code
   * COMPACTION}).
   *
   * @return strategy type string
   */
  String strategyType();

  /**
   * Cheap pre-check to determine whether this handler should be evaluated for the current context.
   * Implementations should avoid heavy work here.
   *
   * @return {@code true} if the strategy should be evaluated, {@code false} otherwise
   */
  boolean shouldTrigger();

  /**
   * Evaluate the strategy and return a scored recommendation. Only invoked if {@link
   * #shouldTrigger} returns {@code true}.
   *
   * @return scored evaluation result with job execution context
   */
  StrategyEvaluation evaluate();
}
