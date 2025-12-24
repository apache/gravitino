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

package org.apache.gravitino.maintenance.optimizer.api.common;

import java.util.Map;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.maintenance.optimizer.api.recommender.StrategyHandler;

/**
 * Strategy definition supplied by the control plane. The recommender pulls strategies from a {@link
 * org.apache.gravitino.maintenance.optimizer.api.recommender.StrategyProvider}, routes them to a
 * {@link StrategyHandler} by {@link #strategyType()}, and lets the handler interpret the remaining
 * fields as needed.
 */
@DeveloperApi
public interface Strategy {

  /**
   * Strategy name.
   *
   * <p>This name is used to identify the strategy in the optimizer, for example, {@code
   * compaction-strategy-1}.
   *
   * @return strategy name
   */
  String name();

  /**
   * Strategy type used to route to a {@link StrategyHandler}. Built-in handlers should document
   * their expected type string.
   *
   * <p>There may be multiple strategy names for the same strategy type.
   *
   * @return strategy type identifier
   */
  String strategyType();

  /**
   * Arbitrary attributes for the strategy.
   *
   * @return strategy-level options
   */
  Map<String, String> properties();

  /**
   * Structured rule payload that the handler can interpret, for example, trigger or score
   * expressions.
   *
   * @return map of rule identifiers to rule objects
   */
  Map<String, Object> rules();

  /**
   * Job-level options that should be applied when the handler builds a {@code JobExecutionContext}
   * (for example, target file size for the compaction job).
   *
   * @return job-specific options
   */
  Map<String, String> jobOptions();

  /**
   * Optional job template name to resolve the job in the job submitter.
   *
   * @return job template name
   */
  String jobTemplateName();
}
