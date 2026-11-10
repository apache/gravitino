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

import java.util.List;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.exceptions.NotFoundException;
import org.apache.gravitino.maintenance.optimizer.api.common.Provider;
import org.apache.gravitino.maintenance.optimizer.api.common.Strategy;

/**
 * Supplies strategies attached to identifiers (for example, tables). The recommender asks this
 * provider for strategies before instantiating {@link
 * org.apache.gravitino.maintenance.optimizer.api.recommender.StrategyHandler strategy handlers}.
 */
@DeveloperApi
public interface StrategyProvider extends Provider {

  /**
   * List all strategies attached to the given identifier.
   *
   * @param nameIdentifier fully qualified table identifier (catalog/schema/table)
   * @return list of strategies for the table (empty if none)
   */
  List<Strategy> strategies(NameIdentifier nameIdentifier);

  /**
   * Fetch a single strategy by name.
   *
   * @param strategyName unique strategy name
   * @return strategy definition; implementations may throw if the strategy is missing or
   *     inaccessible
   * @throws NotFoundException if the strategy is not found
   */
  Strategy strategy(String strategyName) throws NotFoundException;
}
