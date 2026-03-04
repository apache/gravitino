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

package org.apache.gravitino.maintenance.optimizer.recommender.strategy;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.NoSuchPolicyException;
import org.apache.gravitino.exceptions.NotFoundException;
import org.apache.gravitino.maintenance.optimizer.api.common.Strategy;
import org.apache.gravitino.maintenance.optimizer.api.recommender.StrategyProvider;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.util.GravitinoClientUtils;
import org.apache.gravitino.maintenance.optimizer.common.util.IdentifierUtils;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.rel.Table;

/** Strategy provider that loads policies from Gravitino. */
public class GravitinoStrategyProvider implements StrategyProvider {

  public static final String NAME = "gravitino-strategy-provider";
  private GravitinoClient gravitinoClient;

  /**
   * Initializes the provider with a Gravitino client derived from the optimizer configuration.
   *
   * @param optimizerEnv optimizer environment
   */
  @Override
  public void initialize(OptimizerEnv optimizerEnv) {
    this.gravitinoClient = GravitinoClientUtils.createClient(optimizerEnv);
  }

  /**
   * Returns the provider name for configuration lookup.
   *
   * @return provider name
   */
  @Override
  public String name() {
    return NAME;
  }

  /**
   * Lists strategies attached to the specified table identifier.
   *
   * @param nameIdentifier fully qualified table identifier
   * @return list of strategies, possibly empty
   */
  @Override
  public List<Strategy> strategies(NameIdentifier nameIdentifier) {
    IdentifierUtils.requireTableIdentifierNormalized(nameIdentifier);
    Table t =
        gravitinoClient
            .loadCatalog(IdentifierUtils.getCatalogNameFromTableIdentifier(nameIdentifier))
            .asTableCatalog()
            .loadTable(IdentifierUtils.removeCatalogFromIdentifier(nameIdentifier));
    String[] policyNames = t.supportsPolicies().listPolicies();
    List<Strategy> policies =
        Arrays.stream(policyNames)
            .map(t.supportsPolicies()::getPolicy)
            .filter(Objects::nonNull)
            .map(this::toStrategy)
            .collect(Collectors.toList());
    return policies;
  }

  /**
   * Returns a strategy by name.
   *
   * @param strategyName strategy name
   * @return strategy
   * @throws NotFoundException if the strategy does not exist
   */
  @Override
  public Strategy strategy(String strategyName) throws NotFoundException {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(strategyName), "strategyName must not be blank");
    try {
      return toStrategy(gravitinoClient.getPolicy(strategyName));
    } catch (NoSuchPolicyException e) {
      throw new NotFoundException(e, "Strategy '%s' not found", strategyName);
    }
  }

  private Strategy toStrategy(Policy policy) {
    return new GravitinoStrategy(policy);
  }

  /**
   * Closes the underlying Gravitino client.
   *
   * @throws Exception if closing fails
   */
  @Override
  public void close() throws Exception {
    if (gravitinoClient != null) {
      gravitinoClient.close();
    }
  }
}
