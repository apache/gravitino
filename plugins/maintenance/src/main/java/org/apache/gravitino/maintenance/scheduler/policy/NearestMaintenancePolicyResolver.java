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
package org.apache.gravitino.maintenance.scheduler.policy;

import java.util.Optional;
import java.util.TreeSet;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.policy.ObjectPolicyResolver;
import org.apache.gravitino.policy.ObjectPolicyResolver.Match;
import org.apache.gravitino.policy.Policy;

/**
 * Picks the effective maintenance policy of a given {@link Policy.BuiltInType} for a table.
 *
 * <p>{@link ObjectPolicyResolver} already drops farther same-type maintenance matches. This class
 * selects the unique remaining winner of the requested type.
 */
public class NearestMaintenancePolicyResolver {

  private final ObjectPolicyResolver objectPolicyResolver;

  /**
   * @param entityStore relational entity store
   */
  public NearestMaintenancePolicyResolver(EntityStore entityStore) {
    this(new ObjectPolicyResolver(entityStore));
  }

  /**
   * @param objectPolicyResolver shared object-policy resolver
   */
  NearestMaintenancePolicyResolver(ObjectPolicyResolver objectPolicyResolver) {
    this.objectPolicyResolver = objectPolicyResolver;
  }

  /**
   * Resolves the unique effective policy id for {@code policyType}.
   *
   * @param metalake metalake name
   * @param table table metadata object
   * @param policyType maintenance built-in type
   * @return winner policy id, or empty when none match
   * @throws IllegalArgumentException when two or more same-type policies remain
   */
  public Optional<Long> resolveWinnerId(
      String metalake, MetadataObject table, Policy.BuiltInType policyType) {
    Match[] matches = objectPolicyResolver.resolveMatches(metalake, table);
    TreeSet<Long> winners = new TreeSet<>();
    for (Match match : matches) {
      if (match.policy().policyType() != policyType) {
        continue;
      }
      winners.add(match.policy().id());
    }
    if (winners.isEmpty()) {
      return Optional.empty();
    }
    if (winners.size() > 1) {
      throw new IllegalArgumentException(
          String.format(
              "ambiguous %s policies %s on metadata object %s", policyType, winners, table));
    }
    return Optional.of(winners.first());
  }
}
