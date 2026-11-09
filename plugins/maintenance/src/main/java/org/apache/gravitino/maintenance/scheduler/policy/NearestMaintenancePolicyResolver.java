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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.TreeSet;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.policy.ObjectPolicyResolver;
import org.apache.gravitino.policy.ObjectPolicyResolver.Match;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.utils.MetadataObjectUtil;

/**
 * Picks the nearest maintenance policy of a given {@link Policy.BuiltInType} for a table.
 *
 * <p>Matching and effective-tag inheritance come from {@link ObjectPolicyResolver}. This class only
 * applies maintenance-specific nearest-level and same-level conflict rules over the returned
 * metadata-object sources.
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
   * Resolves the unique nearest winner policy id for {@code policyType}.
   *
   * @param metalake metalake name
   * @param table table metadata object
   * @param policyType maintenance built-in type
   * @return winner policy id, or empty when none match
   * @throws IllegalArgumentException when two or more same-type policies share the nearest level
   */
  public Optional<Long> resolveWinnerId(
      String metalake, MetadataObject table, Policy.BuiltInType policyType) {
    Match[] matches = objectPolicyResolver.resolveMatches(metalake, table);
    List<MetadataObject> chain = new ArrayList<>();
    chain.add(table);
    chain.addAll(MetadataObjectUtil.getParentMetadataObjects(table));
    for (MetadataObject level : chain) {
      TreeSet<Long> atLevel = new TreeSet<>();
      for (Match match : matches) {
        if (match.policy().policyType() != policyType) {
          continue;
        }
        if (match.sources().contains(level)) {
          atLevel.add(match.policy().id());
        }
      }
      if (atLevel.isEmpty()) {
        continue;
      }
      if (atLevel.size() > 1) {
        throw new IllegalArgumentException(
            String.format(
                "ambiguous %s policies %s on nearest level %s", policyType, atLevel, level));
      }
      return Optional.of(atLevel.first());
    }
    return Optional.empty();
  }
}
