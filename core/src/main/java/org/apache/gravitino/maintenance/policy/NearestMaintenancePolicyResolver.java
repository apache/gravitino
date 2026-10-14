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
package org.apache.gravitino.maintenance.policy;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyDispatcher;
import org.apache.gravitino.utils.MetadataObjectUtil;

/**
 * Selects attached maintenance policies of a given built-in type for a metadata object.
 *
 * <p>Resolution walks the metadata object and its ancestors from nearest to outermost (for a table:
 * table → schema → catalog). At each level, only policies attached on that object are considered
 * ({@code inherited == false} from {@link PolicyDispatcher#listPolicyInfosForMetadataObject}).
 * Among matches at the same level, the policy with the lexicographically smallest name wins.
 *
 * <p>{@link #resolveNearest} returns the first match in that walk. {@link #resolveDirectChain}
 * returns every level that has a direct match so callers can merge {@code jobOptions} / {@code
 * updateOptions} across the hierarchy.
 */
public class NearestMaintenancePolicyResolver {

  private final PolicyDispatcher policyDispatcher;

  /**
   * Creates a resolver.
   *
   * @param policyDispatcher policy dispatcher
   */
  public NearestMaintenancePolicyResolver(PolicyDispatcher policyDispatcher) {
    this.policyDispatcher = Preconditions.checkNotNull(policyDispatcher, "policyDispatcher");
  }

  /**
   * Returns the nearest enabled policy entity for the built-in maintenance policy type.
   *
   * @param metalake metalake name
   * @param metadataObject metadata object (typically a table)
   * @param builtInType built-in maintenance policy type
   * @return matching policy when found
   */
  public Optional<PolicyEntity> resolveNearest(
      String metalake, MetadataObject metadataObject, Policy.BuiltInType builtInType) {
    List<PolicyEntity> chain = resolveDirectChain(metalake, metadataObject, builtInType);
    return chain.isEmpty() ? Optional.empty() : Optional.of(chain.get(0));
  }

  /**
   * Returns direct matching policies from nearest to outermost (at most one per level).
   *
   * <p>Used when merging {@code jobOptions} / {@code updateOptions} across the hierarchy while
   * triggers and {@code minIntervalMs} still come from the nearest policy only.
   *
   * @param metalake metalake name
   * @param metadataObject metadata object (typically a table)
   * @param builtInType built-in maintenance policy type
   * @return nearest-first chain of direct matches
   */
  public List<PolicyEntity> resolveDirectChain(
      String metalake, MetadataObject metadataObject, Policy.BuiltInType builtInType) {
    Preconditions.checkNotNull(metalake, "metalake cannot be null");
    MetadataObjectUtil.checkMetadataObject(metalake, metadataObject);
    Preconditions.checkNotNull(builtInType, "builtInType cannot be null");
    Preconditions.checkArgument(
        builtInType != Policy.BuiltInType.CUSTOM, "builtInType must be a built-in type");

    List<PolicyEntity> chain = new ArrayList<>();
    for (MetadataObject level : resolutionOrder(metadataObject)) {
      firstDirectMatch(metalake, level, builtInType).ifPresent(chain::add);
    }
    return chain;
  }

  private Optional<PolicyEntity> firstDirectMatch(
      String metalake, MetadataObject level, Policy.BuiltInType builtInType) {
    PolicyEntity[] effective = policyDispatcher.listPolicyInfosForMetadataObject(metalake, level);
    List<PolicyEntity> matches = new ArrayList<>();
    for (PolicyEntity policy : effective) {
      if (matchesType(policy, builtInType) && !policy.inherited().orElse(false)) {
        matches.add(policy);
      }
    }
    if (matches.isEmpty()) {
      return Optional.empty();
    }
    matches.sort(Comparator.comparing(PolicyEntity::name));
    return Optional.of(matches.get(0));
  }

  private static List<MetadataObject> resolutionOrder(MetadataObject metadataObject) {
    List<MetadataObject> order = new ArrayList<>();
    order.add(metadataObject);
    order.addAll(MetadataObjectUtil.getParentMetadataObjects(metadataObject));
    return order;
  }

  private static boolean matchesType(PolicyEntity policy, Policy.BuiltInType builtInType) {
    return policy.enabled() && policy.policyType() == builtInType;
  }
}
