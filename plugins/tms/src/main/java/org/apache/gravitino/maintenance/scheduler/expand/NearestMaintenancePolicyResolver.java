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
package org.apache.gravitino.maintenance.scheduler.expand;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.TreeSet;
import javax.annotation.Nullable;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.policy.MatchedPolicy;
import org.apache.gravitino.policy.ObjectPolicyResolver;
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
   * @param metalake metalake name
   * @param table table metadata object
   * @param policyType maintenance built-in type
   * @return winner, none, or same-level conflict
   */
  public Decision resolve(String metalake, MetadataObject table, Policy.BuiltInType policyType) {
    List<MatchedPolicy> matches = objectPolicyResolver.resolveMatches(metalake, table);
    List<MetadataObject> chain = new ArrayList<>();
    chain.add(table);
    chain.addAll(MetadataObjectUtil.getParentMetadataObjects(table));
    for (MetadataObject level : chain) {
      TreeSet<Long> atLevel = new TreeSet<>();
      for (MatchedPolicy match : matches) {
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
        return Decision.conflict(level, new ArrayList<>(atLevel));
      }
      return Decision.winner(atLevel.first(), level);
    }
    return Decision.none();
  }

  /** Result of nearest maintenance-policy resolution for one table and built-in type. */
  public static final class Decision {

    /** Outcome of nearest-policy resolution. */
    public enum Kind {
      /** Exactly one matching policy on the nearest non-empty level. */
      WINNER,
      /** No matching policy on the inheritance chain. */
      NONE,
      /** Two or more same-type policies on the nearest non-empty level. */
      CONFLICT
    }

    private final Kind kind;
    @Nullable private final Long winnerPolicyId;
    @Nullable private final MetadataObject winnerLevel;
    @Nullable private final MetadataObject conflictLevel;
    private final List<Long> conflictingPolicyIds;

    private Decision(
        Kind kind,
        @Nullable Long winnerPolicyId,
        @Nullable MetadataObject winnerLevel,
        @Nullable MetadataObject conflictLevel,
        List<Long> conflictingPolicyIds) {
      this.kind = kind;
      this.winnerPolicyId = winnerPolicyId;
      this.winnerLevel = winnerLevel;
      this.conflictLevel = conflictLevel;
      this.conflictingPolicyIds = conflictingPolicyIds;
    }

    /**
     * @param policyId winning policy id
     * @param level metadata level that produced the winner
     * @return winner decision
     */
    public static Decision winner(long policyId, MetadataObject level) {
      return new Decision(Kind.WINNER, policyId, level, null, List.of());
    }

    /**
     * @return none decision
     */
    public static Decision none() {
      return new Decision(Kind.NONE, null, null, null, List.of());
    }

    /**
     * @param level metadata level that produced the conflict
     * @param policyIds conflicting policy ids
     * @return conflict decision
     */
    public static Decision conflict(MetadataObject level, List<Long> policyIds) {
      return new Decision(Kind.CONFLICT, null, null, level, List.copyOf(policyIds));
    }

    /**
     * @return decision kind
     */
    public Kind kind() {
      return kind;
    }

    /**
     * @return winning policy id when {@link Kind#WINNER}
     */
    public Optional<Long> winnerPolicyId() {
      return Optional.ofNullable(winnerPolicyId);
    }

    /**
     * @return metadata level that produced the winner, or {@code null}
     */
    @Nullable
    public MetadataObject winnerLevel() {
      return winnerLevel;
    }

    /**
     * @return metadata level that produced the conflict, or {@code null}
     */
    @Nullable
    public MetadataObject conflictLevel() {
      return conflictLevel;
    }

    /**
     * @return conflicting policy ids when {@link Kind#CONFLICT}
     */
    public List<Long> conflictingPolicyIds() {
      return conflictingPolicyIds;
    }
  }
}
