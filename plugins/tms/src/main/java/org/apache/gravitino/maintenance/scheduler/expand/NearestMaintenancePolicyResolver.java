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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.RelationalEntity;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.json.PolicyAssociationSelectorSerde;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.policy.AllValuesSelector;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyAssociationSelector;
import org.apache.gravitino.policy.TagValueSelector;
import org.apache.gravitino.tag.TagAssignment;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Picks the nearest maintenance policy of a given {@link Policy.BuiltInType} for a table.
 *
 * <p>Walks the table then its ancestors (nearest first). At each level, policies selected by tags
 * <em>directly</em> assigned to that object are considered. The first non-empty level decides: one
 * match is the winner; two or more same-type matches are a conflict (fail-closed).
 */
public final class NearestMaintenancePolicyResolver {

  private static final Logger LOG = LoggerFactory.getLogger(NearestMaintenancePolicyResolver.class);

  private final EntityStore entityStore;

  /**
   * @param entityStore relational entity store
   */
  public NearestMaintenancePolicyResolver(EntityStore entityStore) {
    this.entityStore = entityStore;
  }

  /**
   * @param metalake metalake name
   * @param table table metadata object
   * @param policyType maintenance built-in type
   * @return winner, none, or same-level conflict
   */
  public Decision resolve(String metalake, MetadataObject table, Policy.BuiltInType policyType) {
    List<MetadataObject> chain = new ArrayList<>();
    chain.add(table);
    chain.addAll(MetadataObjectUtil.getParentMetadataObjects(table));
    for (MetadataObject object : chain) {
      TreeSet<Long> atLevel = matchingPolicyIds(metalake, object, policyType);
      if (atLevel.isEmpty()) {
        continue;
      }
      if (atLevel.size() > 1) {
        return Decision.conflict(object, new ArrayList<>(atLevel));
      }
      return Decision.winner(atLevel.first(), object);
    }
    return Decision.none();
  }

  private TreeSet<Long> matchingPolicyIds(
      String metalake, MetadataObject object, Policy.BuiltInType policyType) {
    NameIdentifier objectIdent = MetadataObjectUtil.toEntityIdent(metalake, object);
    Entity.EntityType objectType = MetadataObjectUtil.toEntityType(object);
    List<TagEntity> tags;
    try {
      tags =
          entityStore
              .relationOperations()
              .listEntitiesByRelation(
                  SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL, objectIdent, objectType);
    } catch (NoSuchEntityException e) {
      return new TreeSet<>();
    } catch (IOException e) {
      throw new UncheckedIOException(
          "Failed to list tags for metadata object " + object.fullName(), e);
    }
    if (tags.isEmpty()) {
      return new TreeSet<>();
    }

    Map<String, TagEntity> tagsByName =
        tags.stream()
            .collect(
                Collectors.toMap(
                    TagEntity::name, tag -> tag, (left, right) -> left, LinkedHashMap::new));
    List<NameIdentifier> tagIdents =
        tagsByName.keySet().stream()
            .map(name -> NameIdentifierUtil.ofTag(metalake, name))
            .collect(Collectors.toList());

    List<RelationalEntity<?>> relations;
    try {
      relations =
          entityStore
              .relationOperations()
              .batchListEntitiesByRelation(
                  SupportsRelationOperations.Type.POLICY_TAG_REL, tagIdents, Entity.EntityType.TAG);
    } catch (IOException e) {
      LOG.error("Failed to list policies for tags on {}", object.fullName(), e);
      throw new UncheckedIOException(e);
    }

    TreeSet<Long> matched = new TreeSet<>();
    for (RelationalEntity<?> relation : relations) {
      TagEntity tag = tagsByName.get(relation.source().name());
      if (tag == null) {
        continue;
      }
      Object target = relation.targetEntity();
      if (!(target instanceof PolicyEntity)) {
        continue;
      }
      PolicyEntity policy = (PolicyEntity) target;
      if (!policy.enabled() || policy.policyType() != policyType) {
        continue;
      }
      PolicyAssociationSelector selector =
          relation
              .relationValue()
              .map(PolicyAssociationSelectorSerde::deserialize)
              .orElseGet(AllValuesSelector::get);
      TagAssignment assignment = tag.assignment().orElseGet(TagAssignment::noValue);
      if (matches(selector, assignment)) {
        matched.add(policy.id());
      }
    }
    return matched;
  }

  private static boolean matches(PolicyAssociationSelector selector, TagAssignment assignment) {
    if (selector instanceof AllValuesSelector) {
      return true;
    }
    if (selector instanceof TagValueSelector) {
      String expectedValue = ((TagValueSelector) selector).value();
      return Arrays.asList(assignment.values()).contains(expectedValue);
    }
    throw new IllegalArgumentException(
        "Unsupported policy association selector type: " + selector.type());
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

    static Decision winner(long policyId, MetadataObject level) {
      return new Decision(Kind.WINNER, policyId, level, null, List.of());
    }

    static Decision none() {
      return new Decision(Kind.NONE, null, null, null, List.of());
    }

    static Decision conflict(MetadataObject level, List<Long> policyIds) {
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
