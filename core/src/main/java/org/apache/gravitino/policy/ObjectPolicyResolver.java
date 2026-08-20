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
package org.apache.gravitino.policy;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.RelationalEntity;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.tag.EffectiveTagResolver;
import org.apache.gravitino.tag.TagAssignment;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resolves policies for a metadata object from its effective tag assignments.
 *
 * <p>The resolver evaluates relation selectors, rejects mixed match results for the same policy,
 * filters disabled policies, and deduplicates repeated matches by policy entity ID.
 */
public class ObjectPolicyResolver {

  private static final Logger LOG = LoggerFactory.getLogger(ObjectPolicyResolver.class);

  private final EntityStore entityStore;
  private final EffectiveTagResolver effectiveTagResolver;

  /**
   * Creates an object policy resolver.
   *
   * @param entityStore The entity store used to read policy-to-tag relations.
   */
  public ObjectPolicyResolver(EntityStore entityStore) {
    this(entityStore, new EffectiveTagResolver(entityStore));
  }

  ObjectPolicyResolver(EntityStore entityStore, EffectiveTagResolver effectiveTagResolver) {
    this.entityStore = entityStore;
    this.effectiveTagResolver = effectiveTagResolver;
  }

  /**
   * Resolves enabled policies for a metadata object.
   *
   * @param metalake The metalake name.
   * @param metadataObject The metadata object.
   * @return Enabled policies selected by the object's effective tags.
   */
  public PolicyEntity[] resolve(String metalake, MetadataObject metadataObject) {
    TagEntity[] effectiveTags = effectiveTagResolver.resolve(metalake, metadataObject);
    if (effectiveTags.length == 0) {
      return new PolicyEntity[0];
    }

    Map<String, TagEntity> tagsByName =
        Arrays.stream(effectiveTags)
            .collect(
                Collectors.toMap(
                    TagEntity::name, tag -> tag, (left, right) -> left, LinkedHashMap::new));
    List<NameIdentifier> tagIdentifiers =
        tagsByName.keySet().stream()
            .map(tagName -> NameIdentifierUtil.ofTag(metalake, tagName))
            .collect(Collectors.toList());

    List<RelationalEntity<?>> relations;
    try {
      relations =
          entityStore
              .relationOperations()
              .batchListEntitiesByRelation(
                  SupportsRelationOperations.Type.POLICY_TAG_REL,
                  tagIdentifiers,
                  Entity.EntityType.TAG);
    } catch (IOException e) {
      LOG.error("Failed to resolve policies for metadata object {}", metadataObject, e);
      throw new RuntimeException(e);
    }

    Map<Long, MatchState> matchStates = new LinkedHashMap<>();
    for (RelationalEntity<?> relation : relations) {
      TagEntity tag = tagsByName.get(relation.source().name());
      if (tag == null) {
        continue;
      }
      PolicyEntity policy = (PolicyEntity) relation.targetEntity();
      PolicyTagSelector selector =
          PolicyTagSelectorSerde.deserialize(relation.relationValue().orElse(null));
      TagAssignment assignment = tag.assignment().orElseGet(TagAssignment::noValue);
      boolean matches = selector == null || selector.matches(assignment);
      MatchState state =
          matchStates.computeIfAbsent(policy.id(), ignored -> new MatchState(policy));
      state.record(matches);
      if (state.hasConflict()) {
        throw new IllegalStateException(
            String.format(
                "Policy %s has conflicting selector results for metadata object %s",
                policy.name(), metadataObject));
      }
    }

    return matchStates.values().stream()
        .filter(MatchState::matched)
        .map(MatchState::policy)
        .filter(PolicyEntity::enabled)
        .toArray(PolicyEntity[]::new);
  }

  private static final class MatchState {
    private final PolicyEntity policy;
    private boolean matched;
    private boolean unmatched;

    private MatchState(PolicyEntity policy) {
      this.policy = policy;
    }

    private void record(boolean matches) {
      matched |= matches;
      unmatched |= !matches;
    }

    private boolean hasConflict() {
      return matched && unmatched;
    }

    private boolean matched() {
      return matched;
    }

    private PolicyEntity policy() {
      return policy;
    }
  }
}
