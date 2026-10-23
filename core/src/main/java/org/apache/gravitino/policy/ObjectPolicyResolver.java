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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
import org.apache.gravitino.json.PolicyAssociationSelectorSerde;
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
 * <p>The resolver evaluates relation selectors, filters disabled policies, and deduplicates
 * repeated matches by policy entity ID.
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
    List<NameIdentifier> tagIdentifiers =
        Arrays.stream(effectiveTags)
            .map(TagEntity::name)
            .distinct()
            .map(tagName -> NameIdentifierUtil.ofTag(metalake, tagName))
            .collect(Collectors.toList());
    if (tagIdentifiers.isEmpty()) {
      return new PolicyEntity[0];
    }

    return selectPolicies(effectiveTags, loadRelations(tagIdentifiers, metadataObject));
  }

  /**
   * Resolves effective policies for multiple objects using shared tag and policy reads.
   *
   * @param metalake the metalake name
   * @param metadataObjects the objects to resolve
   * @return policies keyed by requested object
   */
  public Map<MetadataObject, PolicyEntity[]> resolve(
      String metalake, List<MetadataObject> metadataObjects) {
    if (metadataObjects.isEmpty()) {
      return new LinkedHashMap<>();
    }
    Map<MetadataObject, TagEntity[]> tagsByObject =
        effectiveTagResolver.resolve(metalake, metadataObjects);
    Map<String, NameIdentifier> tagIdentifiers = new LinkedHashMap<>();
    tagsByObject
        .values()
        .forEach(
            tags -> {
              for (TagEntity tag : tags) {
                tagIdentifiers.putIfAbsent(
                    tag.name(), NameIdentifierUtil.ofTag(metalake, tag.name()));
              }
            });
    List<RelationalEntity<?>> relations =
        tagIdentifiers.isEmpty()
            ? Collections.emptyList()
            : loadRelations(new ArrayList<>(tagIdentifiers.values()), metadataObjects);
    Map<MetadataObject, PolicyEntity[]> result = new LinkedHashMap<>();
    tagsByObject.forEach((object, tags) -> result.put(object, selectPolicies(tags, relations)));
    return result;
  }

  private List<RelationalEntity<?>> loadRelations(
      List<NameIdentifier> tagIdentifiers, Object request) {

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
      LOG.error("Failed to resolve policies for metadata objects {}", request, e);
      throw new RuntimeException(e);
    }
    return relations;
  }

  private static PolicyEntity[] selectPolicies(
      TagEntity[] effectiveTags, List<RelationalEntity<?>> relations) {
    Map<String, TagEntity> tagsByName =
        Arrays.stream(effectiveTags)
            .collect(
                Collectors.toMap(
                    TagEntity::name, tag -> tag, (left, right) -> left, LinkedHashMap::new));

    Map<Long, MatchState> matchStates = new LinkedHashMap<>();
    for (RelationalEntity<?> relation : relations) {
      TagEntity tag = tagsByName.get(relation.source().name());
      if (tag == null) {
        continue;
      }
      PolicyEntity policy = (PolicyEntity) relation.targetEntity();
      PolicyAssociationSelector selector =
          relation
              .relationValue()
              .map(PolicyAssociationSelectorSerde::deserialize)
              .orElseGet(AllValuesSelector::get);
      TagAssignment assignment = tag.assignment().orElseGet(TagAssignment::noValue);
      boolean matches = matches(selector, assignment);
      MatchState state =
          matchStates.computeIfAbsent(policy.id(), ignored -> new MatchState(policy));
      state.record(matches, tag.inherited().orElse(false));
    }

    return matchStates.values().stream()
        .filter(MatchState::matched)
        .map(MatchState::policy)
        .filter(PolicyEntity::enabled)
        .toArray(PolicyEntity[]::new);
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

  private static final class MatchState {

    private final PolicyEntity policy;
    private boolean matched;
    private boolean directMatch;

    private MatchState(PolicyEntity policy) {
      this.policy = policy;
    }

    private void record(boolean matches, boolean inherited) {
      matched |= matches;
      directMatch |= matches && !inherited;
    }

    private boolean matched() {
      return matched;
    }

    private PolicyEntity policy() {
      return policy.copyWithInherited(!directMatch);
    }
  }
}
