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
import org.apache.gravitino.json.PolicyAssociationSelectorSerde;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.tag.EffectiveTag;
import org.apache.gravitino.tag.EffectiveTagResolver;
import org.apache.gravitino.tag.TagAssignment;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resolves policies for a metadata object from its effective tag assignments.
 *
 * <p>The resolver evaluates relation selectors, filters disabled policies, and deduplicates
 * repeated matches by policy entity ID while preserving contributing metadata-object sources.
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
    return resolveMatches(metalake, metadataObject).stream()
        .map(MatchedPolicy::policy)
        .toArray(PolicyEntity[]::new);
  }

  /**
   * Resolves enabled policies together with the metadata-object sources that caused each match.
   *
   * <p>{@link #resolve(String, MetadataObject)} stays the PolicyManager list shape; callers that
   * need nearest-level or multi-source reasoning should use this method.
   *
   * @param metalake The metalake name.
   * @param metadataObject The metadata object.
   * @return Matched enabled policies with contributing sources.
   */
  public List<MatchedPolicy> resolveMatches(String metalake, MetadataObject metadataObject) {
    List<EffectiveTag> effectiveTags =
        effectiveTagResolver.resolveWithSources(metalake, metadataObject);
    if (effectiveTags.isEmpty()) {
      return List.of();
    }

    Map<String, EffectiveTag> tagsByName =
        effectiveTags.stream()
            .collect(
                Collectors.toMap(
                    effectiveTag -> effectiveTag.tag().name(),
                    effectiveTag -> effectiveTag,
                    (left, right) -> left,
                    LinkedHashMap::new));
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

    Map<Long, MatchedPolicy.Builder> matchStates = new LinkedHashMap<>();
    for (RelationalEntity<?> relation : relations) {
      EffectiveTag effectiveTag = tagsByName.get(relation.source().name());
      if (effectiveTag == null) {
        continue;
      }
      TagEntity tag = effectiveTag.tag();
      PolicyEntity policy = (PolicyEntity) relation.targetEntity();
      PolicyAssociationSelector selector =
          relation
              .relationValue()
              .map(PolicyAssociationSelectorSerde::deserialize)
              .orElseGet(AllValuesSelector::get);
      TagAssignment assignment = tag.assignment().orElseGet(TagAssignment::noValue);
      boolean matches = matches(selector, assignment);
      MatchedPolicy.Builder state =
          matchStates.computeIfAbsent(policy.id(), ignored -> new MatchedPolicy.Builder(policy));
      state.record(matches, tag.inherited().orElse(false), effectiveTag.source());
    }

    return matchStates.values().stream()
        .filter(MatchedPolicy.Builder::matched)
        .map(MatchedPolicy.Builder::build)
        .filter(match -> match.policy().enabled())
        .collect(Collectors.toList());
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
}
