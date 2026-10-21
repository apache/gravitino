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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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
import org.apache.gravitino.tag.EffectiveTagResolver.ResolvedTag;
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
        .map(Match::policy)
        .toArray(PolicyEntity[]::new);
  }

  /**
   * Resolves enabled policies together with the metadata-object sources that caused each match.
   *
   * <p>{@link #resolve(String, MetadataObject)} stays the PolicyManager list shape; callers that
   * need contributing sources should use this method.
   *
   * @param metalake The metalake name.
   * @param metadataObject The metadata object.
   * @return Matched enabled policies with contributing sources.
   */
  public List<Match> resolveMatches(String metalake, MetadataObject metadataObject) {
    List<ResolvedTag> effectiveTags =
        effectiveTagResolver.resolveWithSources(metalake, metadataObject);
    if (effectiveTags.isEmpty()) {
      return List.of();
    }

    Map<String, ResolvedTag> tagsByName =
        effectiveTags.stream()
            .collect(
                Collectors.toMap(
                    resolvedTag -> resolvedTag.tag().name(),
                    resolvedTag -> resolvedTag,
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

    Map<Long, MatchState> matchStates = new LinkedHashMap<>();
    for (RelationalEntity<?> relation : relations) {
      ResolvedTag resolvedTag = tagsByName.get(relation.source().name());
      if (resolvedTag == null) {
        continue;
      }
      TagEntity tag = resolvedTag.tag();
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
      state.record(matches, tag.inherited().orElse(false), resolvedTag.source());
    }

    return matchStates.values().stream()
        .filter(MatchState::matched)
        .map(MatchState::toMatch)
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

  /**
   * An enabled policy matched for a metadata object, together with the metadata-object sources that
   * contributed matching effective tags.
   */
  public static final class Match {

    private final PolicyEntity policy;
    private final List<MetadataObject> sources;

    /**
     * @param policy matched enabled policy (inherited flag already applied)
     * @param sources contributing metadata objects, nearest-first encounter order
     */
    public Match(PolicyEntity policy, List<MetadataObject> sources) {
      this.policy = Objects.requireNonNull(policy, "policy");
      this.sources =
          Collections.unmodifiableList(new ArrayList<>(Objects.requireNonNull(sources, "sources")));
    }

    /**
     * @return the matched policy
     */
    public PolicyEntity policy() {
      return policy;
    }

    /**
     * @return metadata objects whose effective tag assignments caused this policy to match
     */
    public List<MetadataObject> sources() {
      return sources;
    }
  }

  private static final class MatchState {

    private final PolicyEntity policy;
    private final Set<MetadataObject> sources = new LinkedHashSet<>();
    private boolean directMatch;

    private MatchState(PolicyEntity policy) {
      this.policy = policy;
    }

    private void record(boolean matches, boolean inherited, MetadataObject source) {
      if (!matches) {
        return;
      }
      sources.add(source);
      directMatch |= !inherited;
    }

    private boolean matched() {
      return !sources.isEmpty();
    }

    private Match toMatch() {
      return new Match(policy.copyWithInherited(!directMatch), new ArrayList<>(sources));
    }
  }
}
