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
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.RelationalEntity;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.catalog.TableDispatcher;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.json.PolicyAssociationSelectorSerde;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.tag.EffectiveTagResolver;
import org.apache.gravitino.tag.EffectiveTagResolver.ResolvedTag;
import org.apache.gravitino.tag.TagAssignment;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resolves policies for a metadata object from its effective tag assignments.
 *
 * <p>The resolver evaluates relation selectors, filters disabled policies, and deduplicates
 * repeated matches by policy entity ID while preserving contributing metadata-object sources.
 * Same-level conflicts for Iceberg maintenance policy types are rejected when associating tags or
 * policies. Callers also use {@link #requireCompatibleBinding} so each {@link PolicyContent} can
 * enforce object-scoped parameter constraints on the same write path.
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
    return policies(resolveMatches(metalake, metadataObject));
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
  public Match[] resolveMatches(String metalake, MetadataObject metadataObject) {
    ResolvedTag[] effectiveTags = effectiveTagResolver.resolveWithSources(metalake, metadataObject);
    if (effectiveTags.length == 0) {
      return new Match[0];
    }
    List<NameIdentifier> tagIdentifiers =
        Arrays.stream(effectiveTags)
            .map(resolvedTag -> resolvedTag.tag().name())
            .distinct()
            .map(tagName -> NameIdentifierUtil.ofTag(metalake, tagName))
            .collect(Collectors.toList());
    return selectMatches(effectiveTags, loadRelations(tagIdentifiers, metadataObject));
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
    Map<MetadataObject, ResolvedTag[]> tagsByObject =
        effectiveTagResolver.resolveWithSources(metalake, metadataObjects);
    Map<String, NameIdentifier> tagIdentifiers = new LinkedHashMap<>();
    tagsByObject
        .values()
        .forEach(
            tags -> {
              for (ResolvedTag resolvedTag : tags) {
                TagEntity tag = resolvedTag.tag();
                tagIdentifiers.putIfAbsent(
                    tag.name(), NameIdentifierUtil.ofTag(metalake, tag.name()));
              }
            });
    List<RelationalEntity<?>> relations =
        tagIdentifiers.isEmpty()
            ? Collections.emptyList()
            : loadRelations(new ArrayList<>(tagIdentifiers.values()), metadataObjects);
    Map<String, List<Integer>> relationPositionsByTag = new LinkedHashMap<>();
    for (int i = 0; i < relations.size(); i++) {
      relationPositionsByTag
          .computeIfAbsent(relations.get(i).source().name(), ignored -> new ArrayList<>())
          .add(i);
    }
    Map<MetadataObject, PolicyEntity[]> result = new LinkedHashMap<>();
    tagsByObject.forEach(
        (object, tags) -> {
          // Retain relation order while visiting only relations for this object's effective tags.
          List<RelationalEntity<?>> objectRelations =
              Arrays.stream(tags)
                  .map(resolvedTag -> resolvedTag.tag().name())
                  .distinct()
                  .flatMap(
                      name ->
                          relationPositionsByTag
                              .getOrDefault(name, Collections.emptyList())
                              .stream())
                  .sorted()
                  .map(relations::get)
                  .collect(Collectors.toList());
          result.put(object, policies(selectMatches(tags, objectRelations)));
        });
    return result;
  }

  /**
   * Ensures projected direct tag assignments on {@code metadataObject} would not yield two Iceberg
   * maintenance policies of the same type at that object level.
   *
   * <p>Unknown tag names are ignored, matching the tag-association store path.
   *
   * @param metalake metalake name
   * @param metadataObject object the tags would be assigned to
   * @param projectedAssignments tag name to the assignment that would be direct on the object
   * @throws IllegalArgumentException when a same-level same-type conflict would exist
   */
  public void checkDirectTagsUnique(
      String metalake,
      MetadataObject metadataObject,
      Map<String, TagAssignment> projectedAssignments) {
    if (projectedAssignments == null || projectedAssignments.isEmpty()) {
      return;
    }
    List<ResolvedTag> directTags = new ArrayList<>(projectedAssignments.size());
    List<NameIdentifier> tagIdentifiers = new ArrayList<>(projectedAssignments.size());
    for (Map.Entry<String, TagAssignment> entry : projectedAssignments.entrySet()) {
      NameIdentifier tagIdent = NameIdentifierUtil.ofTag(metalake, entry.getKey());
      try {
        TagEntity tag = entityStore.get(tagIdent, Entity.EntityType.TAG, TagEntity.class);
        directTags.add(new ResolvedTag(tag.copyWithAssignment(entry.getValue()), metadataObject));
        tagIdentifiers.add(tagIdent);
      } catch (NoSuchEntityException e) {
        // Matching the store: unknown tag names in an association request are ignored.
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    if (directTags.isEmpty()) {
      return;
    }
    Match[] matches =
        selectMatches(
            directTags.toArray(new ResolvedTag[0]), loadRelations(tagIdentifiers, metadataObject));
    Map<Policy.BuiltInType, List<Match>> byType = new LinkedHashMap<>();
    for (Match match : matches) {
      Policy.BuiltInType type = match.policy().policyType();
      if (type.uniqueAtSameLevel()) {
        byType.computeIfAbsent(type, ignored -> new ArrayList<>()).add(match);
      }
    }
    for (Map.Entry<Policy.BuiltInType, List<Match>> entry : byType.entrySet()) {
      TreeSet<String> names = new TreeSet<>();
      for (Match match : entry.getValue()) {
        if (match.sources().contains(metadataObject)) {
          names.add(match.policy().name());
        }
      }
      if (names.size() > 1) {
        throw new IllegalArgumentException(
            String.format(
                "Metadata object %s has conflicting %s policies at the same level: %s",
                metadataObject, entry.getKey().policyType(), names));
      }
    }
  }

  /**
   * Ensures {@code policy} would not share an Iceberg maintenance type with another policy already
   * effective on {@code metadataObject} through the given tag assignment and selector.
   *
   * @param metalake metalake name
   * @param metadataObject object that already has the tag
   * @param assignment current tag assignment on the object
   * @param policy policy that would become effective
   * @param selector policy-to-tag selector
   * @throws IllegalArgumentException when a same-level same-type conflict would exist
   */
  public void checkPolicyUnique(
      String metalake,
      MetadataObject metadataObject,
      TagAssignment assignment,
      PolicyEntity policy,
      PolicyAssociationSelector selector) {
    if (!policy.policyType().uniqueAtSameLevel() || !matches(selector, assignment)) {
      return;
    }
    TreeSet<String> names = new TreeSet<>();
    for (Match match : resolveMatches(metalake, metadataObject)) {
      if (match.policy().policyType() == policy.policyType()
          && match.sources().contains(metadataObject)) {
        names.add(match.policy().name());
      }
    }
    names.add(policy.name());
    if (names.size() > 1) {
      throw new IllegalArgumentException(
          String.format(
              "Metadata object %s has conflicting %s policies at the same level: %s",
              metadataObject, policy.policyType().policyType(), names));
    }
  }

  /**
   * Loads object properties when needed and delegates to {@link
   * PolicyContent#requireCompatibleBinding(MetadataObject.Type, Map)}. Callers walk the pending
   * policy↔tag↔object edges; content types without object-scoped constraints no-op.
   *
   * @param metalake metalake name
   * @param policyName policy name for error messages
   * @param tagName tag linking the policy to the object
   * @param content policy content being placed
   * @param metadataObject object that would receive the policy via the tag
   */
  public void requireCompatibleBinding(
      String metalake,
      String policyName,
      String tagName,
      PolicyContent content,
      MetadataObject metadataObject) {
    Map<String, String> objectProperties = Map.of();
    if (metadataObject.type() == MetadataObject.Type.TABLE) {
      objectProperties = loadTableProperties(metalake, metadataObject);
    }
    try {
      content.requireCompatibleBinding(metadataObject.type(), objectProperties);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          String.format(
              "Policy %s is associated with tag %s regarding %s %s: %s",
              policyName, tagName, metadataObject.type(), metadataObject, e.getMessage()),
          e);
    }
  }

  private Map<String, String> loadTableProperties(String metalake, MetadataObject tableObject) {
    TableDispatcher dispatcher = GravitinoEnv.getInstance().internalTableDispatcher();
    if (dispatcher == null) {
      dispatcher = GravitinoEnv.getInstance().tableDispatcher();
    }
    if (dispatcher == null) {
      throw new IllegalStateException("TableDispatcher is not initialized");
    }
    try {
      Table table = dispatcher.loadTable(MetadataObjectUtil.toEntityIdent(metalake, tableObject));
      Map<String, String> properties = table == null ? null : table.properties();
      return properties == null ? Map.of() : properties;
    } catch (NoSuchTableException e) {
      throw new IllegalArgumentException(
          String.format("Table %s does not exist while validating policy binding", tableObject), e);
    }
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

  private static PolicyEntity[] policies(Match[] matches) {
    return Arrays.stream(matches).map(Match::policy).toArray(PolicyEntity[]::new);
  }

  private static Match[] selectMatches(
      ResolvedTag[] effectiveTags, List<RelationalEntity<?>> relations) {
    Map<String, ResolvedTag> tagsByName =
        Arrays.stream(effectiveTags)
            .collect(
                Collectors.toMap(
                    resolvedTag -> resolvedTag.tag().name(),
                    resolvedTag -> resolvedTag,
                    (left, right) -> left,
                    LinkedHashMap::new));

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
        .toArray(Match[]::new);
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
     * Creates a policy match.
     *
     * @param policy The matched enabled policy (inherited flag already applied).
     * @param sources The contributing metadata objects.
     */
    public Match(PolicyEntity policy, List<MetadataObject> sources) {
      this.policy = Objects.requireNonNull(policy, "policy");
      this.sources =
          Collections.unmodifiableList(new ArrayList<>(Objects.requireNonNull(sources, "sources")));
    }

    /**
     * Returns the matched policy.
     *
     * @return The matched policy.
     */
    public PolicyEntity policy() {
      return policy;
    }

    /**
     * Returns metadata objects whose effective tag assignments caused this policy to match.
     *
     * <p>The list order follows policy-tag relation enumeration. Callers should test membership.
     *
     * @return The contributing metadata objects.
     */
    public List<MetadataObject> sources() {
      return sources;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Match)) {
        return false;
      }
      Match that = (Match) o;
      return Objects.equals(policy, that.policy) && Objects.equals(sources, that.sources);
    }

    @Override
    public int hashCode() {
      return Objects.hash(policy, sources);
    }

    @Override
    public String toString() {
      return "Match{policy=" + policy + ", sources=" + sources + '}';
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
