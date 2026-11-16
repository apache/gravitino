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
package org.apache.gravitino.domain;

import static org.apache.gravitino.SupportsDomainOperations.JOINED_DOMAIN_BATCH_SIZE;
import static org.apache.gravitino.SupportsDomainOperations.MEMBERSHIP_LOOKUP_BATCH_SIZE;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.meta.DomainEntity;
import org.apache.gravitino.meta.FilesetEntity;
import org.apache.gravitino.meta.GenericEntity;
import org.apache.gravitino.meta.ModelEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.SemanticModelEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.meta.TopicEntity;
import org.apache.gravitino.meta.ViewEntity;
import org.apache.gravitino.tag.EffectiveTagResolver;
import org.apache.gravitino.tag.EffectiveTagResolver.ResolvedTag;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Resolves domain scope from effective tag assignments without authorization filtering. */
final class DomainMembershipResolver {
  private static final Logger LOG = LoggerFactory.getLogger(DomainMembershipResolver.class);
  private static final Set<MetadataObject.Type> DIRECT_MEMBER_TYPES =
      Sets.immutableEnumSet(Sets.union(DomainMember.SUPPORTED_TYPES, DomainMember.CONTAINER_TYPES));

  private final EntityStore store;
  private final EffectiveTagResolver tags;

  DomainMembershipResolver(EntityStore store) {
    this.store = store;
    this.tags = new EffectiveTagResolver(store);
  }

  DomainMember[] listMembers(
      String metalake,
      DomainEntity queriedDomain,
      DomainEntity[] subdomains,
      DomainMemberFilter filter) {
    Objects.requireNonNull(filter, "filter");

    // Domain filters select membership paths; relation reporting still uses the full scope.
    List<DomainEntity> scopeDomains = collectScopeDomains(queriedDomain, subdomains);
    List<DomainEntity> selectedDomains =
        scopeDomains.stream()
            .filter(
                d ->
                    filter.domains().isEmpty()
                        || filter.domains().contains(queriedDomain.name())
                        || filter.domains().contains(d.name()))
            .collect(Collectors.toList());

    List<DomainMember> members =
        filter.expand()
            ? listExpandedMembers(metalake, queriedDomain, scopeDomains, selectedDomains, filter)
            : listDirectMembers(metalake, queriedDomain, scopeDomains, selectedDomains, filter);

    members.sort(
        Comparator.comparing((DomainMember m) -> m.object().type().name())
            .thenComparing(m -> m.object().fullName()));
    return members.toArray(new DomainMember[0]);
  }

  /**
   * Lists the objects that carry a backing tag themselves, including catalogs and schemas, without
   * expanding them or resolving effective tags.
   */
  private List<DomainMember> listDirectMembers(
      String metalake,
      DomainEntity queriedDomain,
      List<DomainEntity> scopeDomains,
      List<DomainEntity> selectedDomains,
      DomainMemberFilter filter) {
    Set<Long> selectedIds =
        selectedDomains.stream().map(DomainEntity::id).collect(Collectors.toSet());
    boolean needsOtherBindings =
        scopeDomains.stream()
            .anyMatch(d -> d.backingTagId() != null && !selectedIds.contains(d.id()));
    // A selective name filter can discard most candidates after name resolution. In that case
    // only fetch additional paths for the remaining objects. Bound the IN list for large scopes.
    boolean joinPaths =
        needsOtherBindings
            && !selectedIds.isEmpty()
            && selectedIds.size() <= MEMBERSHIP_LOOKUP_BATCH_SIZE
            && !filter.nameContains().isPresent();
    Map<MetadataObject.Type, Map<Long, MetadataObject>> candidates =
        new EnumMap<>(MetadataObject.Type.class);
    Table<MetadataObject.Type, Long, DirectMemberPaths> relations = HashBasedTable.create();
    List<DomainMember> members = new ArrayList<>();
    try {
      Set<MetadataObject.Type> types =
          filter.types().isEmpty() ? DIRECT_MEMBER_TYPES : filter.types();
      List<DomainEntity> lookupDomains = selectedDomains;
      Map<Long, List<GenericEntity>> assigned =
          listAssignments(metalake, selectedDomains, types, Collections.emptyMap());
      if (assigned.values().stream().allMatch(List::isEmpty)) {
        return members;
      }
      if (joinPaths) {
        List<DomainEntity> remaining =
            scopeDomains.stream()
                .filter(d -> d.backingTagId() != null && !selectedIds.contains(d.id()))
                .collect(Collectors.toList());
        joinPaths = fewerDomainBatches(assigned, remaining.size());
        if (joinPaths) {
          Map<Long, List<GenericEntity>> otherAssignments =
              listAssignments(metalake, remaining, types, assigned);
          assigned = new HashMap<>(assigned);
          assigned.putAll(otherAssignments);
          lookupDomains = scopeDomains;
        }
      }
      for (DomainEntity domain : lookupDomains) {
        if (domain.backingTagId() == null) {
          continue;
        }

        DomainRelation relation =
            Objects.equals(queriedDomain.id(), domain.id())
                ? DomainRelation.direct()
                : DomainRelation.viaSubdomain(domain.name());
        for (GenericEntity assignment :
            assigned.getOrDefault(domain.id(), Collections.emptyList())) {
          MetadataObject object = assignedObject(assignment);
          if (DIRECT_MEMBER_TYPES.contains(object.type())) {
            if (needsOtherBindings && !joinPaths) {
              if (!matchesFilter(object, filter)) {
                continue;
              }
              candidates
                  .computeIfAbsent(object.type(), t -> new LinkedHashMap<>())
                  .put(assignment.id(), object);
            } else {
              DirectMemberPaths paths =
                  relations
                      .row(object.type())
                      .computeIfAbsent(assignment.id(), ignored -> new DirectMemberPaths());
              paths.relations.add(relation);
              if (paths.selectedObject == null && selectedIds.contains(domain.id())) {
                paths.selectedObject = object;
              }
            }
          }
        }
      }

      for (DirectMemberPaths paths : relations.values()) {
        if (paths.selectedObject != null && matchesFilter(paths.selectedObject, filter)) {
          members.add(
              new DomainMemberInfo(
                  paths.selectedObject, paths.relations.toArray(new DomainRelation[0])));
        }
      }
      if (candidates.isEmpty()) {
        return members;
      }
      BindingIndex bindings = new BindingIndex(scopeDomains, selectedIds);
      for (Map.Entry<MetadataObject.Type, Map<Long, MetadataObject>> group :
          candidates.entrySet()) {
        Map<Long, MetadataObject> objects = group.getValue();
        Map<Long, List<TagEntity>> directTags =
            store
                .domainOperations()
                .listDirectTagsByObjectIds(
                    metalake, group.getKey(), new ArrayList<>(objects.keySet()));
        directTags.forEach(
            (id, tags) -> {
              MetadataObject object = objects.get(id);
              ResolvedTag[] assignments =
                  tags.stream()
                      .map(tag -> new ResolvedTag(tag, object))
                      .toArray(ResolvedTag[]::new);
              bindings.addMember(members, object, assignments);
            });
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to list members of domain " + queriedDomain.name(), e);
    }

    return members;
  }

  /**
   * Expands catalogs and schemas that carry a backing tag into the recorded leaf objects under
   * them, and keeps the leaf objects whose effective tags make them members.
   */
  private List<DomainMember> listExpandedMembers(
      String metalake,
      DomainEntity queriedDomain,
      List<DomainEntity> scopeDomains,
      List<DomainEntity> selectedDomains,
      DomainMemberFilter filter) {
    // Expand explicit tag assignments into recorded leaf objects that may inherit the tag.
    Set<MetadataObject> candidates = new LinkedHashSet<>();
    try {
      Set<MetadataObject.Type> types =
          filter.types().isEmpty()
              ? DIRECT_MEMBER_TYPES
              : Sets.union(filter.types(), DomainMember.CONTAINER_TYPES);
      Map<Long, List<GenericEntity>> assigned =
          listAssignments(metalake, selectedDomains, types, Collections.emptyMap());
      // A container assigned to several selected domains needs to be expanded only once.
      Set<MetadataObject> roots = new LinkedHashSet<>();
      assigned
          .values()
          .forEach(objects -> objects.forEach(entity -> roots.add(assignedObject(entity))));
      for (MetadataObject root : roots) {
        collectCandidates(metalake, root, filter, candidates);
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to list members of domain " + queriedDomain.name(), e);
    }

    // A nearer assignment can override an ancestor tag, so candidates need effective-tag checks.
    List<DomainMember> members = new ArrayList<>();
    BindingIndex bindings =
        new BindingIndex(
            scopeDomains,
            selectedDomains.stream().map(DomainEntity::id).collect(Collectors.toSet()));
    for (List<MetadataObject> batch : Lists.partition(new ArrayList<>(candidates), 500)) {
      if (batch.size() > 1) {
        try {
          tags.resolveWithSources(metalake, batch)
              .forEach((object, effective) -> bindings.addMember(members, object, effective));
          continue;
        } catch (NoSuchMetadataObjectException e) {
          // Retry this batch individually if a candidate disappeared during enumeration.
        }
      }
      for (MetadataObject object : batch) {
        try {
          bindings.addMember(members, object, tags.resolveWithSources(metalake, object));
        } catch (NoSuchMetadataObjectException e) {
          LOG.debug("Skipping missing member candidate {} in metalake {}", object, metalake, e);
        }
      }
    }
    return members;
  }

  DomainEntity[] listDomains(String metalake, MetadataObject object) {
    Preconditions.checkArgument(
        object != null && DomainMember.SUPPORTED_TYPES.contains(object.type()),
        "Only supported leaf objects can belong to domains");

    // Restrict the indexed domain lookup to tags effective on this object.
    ResolvedTag[] effectiveTags = tags.resolveWithSources(metalake, object);
    List<Long> effectiveTagIds =
        Arrays.stream(effectiveTags)
            .map(tag -> tag.tag().id())
            .distinct()
            .collect(Collectors.toList());
    if (effectiveTagIds.isEmpty()) {
      return new DomainEntity[0];
    }

    List<DomainEntity> boundDomains;
    try {
      boundDomains = store.domainOperations().listDomainsByBackingTagIds(metalake, effectiveTagIds);
    } catch (IOException e) {
      throw new RuntimeException("Failed to list domains for metadata object " + object, e);
    }

    // Tag IDs narrow the candidates; exact value matching determines actual membership.
    Map<Long, DomainEntity> matchingDomains = new LinkedHashMap<>();
    Map<Long, Set<DomainRelation>> domainRelations = new LinkedHashMap<>();
    for (DomainEntity boundDomain : boundDomains) {
      DomainRelation own = resolveOwnRelation(boundDomain, object, effectiveTags);
      if (own != null) {
        matchingDomains.put(boundDomain.id(), boundDomain);
        domainRelations.computeIfAbsent(boundDomain.id(), id -> new LinkedHashSet<>()).add(own);
      }
    }

    // Load only parents of matching subdomains and preserve any own relation they already have.
    List<NameIdentifier> parentIdentifiers =
        matchingDomains.values().stream()
            .map(DomainEntity::parent)
            .filter(parent -> parent.isPresent())
            .map(parent -> NameIdentifierUtil.ofDomain(metalake, parent.get()))
            .distinct()
            .collect(Collectors.toList());
    if (!parentIdentifiers.isEmpty()) {
      Map<String, DomainEntity> parentDomains =
          store.batchGet(parentIdentifiers, Entity.EntityType.DOMAIN, DomainEntity.class).stream()
              .collect(Collectors.toMap(DomainEntity::name, parent -> parent));

      List<DomainEntity> matchingSubdomains = new ArrayList<>(matchingDomains.values());
      matchingSubdomains.sort(Comparator.comparing(DomainEntity::name));
      for (DomainEntity subdomain : matchingSubdomains) {
        subdomain
            .parent()
            .map(parentDomains::get)
            .ifPresent(
                parent -> {
                  matchingDomains.putIfAbsent(parent.id(), parent);
                  domainRelations
                      .computeIfAbsent(parent.id(), id -> new LinkedHashSet<>())
                      .add(DomainRelation.viaSubdomain(subdomain.name()));
                });
      }
    }

    return matchingDomains.values().stream()
        .sorted(Comparator.comparing(DomainEntity::name))
        .map(
            domain ->
                domain.toBuilder()
                    .withRelations(domainRelations.get(domain.id()).toArray(new DomainRelation[0]))
                    .build())
        .toArray(DomainEntity[]::new);
  }

  private static List<DomainEntity> collectScopeDomains(
      DomainEntity queriedDomain, DomainEntity[] subdomains) {
    List<DomainEntity> scopeDomains = new ArrayList<>();
    scopeDomains.add(queriedDomain);
    if (!queriedDomain.parent().isPresent()) {
      Arrays.stream(subdomains)
          .filter(subdomain -> subdomain.parent().filter(queriedDomain.name()::equals).isPresent())
          .sorted(Comparator.comparing(DomainEntity::name))
          .forEach(scopeDomains::add);
    }

    return scopeDomains;
  }

  // A name can change between domain batches. Group paths by (type, ID) and use the
  // first name observed in a selected domain for display.
  private static final class DirectMemberPaths {
    private final Set<DomainRelation> relations = new LinkedHashSet<>();
    @Nullable private MetadataObject selectedObject;
  }

  private static boolean fewerDomainBatches(
      Map<Long, List<GenericEntity>> selected, int remainingDomains) {
    long domainBatches =
        (remainingDomains + (long) JOINED_DOMAIN_BATCH_SIZE - 1) / JOINED_DOMAIN_BATCH_SIZE;
    Map<Entity.EntityType, Set<Long>> idsByType = new EnumMap<>(Entity.EntityType.class);
    int distinctObjects = 0;
    long objectBatches = 0;
    for (List<GenericEntity> assignments : selected.values()) {
      for (GenericEntity assignment : assignments) {
        Set<Long> ids = idsByType.computeIfAbsent(assignment.type(), ignored -> new HashSet<>());
        if (!ids.add(assignment.id())) {
          continue;
        }
        distinctObjects++;
        if ((ids.size() - 1) % MEMBERSHIP_LOOKUP_BATCH_SIZE == 0) {
          objectBatches++;
        }
        // A semijoin may scan unrelated assignments. Prefer indexed lookups for small
        // selections, even when several object types would require separate statements.
        if (distinctObjects > MEMBERSHIP_LOOKUP_BATCH_SIZE && objectBatches > domainBatches) {
          return true;
        }
      }
    }
    return false;
  }

  /** Matches tag IDs and exact assignment values, preserving the visible scope's relation order. */
  private static final class BindingIndex {
    private final List<DomainEntity> scope;
    private final Set<Long> selected;
    private final Map<Long, Map<String, Integer>> positions = new HashMap<>();

    private BindingIndex(List<DomainEntity> scope, Set<Long> selected) {
      this.scope = scope;
      this.selected = selected;
      for (int i = 0; i < scope.size(); i++) {
        DomainEntity domain = scope.get(i);
        if (domain.backingTagId() != null) {
          positions
              .computeIfAbsent(domain.backingTagId(), ignored -> new HashMap<>())
              .put(domain.backingTagValue(), i);
        }
      }
    }

    private void addMember(
        List<DomainMember> members, MetadataObject object, ResolvedTag[] assignments) {
      Map<Integer, DomainRelation> matches = new TreeMap<>();
      for (ResolvedTag assignment : assignments) {
        Map<String, Integer> byValue = positions.get(assignment.tag().id());
        if (byValue == null) {
          continue;
        }
        addMatch(matches, byValue.get(null), object, assignment.source());
        assignment
            .tag()
            .assignment()
            .ifPresent(
                tag -> {
                  for (String value : tag.values()) {
                    addMatch(matches, byValue.get(value), object, assignment.source());
                  }
                });
      }
      if (matches.keySet().stream().anyMatch(i -> selected.contains(scope.get(i).id()))) {
        members.add(new DomainMemberInfo(object, matches.values().toArray(new DomainRelation[0])));
      }
    }

    private void addMatch(
        Map<Integer, DomainRelation> matches,
        @Nullable Integer position,
        MetadataObject object,
        MetadataObject source) {
      if (position == null) {
        return;
      }
      if (position == 0) {
        matches.put(
            position,
            source.equals(object) ? DomainRelation.direct() : DomainRelation.inherited(source));
      } else {
        matches.put(position, DomainRelation.viaSubdomain(scope.get(position).name()));
      }
    }
  }

  @Nullable
  private static DomainRelation resolveOwnRelation(
      DomainEntity domain, MetadataObject object, ResolvedTag[] effective) {
    if (domain.backingTagId() == null) {
      return null;
    }

    // A null binding value accepts any assignment; a specified value must match exactly.
    for (ResolvedTag assignment : effective) {
      TagEntity tag = assignment.tag();
      if (Objects.equals(domain.backingTagId(), tag.id())
          && (domain.backingTagValue() == null
              || tag.assignment()
                  .map(a -> Arrays.asList(a.values()).contains(domain.backingTagValue()))
                  .orElse(false))) {
        return assignment.source().equals(object)
            ? DomainRelation.direct()
            : DomainRelation.inherited(assignment.source());
      }
    }
    return null;
  }

  private Map<Long, List<GenericEntity>> listAssignments(
      String metalake,
      List<DomainEntity> domains,
      Set<MetadataObject.Type> types,
      Map<Long, List<GenericEntity>> selectedAssignments)
      throws IOException {
    List<DomainEntity> bound =
        domains.stream().filter(d -> d.backingTagId() != null).collect(Collectors.toList());
    return bound.isEmpty()
        ? Collections.emptyMap()
        : store
            .domainOperations()
            .listDomainAssignments(metalake, bound, types, selectedAssignments);
  }

  private static MetadataObject assignedObject(GenericEntity assignment) {
    return MetadataObjects.parse(
        assignment.name(), MetadataObject.Type.valueOf(assignment.type().name()));
  }

  private static boolean matchesFilter(MetadataObject object, DomainMemberFilter filter) {
    return (filter.types().isEmpty() || filter.types().contains(object.type()))
        && filter
            .nameContains()
            .map(
                text ->
                    object
                        .fullName()
                        .toLowerCase(Locale.ROOT)
                        .contains(text.toLowerCase(Locale.ROOT)))
            .orElse(true);
  }

  private void collectCandidates(
      String metalake,
      MetadataObject object,
      DomainMemberFilter filter,
      Set<MetadataObject> candidates)
      throws IOException {
    if (DomainMember.SUPPORTED_TYPES.contains(object.type())) {
      if (matchesFilter(object, filter)) {
        candidates.add(object);
      }
      return;
    }

    if (object.type() != MetadataObject.Type.CATALOG
        && object.type() != MetadataObject.Type.SCHEMA) {
      return;
    }

    try {
      // Catalog listing already includes nested schemas; expand each schema only once.
      if (object.type() == MetadataObject.Type.CATALOG) {
        for (MetadataObject schema :
            listMetadataObjects(
                Namespace.of(metalake, object.name()),
                SchemaEntity.class,
                Entity.EntityType.SCHEMA)) {
          expandSchemaLeaves(metalake, schema, filter, candidates);
        }
      } else {
        expandSchemaLeaves(metalake, object, filter, candidates);

        // One prefix lookup includes all nested schemas without repeated recursive queries.
        for (SchemaEntity descendant :
            store.listDescendantSchemas(MetadataObjectUtil.toEntityIdent(metalake, object))) {
          expandSchemaLeaves(
              metalake,
              MetadataObjects.of(object.parent(), descendant.name(), MetadataObject.Type.SCHEMA),
              filter,
              candidates);
        }
      }
    } catch (NoSuchEntityException e) {
      // A catalog or schema may have been dropped after its assignment was read.
      LOG.debug("Skipping missing assignment scope {} in metalake {}", object, metalake, e);
    }
  }

  private void expandSchemaLeaves(
      String metalake,
      MetadataObject schema,
      DomainMemberFilter filter,
      Set<MetadataObject> candidates)
      throws IOException {
    Namespace namespace = Namespace.of(metalake, schema.parent(), schema.name());
    try {
      for (MetadataObject.Type type : DomainMember.SUPPORTED_TYPES) {
        if (!filter.types().isEmpty() && !filter.types().contains(type)) {
          continue;
        }

        List<MetadataObject> leaves;
        switch (type) {
          case TABLE:
            leaves = listMetadataObjects(namespace, TableEntity.class, Entity.EntityType.TABLE);
            break;
          case VIEW:
            leaves = listMetadataObjects(namespace, ViewEntity.class, Entity.EntityType.VIEW);
            break;
          case FILESET:
            leaves = listMetadataObjects(namespace, FilesetEntity.class, Entity.EntityType.FILESET);
            break;
          case TOPIC:
            leaves = listMetadataObjects(namespace, TopicEntity.class, Entity.EntityType.TOPIC);
            break;
          case MODEL:
            leaves = listMetadataObjects(namespace, ModelEntity.class, Entity.EntityType.MODEL);
            break;
          case SEMANTIC_MODEL:
            leaves =
                listMetadataObjects(
                    namespace, SemanticModelEntity.class, Entity.EntityType.SEMANTIC_MODEL);
            break;
          default:
            throw new IllegalArgumentException("Unsupported domain member type " + type);
        }

        for (MetadataObject leaf : leaves) {
          collectCandidates(metalake, leaf, filter, candidates);
        }
      }
    } catch (NoSuchEntityException e) {
      // Skip this schema if it was dropped; other schemas can still contribute members.
      LOG.debug("Skipping missing schema {} in metalake {}", schema, metalake, e);
    }
  }

  private <E extends Entity & HasIdentifier> List<MetadataObject> listMetadataObjects(
      Namespace namespace, Class<E> entityClass, Entity.EntityType type) throws IOException {
    String prefix =
        String.join(".", Arrays.copyOfRange(namespace.levels(), 1, namespace.length())) + ".";
    return store.list(namespace, entityClass, type).stream()
        .map(
            entity ->
                MetadataObjects.parse(
                    prefix + entity.name(), MetadataObject.Type.valueOf(type.name())))
        .collect(Collectors.toList());
  }
}
