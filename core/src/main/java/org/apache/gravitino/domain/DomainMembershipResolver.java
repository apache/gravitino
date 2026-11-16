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

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.RelationQuery;
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

    // Expand explicit tag assignments into recorded leaf objects that may inherit the tag.
    Set<MetadataObject> candidates = new LinkedHashSet<>();
    try {
      for (DomainEntity selectedDomain : selectedDomains) {
        if (selectedDomain.backingTagId() == null) {
          continue;
        }

        // Query by stable ID so a recreated tag with the same name cannot take over the binding.
        List<GenericEntity> assigned =
            store
                .relationOperations()
                .listEntitiesByRelation(
                    RelationQuery.forTagId(
                        metalake, selectedDomain.backingTagId(), selectedDomain.backingTagValue()));
        for (GenericEntity assignment : assigned) {
          collectCandidates(
              metalake,
              MetadataObjects.parse(
                  assignment.name(), MetadataObject.Type.valueOf(assignment.type().name())),
              filter,
              candidates);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to list members of domain " + queriedDomain.name(), e);
    }

    // A nearer assignment can override an ancestor tag, so candidates need effective-tag checks.
    List<DomainMember> members = new ArrayList<>();
    for (MetadataObject object : candidates) {
      try {
        ResolvedTag[] effective = tags.resolveWithSources(metalake, object);
        if (selectedDomains.stream()
            .noneMatch(d -> resolveOwnRelation(d, object, effective) != null)) {
          continue;
        }

        // Include every applicable relation, even when the filter selected only one subdomain.
        DomainRelation[] relations =
            resolveRelations(queriedDomain, scopeDomains, object, effective);
        if (relations.length > 0) {
          members.add(new DomainMemberInfo(object, relations));
        }
      } catch (NoSuchMetadataObjectException e) {
        // An object or its ancestor may have been dropped after candidate enumeration.
        LOG.debug("Skipping missing member candidate {} in metalake {}", object, metalake, e);
      }
    }

    members.sort(
        Comparator.comparing((DomainMember m) -> m.object().type().name())
            .thenComparing(m -> m.object().fullName()));
    return members.toArray(new DomainMember[0]);
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

  private static DomainRelation[] resolveRelations(
      DomainEntity queriedDomain,
      List<DomainEntity> scopeDomains,
      MetadataObject object,
      ResolvedTag[] effective) {
    Set<DomainRelation> relations = new LinkedHashSet<>();
    for (DomainEntity memberDomain : scopeDomains) {
      DomainRelation own = resolveOwnRelation(memberDomain, object, effective);
      if (own != null) {
        relations.add(
            Objects.equals(queriedDomain.id(), memberDomain.id())
                ? own
                : DomainRelation.viaSubdomain(memberDomain.name()));
      }
    }

    return relations.toArray(new DomainRelation[0]);
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

  private void collectCandidates(
      String metalake,
      MetadataObject object,
      DomainMemberFilter filter,
      Set<MetadataObject> candidates)
      throws IOException {
    if (DomainMember.SUPPORTED_TYPES.contains(object.type())) {
      if ((filter.types().isEmpty() || filter.types().contains(object.type()))
          && filter
              .nameContains()
              .map(
                  text ->
                      object
                          .fullName()
                          .toLowerCase(Locale.ROOT)
                          .contains(text.toLowerCase(Locale.ROOT)))
              .orElse(true)) {
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
