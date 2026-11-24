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
package org.apache.gravitino.glossary;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.dto.authorization.OwnerDTO;
import org.apache.gravitino.dto.glossary.TermDTO;
import org.apache.gravitino.dto.glossary.TermLinkDTO;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.meta.GlossaryTermEntity;
import org.apache.gravitino.storage.relational.service.GlossaryTermMetaService;
import org.apache.gravitino.storage.relational.service.GlossaryTermRelService;
import org.apache.gravitino.storage.relational.service.MetalakeMetaService;
import org.apache.gravitino.storage.relational.service.OwnerMetaService;
import org.apache.gravitino.storage.relational.session.SqlSessions;
import org.apache.gravitino.storage.relational.utils.SessionUtils;

/**
 * Builds caller-visible term snapshots with their owner and direct relations. On reads, the
 * optional status filter applies to roots, traversed terms and returned relation names.
 */
public final class GlossaryTermViews {
  private final GlossaryManager manager;

  /** Uses the manager's authorization and term visibility for roots and relation endpoints. */
  public GlossaryTermViews(GlossaryManager manager) {
    this.manager = manager;
  }

  /** Returns the terms matching a domain and case-insensitive name fragment. */
  public TermDTO[] listTerms(
      String metalake,
      @Nullable String domain,
      @Nullable String nameContains,
      @Nullable GlossaryTerm.Status status) {
    return SqlSessions.readSnapshot(
        () -> {
          List<GlossaryTermEntity> terms =
              manager.listTermsInSnapshot(metalake, domain, matchesName(nameContains), status);
          return projectTermSnapshots(metalake, terms, status);
        });
  }

  /** Lists names without loading owner or relation projections. */
  public String[] listTermNames(
      String metalake,
      @Nullable String domain,
      @Nullable String nameContains,
      @Nullable GlossaryTerm.Status status) {
    return SqlSessions.readSnapshot(
        () -> manager.listTermNamesInSnapshot(metalake, domain, matchesName(nameContains), status));
  }

  /** Drops the term if it is visible and the caller may delete it. */
  public boolean dropTerm(String metalake, String name) {
    return manager.dropTerm(metalake, name);
  }

  /** Returns a term and its visible neighbors from one database snapshot. */
  public TermDTO getTerm(String metalake, String name, @Nullable GlossaryTerm.Status status) {
    return SqlSessions.readSnapshot(
        () ->
            projectTermSnapshots(
                metalake, List.of(manager.getTermInSnapshot(metalake, name, status)), status)[0]);
  }

  /** Returns each visible ancestor or descendant once, excluding the root. */
  public TermDTO[] getTermHierarchy(
      String metalake,
      String name,
      GlossaryManager.Direction direction,
      @Nullable GlossaryTerm.Status status) {
    Preconditions.checkArgument(
        direction == GlossaryManager.Direction.BROADER
            || direction == GlossaryManager.Direction.NARROWER,
        "direction must be BROADER or NARROWER");
    return SqlSessions.readSnapshot(
        () ->
            projectTermSnapshots(
                metalake,
                manager.listRelationsInSnapshot(metalake, name, direction, true, status),
                status));
  }

  /**
   * Creates and projects the new term in one transaction. A caller supplying an outer transaction
   * must roll it back if this method fails.
   */
  public TermDTO createTerm(String metalake, TermSpec spec) {
    return applyChangeAndProjectTerm(metalake, () -> manager.createTerm(metalake, spec));
  }

  /**
   * Applies the ordered edits and projects the resulting revision in one transaction. A caller
   * supplying an outer transaction must roll it back if this method fails.
   */
  public TermDTO alterTerm(
      String metalake, String name, @Nullable String revision, TermChange[] changes) {
    return applyChangeAndProjectTerm(
        metalake,
        () ->
            revision == null
                ? manager.alterTerm(metalake, name, changes)
                : manager.alterTerm(metalake, name, revision, changes));
  }

  private static Predicate<GlossaryTermEntity> matchesName(@Nullable String nameContains) {
    if (nameContains == null) {
      return term -> true;
    }
    String fragment = nameContains.toLowerCase(Locale.ROOT);
    return term -> term.name().toLowerCase(Locale.ROOT).contains(fragment);
  }

  private TermDTO applyChangeAndProjectTerm(String metalake, Supplier<GlossaryTermEntity> change) {
    SessionUtils.beginTransaction();
    try {
      TermDTO result = projectTermSnapshots(metalake, List.of(change.get()), null)[0];
      SessionUtils.commitTransaction();
      return result;
    } catch (RuntimeException | Error e) {
      SessionUtils.rollbackTransaction();
      throw e;
    }
  }

  private TermDTO[] projectTermSnapshots(
      String metalake, List<GlossaryTermEntity> roots, @Nullable GlossaryTerm.Status status) {
    if (roots.isEmpty()) {
      return new TermDTO[0];
    }
    long lake = MetalakeMetaService.getInstance().getMetalakeIdByName(metalake);
    Map<Long, NameIdentifier> identifiers =
        roots.stream()
            .collect(Collectors.toMap(GlossaryTermEntity::id, GlossaryTermEntity::nameIdentifier));
    Set<Long> ids = identifiers.keySet();
    GlossaryTermRelService edges = GlossaryTermRelService.getInstance();
    Map<Long, List<Long>> broader = new HashMap<>();
    Map<Long, List<Long>> narrower = new HashMap<>();
    Map<Long, List<Long>> related = new HashMap<>();
    Set<Long> endpoints = new HashSet<>();
    edges
        .listBroaderEdges(lake, ids)
        .forEach(
            e -> recordRelationEndpoint(broader, e.getFromTermId(), e.getToTermId(), endpoints));
    edges
        .listNarrowerEdges(lake, ids)
        .forEach(
            e -> recordRelationEndpoint(narrower, e.getToTermId(), e.getFromTermId(), endpoints));
    edges
        .listRelatedEdges(lake, ids)
        .forEach(
            e -> {
              if (ids.contains(e.getFromTermId())) {
                recordRelationEndpoint(related, e.getFromTermId(), e.getToTermId(), endpoints);
              }
              if (ids.contains(e.getToTermId())) {
                recordRelationEndpoint(related, e.getToTermId(), e.getFromTermId(), endpoints);
              }
            });
    Map<Long, String> names = new HashMap<>();
    // Every result root has already passed the same read and status filters in this snapshot.
    for (long id : ids) {
      if (endpoints.remove(id)) {
        names.put(id, identifiers.get(id).name());
      }
    }
    if (!endpoints.isEmpty()) {
      GlossaryAccess access = manager.accessForMetalake(metalake);
      for (GlossaryTermEntity term :
          GlossaryTermMetaService.getInstance()
              .listTermsByIds(roots.get(0).namespace(), lake, endpoints)) {
        if (manager.isTermVisible(term, access, status)) {
          names.put(term.id(), term.name());
        }
      }
    }
    Map<String, OwnerDTO> owners = new HashMap<>();
    OwnerMetaService.getInstance()
        .listOwnersByObjectIds(metalake, identifiers, EntityType.GLOSSARY_TERM)
        .forEach(
            rel ->
                owners.put(
                    rel.source().name(),
                    OwnerDTO.builder()
                        .withName(rel.targetEntity().name())
                        .withType(Owner.Type.valueOf(rel.targetEntity().type().name()))
                        .build()));
    return roots.stream()
        .map(
            term ->
                new TermDTO(
                    term.name(),
                    term.definition(),
                    term.synonyms(),
                    term.status(),
                    term.domains(),
                    visibleRelationNames(broader, term.id(), names),
                    visibleRelationNames(narrower, term.id(), names),
                    visibleRelationNames(related, term.id(), names),
                    Arrays.stream(term.links())
                        .map(link -> new TermLinkDTO(link.label(), link.url()))
                        .toArray(TermLinkDTO[]::new),
                    owners.get(term.name()),
                    term.sourceRef(),
                    term.properties(),
                    term.version(),
                    term.revisionToken(),
                    DTOConverters.toDTO(term.auditInfo())))
        .toArray(TermDTO[]::new);
  }

  private static void recordRelationEndpoint(
      Map<Long, List<Long>> edges, long root, long neighbor, Set<Long> endpoints) {
    edges.computeIfAbsent(root, ignored -> new ArrayList<>()).add(neighbor);
    endpoints.add(neighbor);
  }

  private static String[] visibleRelationNames(
      Map<Long, List<Long>> edges, long id, Map<Long, String> visible) {
    return edges.getOrDefault(id, List.of()).stream()
        .filter(visible::containsKey)
        .map(visible::get)
        .sorted()
        .toArray(String[]::new);
  }
}
