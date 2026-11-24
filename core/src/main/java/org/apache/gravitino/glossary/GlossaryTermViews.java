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

/** Builds caller-visible term snapshots with their owner and direct relations. */
public final class GlossaryTermViews {
  private final GlossaryManager manager;

  /** Uses the manager's authorization and term visibility for roots and relation endpoints. */
  public GlossaryTermViews(GlossaryManager manager) {
    this.manager = manager;
  }

  /** Returns the terms matching a domain and case-insensitive name fragment. */
  public TermDTO[] list(String metalake, @Nullable String domain, @Nullable String nameContains) {
    return SqlSessions.readSnapshot(
        () -> {
          List<GlossaryTermEntity> terms = manager.listTermsInSnapshot(metalake, domain);
          terms.removeIf(matchesName(nameContains).negate());
          return project(metalake, terms);
        });
  }

  /** Restricts roots, traversed nodes and relation names to the supplied status. */
  public GlossaryTermViews withStatus(GlossaryTerm.Status status) {
    return new GlossaryTermViews(manager.withTermVisibility(term -> term.status() == status));
  }

  /** Lists names without loading owner or relation projections. */
  public String[] names(String metalake, @Nullable String domain, @Nullable String nameContains) {
    return manager.listTerms(metalake, domain).stream()
        .filter(matchesName(nameContains))
        .map(GlossaryTermEntity::name)
        .toArray(String[]::new);
  }

  /** Drops the term if it is visible and the caller may delete it. */
  public boolean drop(String metalake, String name) {
    return manager.dropTerm(metalake, name);
  }

  /** Returns a term and its visible neighbors from one database snapshot. */
  public TermDTO get(String metalake, String name) {
    return SqlSessions.readSnapshot(
        () -> project(metalake, List.of(manager.getTermInSnapshot(metalake, name)))[0]);
  }

  /** Returns each visible ancestor or descendant once, excluding the root. */
  public TermDTO[] hierarchy(String metalake, String name, GlossaryManager.Direction direction) {
    Preconditions.checkArgument(
        direction == GlossaryManager.Direction.BROADER
            || direction == GlossaryManager.Direction.NARROWER,
        "direction must be BROADER or NARROWER");
    return SqlSessions.readSnapshot(
        () ->
            project(
                metalake, manager.listRelationsInSnapshot(metalake, name, direction, true, null)));
  }

  /** Creates and projects the new term before the write transaction commits. */
  public TermDTO create(String metalake, TermSpec spec) {
    return write(metalake, () -> manager.createTerm(metalake, spec));
  }

  /** Applies the ordered edits and returns that revision's projection in the same transaction. */
  public TermDTO alter(
      String metalake, String name, @Nullable String revision, TermChange[] changes) {
    return write(
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

  private TermDTO write(String metalake, Supplier<GlossaryTermEntity> change) {
    SessionUtils.beginTransaction();
    try {
      TermDTO result = project(metalake, List.of(change.get()))[0];
      SessionUtils.commitTransaction();
      return result;
    } catch (RuntimeException | Error e) {
      SessionUtils.rollbackTransaction();
      throw e;
    }
  }

  private TermDTO[] project(String metalake, List<GlossaryTermEntity> roots) {
    if (roots.isEmpty()) {
      return new TermDTO[0];
    }
    long lake = MetalakeMetaService.getInstance().getMetalakeIdByName(metalake);
    GlossaryAccess access = manager.access(metalake);
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
        .forEach(e -> add(broader, e.getFromTermId(), e.getToTermId(), endpoints));
    edges
        .listNarrowerEdges(lake, ids)
        .forEach(e -> add(narrower, e.getToTermId(), e.getFromTermId(), endpoints));
    edges
        .listRelatedEdges(lake, ids)
        .forEach(
            e -> {
              if (ids.contains(e.getFromTermId())) {
                add(related, e.getFromTermId(), e.getToTermId(), endpoints);
              }
              if (ids.contains(e.getToTermId())) {
                add(related, e.getToTermId(), e.getFromTermId(), endpoints);
              }
            });
    Map<Long, String> names = new HashMap<>();
    for (GlossaryTermEntity term :
        GlossaryTermMetaService.getInstance().getByIds(roots.get(0).namespace(), lake, endpoints)) {
      if (manager.canProject(term, access)) {
        names.put(term.id(), term.name());
      }
    }
    Map<String, OwnerDTO> owners = new HashMap<>();
    OwnerMetaService.getInstance()
        .batchGetOwnersById(metalake, identifiers, EntityType.GLOSSARY_TERM)
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
                    names(broader, term.id(), names),
                    names(narrower, term.id(), names),
                    names(related, term.id(), names),
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

  private static void add(
      Map<Long, List<Long>> edges, long root, long neighbor, Set<Long> endpoints) {
    edges.computeIfAbsent(root, ignored -> new ArrayList<>()).add(neighbor);
    endpoints.add(neighbor);
  }

  private static String[] names(Map<Long, List<Long>> edges, long id, Map<Long, String> visible) {
    return edges.getOrDefault(id, List.of()).stream()
        .filter(visible::containsKey)
        .map(visible::get)
        .sorted()
        .toArray(String[]::new);
  }
}
