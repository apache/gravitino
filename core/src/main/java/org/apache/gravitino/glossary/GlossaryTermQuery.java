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
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.dto.glossary.TermBindingDTO;
import org.apache.gravitino.dto.glossary.TermMatchDTO;
import org.apache.gravitino.meta.GlossaryTermEntity;
import org.apache.gravitino.meta.GlossaryTermNames;
import org.apache.gravitino.storage.relational.mapper.GlossaryTermLookupMapper;
import org.apache.gravitino.storage.relational.mapper.GlossaryTermRelMapper;
import org.apache.gravitino.storage.relational.po.GlossaryTermBindingPO;
import org.apache.gravitino.storage.relational.po.GlossaryTermPO;
import org.apache.gravitino.storage.relational.po.GlossaryTermRelPO;
import org.apache.gravitino.storage.relational.service.GlossaryTermMetaService;
import org.apache.gravitino.storage.relational.service.GlossaryTermRelService.RelationType;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NamespaceUtil;

/**
 * Selects and authorizes list roots and optional lookup projections in the caller's read snapshot.
 */
final class GlossaryTermQuery {
  private static final int MAX_WORDS = 20;
  private static final int MAX_VISIBLE_BINDINGS = 20;
  private static final int MAX_BINDING_CANDIDATES = 1000;
  private static final int ID_BATCH_SIZE = 500;

  final long lake;
  final GlossaryAccess access;
  private final GlossaryManager manager;
  private final String metalake;
  private final Map<String, String> wordKeys;
  @Nullable private final TermLookupBudget budget;
  private final List<GlossaryTermPO> visible;
  // Column permissions follow the containing table, and metric permissions follow the model.
  private final Map<MetadataObject, Boolean> readableTargets = new HashMap<>();
  @Nullable private GlossaryBindingAccess bindingAccess;

  GlossaryTermQuery(
      GlossaryManager manager,
      String metalake,
      @Nullable String domain,
      @Nullable String fragment,
      @Nullable GlossaryTerm.Status status,
      Map<String, String> wordKeys) {
    this.manager = manager;
    this.metalake = metalake;
    this.wordKeys = wordKeys;
    budget = wordKeys.isEmpty() ? null : new TermLookupBudget();
    lake = GlossaryManager.resolveMetalakeId(metalake);
    access = manager.accessForMetalake(metalake);
    if (!access.canView()) {
      visible = List.of();
      return;
    }
    Set<String> keys = new HashSet<>(wordKeys.values());
    String literal = fragment == null ? null : fragment.toLowerCase(Locale.ROOT);
    // The stored key uses Java's Unicode case mapping on every backend. Its whitespace is
    // collapsed, so recheck literal spacing on the lightweight rows before hydrating definitions.
    String pattern =
        literal == null
            ? null
            : "%"
                + GlossaryTermNames.key(literal)
                    .replace("!", "!!")
                    .replace("%", "!%")
                    .replace("_", "!_")
                + "%";
    String state = status == null ? null : status.name();
    Set<Long> scope =
        domain == null ? null : manager.domainScope(metalake, lake, domain, access, budget);
    List<GlossaryTermPO> rows;
    if (scope == null || scope.size() <= ID_BATCH_SIZE || budget != null) {
      // A lookup's domain scope already fits the request budget. One query counts each root once.
      rows = candidates(scope, pattern, state, keys);
      if (budget != null) {
        budget.consume(rows.size());
      }
    } else {
      Map<Long, GlossaryTermPO> distinct = new TreeMap<>();
      for (List<Long> batch : Lists.partition(List.copyOf(scope), ID_BATCH_SIZE)) {
        for (GlossaryTermPO row : candidates(batch, pattern, state, keys)) {
          distinct.put(row.getTermId(), row);
        }
      }
      rows = List.copyOf(distinct.values());
    }
    visible =
        rows.stream()
            .filter(
                row -> literal == null || row.getName().toLowerCase(Locale.ROOT).contains(literal))
            .filter(
                row ->
                    access.canRead(row.getTermId(), GlossaryTerm.Status.valueOf(row.getStatus())))
            .toList();
  }

  static Map<String, String> wordKeys(List<String> words) {
    Map<String, String> wordKeys = new LinkedHashMap<>();
    Preconditions.checkArgument(words.size() <= MAX_WORDS, "At most 20 match words are supported");
    for (String word : words) {
      String key = GlossaryTermNames.key(word);
      Preconditions.checkArgument(!key.isEmpty(), "match words must not be blank");
      wordKeys.put(word, key);
    }
    return wordKeys;
  }

  String[] names() {
    if (manager.hasTermVisibilityFilter()) {
      return definitions(false).stream().map(GlossaryTermEntity::name).toArray(String[]::new);
    }
    return visible.stream().map(GlossaryTermPO::getName).toArray(String[]::new);
  }

  List<GlossaryTermEntity> definitions(boolean projectDomains) {
    if (visible.isEmpty()) {
      return new ArrayList<>();
    }
    Set<Long> ids =
        visible.stream()
            .map(GlossaryTermPO::getTermId)
            .collect(Collectors.toCollection(LinkedHashSet::new));
    return loadDefinitions(ids, projectDomains);
  }

  void addEndpointNames(
      Set<Long> ids, @Nullable GlossaryTerm.Status status, Map<Long, String> names) {
    for (List<Long> batch : Lists.partition(List.copyOf(ids), ID_BATCH_SIZE)) {
      List<GlossaryTermPO> endpoints =
          SessionUtils.getWithoutCommit(
              GlossaryTermLookupMapper.class,
              mapper -> mapper.endpoints(lake, batch, status == null ? null : status.name()));
      Set<Long> custom = new LinkedHashSet<>();
      for (GlossaryTermPO row : endpoints) {
        if (access.canRead(row.getTermId(), GlossaryTerm.Status.valueOf(row.getStatus()))) {
          if (manager.hasTermVisibilityFilter()) {
            custom.add(row.getTermId());
          } else {
            names.put(row.getTermId(), row.getName());
          }
        }
      }
      if (!custom.isEmpty()) {
        loadDefinitions(custom, false).forEach(term -> names.put(term.id(), term.name()));
      }
    }
  }

  List<TermMatchDTO> matches(GlossaryTermEntity term) {
    Set<String> synonyms = new HashSet<>();
    for (String synonym : term.synonyms()) {
      synonyms.add(GlossaryTermNames.key(synonym));
    }
    String name = GlossaryTermNames.key(term.name());
    List<TermMatchDTO> matches = new ArrayList<>();
    wordKeys.forEach(
        (word, key) -> {
          if (name.equals(key)) {
            matches.add(new TermMatchDTO(word, TermMatch.Field.NAME));
          } else if (synonyms.contains(key)) {
            matches.add(new TermMatchDTO(word, TermMatch.Field.SYNONYM));
          }
        });
    return matches;
  }

  /** Fetches at most one overflow row before any relation endpoint authorization. */
  List<GlossaryTermRelPO> edges(Set<Long> ids, RelationType type, boolean reverse) {
    List<GlossaryTermRelPO> rows =
        SessionUtils.getWithoutCommit(
            GlossaryTermRelMapper.class,
            mapper -> mapper.listEdges(lake, ids, type.name(), reverse, budget.remaining() + 1));
    budget.consume(rows.size());
    if (type == RelationType.RELATED) {
      // A symmetric edge between two roots is projected once for each root.
      for (GlossaryTermRelPO edge : rows) {
        if (ids.contains(edge.getFromTermId()) && ids.contains(edge.getToTermId())) {
          budget.consume(1);
        }
      }
    }
    return rows;
  }

  record Preview(List<TermBindingDTO> bindings, boolean truncated) {}

  Preview preview(long termId) {
    List<TermBindingDTO> visibleBindings = new ArrayList<>();
    GlossaryTermBindingPO last = null;
    int remaining = Math.min(MAX_BINDING_CANDIDATES, budget.remaining());
    while (true) {
      int pageSize = Math.min(MAX_VISIBLE_BINDINGS + 1, remaining + 1);
      GlossaryTermBindingPO after = last;
      List<GlossaryTermBindingPO> rows =
          SessionUtils.getWithoutCommit(
              GlossaryTermLookupMapper.class,
              mapper -> mapper.bindingCandidates(lake, termId, after, pageSize));
      // An overflow row proves there is more work, but must never be resolved or authorized.
      List<GlossaryTermBindingPO> batch = rows.subList(0, Math.min(rows.size(), remaining));
      if (!batch.isEmpty()) {
        if (bindingAccess == null) {
          bindingAccess = manager.bindingAccess(metalake, lake);
        }
        budget.consume(batch.size());
        remaining -= batch.size();
        for (TermBinding binding : GlossaryManager.resolveBindings(batch, bindingAccess)) {
          if (readableTargets.computeIfAbsent(
              GlossaryBindingAccess.normalizeTarget(binding.object()),
              bindingAccess::canReadTarget)) {
            if (visibleBindings.size() == MAX_VISIBLE_BINDINGS) {
              return new Preview(visibleBindings, true);
            }
            visibleBindings.add(TermBindingDTO.from(binding));
          }
        }
      }
      if (rows.size() > batch.size()) {
        throw TermLookupBudget.exceeded();
      }
      if (rows.size() < pageSize) {
        return new Preview(visibleBindings, false);
      }
      last = rows.get(rows.size() - 1);
    }
  }

  private List<GlossaryTermEntity> loadDefinitions(Set<Long> ids, boolean projectDomains) {
    if (budget != null) {
      for (List<Long> batch : Lists.partition(List.copyOf(ids), ID_BATCH_SIZE)) {
        budget.consume(
            SessionUtils.getWithoutCommit(
                GlossaryTermLookupMapper.class,
                mapper -> mapper.countMemberships(lake, batch, budget.remaining() + 1)));
      }
    }
    return GlossaryTermMetaService.getInstance()
        .listTermsByIds(NamespaceUtil.ofGlossaryTerm(metalake), lake, ids)
        .stream()
        .filter(term -> manager.isTermVisible(term, access, null))
        .map(term -> projectDomains ? GlossaryManager.visibleDomains(term, access) : term)
        .collect(Collectors.toCollection(ArrayList::new));
  }

  private List<GlossaryTermPO> candidates(
      @Nullable Collection<Long> domains,
      @Nullable String pattern,
      @Nullable String status,
      Set<String> keys) {
    Integer limit = budget == null ? null : budget.remaining() + 1;
    return SessionUtils.getWithoutCommit(
        GlossaryTermLookupMapper.class,
        mapper -> mapper.candidates(lake, domains, pattern, status, keys, limit));
  }
}
