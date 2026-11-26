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
import java.util.Collections;
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
import org.apache.gravitino.storage.relational.mapper.GlossaryTermBindingMapper;
import org.apache.gravitino.storage.relational.mapper.GlossaryTermDomainRelMapper;
import org.apache.gravitino.storage.relational.mapper.GlossaryTermMetaMapper;
import org.apache.gravitino.storage.relational.mapper.GlossaryTermRelMapper;
import org.apache.gravitino.storage.relational.po.GlossaryTermBindingPO;
import org.apache.gravitino.storage.relational.po.GlossaryTermPO;
import org.apache.gravitino.storage.relational.po.GlossaryTermRelPO;
import org.apache.gravitino.storage.relational.service.GlossaryTermMetaService;
import org.apache.gravitino.storage.relational.service.GlossaryTermRelService.RelationType;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.storage.relational.utils.SqlLikePattern;
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
  @Nullable private final String status;
  private final Map<String, String> wordKeys;
  @Nullable private final TermLookupBudget budget;
  private final List<GlossaryTermPO> visible;
  // Column permissions follow the containing table, and metric permissions follow the model.
  private final Map<MetadataObject, Boolean> readableTargets = new HashMap<>();

  GlossaryTermQuery(
      GlossaryManager manager,
      String metalake,
      @Nullable String domain,
      @Nullable String fragment,
      @Nullable GlossaryTerm.Status status,
      Map<String, String> wordKeys) {
    this.manager = manager;
    this.metalake = metalake;
    this.status = status == null ? null : status.name();
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
            : "%" + SqlLikePattern.escapeLiteral(GlossaryTermNames.key(literal)) + "%";
    Set<Long> scope =
        domain == null ? null : manager.domainScope(metalake, lake, domain, access, budget);
    List<GlossaryTermPO> rows;
    if (scope == null || scope.size() <= ID_BATCH_SIZE || budget != null) {
      // A lookup's domain scope already fits the request budget. One query counts each root once.
      rows = candidates(scope, pattern, literal, keys);
    } else {
      Map<Long, GlossaryTermPO> distinct = new TreeMap<>();
      for (List<Long> batch : Lists.partition(List.copyOf(scope), ID_BATCH_SIZE)) {
        for (GlossaryTermPO row : candidates(batch, pattern, literal, keys)) {
          distinct.put(row.getTermId(), row);
        }
      }
      rows = List.copyOf(distinct.values());
    }
    visible =
        rows.stream()
            .filter(
                row ->
                    access.canRead(row.getTermId(), GlossaryTerm.Status.valueOf(row.getStatus())))
            .toList();
  }

  static Map<String, String> wordKeys(List<String> words) {
    Map<String, String> wordKeys = new LinkedHashMap<>();
    Preconditions.checkArgument(
        words.size() <= MAX_WORDS, "At most %s match words are supported", MAX_WORDS);
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

  void addEndpointNames(Set<Long> ids, Map<Long, String> names) {
    for (List<Long> batch : Lists.partition(List.copyOf(ids), ID_BATCH_SIZE)) {
      List<GlossaryTermPO> endpoints =
          SessionUtils.getWithoutCommit(
              GlossaryTermMetaMapper.class, mapper -> mapper.listLookupEndpoints(lake, batch));
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
            mapper ->
                mapper.listEdges(lake, ids, type.name(), reverse, status, budget.remaining() + 1));
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

  static final class Preview {
    private final List<TermBindingDTO> bindings = new ArrayList<>();
    private boolean truncated;

    List<TermBindingDTO> bindings() {
      return bindings;
    }

    boolean truncated() {
      return truncated;
    }

    void add(TermBinding binding) {
      if (bindings.size() == MAX_VISIBLE_BINDINGS) {
        truncated = true;
      } else {
        bindings.add(TermBindingDTO.from(binding));
      }
    }
  }

  Map<Long, Preview> previews(Set<Long> ids) {
    // The full binding list requires the same capability even for a term with no bindings.
    GlossaryBindingAccess access = manager.bindingAccess(metalake, lake);
    Map<Long, Preview> results = new HashMap<>();
    ids.forEach(id -> results.put(id, new Preview()));
    List<Long> bound = new ArrayList<>();
    if (ids.size() == 1) {
      bound.addAll(ids);
    } else {
      for (List<Long> batch : Lists.partition(ids.stream().sorted().toList(), ID_BATCH_SIZE)) {
        bound.addAll(
            SessionUtils.getWithoutCommit(
                GlossaryTermBindingMapper.class,
                mapper -> mapper.listTermsWithBindings(lake, batch)));
      }
    }

    for (int start = 0; start < bound.size(); ) {
      List<Long> roots = bound.subList(start, Math.min(start + ID_BATCH_SIZE, bound.size()));
      int limit = Math.min(MAX_VISIBLE_BINDINGS + 1, budget.remaining() + 1);
      List<GlossaryTermBindingPO> page = bindingCandidates(roots, null, limit);
      // Every row on this first page is needed. Even a single root can contribute at most its
      // first 21 candidates. Resolve all targets together, preserving each row's owning term.
      int available = Math.min(page.size(), budget.remaining());
      appendVisibleBindings(page.subList(0, available), access, results);
      if (available < page.size()) {
        throw TermLookupBudget.exceeded();
      }
      if (page.size() < limit) {
        start += roots.size();
        continue;
      }

      // The term order proves exhaustion for every root before the final row's term. Only that
      // term can continue on the next page.
      GlossaryTermBindingPO last = page.get(page.size() - 1);
      long id = last.getTermId();
      start += Collections.binarySearch(roots, id) + 1;
      if (!results.get(id).truncated()) {
        int scanned = 0;
        for (int i = page.size() - 1; i >= 0 && page.get(i).getTermId() == id; i--) {
          scanned++;
        }
        int pending = bound.size() - start + 1;
        boolean readAhead =
            pending == 1 || (long) pending * MAX_BINDING_CANDIDATES <= budget.remaining();
        finishPreview(last, scanned, readAhead, access, results);
      }
    }
    return results;
  }

  private List<GlossaryTermBindingPO> bindingCandidates(
      List<Long> ids, @Nullable GlossaryTermBindingPO after, int limit) {
    return SessionUtils.getWithoutCommit(
        GlossaryTermBindingMapper.class,
        mapper -> mapper.listBindingCandidates(lake, ids, after, limit));
  }

  private void finishPreview(
      GlossaryTermBindingPO last,
      int scanned,
      boolean readAhead,
      GlossaryBindingAccess access,
      Map<Long, Preview> results) {
    long id = last.getTermId();
    Preview preview = results.get(id);
    int remaining = Math.min(MAX_BINDING_CANDIDATES - scanned, budget.remaining());
    while (true) {
      int limit = Math.min(MAX_VISIBLE_BINDINGS + 1, remaining + 1);
      List<GlossaryTermBindingPO> rows = bindingCandidates(List.of(id), last, limit);
      int available = Math.min(rows.size(), remaining);
      for (int offset = 0; offset < available; ) {
        // When other roots still need the shared budget, resolve only as far as the next
        // potentially decisive visible binding.
        int needed = MAX_VISIBLE_BINDINGS + 1 - preview.bindings().size();
        int end = readAhead ? available : Math.min(available, offset + needed);
        appendVisibleBindings(rows.subList(offset, end), access, results);
        if (preview.truncated()) {
          return;
        }
        offset = end;
      }
      if (rows.size() > available) {
        throw TermLookupBudget.exceeded();
      }
      if (rows.size() < limit) {
        return;
      }
      remaining -= available;
      last = rows.get(rows.size() - 1);
    }
  }

  private void appendVisibleBindings(
      List<GlossaryTermBindingPO> rows, GlossaryBindingAccess access, Map<Long, Preview> results) {
    budget.consume(rows.size());
    for (GlossaryManager.ResolvedBinding resolved : GlossaryManager.resolveBindings(rows, access)) {
      TermBinding binding = resolved.binding();
      Preview preview = results.get(resolved.termId());
      if (!preview.truncated()
          && readableTargets.computeIfAbsent(
              GlossaryBindingAccess.normalizeTarget(binding.object()), access::canReadTarget)) {
        preview.add(binding);
      }
    }
  }

  private List<GlossaryTermEntity> loadDefinitions(Set<Long> ids, boolean projectDomains) {
    if (budget != null) {
      for (List<Long> batch : Lists.partition(List.copyOf(ids), ID_BATCH_SIZE)) {
        budget.consume(
            SessionUtils.getWithoutCommit(
                GlossaryTermDomainRelMapper.class,
                mapper -> mapper.countLiveMemberships(lake, batch, budget.remaining() + 1)));
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
      @Nullable String literal,
      Set<String> keys) {
    Integer firstPageSize =
        budget == null
            ? null
            : Math.min(budget.remaining() + 1, literal == null ? Integer.MAX_VALUE : ID_BATCH_SIZE);
    List<GlossaryTermPO> matches = new ArrayList<>();
    Long cursor = null;
    while (true) {
      Long after = cursor;
      // A nearly exhausted budget can still have many SQL rows with different literal spacing.
      // Continue in batches after the first probe without charging those false positives.
      Integer limit = after == null ? firstPageSize : Integer.valueOf(ID_BATCH_SIZE);
      List<GlossaryTermPO> page =
          SessionUtils.getWithoutCommit(
              GlossaryTermMetaMapper.class,
              mapper ->
                  mapper.listLookupCandidates(lake, domains, pattern, status, keys, after, limit));
      List<GlossaryTermPO> eligible =
          literal == null
              ? page
              : page.stream()
                  .filter(row -> row.getName().toLowerCase(Locale.ROOT).contains(literal))
                  .toList();
      if (budget != null) {
        budget.consume(eligible.size());
      }
      if (literal == null) {
        return page;
      }
      matches.addAll(eligible);
      if (limit == null || page.size() < limit) {
        return matches;
      }
      cursor = page.get(page.size() - 1).getTermId();
    }
  }
}
