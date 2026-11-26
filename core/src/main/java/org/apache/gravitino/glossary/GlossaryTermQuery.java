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

  record Preview(List<TermBindingDTO> bindings, boolean truncated) {
    private static final Preview EMPTY = new Preview(List.of(), false);
  }

  Map<Long, Preview> previews(Set<Long> ids) {
    // The full binding list requires the same capability even for a term with no bindings.
    GlossaryBindingAccess bindingAccess = manager.bindingAccess(metalake, lake);
    Map<Long, Preview> results = new HashMap<>();
    ids.forEach(id -> results.put(id, Preview.EMPTY));
    List<Long> bound = new ArrayList<>();
    if (ids.size() == 1) {
      bound.addAll(ids);
    } else {
      for (List<Long> batch : Lists.partition(List.copyOf(ids), ID_BATCH_SIZE)) {
        bound.addAll(
            SessionUtils.getWithoutCommit(
                GlossaryTermBindingMapper.class,
                mapper -> mapper.listTermsWithBindings(lake, batch)));
      }
    }
    for (int i = 0; i < bound.size(); i++) {
      long id = bound.get(i);
      // Spare capacity permits full batches even when only one more visible binding is needed.
      // Otherwise preserve the remaining roots' budget by resolving only what can affect this
      // preview.
      int pending = bound.size() - i;
      boolean readAhead =
          pending == 1 || (long) pending * MAX_BINDING_CANDIDATES <= budget.remaining();
      results.put(id, preview(id, bindingAccess, readAhead));
    }
    return results;
  }

  private Preview preview(long termId, GlossaryBindingAccess bindingAccess, boolean readAhead) {
    List<TermBindingDTO> visibleBindings = new ArrayList<>();
    GlossaryTermBindingPO last = null;
    int remaining = Math.min(MAX_BINDING_CANDIDATES, budget.remaining());
    while (true) {
      int pageSize = Math.min(MAX_VISIBLE_BINDINGS + 1, remaining + 1);
      GlossaryTermBindingPO after = last;
      List<GlossaryTermBindingPO> rows =
          SessionUtils.getWithoutCommit(
              GlossaryTermBindingMapper.class,
              mapper -> mapper.listBindingCandidates(lake, termId, after, pageSize));
      // An overflow row proves there is more work, but must never be resolved or authorized.
      int available = Math.min(rows.size(), remaining);
      int offset = 0;
      while (offset < available) {
        int needed = MAX_VISIBLE_BINDINGS + 1 - visibleBindings.size();
        int end = readAhead ? available : Math.min(available, offset + needed);
        List<GlossaryTermBindingPO> batch = rows.subList(offset, end);
        budget.consume(batch.size());
        if (appendVisibleBindings(batch, bindingAccess, visibleBindings)) {
          return new Preview(visibleBindings, true);
        }
        offset = end;
      }
      remaining -= available;
      if (rows.size() > available) {
        throw TermLookupBudget.exceeded();
      }
      if (rows.size() < pageSize) {
        return new Preview(visibleBindings, false);
      }
      last = rows.get(rows.size() - 1);
    }
  }

  /** Appends readable bindings and reports whether the first omitted binding was found. */
  private boolean appendVisibleBindings(
      List<GlossaryTermBindingPO> batch,
      GlossaryBindingAccess access,
      List<TermBindingDTO> visibleBindings) {
    for (TermBinding binding : GlossaryManager.resolveBindings(batch, access)) {
      if (readableTargets.computeIfAbsent(
          GlossaryBindingAccess.normalizeTarget(binding.object()), access::canReadTarget)) {
        if (visibleBindings.size() == MAX_VISIBLE_BINDINGS) {
          return true;
        }
        visibleBindings.add(TermBindingDTO.from(binding));
      }
    }
    return false;
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
