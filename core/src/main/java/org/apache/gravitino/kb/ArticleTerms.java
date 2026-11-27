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

package org.apache.gravitino.kb;

import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.gravitino.exceptions.NoSuchTermException;
import org.apache.gravitino.exceptions.UnapprovedTermException;
import org.apache.gravitino.glossary.GlossaryAccess;
import org.apache.gravitino.glossary.GlossaryTerm;
import org.apache.gravitino.kb.KnowledgeBaseArticleEntity.Binding;
import org.apache.gravitino.meta.GlossaryTermNames;
import org.apache.gravitino.storage.relational.mapper.ArticleSummaryMapper;
import org.apache.gravitino.storage.relational.mapper.ArticleSummaryMapper.BoundTerm;
import org.apache.gravitino.storage.relational.mapper.KnowledgeBaseArticleMapper;
import org.apache.gravitino.storage.relational.po.GlossaryTermPO;
import org.apache.gravitino.storage.relational.service.GlossaryTermMetaService;
import org.apache.gravitino.storage.relational.service.MetalakeMetaService;
import org.apache.gravitino.storage.relational.utils.SessionUtils;

/** Resolves and authorizes Term bindings using the Glossary's name and visibility rules. */
final class ArticleTerms {
  private static final int BATCH_SIZE = 128;
  private static final String TERM = "GLOSSARY_TERM";
  private final String metalake;
  private final boolean authorization;

  ArticleTerms(String metalake, boolean authorization) {
    this.metalake = metalake;
    this.authorization = authorization;
  }

  record Edit(boolean add, List<String> names) {}

  record Plan(List<Edit> edits, Map<String, Long> ids) {
    Set<Binding> targets() {
      return ids.values().stream().map(id -> new Binding(TERM, id)).collect(Collectors.toSet());
    }
  }

  static List<Edit> edits(KnowledgeBaseArticleChange[] changes) {
    List<Edit> edits = new ArrayList<>();
    for (KnowledgeBaseArticleChange change : changes) {
      if (change instanceof KnowledgeBaseArticleChange.AddTerms add) {
        edits.add(new Edit(true, add.getTerms()));
      } else if (change instanceof KnowledgeBaseArticleChange.RemoveTerms remove) {
        edits.add(new Edit(false, remove.getTerms()));
      }
    }
    return edits;
  }

  Plan resolve(long lake, List<Edit> edits) {
    Set<String> keys = new HashSet<>();
    edits.forEach(edit -> edit.names().forEach(name -> keys.add(GlossaryTermNames.key(name))));
    Map<String, Long> ids = GlossaryTermMetaService.getInstance().getIdsByKeys(lake, keys);
    return new Plan(edits, ids);
  }

  long lake() {
    return MetalakeMetaService.getInstance().getMetalakeIdByName(metalake);
  }

  /** Rechecks the name, stable ID and current status while the target rows are fenced. */
  Collection<CheckedEdit> check(long lake, Plan plan) {
    Map<Long, GlossaryTermPO> live = new HashMap<>();
    for (List<Long> batch : Lists.partition(List.copyOf(plan.ids().values()), BATCH_SIZE)) {
      for (var row :
          SessionUtils.getWithoutCommitFresh(
              KnowledgeBaseArticleMapper.class, mapper -> mapper.termTargets(lake, batch))) {
        live.put(row.getTermId(), row);
      }
    }
    GlossaryAccess access = GlossaryAccess.forRequest(metalake, authorization);
    Map<Long, CheckedEdit> checked = new LinkedHashMap<>();
    for (Edit edit : plan.edits()) {
      for (String name : edit.names()) {
        String key = GlossaryTermNames.key(name);
        GlossaryTermPO row = live.get(plan.ids().get(key));
        if (row == null || !key.equals(row.getNameKey()) || !readable(access, row)) {
          throw missing(name);
        }
        checked.put(row.getTermId(), new CheckedEdit(edit.add(), row));
      }
    }
    return checked.values();
  }

  record CheckedEdit(boolean add, GlossaryTermPO target) {}

  static Set<Binding> apply(Collection<CheckedEdit> checked, Set<Binding> before) {
    if (checked.isEmpty()) {
      return before;
    }
    Set<Binding> result = new HashSet<>(before);
    for (CheckedEdit edit : checked) {
      GlossaryTermPO row = edit.target();
      Binding binding = new Binding(TERM, row.getTermId());
      if (edit.add()) {
        if (result.add(binding) && !GlossaryTerm.Status.APPROVED.name().equals(row.getStatus())) {
          throw new UnapprovedTermException(row.getName());
        }
      } else {
        result.remove(binding);
      }
    }
    return result;
  }

  long readableTarget(long lake, String name) {
    String key = GlossaryTermNames.key(name);
    Long id = GlossaryTermMetaService.getInstance().getIdsByKeys(lake, Set.of(key)).get(key);
    if (id != null) {
      var rows =
          SessionUtils.getWithoutCommit(
              KnowledgeBaseArticleMapper.class, mapper -> mapper.termTargets(lake, List.of(id)));
      if (!rows.isEmpty()
          && readable(GlossaryAccess.forRequest(metalake, authorization), rows.get(0))) {
        return id;
      }
    }
    throw missing(name);
  }

  static void checkPage(int limit, int offset) {
    Preconditions.checkArgument(
        limit > 0 && limit <= ArticleListFilter.MAX_LIMIT,
        "limit must be between 1 and %s",
        ArticleListFilter.MAX_LIMIT);
    Preconditions.checkArgument(offset >= 0, "offset must not be negative");
  }

  record Term(String name, ArticleTermBinding.Status status) implements ArticleTermBinding {}

  record Window(TermBindingPage page, Set<Binding> ids) {}

  TermBindingPage page(long lake, long article, int limit, int offset) {
    return window(lake, article, limit, offset).page();
  }

  KnowledgeBaseArticleEntity visible(KnowledgeBaseArticleEntity article, int limit, int offset) {
    if (article.bindings().stream().noneMatch(binding -> TERM.equals(binding.targetType()))) {
      return article;
    }
    return preview(lake(), article, limit, offset);
  }

  KnowledgeBaseArticleEntity preview(
      long lake, KnowledgeBaseArticleEntity article, int limit, int offset) {
    Window visible = window(lake, article.id(), limit, offset);
    Set<Binding> bindings = new HashSet<>(article.bindings());
    bindings.removeIf(binding -> TERM.equals(binding.targetType()));
    bindings.addAll(visible.ids());
    return article.toBuilder()
        .withTerms(visible.page().terms())
        .withTermsHasMore(visible.page().hasMore())
        .withBindings(bindings)
        .build();
  }

  Map<Long, List<ArticleTermBinding>> previews(long lake, Set<Long> articles) {
    var access = Suppliers.memoize(() -> GlossaryAccess.forRequest(metalake, authorization));
    Map<Long, List<ArticleTermBinding>> previews = new HashMap<>();
    Set<Long> pending = new HashSet<>(articles);
    BoundTerm cursor = null;
    while (!pending.isEmpty()) {
      BoundTerm after = cursor;
      var rows =
          SessionUtils.getWithoutCommit(
              ArticleSummaryMapper.class,
              mapper -> mapper.pageTerms(lake, pending, after, BATCH_SIZE));
      for (BoundTerm row : rows) {
        if (pending.contains(row.articleId()) && access.get().canRead(row.termId(), row.status())) {
          var preview = previews.computeIfAbsent(row.articleId(), unused -> new ArrayList<>());
          preview.add(new Term(row.name(), ArticleTermBinding.Status.valueOf(row.status().name())));
          if (preview.size() > ArticleListFilter.DEFAULT_LIMIT) {
            // Keep the extra readable Term for hasMore and skip all later bindings of this Article.
            pending.remove(row.articleId());
          }
        }
      }
      if (rows.size() < BATCH_SIZE) {
        break;
      }
      cursor = rows.get(rows.size() - 1);
    }
    return previews;
  }

  private Window window(long lake, long article, int limit, int offset) {
    var access = Suppliers.memoize(() -> GlossaryAccess.forRequest(metalake, authorization));
    List<ArticleTermBinding> terms = new ArrayList<>();
    Set<Binding> ids = new HashSet<>();
    Long after = null;
    int remaining = offset;
    while (true) {
      Long cursor = after;
      List<GlossaryTermPO> rows =
          SessionUtils.getWithoutCommit(
              KnowledgeBaseArticleMapper.class,
              mapper -> mapper.boundTerms(lake, article, cursor, BATCH_SIZE));
      for (GlossaryTermPO row : rows) {
        if (readable(access.get(), row)) {
          if (remaining > 0) {
            remaining--;
          } else if (terms.size() == limit) {
            return new Window(new TermBindingPage(terms, true), ids);
          } else {
            terms.add(new Term(row.getName(), ArticleTermBinding.Status.valueOf(row.getStatus())));
            ids.add(new Binding(TERM, row.getTermId()));
          }
        }
      }
      if (rows.size() < BATCH_SIZE) {
        return new Window(new TermBindingPage(terms, false), ids);
      }
      after = rows.get(rows.size() - 1).getTermId();
    }
  }

  private static boolean readable(GlossaryAccess access, GlossaryTermPO row) {
    return access.canRead(row.getTermId(), GlossaryTerm.Status.valueOf(row.getStatus()));
  }

  private static NoSuchTermException missing(String name) {
    return new NoSuchTermException("Term %s does not exist", name);
  }
}
