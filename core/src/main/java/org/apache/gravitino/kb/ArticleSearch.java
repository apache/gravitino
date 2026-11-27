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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.authorization.GravitinoAuthorizer.ArticleOwnerIds;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.kb.KnowledgeBaseArticleEntity.Binding;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.storage.relational.mapper.ArticleSummaryMapper;
import org.apache.gravitino.storage.relational.po.KnowledgeBaseArticlePO;
import org.apache.gravitino.storage.relational.service.ArticleSearchService;
import org.apache.gravitino.storage.relational.utils.SessionUtils;

/** Resolves Article search filters and projects one page within the caller's read snapshot. */
final class ArticleSearch {
  private final String metalake;
  private final boolean authorization;
  private final ArticleTerms terms;
  private final ArticleDomains domains;

  ArticleSearch(String metalake, boolean authorization) {
    this.metalake = metalake;
    this.authorization = authorization;
    this.terms = new ArticleTerms(metalake, authorization);
    this.domains = new ArticleDomains(metalake, authorization);
  }

  ArticlePage list(ArticleListFilter filter) {
    long lake;
    try {
      lake = terms.lake();
    } catch (NoSuchEntityException missing) {
      throw new NoSuchMetalakeException(missing, "Metalake %s does not exist", metalake);
    }

    List<Binding> targets = new ArrayList<>();
    if (filter.term() != null) {
      targets.add(
          new Binding(EntityType.GLOSSARY_TERM.name(), terms.readableTarget(lake, filter.term())));
    }
    if (filter.object() != null) {
      targets.add(new ArticleAssets(metalake, authorization).readableTarget(lake, filter.object()));
    }
    ArticleAccess access = authorization ? ArticleAccess.forRequest(metalake) : null;
    if (access != null && !access.canView()) {
      return ArticlePage.of(List.of(), false);
    }
    Set<Long> scope = filter.domain() == null ? null : domains.scope(filter.domain());
    boolean mayIncludeDrafts =
        targets.isEmpty() && (filter.status() == null || filter.status() == ArticleStatus.DRAFT);
    boolean allDrafts = mayIncludeDrafts && (access == null || access.canManage());
    ArticleOwnerIds ownerIds =
        mayIncludeDrafts && !allDrafts ? access.draftOwnerIds() : ArticleOwnerIds.NONE;
    var rows = ArticleSearchService.search(lake, filter, scope, targets, allDrafts, ownerIds);
    return page(lake, rows, filter.limit());
  }

  private ArticlePage page(long lake, List<KnowledgeBaseArticlePO> rows, int limit) {
    if (rows.isEmpty()) {
      return ArticlePage.of(List.of(), false);
    }
    boolean more = rows.size() > limit;
    var page = rows.subList(0, Math.min(rows.size(), limit));
    var ids = page.stream().map(KnowledgeBaseArticlePO::getArticleId).collect(Collectors.toSet());
    Map<Long, Owner> owners = new HashMap<>();
    for (var owner :
        SessionUtils.getWithoutCommit(
            ArticleSummaryMapper.class, mapper -> mapper.owners(lake, ids))) {
      owners.put(owner.articleId(), new DirectOwner(owner.name(), owner.type()));
    }
    var memberships = domains.pageNames(lake, ids);
    var previews = terms.previews(lake, ids);
    List<ArticleSummary> summaries = new ArrayList<>(page.size());
    for (var row : page) {
      long id = row.getArticleId();
      summaries.add(
          new Summary(
              row.getArticleName(),
              row.getTitle(),
              ArticleStatus.valueOf(row.getStatus()),
              memberships.getOrDefault(id, List.of()),
              owners.get(id),
              auditInfo(row),
              previews.getOrDefault(id, List.of())));
    }
    return ArticlePage.of(summaries, more);
  }

  private static AuditInfo auditInfo(KnowledgeBaseArticlePO row) {
    try {
      return JsonUtils.anyFieldMapper().readValue(row.getAuditInfo(), AuditInfo.class);
    } catch (IOException failure) {
      throw new UncheckedIOException(failure);
    }
  }

  record DirectOwner(String name, Owner.Type type) implements Owner {}

  record Summary(
      String name,
      String title,
      ArticleStatus status,
      List<String> domainNames,
      @Nullable Owner owner,
      AuditInfo auditInfo,
      List<ArticleTermBinding> preview)
      implements ArticleSummary {
    @Override
    public String[] domains() {
      return domainNames.toArray(String[]::new);
    }

    @Override
    public ArticleTermBinding[] terms() {
      return preview
          .subList(0, Math.min(preview.size(), ArticleListFilter.DEFAULT_LIMIT))
          .toArray(ArticleTermBinding[]::new);
    }

    @Override
    public boolean termsHasMore() {
      return preview.size() > ArticleListFilter.DEFAULT_LIMIT;
    }
  }
}
