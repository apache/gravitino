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

package org.apache.gravitino.storage.relational.service;

import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.gravitino.authorization.GravitinoAuthorizer.ArticleOwnerIds;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.kb.ArticleListFilter;
import org.apache.gravitino.kb.KnowledgeBaseArticleEntity.Binding;
import org.apache.gravitino.storage.relational.mapper.KnowledgeBaseArticleMapper;
import org.apache.gravitino.storage.relational.po.KnowledgeBaseArticlePO;
import org.apache.gravitino.storage.relational.utils.SessionUtils;

/** Retrieves current summary candidates from the relational store inside the caller's snapshot. */
public final class ArticleSearchService {
  private ArticleSearchService() {}

  /**
   * Returns up to one extra matching row to determine whether a readable page follows. The caller
   * authorizes VIEW, resolves readable Domains and binding targets and supplies only the principal
   * IDs eligible to read owned drafts. A null Domain set leaves Domain membership unrestricted.
   * Keyword matching uses the database's native LOWER. For example, {@code izmir} matches {@code
   * İzmir} on MySQL (utf8mb4_bin) and PostgreSQL (en_US.utf8). H2 lowercases the initial letter to
   * {@code i} followed by a combining dot, so the same query does not match.
   */
  public static List<KnowledgeBaseArticlePO> search(
      long lake,
      ArticleListFilter filter,
      @Nullable Set<Long> domains,
      List<Binding> targets,
      boolean allDrafts,
      ArticleOwnerIds ownerIds) {
    if (domains != null && domains.isEmpty()) {
      return List.of();
    }
    String pattern =
        filter.query() == null
            ? null
            : "%" + SchemaPOStorageOps.escapeLikeMetacharacters(filter.query()) + "%";
    // PostgreSQL cannot bind a NUL in text. Preserve each substring for database-side LOWER().
    String nulPatternParts =
        pattern != null && pattern.indexOf('\0') >= 0
            ? JsonUtils.anyFieldMapper().valueToTree(pattern.split("\0", -1)).toString()
            : null;
    return SessionUtils.getWithoutCommit(
        KnowledgeBaseArticleMapper.class,
        mapper ->
            mapper.search(
                lake, filter, domains, targets, allDrafts, ownerIds, pattern, nulPatternParts));
  }
}
