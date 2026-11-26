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
package org.apache.gravitino.storage.relational.mapper;

import javax.annotation.Nullable;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.builder.annotation.ProviderContext;

/** Single-statement Article metadata and relation lookups. */
public final class KnowledgeBaseArticleLookupSQL {
  private KnowledgeBaseArticleLookupSQL() {}

  /** Returns the query for one Article or all Articles in a metalake. */
  public static String joined(@Param("name") @Nullable String name, ProviderContext context) {
    return lookup(name == null ? "" : "AND a.article_name=#{name}", context);
  }

  /** Returns the query for a batch of names in a metalake. */
  public static String joinedBatch(ProviderContext context) {
    return lookup(
        """
        AND a.article_name IN
        <foreach item="name" collection="names" open="(" separator="," close=")">#{name}</foreach>
        """,
        context);
  }

  private static String lookup(String names, ProviderContext context) {
    // For named lookups, MySQL can otherwise scan every active relation in the metalake.
    boolean useArticleIndex = !names.isEmpty() && "mysql".equals(context.getDatabaseId());
    // Scope each relation branch before the union. MySQL 5.7 also supports this derived table.
    String scope = " WHERE m.metalake_name=#{metalake} AND m.deleted_at=0 " + names;
    String articles =
        """
        FROM metalake_meta m
        JOIN kb_article_meta a ON a.metalake_id=m.metalake_id AND a.deleted_at=0
        """;
    return """
        <script>
        SELECT a.article_id AS articleId, m.metalake_id AS metalakeId, a.article_name AS articleName,
            a.title, a.status, a.article_comment AS articleComment, a.synonyms,
            a.source_reference AS sourceReference, a.properties, a.revision, a.audit_info AS auditInfo,
            NULL AS targetType, NULL AS targetId, NULL AS domainId
        FROM metalake_meta m
        LEFT JOIN kb_article_meta a ON a.metalake_id=m.metalake_id AND a.deleted_at=0
        %1$s
        UNION ALL
        SELECT r.article_id, r.metalake_id, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
            r.target_type, r.target_id, r.domain_id
        FROM (
          SELECT a.article_id, a.metalake_id, b.target_type, b.target_id, NULL AS domain_id
          %2$s
          JOIN kb_article_relation_meta b %3$s ON b.metalake_id=a.metalake_id AND b.article_id=a.article_id
              AND b.deleted_at=0
          %1$s
          UNION ALL
          SELECT a.article_id, a.metalake_id, NULL, NULL, d.domain_id
          %2$s
          JOIN domain_member_rel d %4$s ON d.metalake_id=a.metalake_id AND d.member_id=a.article_id
              AND d.member_type='KNOWLEDGE_BASE_ARTICLE' AND d.deleted_at=0
          %1$s
        ) r
        ORDER BY articleId
        </script>
        """
        .formatted(
            scope,
            articles,
            useArticleIndex ? "FORCE INDEX (uk_kba_binding)" : "",
            useArticleIndex ? "FORCE INDEX (uk_domain_member)" : "");
  }
}
