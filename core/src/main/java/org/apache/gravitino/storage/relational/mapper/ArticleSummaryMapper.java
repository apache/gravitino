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

import java.util.Collection;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.glossary.GlossaryTerm;
import org.apache.ibatis.annotations.Arg;
import org.apache.ibatis.annotations.ConstructorArgs;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

/** Bulk reads of live bindings and memberships for a page of Article summaries. */
public interface ArticleSummaryMapper {
  /** A live Term bound to one Article on the page. */
  record BoundTerm(long articleId, long termId, String name, GlossaryTerm.Status status) {}

  /** A live Domain assigned to one Article on the page. */
  record MemberDomain(long articleId, String name) {}

  /** The live principal named by an Article's current owner relation. */
  record ArticleOwner(long articleId, String name, Owner.Type type) {}

  /** Resolves the same current owner used to filter drafts, for a bounded page of Articles. */
  @ConstructorArgs({
    @Arg(column = "article_id", javaType = long.class),
    @Arg(column = "owner_name", javaType = String.class),
    @Arg(column = "owner_type", javaType = Owner.Type.class)
  })
  @Select(
      """
      <script>
      SELECT a.article_id, COALESCE(u.user_name, g.group_name) AS owner_name, o.owner_type
      FROM kb_article_meta a JOIN owner_meta o ON o.id=
      """
          + ArticleSearchSQLProvider.CURRENT_OWNER_ID
          + """
      LEFT JOIN user_meta u ON o.owner_type='USER' AND o.owner_id=u.user_id
        AND o.metalake_id=u.metalake_id AND u.deleted_at=0
      LEFT JOIN group_meta g ON o.owner_type='GROUP' AND o.owner_id=g.group_id
        AND o.metalake_id=g.metalake_id AND g.deleted_at=0
      WHERE a.metalake_id=#{lake} AND a.deleted_at=0
        AND (u.user_id IS NOT NULL OR g.group_id IS NOT NULL) AND a.article_id IN
        <foreach item="id" collection="articles" open="(" separator="," close=")">#{id}</foreach>
      </script>
      """)
  List<ArticleOwner> owners(@Param("lake") long lake, @Param("articles") Collection<Long> articles);

  /** Reads a bounded batch in stable Article and Term identity order. */
  @ConstructorArgs({
    @Arg(column = "article_id", javaType = long.class),
    @Arg(column = "term_id", javaType = long.class),
    @Arg(column = "name", javaType = String.class),
    @Arg(column = "status", javaType = GlossaryTerm.Status.class)
  })
  @Select(
      """
      <script>
      SELECT r.article_id, t.term_id, t.name, t.status
      FROM kb_article_relation_meta r JOIN glossary_term_meta t
        ON r.target_id=t.term_id AND r.metalake_id=t.metalake_id AND t.deleted_at=0
      WHERE r.metalake_id=#{lake} AND r.target_type='GLOSSARY_TERM' AND r.deleted_at=0
        AND r.article_id IN
        <foreach item="id" collection="articles" open="(" separator="," close=")">#{id}</foreach>
        <if test="after != null">AND (r.article_id &gt; #{after.articleId}
          OR (r.article_id = #{after.articleId} AND r.target_id &gt; #{after.termId}))</if>
      ORDER BY r.article_id, r.target_id LIMIT #{limit}
      </script>
      """)
  List<BoundTerm> pageTerms(
      @Param("lake") long lake,
      @Param("articles") Collection<Long> articles,
      @Param("after") @Nullable BoundTerm after,
      @Param("limit") int limit);

  /** Lists current Domain names for the selected Article identities in the same snapshot. */
  @ConstructorArgs({
    @Arg(column = "member_id", javaType = long.class),
    @Arg(column = "domain_name", javaType = String.class)
  })
  @Select(
      """
      <script>
      SELECT r.member_id, d.domain_name FROM domain_member_rel r JOIN domain_meta d
        ON r.domain_id=d.domain_id AND r.metalake_id=d.metalake_id AND d.deleted_at=0
      WHERE r.metalake_id=#{lake} AND r.member_type='KNOWLEDGE_BASE_ARTICLE' AND r.deleted_at=0
        AND r.member_id IN
        <foreach item="id" collection="articles" open="(" separator="," close=")">#{id}</foreach>
      </script>
      """)
  List<MemberDomain> domains(
      @Param("lake") long lake, @Param("articles") Collection<Long> articles);
}
