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

import java.util.List;
import javax.annotation.Nullable;
import org.apache.gravitino.kb.KnowledgeBaseArticleEntity.Binding;
import org.apache.gravitino.storage.relational.po.KnowledgeBaseArticlePO;
import org.apache.ibatis.annotations.Arg;
import org.apache.ibatis.annotations.ConstructorArgs;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

/** Article row and binding operations on the caller's SQL session. */
public interface KnowledgeBaseArticleMapper {
  /** Table used by metadata relation cleanup. */
  String TABLE_NAME = "kb_article_meta";

  /** Current Article columns mapped to the persistent object. */
  String COLUMNS =
      """
      article_id AS articleId, metalake_id AS metalakeId, article_name AS articleName, title,
          body_format AS bodyFormat, body_content AS bodyContent, status, article_comment AS
          articleComment, synonyms, source_reference AS sourceReference, properties, revision,
          audit_info AS auditInfo
      """;
  /** Predicate for live rows in the selected metalake. */
  String LIVE = " FROM kb_article_meta WHERE metalake_id=#{metalakeId} AND deleted_at=0";

  /** Loads the live row under an exclusive lock. */
  @Select("SELECT " + COLUMNS + LIVE + " AND article_name=#{name} FOR UPDATE")
  @Nullable
  KnowledgeBaseArticlePO lock(@Param("metalakeId") long metalakeId, @Param("name") String name);

  /** Retrieves a current row within the read snapshot. */
  @Select("SELECT " + COLUMNS + LIVE + " AND article_name=#{name}")
  @Nullable
  KnowledgeBaseArticlePO get(@Param("metalakeId") long metalakeId, @Param("name") String name);

  /** Lists current rows in stable identity order. */
  @Select("SELECT " + COLUMNS + LIVE + " ORDER BY article_id")
  List<KnowledgeBaseArticlePO> list(@Param("metalakeId") long metalakeId);

  /** Resolves identity without reading the body. */
  @Select(
      """
      SELECT a.article_id AS articleId, a.metalake_id AS metalakeId
      FROM kb_article_meta a
      JOIN metalake_meta m ON a.metalake_id=m.metalake_id
      WHERE m.metalake_name=#{metalake} AND m.deleted_at=0 AND a.article_name=#{name} AND
          a.deleted_at=0
      """)
  @Nullable
  KnowledgeBaseArticlePO ids(@Param("metalake") String metalake, @Param("name") String name);

  /**
   * Reads a complete Article in one statement when participating in a caller's write transaction.
   */
  @Select(
      """
      <script>
      SELECT a.title, a.status, a.synonyms, a.properties, a.revision, a.article_id AS articleId,
          a.metalake_id AS metalakeId, a.article_name AS articleName, a.body_format AS
          bodyFormat, a.body_content AS bodyContent, a.article_comment AS articleComment,
          a.source_reference AS sourceReference, a.audit_info AS auditInfo, b.target_type AS
          targetType, b.target_id AS targetId, b.domain_id AS domainId
      FROM kb_article_meta a
      JOIN metalake_meta m ON m.metalake_id=a.metalake_id
      LEFT JOIN (
        SELECT article_id, target_type, target_id, NULL AS domain_id
        FROM kb_article_relation_meta WHERE deleted_at=0
        UNION ALL
        SELECT member_id, NULL, NULL, domain_id FROM domain_member_rel
        WHERE member_type='KNOWLEDGE_BASE_ARTICLE' AND deleted_at=0
      ) b ON b.article_id=a.article_id
      WHERE m.metalake_name=#{metalake} AND m.deleted_at=0 AND a.deleted_at=0
      <if test='name != null'>AND a.article_name=#{name}</if>
      ORDER BY a.article_id
      </script>
      """)
  List<KnowledgeBaseArticlePO> joined(
      @Param("metalake") String metalake, @Param("name") @Nullable String name);

  /** Loads the stored fields used for an Article summary, without fetching content. */
  @Select(
      "SELECT article_id AS articleId, article_name AS articleName, title, status, audit_info AS auditInfo"
          + LIVE
          + " ORDER BY article_id")
  List<KnowledgeBaseArticlePO> summaries(@Param("metalakeId") long metalakeId);

  /** Returns complete binding identities for one Article. */
  @Select(
      """
      SELECT target_type, target_id
      FROM kb_article_relation_meta
      WHERE article_id=#{id} AND deleted_at=0
      ORDER BY target_type, target_id
      """)
  @ConstructorArgs({
    @Arg(column = "target_type", javaType = String.class),
    @Arg(column = "target_id", javaType = long.class)
  })
  List<Binding> bindings(@Param("id") long id);

  /** Loads stable binding identities for a full-metalake snapshot without joining large bodies. */
  @Select(
      """
      SELECT article_id AS articleId, target_type AS targetType, target_id AS targetId
      FROM kb_article_relation_meta
      WHERE metalake_id = #{metalakeId} AND deleted_at = 0
      """)
  List<KnowledgeBaseArticlePO> listBindings(@Param("metalakeId") long metalakeId);

  /** Inserts current content and the initial revision. */
  @Insert(
      """
      INSERT INTO kb_article_meta (article_id, metalake_id, article_name, title, body_format,
          body_content, status, article_comment, synonyms, source_reference, properties,
          revision, audit_info)
      VALUES (#{articleId}, #{metalakeId}, #{articleName}, #{title}, #{bodyFormat},
          #{bodyContent}, #{status}, #{articleComment}, #{synonyms}, #{sourceReference},
          #{properties}, #{revision}, #{auditInfo})
      """)
  void insert(KnowledgeBaseArticlePO row);

  /** Replaces the locked row's mutable fields. */
  @Update(
      """
      UPDATE kb_article_meta
      SET title=#{title}, body_format=#{bodyFormat}, body_content=#{bodyContent},
          status=#{status}, article_comment=#{articleComment}, synonyms=#{synonyms},
          source_reference=#{sourceReference}, properties=#{properties}, revision=#{revision},
          audit_info=#{auditInfo}
      WHERE article_id=#{articleId} AND deleted_at=0
      """)
  void update(KnowledgeBaseArticlePO row);

  /** Adds a new binding in the same transaction as the Article revision. */
  @Insert(
      """
      INSERT INTO kb_article_relation_meta (metalake_id, article_id, target_type, target_id,
          audit_info)
      VALUES (#{p.metalakeId}, #{p.articleId}, #{b.targetType}, #{b.targetId}, #{p.auditInfo})
      """)
  void bind(@Param("p") KnowledgeBaseArticlePO row, @Param("b") Binding binding);

  /** Retires a removed binding. */
  @Update(
      """
      UPDATE kb_article_relation_meta
      SET deleted_at=#{now}
      WHERE article_id=#{id} AND target_type=#{b.targetType} AND target_id=#{b.targetId} AND
          deleted_at=0
      """)
  void unbind(@Param("id") long id, @Param("b") Binding binding, @Param("now") long now);

  /** Retires the locked Article. */
  @Update("UPDATE kb_article_meta SET deleted_at=#{now} WHERE article_id=#{id} AND deleted_at=0")
  void delete(@Param("id") long id, @Param("now") long now);

  /** Retires all live bindings owned by an Article. */
  @Update(
      "UPDATE kb_article_relation_meta SET deleted_at=#{now} WHERE article_id=#{id} AND deleted_at=0")
  void deleteBindings(@Param("id") long id, @Param("now") long now);

  /** Retires all Articles in a fenced metalake. */
  @Update("UPDATE kb_article_meta SET deleted_at=#{now} WHERE metalake_id=#{id} AND deleted_at=0")
  void dropMetalake(@Param("id") long id, @Param("now") long now);

  /** Retires all bindings in a fenced metalake. */
  @Update(
      "UPDATE kb_article_relation_meta SET deleted_at=#{now} WHERE metalake_id=#{id} AND deleted_at=0")
  void dropMetalakeBindings(@Param("id") long id, @Param("now") long now);

  /** Purges a bounded batch of retired Articles. */
  @Delete(
      """
      DELETE
      FROM kb_article_meta
      WHERE article_id IN (SELECT article_id
      FROM (SELECT article_id
      FROM kb_article_meta
      WHERE deleted_at>0 AND deleted_at<#{cutoff}
      ORDER BY article_id
      LIMIT #{limit}) expired)
      """)
  int purge(@Param("cutoff") long cutoff, @Param("limit") int limit);

  /** Purges retired bindings, including those detached from still-live Articles. */
  @Delete(
      """
      DELETE
      FROM kb_article_relation_meta
      WHERE id IN (SELECT id
      FROM (SELECT id
      FROM kb_article_relation_meta
      WHERE deleted_at>0 AND deleted_at<#{cutoff}
      ORDER BY id
      LIMIT #{limit}) expired)
      """)
  int purgeBindings(@Param("cutoff") long cutoff, @Param("limit") int limit);
}
