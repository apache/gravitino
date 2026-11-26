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
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.GravitinoAuthorizer.ArticleOwnerIds;
import org.apache.gravitino.kb.ArticleListFilter;
import org.apache.gravitino.kb.KnowledgeBaseArticleEntity.Binding;
import org.apache.gravitino.storage.relational.po.GlossaryTermPO;
import org.apache.gravitino.storage.relational.po.KnowledgeBaseArticlePO;
import org.apache.ibatis.annotations.Arg;
import org.apache.ibatis.annotations.ConstructorArgs;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.Update;

/** Article row and binding operations on the caller's SQL session. */
public interface KnowledgeBaseArticleMapper {
  /** Table used by metadata relation cleanup. */
  String TABLE_NAME = "kb_article_meta";

  /** Current Article columns mapped to the persistent object. */
  String COLUMNS =
      """
      article_id AS articleId, metalake_id AS metalakeId, article_name AS articleName, title,
          status, article_comment AS
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

  /** Reads the stored body and its revision in one statement. */
  @Select(
      """
      SELECT a.metalake_id AS metalakeId, a.article_id AS articleId, a.revision, a.status,
          a.body_format AS bodyFormat, a.body_content AS bodyContent
      FROM kb_article_meta a JOIN metalake_meta m ON a.metalake_id=m.metalake_id
      WHERE m.metalake_name=#{metalake} AND m.deleted_at=0 AND a.article_name=#{name} AND a.deleted_at=0
      """)
  @Nullable
  KnowledgeBaseArticlePO content(@Param("metalake") String metalake, @Param("name") String name);

  /** Resolves identity without reading the body. */
  @Select(
      """
      SELECT a.article_id AS articleId, a.metalake_id AS metalakeId, a.status
      FROM kb_article_meta a
      JOIN metalake_meta m ON a.metalake_id=m.metalake_id
      WHERE m.metalake_name=#{metalake} AND m.deleted_at=0 AND a.article_name=#{name} AND
          a.deleted_at=0
      """)
  @Nullable
  KnowledgeBaseArticlePO ids(@Param("metalake") String metalake, @Param("name") String name);

  /** Reads live Term identities, names and statuses for a bounded batch of binding targets. */
  @Select(
      """
      <script>
      SELECT term_id AS termId, name, name_key AS nameKey, status FROM glossary_term_meta
      WHERE metalake_id=#{lake} AND deleted_at=0 AND term_id IN
      <foreach item="id" collection="ids" open="(" separator="," close=")">#{id}</foreach>
      </script>
      """)
  List<GlossaryTermPO> termTargets(@Param("lake") long lake, @Param("ids") List<Long> ids);

  /** Scans live directly bound Terms in stable ID order without loading their definitions. */
  @Select(
      """
      <script>
      SELECT t.term_id AS termId, t.name, t.status FROM kb_article_relation_meta r
      JOIN glossary_term_meta t ON t.term_id=r.target_id AND t.metalake_id=r.metalake_id AND t.deleted_at=0
      WHERE r.metalake_id=#{lake} AND r.article_id=#{article} AND r.target_type='GLOSSARY_TERM'
        AND r.deleted_at=0
      <if test='after != null'>AND r.target_id &gt; #{after}</if>
      ORDER BY r.target_id LIMIT #{limit}
      </script>
      """)
  List<GlossaryTermPO> boundTerms(
      @Param("lake") long lake,
      @Param("article") long article,
      @Param("after") @Nullable Long after,
      @Param("limit") int limit);

  /** Selects the readable summary window after applying all resolved filters. */
  @SelectProvider(type = ArticleSearchSQLProvider.class, method = "search")
  List<KnowledgeBaseArticlePO> search(
      @Param("lake") long lake,
      @Param("filter") ArticleListFilter filter,
      @Param("domains") @Nullable Set<Long> domains,
      @Param("targets") List<Binding> targets,
      @Param("allDrafts") boolean allDrafts,
      @Param("ownerIds") ArticleOwnerIds ownerIds,
      @Param("pattern") @Nullable String pattern,
      @Param("nulPatternParts") @Nullable String nulPatternParts);

  /** Reads metadata and relations together in a caller's transaction. */
  @SelectProvider(type = KnowledgeBaseArticleLookupSQL.class, method = "joined")
  List<KnowledgeBaseArticlePO> joined(
      @Param("metalake") String metalake,
      @Param("name") @Nullable String name,
      @Param("domains") @Nullable List<Long> domains,
      @Param("includeBindings") boolean includeBindings);

  /** Reads metadata and relations for a batch of Articles from the same snapshot. */
  @SelectProvider(type = KnowledgeBaseArticleLookupSQL.class, method = "joinedBatch")
  List<KnowledgeBaseArticlePO> joinedBatch(
      @Param("metalake") String metalake, @Param("names") List<String> names);

  /** Returns the occupied candidates without loading Article content. */
  @Select(
      """
      <script>
      SELECT a.article_name FROM kb_article_meta a JOIN metalake_meta m ON m.metalake_id=a.metalake_id
      WHERE m.metalake_name=#{metalake} AND m.deleted_at=0 AND a.deleted_at=0 AND a.article_name IN
      <foreach item="name" collection="names" open="(" separator="," close=")">#{name}</foreach>
      </script>
      """)
  Set<String> occupiedNames(@Param("metalake") String metalake, @Param("names") List<String> names);

  /** Loads the stored fields used for an Article summary, without fetching content. */
  @Select(
      "SELECT article_id AS articleId, article_name AS articleName, title, status, audit_info AS auditInfo"
          + LIVE
          + " ORDER BY article_id")
  List<KnowledgeBaseArticlePO> summaries(@Param("metalakeId") long metalakeId);

  /** Current name and ancestor IDs for a live asset in a binding page. */
  record AssetRow(
      long id,
      String name,
      @Nullable Long catalogId,
      @Nullable String catalogName,
      @Nullable Long schemaId,
      @Nullable String schemaName,
      @Nullable Long tableId,
      @Nullable String tableName) {}

  /** Resolves live targets in the lake, including the current version of table columns. */
  @SelectProvider(type = ArticleAssetSQLProvider.class, method = "liveAssets")
  @ConstructorArgs({
    @Arg(column = "id", javaType = long.class),
    @Arg(column = "name", javaType = String.class),
    @Arg(column = "catalogId", javaType = Long.class),
    @Arg(column = "catalogName", javaType = String.class),
    @Arg(column = "schemaId", javaType = Long.class),
    @Arg(column = "schemaName", javaType = String.class),
    @Arg(column = "tableId", javaType = Long.class),
    @Arg(column = "tableName", javaType = String.class)
  })
  List<AssetRow> liveAssets(
      @Param("lake") long lake,
      @Param("type") MetadataObject.Type type,
      @Param("ids") List<Long> ids);

  /** Scans a bounded batch of asset bindings after a stable composite key. */
  @Select(
      """
      <script>
      SELECT target_type, target_id FROM kb_article_relation_meta
      WHERE metalake_id=#{lake} AND article_id=#{article} AND deleted_at=0
        AND target_type != 'GLOSSARY_TERM'
      <if test='after != null'>
        AND (target_id &gt; #{after.targetId} OR
          (target_id=#{after.targetId} AND target_type &gt; #{after.targetType}))
      </if>
      ORDER BY target_id, target_type LIMIT #{limit}
      </script>
      """)
  @ConstructorArgs({
    @Arg(column = "target_type", javaType = String.class),
    @Arg(column = "target_id", javaType = long.class)
  })
  List<Binding> boundAssets(
      @Param("lake") long lake,
      @Param("article") long article,
      @Param("after") @Nullable Binding after,
      @Param("limit") int limit);

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

  /**
   * Compares an explicit replacement without returning the stored text. The byte length also
   * distinguishes trailing spaces under MySQL PAD SPACE collations.
   */
  @Select(
      """
      SELECT COUNT(*) FROM kb_article_meta WHERE article_id=#{id} AND deleted_at=0
          AND body_format=#{format} AND body_content=#{content} AND OCTET_LENGTH(body_content)=#{bytes}
      """)
  boolean matchesBody(
      @Param("id") long id,
      @Param("format") String format,
      @Param("content") String content,
      @Param("bytes") int bytes);

  /** Updates the live Article at the given revision and returns the number of rows updated. */
  @Update(
      """
      <script>
      UPDATE kb_article_meta SET
        <if test="before.title != after.title">title=#{after.title},</if>
        <if test="before.status != after.status">status=#{after.status},</if>
        <if test="before.articleComment != after.articleComment">article_comment=#{after.articleComment},</if>
        <if test="before.synonyms != after.synonyms">synonyms=#{after.synonyms},</if>
        <if test="before.sourceReference != after.sourceReference">source_reference=#{after.sourceReference},</if>
        <if test="before.properties != after.properties">properties=#{after.properties},</if>
        <if test="after.bodyContent != null">body_format=#{after.bodyFormat}, body_content=#{after.bodyContent},</if>
        revision=#{after.revision}, audit_info=#{after.auditInfo}
      WHERE article_id=#{after.articleId} AND revision=#{before.revision} AND deleted_at=0
      </script>
      """)
  int update(
      @Param("before") KnowledgeBaseArticlePO before, @Param("after") KnowledgeBaseArticlePO after);

  /** Adds a new binding in the same transaction as the Article revision. */
  @Insert(
      """
      INSERT INTO kb_article_relation_meta (metalake_id, article_id, target_type, target_id,
          audit_info)
      VALUES (#{p.metalakeId}, #{p.articleId}, #{b.targetType}, #{b.targetId}, #{audit})
      """)
  void bind(
      @Param("p") KnowledgeBaseArticlePO row,
      @Param("b") Binding binding,
      @Param("audit") String audit);

  /** Retires a removed binding. */
  @Update(
      """
      UPDATE kb_article_relation_meta
      SET deleted_at=#{now}
      WHERE article_id=#{id} AND target_type=#{b.targetType} AND target_id=#{b.targetId} AND
          deleted_at=0
      """)
  void unbind(@Param("id") long id, @Param("b") Binding binding, @Param("now") long now);

  /** Retires the live Article at the given revision and returns the number of rows updated. */
  @Update(
      """
      UPDATE kb_article_meta SET deleted_at=#{now}
      WHERE article_id=#{id} AND revision=#{revision} AND deleted_at=0
      """)
  int delete(@Param("id") long id, @Param("revision") long revision, @Param("now") long now);

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
      ORDER BY deleted_at, article_id
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
      ORDER BY deleted_at, id
      LIMIT #{limit}) expired)
      """)
  int purgeBindings(@Param("cutoff") long cutoff, @Param("limit") int limit);
}
