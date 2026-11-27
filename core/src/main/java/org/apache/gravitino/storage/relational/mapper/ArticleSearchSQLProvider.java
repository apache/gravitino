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

import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.gravitino.kb.KnowledgeBaseArticle;
import org.apache.gravitino.storage.relational.JDBCBackend.JDBCBackendType;
import org.apache.ibatis.builder.annotation.ProviderContext;

/** Builds a body-free Article projection and applies resolved visibility before paging. */
public final class ArticleSearchSQLProvider {
  // Use the same live owner for draft visibility, direct-owner filtering and page projection.
  static final String CURRENT_OWNER_ID =
      """
      (SELECT current_owner.id FROM owner_meta current_owner
       WHERE current_owner.metadata_object_id=a.article_id AND current_owner.metalake_id=a.metalake_id
         AND current_owner.metadata_object_type='KNOWLEDGE_BASE_ARTICLE' AND current_owner.deleted_at=0
       ORDER BY current_owner.updated_at DESC, current_owner.id DESC LIMIT 1)
      """;
  private static final String LIKE = " LIKE LOWER(#{pattern}) ESCAPE '!'";
  // MySQL 5.7 lacks JSON_TABLE. The Article contract bounds the number of array elements.
  private static final String MYSQL_SLOTS =
      IntStream.range(0, KnowledgeBaseArticle.SYNONYMS_MAX_COUNT)
          .mapToObj(index -> "SELECT " + index + " AS pos")
          .collect(Collectors.joining(" UNION ALL "));

  private ArticleSearchSQLProvider() {}

  /** Generates the search statement for the configured metastore database. */
  public static String search(ProviderContext context) {
    return """
        <script>
        <bind name="limit" value="filter.limit() + 1"/>
        <bind name="owner" value="filter.owner()"/>
        <bind name="ownedDrafts" value="!allDrafts and targets.isEmpty and (ownerIds.userId != null or !ownerIds.groupIds.isEmpty)"/>
        SELECT a.article_id AS articleId, a.article_name AS articleName, a.title, a.status, a.audit_info AS auditInfo
        FROM kb_article_meta a
        <if test='owner != null or ownedDrafts'>
          LEFT JOIN owner_meta o ON o.id=%s
        </if>
        WHERE a.metalake_id=#{lake} AND a.deleted_at=0
        <if test='filter.status != null'>AND a.status=#{filter.status}</if>
        <choose>
          <when test='!targets.isEmpty'>
            AND (a.status='PUBLISHED' <if test='filter.includeDeprecated'>OR a.status='DEPRECATED'</if>)
          </when>
          <when test='!allDrafts'>
            AND (a.status IN ('PUBLISHED','DEPRECATED')
              <if test='ownerIds.userId != null'>OR (o.owner_type='USER' AND o.owner_id=#{ownerIds.userId})</if>
              <if test='!ownerIds.groupIds.isEmpty'>OR (o.owner_type='GROUP' AND o.owner_id IN
                <foreach collection='ownerIds.groupIds' item='id' open='(' separator=',' close=')'>#{id}</foreach>)
              </if>)
          </when>
        </choose>
        <if test='owner != null'>
          <bind name="ownerName" value="owner.name()"/>
          <bind name="ownerType" value="owner.type()"/>
          AND o.owner_type=#{ownerType} AND o.owner_id=(
          <choose>
            <when test='ownerType.name() == "USER"'>
              SELECT user_id FROM user_meta WHERE metalake_id=#{lake} AND deleted_at=0 AND user_name=#{ownerName}
            </when>
            <otherwise>
              SELECT group_id FROM group_meta WHERE metalake_id=#{lake} AND deleted_at=0 AND group_name=#{ownerName}
            </otherwise>
          </choose>)
        </if>
        <if test='domains != null'>
          AND EXISTS (SELECT 1 FROM domain_member_rel d
            WHERE d.metalake_id=a.metalake_id AND d.member_id=a.article_id
              AND d.member_type='KNOWLEDGE_BASE_ARTICLE' AND d.deleted_at=0 AND d.domain_id IN
            <foreach collection='domains' item='id' open='(' separator=',' close=')'>#{id}</foreach>)
        </if>
        <foreach collection='targets' item='target'>
          AND EXISTS (SELECT 1 FROM kb_article_relation_meta r
            WHERE r.metalake_id=a.metalake_id AND r.article_id=a.article_id AND r.deleted_at=0
              AND r.target_type=#{target.targetType} AND r.target_id=#{target.targetId})
        </foreach>
        <if test='pattern != null'>AND (%s)</if>
        ORDER BY a.title, a.article_name LIMIT #{limit} OFFSET #{filter.offset}
        </script>
        """
        .formatted(CURRENT_OWNER_ID, keyword(JDBCBackendType.fromString(context.getDatabaseId())));
  }

  private static String keyword(JDBCBackendType database) {
    String fields = "LOWER(a.title)" + LIKE + " OR LOWER(a.body_content)" + LIKE + " OR ";
    return switch (database) {
      case H2 ->
      // Array indexing preserves element boundaries. H2 returns a serialized JSON string, so
      // remove its outer quotes and decode the canonical escapes before applying LOWER().
      fields
          + "EXISTS (SELECT 1 FROM SYSTEM_RANGE(1,"
          + KnowledgeBaseArticle.SYNONYMS_MAX_COUNT
          + ") slots WHERE LOWER(STRINGDECODE(REGEXP_REPLACE(CAST((a.synonyms FORMAT JSON)[slots.x] AS VARCHAR), '^\"|\"$', '')))"
          + LIKE
          + ")";
      case MYSQL -> fields
          + "EXISTS (SELECT 1 FROM ("
          + MYSQL_SLOTS
          + ") slots WHERE LOWER(JSON_UNQUOTE(JSON_EXTRACT(a.synonyms, CONCAT('$[',slots.pos,']'))))"
          + LIKE
          + ")";
      case POSTGRESQL -> postgresKeyword(fields);
    };
  }

  private static String postgresKeyword(String fields) {
    // PostgreSQL text cannot contain NUL, but a JSON synonym may legitimately contain \u0000.
    // Split at real JSON NUL escapes while preserving escaped backslashes such as "\\u0000".
    String split = "'(?&lt;!\\\\)((?:\\\\\\\\)*)\\\\u0000', '\\1\",\"', 'g'";
    String fragments = "regexp_replace(a.synonyms, " + split + ")";
    String ordinary =
        fields
            + "EXISTS (SELECT 1 FROM json_array_elements_text(CAST("
            + fragments
            + " AS json)) s(value) WHERE LOWER(s.value)"
            + LIKE
            + ")";

    // Only a synonym can match a query containing NUL. Reassemble each lowercased synonym and
    // pattern as UTF-8 bytes so LIKE sees the original boundaries, including consecutive NULs.
    String folded =
        "SELECT string_agg(convert_to(LOWER(s.value), 'UTF8'), decode('00','hex') ORDER BY s.ord)"
            + " FROM json_array_elements_text(CAST(%s AS json)) WITH ORDINALITY s(value,ord)";
    String synonym = "'[' || regexp_replace(CAST(j.entry AS text), " + split + ") || ']'";
    return "<choose><when test='nulPatternParts != null'>"
        + "EXISTS (SELECT 1 FROM json_array_elements(CAST(a.synonyms AS json)) j(entry) WHERE ("
        + folded.formatted(synonym)
        + ") LIKE ("
        + folded.formatted("#{nulPatternParts}")
        + ") ESCAPE '!')</when><otherwise>"
        + ordinary
        + "</otherwise></choose>";
  }
}
