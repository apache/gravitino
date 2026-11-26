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
import org.apache.gravitino.storage.relational.po.GlossaryTermBindingPO;
import org.apache.gravitino.storage.relational.po.GlossaryTermPO;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

/** Lightweight reads for glossary browsing and exact word lookup in the current snapshot. */
public interface GlossaryTermLookupMapper {
  /**
   * Selects only the identity, name and status needed to authorize list results.
   *
   * @param lake the metalake ID
   * @param domains optional IDs of the visible domains in scope
   * @param pattern optional escaped, case-insensitive substring pattern
   * @param status optional root status
   * @param keys normalized names and synonyms, empty when browsing
   * @param limit optional lookup candidate bound including one overflow row
   * @return matching root candidates in stable ID order
   */
  @Select(
      """
      <script>
      SELECT t.term_id AS termId, t.name, t.status FROM glossary_term_meta t
      WHERE t.metalake_id=#{lake} AND t.deleted_at=0
      <if test="status != null">AND t.status=#{status}</if>
      <if test="pattern != null">AND LOWER(t.name) LIKE #{pattern} ESCAPE '!'</if>
      <if test="!keys.isEmpty()">AND (t.name_key IN
        <foreach collection="keys" item="k" open="(" separator="," close=")">#{k}</foreach>
        OR t.term_id IN (SELECT s.term_id FROM glossary_term_synonym s
          WHERE s.metalake_id=#{lake} AND s.deleted_at=0 AND s.synonym_key IN
          <foreach collection="keys" item="k" open="(" separator="," close=")">#{k}</foreach>))
      </if>
      <if test="domains != null">AND EXISTS (SELECT 1 FROM domain_member_rel m
        WHERE m.metalake_id=t.metalake_id AND m.member_type='GLOSSARY_TERM'
        AND m.member_id=t.term_id AND m.deleted_at=0 AND m.domain_id IN
        <foreach collection="domains" item="id" open="(" separator="," close=")">#{id}</foreach>)
      </if>
      ORDER BY t.term_id
      <if test="limit != null">LIMIT #{limit}</if>
      </script>
      """)
  List<GlossaryTermPO> candidates(
      @Param("lake") long lake,
      @Param("domains") @Nullable Collection<Long> domains,
      @Param("pattern") @Nullable String pattern,
      @Param("status") @Nullable String status,
      @Param("keys") Collection<String> keys,
      @Param("limit") @Nullable Integer limit);

  /**
   * Reads the names and visibility fields of live relation endpoints.
   *
   * @param lake the metalake ID
   * @param ids the endpoint IDs
   * @param status optional status filter
   * @return the endpoint candidates
   */
  @Select(
      """
      <script>
      SELECT term_id AS termId, name, status FROM glossary_term_meta
      WHERE metalake_id=#{lake} AND deleted_at=0 AND term_id IN
      <foreach collection="ids" item="id" open="(" separator="," close=")">#{id}</foreach>
      <if test="status != null">AND status=#{status}</if>
      </script>
      """)
  List<GlossaryTermPO> endpoints(
      @Param("lake") long lake,
      @Param("ids") Collection<Long> ids,
      @Param("status") @Nullable String status);

  /**
   * Counts at most the remaining membership budget and one overflow row before hydrating roots.
   *
   * @param lake the metalake ID
   * @param ids the root term IDs
   * @param limit the maximum count to return
   * @return number of live membership candidates up to the limit
   */
  @Select(
      """
      <script>
      SELECT COUNT(*) FROM (SELECT 1 FROM domain_member_rel m
        JOIN domain_meta d ON d.domain_id=m.domain_id AND d.metalake_id=m.metalake_id AND d.deleted_at=0
        WHERE m.metalake_id=#{lake} AND m.member_type='GLOSSARY_TERM' AND m.deleted_at=0
        AND m.member_id IN
        <foreach collection="ids" item="id" open="(" separator="," close=")">#{id}</foreach>
        LIMIT #{limit}) memberships
      </script>
      """)
  int countMemberships(
      @Param("lake") long lake, @Param("ids") Collection<Long> ids, @Param("limit") int limit);

  /**
   * Bounds authorization work before reading the subdomains of a domain filter.
   *
   * @param lake the metalake ID
   * @param parent the parent domain ID
   * @param limit the maximum count to return
   * @return the number of live child domains up to the limit
   */
  @Select(
      """
      SELECT COUNT(*) FROM (SELECT 1 FROM domain_rel r JOIN domain_meta d
        ON d.domain_id=r.child_id AND d.deleted_at=0 AND d.metalake_id=#{lake}
        WHERE r.parent_id=#{parent} AND r.deleted_at=0 LIMIT #{limit}) children
      """)
  int countChildren(
      @Param("lake") long lake, @Param("parent") long parent, @Param("limit") int limit);

  /**
   * Fetches binding candidates for a lookup preview in stable target order.
   *
   * @param lake the metalake ID
   * @param term the root term ID
   * @param limit the candidate budget including one overflow row
   * @return live candidates in target order
   */
  @Select(
      """
      SELECT target_type AS targetType, target_id AS targetId FROM glossary_term_binding
      WHERE metalake_id=#{lake} AND term_id=#{term} AND deleted_at=0
      ORDER BY target_type, target_id LIMIT #{limit}
      """)
  List<GlossaryTermBindingPO> bindingCandidates(
      @Param("lake") long lake, @Param("term") long term, @Param("limit") int limit);
}
