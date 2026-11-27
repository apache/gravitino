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

package org.apache.gravitino.storage.relational.mapper.provider.base;

import java.util.Collection;
import org.apache.gravitino.storage.relational.mapper.DomainMemberMapper;
import org.apache.gravitino.storage.relational.mapper.DomainMetaMapper;
import org.apache.ibatis.annotations.Param;

/** Portable SQL for glossary term domain membership on H2, MySQL and PostgreSQL. */
public class GlossaryTermDomainRelBaseSQLProvider {
  private static final String TABLE = DomainMemberMapper.TABLE_NAME;

  /** Inserts a live membership row. */
  public String insert(
      @Param("metalakeId") long metalakeId,
      @Param("termId") long termId,
      @Param("domainId") long domainId) {
    return "INSERT INTO "
        + TABLE
        + " (metalake_id, member_type, member_id, domain_id, audit_info)"
        + " SELECT metalake_id, 'GLOSSARY_TERM', term_id, #{domainId}, #{audit} FROM glossary_term_meta"
        + " WHERE metalake_id=#{metalakeId} AND term_id=#{termId} AND deleted_at=0";
  }

  /** Lists live memberships of a batch of terms, joined to live domains, in a stable order. */
  public String listDomainNames(
      @Param("metalakeId") long metalakeId, @Param("termIds") Collection<Long> termIds) {
    return "<script>SELECT r.member_id AS termId, r.domain_id AS domainId,"
        + " d.domain_name AS domainName FROM "
        + TABLE
        + " r JOIN "
        + DomainMetaMapper.DOMAIN_TABLE_NAME
        + " d ON d.domain_id=r.domain_id AND d.metalake_id=r.metalake_id AND d.deleted_at=0"
        + " WHERE r.member_type='GLOSSARY_TERM' AND r.metalake_id=#{metalakeId} AND r.deleted_at=0 AND r.member_id IN "
        + "<foreach collection='termIds' item='id' open='(' separator=',' close=')'>#{id}</foreach>"
        + " ORDER BY r.member_id, d.domain_name</script>";
  }

  /** Lists the live member terms of a set of domains. */
  public String listTermIds(
      @Param("metalakeId") long metalakeId, @Param("domainIds") Collection<Long> domainIds) {
    return "<script>SELECT DISTINCT member_id FROM "
        + TABLE
        + " WHERE member_type='GLOSSARY_TERM' AND metalake_id=#{metalakeId} AND deleted_at=0 AND domain_id IN "
        + "<foreach collection='domainIds' item='id' open='(' separator=',' close=')'>#{id}"
        + "</foreach></script>";
  }

  /** Counts at most the remaining membership budget and one overflow row before hydrating roots. */
  public String countLiveMemberships() {
    return """
        <script>
        SELECT COUNT(*) FROM (SELECT 1 FROM %s m
          JOIN %s d ON d.domain_id=m.domain_id AND d.metalake_id=m.metalake_id AND d.deleted_at=0
          WHERE m.metalake_id=#{metalakeId} AND m.member_type='GLOSSARY_TERM' AND m.deleted_at=0
          AND m.member_id IN
          <foreach collection="termIds" item="id" open="(" separator="," close=")">#{id}</foreach>
          LIMIT #{limit}) memberships
        </script>
        """
        .formatted(TABLE, DomainMetaMapper.DOMAIN_TABLE_NAME);
  }
}
