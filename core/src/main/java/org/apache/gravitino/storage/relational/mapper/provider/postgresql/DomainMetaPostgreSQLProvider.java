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
package org.apache.gravitino.storage.relational.mapper.provider.postgresql;

import static org.apache.gravitino.storage.relational.mapper.DomainMetaMapper.DOMAIN_TABLE_NAME;

import org.apache.gravitino.storage.relational.mapper.provider.DatabaseTimeSQL;
import org.apache.gravitino.storage.relational.mapper.provider.base.DomainMetaBaseSQLProvider;
import org.apache.ibatis.annotations.Param;

/** The SQL of the {@code domain_meta} table for PostgreSQL. */
public class DomainMetaPostgreSQLProvider extends DomainMetaBaseSQLProvider {

  @Override
  public String selectDomainMetaByIdForShare(@Param("domainId") Long domainId) {
    return selectLockableDomainMetaById(domainId) + " FOR SHARE";
  }

  @Override
  public String softDeleteDomainMetaByIdAndVersion(
      @Param("domainId") Long domainId, @Param("currentVersion") Long currentVersion) {
    return "UPDATE "
        + DOMAIN_TABLE_NAME
        + " SET deleted_at = "
        + DatabaseTimeSQL.POSTGRESQL
        + " WHERE domain_id = #{domainId} AND current_version = #{currentVersion}"
        + " AND deleted_at = 0";
  }

  @Override
  public String softDeleteDomainMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "UPDATE "
        + DOMAIN_TABLE_NAME
        + " SET deleted_at = "
        + DatabaseTimeSQL.POSTGRESQL
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }

  @Override
  public String deleteDomainMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + DOMAIN_TABLE_NAME
        + " WHERE domain_id IN (SELECT domain_id FROM "
        + DOMAIN_TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit})";
  }
}
