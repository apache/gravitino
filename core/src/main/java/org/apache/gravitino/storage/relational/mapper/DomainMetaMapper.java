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
import org.apache.gravitino.storage.relational.po.DomainPO;
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.UpdateProvider;

/** MyBatis mapper of the {@code domain_meta} table. */
public interface DomainMetaMapper {

  /** The name of the domain table. */
  String DOMAIN_TABLE_NAME = "domain_meta";

  /**
   * Counts deleted rows that still reserve the requested domain ID.
   *
   * @param domainId The domain ID.
   * @return The number of deleted rows with this ID.
   */
  @Select(
      "SELECT COUNT(*) FROM "
          + DOMAIN_TABLE_NAME
          + " WHERE domain_id = #{domainId} AND deleted_at > 0")
  int countDeletedDomainMetasById(@Param("domainId") Long domainId);

  /**
   * Lists the active domains of a metalake.
   *
   * @param metalakeName The metalake name.
   * @return The active domains.
   */
  @SelectProvider(type = DomainMetaSQLProviderFactory.class, method = "listDomainPOsByMetalake")
  List<DomainPO> listDomainPOsByMetalake(@Param("metalakeName") String metalakeName);

  /**
   * Selects an active domain by metalake name and domain name.
   *
   * @param metalakeName The metalake name.
   * @param domainName The domain name.
   * @return The domain, or null if it is not active.
   */
  @SelectProvider(
      type = DomainMetaSQLProviderFactory.class,
      method = "selectDomainMetaByMetalakeAndName")
  DomainPO selectDomainMetaByMetalakeAndName(
      @Param("metalakeName") String metalakeName, @Param("domainName") String domainName);

  /**
   * Selects an active domain by metalake ID and domain name, for ID lookups. The backing tag name
   * is not read.
   *
   * @param metalakeId The metalake ID.
   * @param domainName The domain name.
   * @return The domain, or null if it is not active.
   */
  @SelectProvider(
      type = DomainMetaSQLProviderFactory.class,
      method = "selectDomainMetaByMetalakeIdAndName")
  DomainPO selectDomainMetaByMetalakeIdAndName(
      @Param("metalakeId") Long metalakeId, @Param("domainName") String domainName);

  /**
   * Selects and exclusively locks an active domain by its natural key. The backing tag name is not
   * read, so that no tag row is locked.
   *
   * @param metalakeId The metalake ID.
   * @param domainName The domain name.
   * @return The locked domain, or null if the natural key is not active.
   */
  @SelectProvider(
      type = DomainMetaSQLProviderFactory.class,
      method = "selectDomainMetaByMetalakeIdAndNameForUpdate")
  DomainPO selectDomainMetaByMetalakeIdAndNameForUpdate(
      @Param("metalakeId") Long metalakeId, @Param("domainName") String domainName);

  /**
   * Selects an active domain by ID.
   *
   * @param domainId The domain ID.
   * @return The domain, or null if it is not active.
   */
  @SelectProvider(type = DomainMetaSQLProviderFactory.class, method = "selectDomainMetaById")
  DomainPO selectDomainMetaById(@Param("domainId") Long domainId);

  /**
   * Selects and exclusively locks an active domain by ID. The backing tag name is not read, so that
   * no tag row is locked.
   *
   * @param domainId The domain ID.
   * @return The locked domain, or null if it is not active.
   */
  @SelectProvider(
      type = DomainMetaSQLProviderFactory.class,
      method = "selectDomainMetaByIdForUpdate")
  DomainPO selectDomainMetaByIdForUpdate(@Param("domainId") Long domainId);

  /**
   * Selects an active domain by ID and locks it for shared access. The backing tag name is not
   * read, so that no tag row is locked.
   *
   * @param domainId The domain ID.
   * @return The locked domain, or null if it is not active.
   */
  @SelectProvider(
      type = DomainMetaSQLProviderFactory.class,
      method = "selectDomainMetaByIdForShare")
  DomainPO selectDomainMetaByIdForShare(@Param("domainId") Long domainId);

  /**
   * Lists the active domains with the given IDs, for name lookups. The backing tag name is not
   * read.
   *
   * @param domainIds The domain IDs.
   * @return The active domains.
   */
  @SelectProvider(type = DomainMetaSQLProviderFactory.class, method = "listDomainPOsByDomainIds")
  List<DomainPO> listDomainPOsByDomainIds(@Param("domainIds") List<Long> domainIds);

  /**
   * Lists the active domains of a metalake with the given names.
   *
   * @param metalakeName The metalake name.
   * @param domainNames The domain names.
   * @return The active domains.
   */
  @SelectProvider(
      type = DomainMetaSQLProviderFactory.class,
      method = "listDomainPOsByMetalakeAndDomainNames")
  List<DomainPO> listDomainPOsByMetalakeAndDomainNames(
      @Param("metalakeName") String metalakeName, @Param("domainNames") List<String> domainNames);

  /**
   * Lists the active sub-domains of a domain.
   *
   * @param parentId The ID of the parent domain.
   * @return The active sub-domains.
   */
  @SelectProvider(type = DomainMetaSQLProviderFactory.class, method = "listDomainPOsByParentId")
  List<DomainPO> listDomainPOsByParentId(@Param("parentId") Long parentId);

  /**
   * Inserts a domain.
   *
   * @param domainPO The domain to insert.
   */
  @InsertProvider(type = DomainMetaSQLProviderFactory.class, method = "insertDomainMeta")
  void insertDomainMeta(@Param("domainMeta") DomainPO domainPO);

  /**
   * Updates an active domain when its version still matches the observed one.
   *
   * @param newDomainPO The new domain row.
   * @param oldDomainPO The domain row observed by the caller.
   * @return The number of affected rows.
   */
  @UpdateProvider(type = DomainMetaSQLProviderFactory.class, method = "updateDomainMeta")
  Integer updateDomainMeta(
      @Param("newDomainMeta") DomainPO newDomainPO, @Param("oldDomainMeta") DomainPO oldDomainPO);

  /**
   * Soft-deletes an active domain when its version still matches.
   *
   * @param domainId The domain ID.
   * @param currentVersion The version observed by the caller.
   * @return The number of affected rows.
   */
  @UpdateProvider(
      type = DomainMetaSQLProviderFactory.class,
      method = "softDeleteDomainMetaByIdAndVersion")
  Integer softDeleteDomainMetaByIdAndVersion(
      @Param("domainId") Long domainId, @Param("currentVersion") Long currentVersion);

  /**
   * Soft-deletes all active domains of a metalake.
   *
   * @param metalakeId The metalake ID.
   */
  @UpdateProvider(
      type = DomainMetaSQLProviderFactory.class,
      method = "softDeleteDomainMetasByMetalakeId")
  void softDeleteDomainMetasByMetalakeId(@Param("metalakeId") Long metalakeId);

  /**
   * Hard-deletes domains soft-deleted before the given time.
   *
   * @param legacyTimeline The time before which soft-deleted rows are removed.
   * @param limit The maximum number of rows to remove.
   * @return The number of removed rows.
   */
  @DeleteProvider(
      type = DomainMetaSQLProviderFactory.class,
      method = "deleteDomainMetasByLegacyTimeline")
  Integer deleteDomainMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);
}
