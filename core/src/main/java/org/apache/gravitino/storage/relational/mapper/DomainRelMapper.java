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
import org.apache.gravitino.storage.relational.po.DomainRelPO;
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.UpdateProvider;

/** MyBatis mapper of the {@code domain_rel} table, which holds the parent edge of sub-domains. */
public interface DomainRelMapper {

  /** The name of the domain relation table. */
  String DOMAIN_REL_TABLE_NAME = "domain_rel";

  /**
   * Selects the active parent edge of a domain. The parent name is not read.
   *
   * @param childId The ID of the domain.
   * @return The edge, or null if the domain is top-level.
   */
  @SelectProvider(type = DomainRelSQLProviderFactory.class, method = "selectDomainRelByChildId")
  DomainRelPO selectDomainRelByChildId(@Param("childId") Long childId);

  /**
   * Lists the active parent edges of the given domains, with the name of each parent.
   *
   * @param childIds The IDs of the domains.
   * @return The edges of the domains that have a parent.
   */
  @SelectProvider(type = DomainRelSQLProviderFactory.class, method = "listDomainRelsByChildIds")
  List<DomainRelPO> listDomainRelsByChildIds(@Param("childIds") List<Long> childIds);

  /**
   * Counts the active sub-domains of a domain.
   *
   * @param parentId The ID of the parent domain.
   * @return The number of active edges to the domain.
   */
  @SelectProvider(type = DomainRelSQLProviderFactory.class, method = "countDomainRelsByParentId")
  int countDomainRelsByParentId(@Param("parentId") Long parentId);

  /**
   * Inserts a parent edge.
   *
   * @param domainRelPO The edge to insert.
   */
  @InsertProvider(type = DomainRelSQLProviderFactory.class, method = "insertDomainRel")
  void insertDomainRel(@Param("domainRel") DomainRelPO domainRelPO);

  /**
   * Updates an active parent edge when its version still matches the observed one.
   *
   * @param newDomainRelPO The new edge.
   * @param oldDomainRelPO The edge observed by the caller.
   * @return The number of affected rows.
   */
  @UpdateProvider(type = DomainRelSQLProviderFactory.class, method = "updateDomainRel")
  Integer updateDomainRel(
      @Param("newDomainRel") DomainRelPO newDomainRelPO,
      @Param("oldDomainRel") DomainRelPO oldDomainRelPO);

  /**
   * Soft-deletes the active parent edge of a domain.
   *
   * @param childId The ID of the domain.
   * @return The number of affected rows.
   */
  @UpdateProvider(type = DomainRelSQLProviderFactory.class, method = "softDeleteDomainRelByChildId")
  Integer softDeleteDomainRelByChildId(@Param("childId") Long childId);

  /**
   * Soft-deletes all active parent edges of a metalake.
   *
   * @param metalakeId The metalake ID.
   */
  @UpdateProvider(
      type = DomainRelSQLProviderFactory.class,
      method = "softDeleteDomainRelsByMetalakeId")
  void softDeleteDomainRelsByMetalakeId(@Param("metalakeId") Long metalakeId);

  /**
   * Hard-deletes parent edges soft-deleted before the given time.
   *
   * @param legacyTimeline The time before which soft-deleted rows are removed.
   * @param limit The maximum number of rows to remove.
   * @return The number of removed rows.
   */
  @DeleteProvider(
      type = DomainRelSQLProviderFactory.class,
      method = "deleteDomainRelsByLegacyTimeline")
  Integer deleteDomainRelsByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);
}
