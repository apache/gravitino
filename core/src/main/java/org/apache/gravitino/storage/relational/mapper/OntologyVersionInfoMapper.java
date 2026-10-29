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
import org.apache.gravitino.storage.relational.po.OntologyVersionInfoPO;
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.UpdateProvider;

/** A MyBatis mapper for ontology version snapshot operations. */
public interface OntologyVersionInfoMapper {

  /** The ontology version snapshot table name. */
  String TABLE_NAME = "ontology_version_info";

  /** Inserts an ontology version snapshot. */
  @InsertProvider(
      type = OntologyVersionInfoSQLProviderFactory.class,
      method = "insertOntologyVersionInfo")
  void insertOntologyVersionInfo(@Param("ontologyVersionInfo") OntologyVersionInfoPO versionInfoPO);

  /** Selects an ontology version snapshot. */
  @SelectProvider(
      type = OntologyVersionInfoSQLProviderFactory.class,
      method = "selectOntologyVersionInfoByOntologyIdAndVersion")
  OntologyVersionInfoPO selectOntologyVersionInfoByOntologyIdAndVersion(
      @Param("ontologyId") Long ontologyId, @Param("version") Integer version);

  /** Soft-deletes all snapshots for an ontology ID. */
  @UpdateProvider(
      type = OntologyVersionInfoSQLProviderFactory.class,
      method = "softDeleteOntologyVersionsByOntologyId")
  Integer softDeleteOntologyVersionsByOntologyId(@Param("ontologyId") Long ontologyId);

  /** Soft-deletes ontology snapshots under a metalake. */
  @UpdateProvider(
      type = OntologyVersionInfoSQLProviderFactory.class,
      method = "softDeleteOntologyVersionsByMetalakeId")
  Integer softDeleteOntologyVersionsByMetalakeId(@Param("metalakeId") Long metalakeId);

  /** Permanently deletes soft-deleted snapshots older than a timeline. */
  @DeleteProvider(
      type = OntologyVersionInfoSQLProviderFactory.class,
      method = "deleteOntologyVersionsByLegacyTimeline")
  Integer deleteOntologyVersionsByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);

  /** Selects active ontologies whose version count exceeds the retention count. */
  @SelectProvider(
      type = OntologyVersionInfoSQLProviderFactory.class,
      method = "selectOntologyVersionsByRetentionCount")
  List<OntologyVersionInfoPO> selectOntologyVersionsByRetentionCount(
      @Param("versionRetentionCount") Long versionRetentionCount);

  /** Soft-deletes old snapshots through a per-ontology retention line. */
  @UpdateProvider(
      type = OntologyVersionInfoSQLProviderFactory.class,
      method = "softDeleteOntologyVersionsByRetentionLine")
  Integer softDeleteOntologyVersionsByRetentionLine(
      @Param("ontologyId") Long ontologyId,
      @Param("versionRetentionLine") long versionRetentionLine,
      @Param("limit") int limit);
}
