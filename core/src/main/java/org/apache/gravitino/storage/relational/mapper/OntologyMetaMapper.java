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
import org.apache.gravitino.storage.relational.po.OntologyPO;
import org.apache.gravitino.storage.relational.po.OntologyVersionInfoPO;
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.One;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.ResultMap;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.UpdateProvider;

/**
 * A MyBatis mapper for ontology identity metadata operations.
 *
 * <p>Every read that returns a whole ontology joins the identity row with the snapshot its {@code
 * current_version} points at, so a caller never has to resolve the version itself.
 */
public interface OntologyMetaMapper {

  /** The ontology identity table name. */
  String TABLE_NAME = "ontology_meta";

  /** The ontology version snapshot table name. */
  String VERSION_TABLE_NAME = "ontology_version_info";

  /** Declares the nested result mapping for an ontology version snapshot. */
  @Results(
      id = "mapToOntologyVersionInfoPO",
      value = {
        @Result(property = "id", column = "id", id = true),
        @Result(property = "metalakeId", column = "version_metalake_id"),
        @Result(property = "ontologyId", column = "version_ontology_id"),
        @Result(property = "version", column = "version"),
        @Result(property = "ontologyName", column = "version_ontology_name"),
        @Result(property = "ontologyComment", column = "ontology_comment"),
        @Result(property = "aiContext", column = "ai_context"),
        @Result(property = "requires", column = "requires_sql"),
        @Result(property = "ontology", column = "ontology"),
        @Result(property = "prefixes", column = "prefixes"),
        @Result(property = "properties", column = "properties"),
        @Result(property = "auditInfo", column = "version_audit_info"),
        @Result(property = "deletedAt", column = "version_deleted_at")
      })
  @Select("SELECT 1")
  OntologyVersionInfoPO mapToOntologyVersionInfoPO();

  /** Lists current ontology snapshots under a metalake ID. */
  @Results(
      id = "ontologyPOResultMap",
      value = {
        @Result(property = "ontologyId", column = "ontology_id", id = true),
        @Result(property = "ontologyName", column = "ontology_name"),
        @Result(property = "metalakeId", column = "metalake_id"),
        @Result(property = "status", column = "status"),
        @Result(property = "currentVersion", column = "current_version"),
        @Result(property = "lastVersion", column = "last_version"),
        @Result(property = "auditInfo", column = "audit_info"),
        @Result(property = "deletedAt", column = "deleted_at"),
        @Result(
            property = "ontologyVersionInfoPO",
            javaType = OntologyVersionInfoPO.class,
            column =
                "{id,version_metalake_id,version_ontology_id,version,version_ontology_name,"
                    + "ontology_comment,ai_context,requires_sql,ontology,prefixes,properties,"
                    + "version_audit_info,version_deleted_at}",
            one = @One(resultMap = "mapToOntologyVersionInfoPO"))
      })
  @SelectProvider(
      type = OntologyMetaSQLProviderFactory.class,
      method = "listOntologyPOsByMetalakeId")
  List<OntologyPO> listOntologyPOsByMetalakeId(@Param("metalakeId") Long metalakeId);

  /** Lists current ontology snapshots under a metalake name. */
  @ResultMap("ontologyPOResultMap")
  @SelectProvider(
      type = OntologyMetaSQLProviderFactory.class,
      method = "listOntologyPOsByFullQualifiedName")
  List<OntologyPO> listOntologyPOsByFullQualifiedName(@Param("metalakeName") String metalakeName);

  /** Selects an active ontology ID by metalake ID and name. */
  @SelectProvider(
      type = OntologyMetaSQLProviderFactory.class,
      method = "selectOntologyIdByMetalakeIdAndName")
  Long selectOntologyIdByMetalakeIdAndName(
      @Param("metalakeId") Long metalakeId, @Param("ontologyName") String ontologyName);

  /** Selects a current ontology snapshot by metalake ID and name. */
  @ResultMap("ontologyPOResultMap")
  @SelectProvider(
      type = OntologyMetaSQLProviderFactory.class,
      method = "selectOntologyMetaByMetalakeIdAndName")
  OntologyPO selectOntologyMetaByMetalakeIdAndName(
      @Param("metalakeId") Long metalakeId, @Param("ontologyName") String ontologyName);

  /** Selects and locks an active ontology identity by metalake ID and name. */
  @SelectProvider(
      type = OntologyMetaSQLProviderFactory.class,
      method = "selectOntologyMetaByMetalakeIdAndNameForUpdate")
  OntologyPO selectOntologyMetaByMetalakeIdAndNameForUpdate(
      @Param("metalakeId") Long metalakeId, @Param("ontologyName") String ontologyName);

  /** Selects a current ontology snapshot by stable ID. */
  @ResultMap("ontologyPOResultMap")
  @SelectProvider(type = OntologyMetaSQLProviderFactory.class, method = "selectOntologyMetaById")
  OntologyPO selectOntologyMetaById(@Param("ontologyId") Long ontologyId);

  /** Selects and locks an ontology identity by stable ID. */
  @SelectProvider(
      type = OntologyMetaSQLProviderFactory.class,
      method = "selectOntologyMetaByIdForUpdate")
  OntologyPO selectOntologyMetaByIdForUpdate(@Param("ontologyId") Long ontologyId);

  /** Selects a current ontology snapshot by metalake and ontology name. */
  @ResultMap("ontologyPOResultMap")
  @SelectProvider(
      type = OntologyMetaSQLProviderFactory.class,
      method = "selectOntologyByFullQualifiedName")
  OntologyPO selectOntologyByFullQualifiedName(
      @Param("metalakeName") String metalakeName, @Param("ontologyName") String ontologyName);

  /** Inserts an ontology identity row. */
  @InsertProvider(type = OntologyMetaSQLProviderFactory.class, method = "insertOntologyMeta")
  void insertOntologyMeta(@Param("ontologyMeta") OntologyPO ontologyPO);

  /** Updates an ontology identity together with a newly allocated current version. */
  @UpdateProvider(type = OntologyMetaSQLProviderFactory.class, method = "updateOntologyMeta")
  Integer updateOntologyMeta(
      @Param("newOntologyMeta") OntologyPO newOntologyPO,
      @Param("oldOntologyMeta") OntologyPO oldOntologyPO);

  /** Updates only the status of an ontology identity, without allocating a version. */
  @UpdateProvider(type = OntologyMetaSQLProviderFactory.class, method = "updateOntologyMetaStatus")
  Integer updateOntologyMetaStatus(
      @Param("newOntologyMeta") OntologyPO newOntologyPO,
      @Param("oldOntologyMeta") OntologyPO oldOntologyPO);

  /** Soft-deletes an ontology identity by stable ID and expected current version. */
  @UpdateProvider(
      type = OntologyMetaSQLProviderFactory.class,
      method = "softDeleteOntologyMetasByOntologyId")
  Integer softDeleteOntologyMetasByOntologyId(
      @Param("ontologyId") Long ontologyId, @Param("currentVersion") Integer currentVersion);

  /** Soft-deletes ontology identities under a metalake. */
  @UpdateProvider(
      type = OntologyMetaSQLProviderFactory.class,
      method = "softDeleteOntologyMetasByMetalakeId")
  Integer softDeleteOntologyMetasByMetalakeId(@Param("metalakeId") Long metalakeId);

  /** Permanently deletes soft-deleted ontology identities older than a timeline. */
  @DeleteProvider(
      type = OntologyMetaSQLProviderFactory.class,
      method = "deleteOntologyMetasByLegacyTimeline")
  Integer deleteOntologyMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);
}
