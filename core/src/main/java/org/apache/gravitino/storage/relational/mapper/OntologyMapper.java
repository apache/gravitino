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
import org.apache.gravitino.storage.relational.po.OntologySnapshotPO;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

/** Storage queries for ontology identities, mappings, and their snapshots. */
public interface OntologyMapper {

  /** Ontology identity table. */
  String ONTOLOGY_TABLE = "ontology_meta";

  /** Ontology identity and its current snapshot. */
  String ONTOLOGY_SELECT =
      "SELECT o.ontology_id AS id, o.ontology_name AS name, o.metalake_id AS metalakeId,"
          + " o.current_version AS currentVersion, o.last_version AS lastVersion,"
          + " o.audit_info AS auditInfo, v.ontology_comment AS comment, v.definition, v.properties"
          + " FROM ontology_meta o JOIN ontology_version_info v"
          + " ON o.ontology_id = v.ontology_id AND o.current_version = v.version"
          + " WHERE o.deleted_at = 0 AND v.deleted_at = 0";

  /** Live mapping identities with their current parent names, without definitions. */
  String MAPPING_IDENTITY_SELECT =
      "SELECT m.mapping_id AS id, m.mapping_name AS name, m.ontology_id AS ontologyId,"
          + " m.metalake_id AS metalakeId, m.catalog_id AS catalogId, m.schema_id AS schemaId,"
          + " m.semantic_model_id AS semanticModelId, c.catalog_name AS catalogName,"
          + " s.schema_name AS schemaName, sm.semantic_model_name AS semanticModelName,"
          + " sm.active_version AS semanticModelActiveVersion"
          + " FROM ontology_mapping_meta m"
          + " JOIN semantic_model_meta sm ON m.semantic_model_id = sm.semantic_model_id AND sm.deleted_at = 0"
          + " JOIN schema_meta s ON m.schema_id = s.schema_id AND s.deleted_at = 0"
          + " JOIN catalog_meta c ON m.catalog_id = c.catalog_id AND c.deleted_at = 0"
          + " WHERE m.deleted_at = 0";

  /** Live mappings with their current model and ontology names. */
  String MAPPING_SELECT =
      "SELECT m.mapping_id AS id, m.mapping_name AS name, m.ontology_id AS ontologyId,"
          + " m.metalake_id AS metalakeId, m.catalog_id AS catalogId, m.schema_id AS schemaId,"
          + " m.semantic_model_id AS semanticModelId, m.current_version AS currentVersion,"
          + " m.last_version AS lastVersion, m.audit_info AS auditInfo, v.mapping_comment AS comment,"
          + " v.definition, v.properties, o.ontology_name AS ontologyName,"
          + " c.catalog_name AS catalogName, s.schema_name AS schemaName,"
          + " sm.semantic_model_name AS semanticModelName, sm.active_version AS semanticModelActiveVersion"
          + " FROM ontology_mapping_meta m JOIN ontology_mapping_version_info v"
          + " ON m.mapping_id = v.mapping_id AND m.current_version = v.version"
          + " JOIN ontology_meta o ON m.ontology_id = o.ontology_id AND o.deleted_at = 0"
          + " JOIN semantic_model_meta sm ON m.semantic_model_id = sm.semantic_model_id AND sm.deleted_at = 0"
          + " JOIN schema_meta s ON m.schema_id = s.schema_id AND s.deleted_at = 0"
          + " JOIN catalog_meta c ON m.catalog_id = c.catalog_id AND c.deleted_at = 0"
          + " WHERE m.deleted_at = 0 AND v.deleted_at = 0";

  /** Returns an ontology by name within its metalake. */
  @Select(ONTOLOGY_SELECT + " AND o.metalake_id = #{metalakeId} AND o.ontology_name = #{name}")
  OntologySnapshotPO loadOntology(@Param("metalakeId") long metalakeId, @Param("name") String name);

  /** Returns the current snapshot by stable ontology ID. */
  @Select(ONTOLOGY_SELECT + " AND o.ontology_id = #{id}")
  OntologySnapshotPO loadOntologyById(@Param("id") long id);

  /** Looks up the identity without loading the definition. */
  @Select(
      "SELECT ontology_id FROM ontology_meta WHERE metalake_id = #{metalakeId}"
          + " AND ontology_name = #{name} AND deleted_at = 0")
  Long getOntologyId(@Param("metalakeId") long metalakeId, @Param("name") String name);

  /** Locks the named live ontology for the enclosing transaction. */
  @Select(
      "SELECT ontology_id FROM ontology_meta WHERE metalake_id = #{metalakeId}"
          + " AND ontology_name = #{name} AND deleted_at = 0 FOR UPDATE")
  Long lockOntology(@Param("metalakeId") long metalakeId, @Param("name") String name);

  /** Locks an ontology by stable ID for the enclosing transaction. */
  @Select(
      "SELECT ontology_id FROM ontology_meta WHERE ontology_id = #{id} AND deleted_at = 0 FOR UPDATE")
  Long lockOntologyById(@Param("id") long id);

  /** Lists ontology names in a metalake. */
  @Select(
      "SELECT ontology_name FROM ontology_meta WHERE metalake_id = #{metalakeId}"
          + " AND deleted_at = 0 ORDER BY ontology_name")
  List<String> listOntologyNames(@Param("metalakeId") long metalakeId);

  /** Returns ontology IDs and names for metadata governance. */
  @Select({
    "<script>",
    "SELECT ontology_id AS id, ontology_name AS name FROM ontology_meta WHERE deleted_at = 0 AND ontology_id IN",
    "<foreach item='id' collection='ids' open='(' separator=',' close=')'>#{id}</foreach>",
    "</script>"
  })
  List<OntologySnapshotPO> listOntologyNamesByIds(@Param("ids") List<Long> ids);

  /** Returns a mapping's latest snapshot and its live parent names. */
  @Select(MAPPING_SELECT + " AND m.semantic_model_id = #{modelId} AND m.mapping_name = #{name}")
  OntologySnapshotPO loadMapping(@Param("modelId") long modelId, @Param("name") String name);

  /** Finds every mapping of an ontology in all of its metalake's catalogs. */
  @Select(
      MAPPING_SELECT
          + " AND m.ontology_id = #{id}"
          + " ORDER BY c.catalog_name, s.schema_name, sm.semantic_model_name, m.mapping_name")
  List<OntologySnapshotPO> listOntologyMappings(@Param("id") long id);

  /** Finds every mapping identity of an ontology, without loading definitions. */
  @Select(
      MAPPING_IDENTITY_SELECT
          + " AND m.ontology_id = #{id}"
          + " ORDER BY c.catalog_name, s.schema_name, sm.semantic_model_name, m.mapping_name")
  List<OntologySnapshotPO> listOntologyMappingIdentities(@Param("id") long id);

  /** Looks up a live mapping ID under one model. */
  @Select(
      "SELECT mapping_id FROM ontology_mapping_meta WHERE semantic_model_id = #{modelId}"
          + " AND mapping_name = #{name} AND deleted_at = 0")
  Long getMappingId(@Param("modelId") long modelId, @Param("name") String name);

  /** Lists the names of mappings under one model. */
  @Select(
      "SELECT mapping_name FROM ontology_mapping_meta WHERE semantic_model_id = #{id}"
          + " AND deleted_at = 0 ORDER BY mapping_name")
  List<String> listModelMappings(@Param("id") long id);

  /** Inserts a live ontology identity at version one. */
  @Insert(
      "INSERT INTO ontology_meta (ontology_id, ontology_name, metalake_id, audit_info)"
          + " VALUES (#{id}, #{name}, #{metalakeId}, #{auditInfo})")
  void insertOntology(OntologySnapshotPO snapshot);

  /** Appends the new ontology snapshot. */
  @Insert(
      "INSERT INTO ontology_version_info (metalake_id, ontology_id, version, ontology_comment, definition, properties, audit_info)"
          + " VALUES (#{metalakeId}, #{id}, #{lastVersion}, #{comment}, #{definition}, #{properties}, #{auditInfo})")
  void appendOntologyVersion(OntologySnapshotPO snapshot);

  /** Updates the ontology identity and version pointers atomically. */
  @Update(
      "UPDATE ontology_meta SET ontology_name = #{row.name}, current_version = #{row.lastVersion},"
          + " last_version = #{row.lastVersion}, audit_info = #{row.auditInfo}"
          + " WHERE ontology_id = #{row.id} AND current_version = #{row.currentVersion} AND deleted_at = 0")
  int updateOntology(@Param("row") OntologySnapshotPO snapshot);

  /** Marks the ontology identity as deleted. */
  @Update(
      "UPDATE ontology_meta SET deleted_at = #{at} WHERE ontology_id = #{id} AND deleted_at = 0")
  void dropOntology(@Param("id") long id, @Param("at") long at);

  /** Marks snapshots of the dropped ontology as deleted. */
  @Update(
      "UPDATE ontology_version_info SET deleted_at = #{at} WHERE ontology_id = #{id} AND deleted_at = 0")
  void dropOntologyVersions(@Param("id") long id, @Param("at") long at);

  /** Retires a batch of snapshots past the retention cutoff, preserving the current version. */
  @Update(
      "UPDATE ontology_version_info SET deleted_at = #{at} WHERE deleted_at = 0 AND id IN ("
          + " SELECT expired.id FROM (SELECT v.id FROM ontology_version_info v JOIN ontology_meta m"
          + " ON v.ontology_id = m.ontology_id WHERE m.deleted_at = 0 AND v.deleted_at = 0"
          + " AND v.version < m.current_version AND m.current_version - v.version >= #{count}"
          + " ORDER BY v.id LIMIT #{limit}) expired)")
  int retireOntologyVersions(
      @Param("count") long count, @Param("limit") int limit, @Param("at") long at);

  /** Permanently removes a batch of retired ontology identities. */
  @Delete(
      "DELETE FROM ontology_meta WHERE ontology_id IN (SELECT expired.ontology_id FROM"
          + " (SELECT ontology_id FROM ontology_meta WHERE deleted_at > 0 AND deleted_at < #{before}"
          + " ORDER BY ontology_id LIMIT #{limit}) expired)")
  int purgeOntologyIdentities(@Param("before") long before, @Param("limit") int limit);

  /** Permanently removes a batch of retired ontology versions. */
  @Delete(
      "DELETE FROM ontology_version_info WHERE id IN (SELECT expired.id FROM"
          + " (SELECT id FROM ontology_version_info WHERE deleted_at > 0 AND deleted_at < #{before}"
          + " ORDER BY id LIMIT #{limit}) expired)")
  int purgeOntologyVersions(@Param("before") long before, @Param("limit") int limit);

  /** Inserts a live mapping identity at version one. */
  @Insert(
      "INSERT INTO ontology_mapping_meta (mapping_id, mapping_name, metalake_id, catalog_id, schema_id, semantic_model_id, ontology_id, audit_info)"
          + " VALUES (#{id}, #{name}, #{metalakeId}, #{catalogId}, #{schemaId}, #{semanticModelId}, #{ontologyId}, #{auditInfo})")
  void insertMapping(OntologySnapshotPO snapshot);

  /** Appends the new mapping snapshot. */
  @Insert(
      "INSERT INTO ontology_mapping_version_info (metalake_id, catalog_id, schema_id, mapping_id, version, mapping_comment, definition, properties, audit_info)"
          + " VALUES (#{metalakeId}, #{catalogId}, #{schemaId}, #{id}, #{lastVersion}, #{comment}, #{definition}, #{properties}, #{auditInfo})")
  void appendMappingVersion(OntologySnapshotPO snapshot);

  /** Updates the mapping identity and version pointers atomically. */
  @Update(
      "UPDATE ontology_mapping_meta SET mapping_name = #{row.name}, current_version = #{row.lastVersion},"
          + " last_version = #{row.lastVersion}, audit_info = #{row.auditInfo}"
          + " WHERE mapping_id = #{row.id} AND current_version = #{row.currentVersion} AND deleted_at = 0")
  int updateMapping(@Param("row") OntologySnapshotPO snapshot);

  /** Marks the mapping identity as deleted. */
  @Update(
      "UPDATE ontology_mapping_meta SET deleted_at = #{at} WHERE mapping_id = #{id} AND deleted_at = 0")
  void dropMapping(@Param("id") long id, @Param("at") long at);

  /** Marks snapshots of the dropped mapping as deleted. */
  @Update(
      "UPDATE ontology_mapping_version_info SET deleted_at = #{at} WHERE mapping_id = #{id} AND deleted_at = 0")
  void dropMappingVersions(@Param("id") long id, @Param("at") long at);

  /** Retires a batch of snapshots past the retention cutoff, preserving the current version. */
  @Update(
      "UPDATE ontology_mapping_version_info SET deleted_at = #{at} WHERE deleted_at = 0 AND id IN ("
          + " SELECT expired.id FROM (SELECT v.id FROM ontology_mapping_version_info v JOIN ontology_mapping_meta m"
          + " ON v.mapping_id = m.mapping_id WHERE m.deleted_at = 0 AND v.deleted_at = 0"
          + " AND v.version < m.current_version AND m.current_version - v.version >= #{count}"
          + " ORDER BY v.id LIMIT #{limit}) expired)")
  int retireMappingVersions(
      @Param("count") long count, @Param("limit") int limit, @Param("at") long at);

  /** Permanently removes a batch of retired mapping identities. */
  @Delete(
      "DELETE FROM ontology_mapping_meta WHERE mapping_id IN (SELECT expired.mapping_id FROM"
          + " (SELECT mapping_id FROM ontology_mapping_meta WHERE deleted_at > 0 AND deleted_at < #{before}"
          + " ORDER BY mapping_id LIMIT #{limit}) expired)")
  int purgeMappingIdentities(@Param("before") long before, @Param("limit") int limit);

  /** Permanently removes a batch of retired mapping versions. */
  @Delete(
      "DELETE FROM ontology_mapping_version_info WHERE id IN (SELECT expired.id FROM"
          + " (SELECT id FROM ontology_mapping_version_info WHERE deleted_at > 0 AND deleted_at < #{before}"
          + " ORDER BY id LIMIT #{limit}) expired)")
  int purgeMappingVersions(@Param("before") long before, @Param("limit") int limit);

  /** Cascades deletion to ontology versions within the metalake. */
  @Update(
      "UPDATE ontology_version_info SET deleted_at = #{at}"
          + " WHERE deleted_at = 0 AND metalake_id = #{id}")
  void cascadeOntologyVersionsByMetalake(@Param("id") long id, @Param("at") long at);

  /** Cascades deletion to ontology identities within the metalake. */
  @Update(
      "UPDATE ontology_meta SET deleted_at = #{at}"
          + " WHERE deleted_at = 0 AND metalake_id = #{id}")
  void cascadeOntologyByMetalake(@Param("id") long id, @Param("at") long at);

  /** Cascades deletion to mapping versions within the metalake. */
  @Update(
      "UPDATE ontology_mapping_version_info SET deleted_at = #{at}"
          + " WHERE deleted_at = 0 AND metalake_id = #{id}")
  void cascadeMappingVersionsByMetalake(@Param("id") long id, @Param("at") long at);

  /** Cascades deletion to mapping identities within the metalake. */
  @Update(
      "UPDATE ontology_mapping_meta SET deleted_at = #{at}"
          + " WHERE deleted_at = 0 AND metalake_id = #{id}")
  void cascadeMappingByMetalake(@Param("id") long id, @Param("at") long at);

  /** Cascades deletion to mapping versions within the catalog. */
  @Update(
      "UPDATE ontology_mapping_version_info SET deleted_at = #{at}"
          + " WHERE deleted_at = 0 AND catalog_id = #{id}")
  void cascadeMappingVersionsByCatalog(@Param("id") long id, @Param("at") long at);

  /** Cascades deletion to mapping identities within the catalog. */
  @Update(
      "UPDATE ontology_mapping_meta SET deleted_at = #{at}"
          + " WHERE deleted_at = 0 AND catalog_id = #{id}")
  void cascadeMappingByCatalog(@Param("id") long id, @Param("at") long at);

  /** Cascades deletion to mapping versions within the schemas. */
  @Update({
    "<script>",
    "UPDATE ontology_mapping_version_info SET deleted_at = #{at}"
        + " WHERE deleted_at = 0 AND schema_id IN",
    "<foreach item='id' collection='ids' open='(' separator=',' close=')'>#{id}</foreach>",
    "</script>"
  })
  void cascadeMappingVersionsBySchemas(@Param("ids") List<Long> ids, @Param("at") long at);

  /** Cascades deletion to mapping identities within the schemas. */
  @Update({
    "<script>",
    "UPDATE ontology_mapping_meta SET deleted_at = #{at}"
        + " WHERE deleted_at = 0 AND schema_id IN",
    "<foreach item='id' collection='ids' open='(' separator=',' close=')'>#{id}</foreach>",
    "</script>"
  })
  void cascadeMappingBySchemas(@Param("ids") List<Long> ids, @Param("at") long at);

  /** Cascades deletion to mapping versions within the model. */
  @Update(
      "UPDATE ontology_mapping_version_info SET deleted_at = #{at}"
          + " WHERE deleted_at = 0 AND mapping_id IN (SELECT mapping_id FROM ontology_mapping_meta WHERE semantic_model_id = #{id} AND deleted_at = 0)")
  void cascadeMappingVersionsByModel(@Param("id") long id, @Param("at") long at);

  /** Cascades deletion to mapping identities within the model. */
  @Update(
      "UPDATE ontology_mapping_meta SET deleted_at = #{at}"
          + " WHERE deleted_at = 0 AND semantic_model_id = #{id}")
  void cascadeMappingByModel(@Param("id") long id, @Param("at") long at);
}
