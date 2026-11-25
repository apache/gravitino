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

/** Storage queries for ontology identities and their snapshots. */
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

  /** Returns an ontology by name within its metalake. */
  @Select(ONTOLOGY_SELECT + " AND o.metalake_id = #{metalakeId} AND o.ontology_name = #{name}")
  OntologySnapshotPO loadOntology(@Param("metalakeId") long metalakeId, @Param("name") String name);

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
}
