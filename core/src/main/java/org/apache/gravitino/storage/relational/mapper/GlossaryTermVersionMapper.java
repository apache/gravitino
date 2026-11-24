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
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

/** Immutable glossary definition snapshots and the pointers protected by the term write lock. */
public interface GlossaryTermVersionMapper {
  /**
   * Current pointers for a stable identity, including a name reservation before first publication.
   */
  @Getter
  @Setter
  class Head {
    private long termId;
    private Long publishedVersion;
    private Long candidateVersion;
    private long lastVersion;
    private String reservedName;
  }

  /** A frozen definition and its decision state. */
  @Getter
  @Setter
  class Version {
    private long versionId;
    private long baseRevision;
    private String lifecycle;
    private String snapshot;
  }

  /** Reads pointers scoped by both identity and metalake. */
  @Nullable
  @Select(
      """
          SELECT term_id AS termId,
                 published_version AS publishedVersion,
                 candidate_version AS candidateVersion,
                 last_version AS lastVersion,
                 reserved_name AS reservedName
          FROM glossary_term_version_head
          WHERE metalake_id = #{metalakeId}
            AND term_id = #{termId}
          """)
  Head getVersionHeadByMetalakeAndTermId(
      @Param("metalakeId") long metalakeId, @Param("termId") long termId);

  /** Finds the identity reserved by an unpublished create. */
  @Nullable
  @Select(
      """
          SELECT term_id
          FROM glossary_term_version_head
          WHERE metalake_id = #{metalakeId}
            AND reserved_name = #{normalizedName}
          """)
  Long findTermIdByReservedName(
      @Param("metalakeId") long metalakeId, @Param("normalizedName") String normalizedName);

  /** Initializes pointers in the caller's transaction. */
  @Insert(
      """
          INSERT INTO glossary_term_version_head(term_id, metalake_id, last_version, reserved_name)
          VALUES (#{termId}, #{metalakeId}, 0, #{normalizedName})
          """)
  void insertVersionHead(
      @Param("metalakeId") long metalakeId,
      @Param("termId") long termId,
      @Param("normalizedName") @Nullable String normalizedName);

  /** Advances the pointers after the snapshot or decision is persisted. */
  @Update(
      """
          UPDATE glossary_term_version_head
          SET last_version = #{lastVersion},
              published_version = #{publishedVersion},
              candidate_version = #{candidateVersion},
              reserved_name = #{reservedName}
          WHERE term_id = #{termId}
          """)
  void updateVersionHead(Head head);

  /** Appends the proposed or published content; subsequent decisions only change lifecycle. */
  @Insert(
      """
          INSERT INTO glossary_term_version_info(term_id, version_id, base_revision, lifecycle, snapshot)
          VALUES (#{termId}, #{version.versionId}, #{version.baseRevision}, #{version.lifecycle}, #{version.snapshot})
          """)
  void insertTermVersion(@Param("termId") long termId, @Param("version") Version version);

  /** Reads one exact version. */
  @Nullable
  @Select(
      """
          SELECT version_id AS versionId, base_revision AS baseRevision, lifecycle, snapshot
          FROM glossary_term_version_info
          WHERE term_id = #{termId}
            AND version_id = #{versionId}
          """)
  Version getVersionByTermIdAndVersionId(
      @Param("termId") long termId, @Param("versionId") long versionId);

  /** Lists snapshots and their publication states, newest first. */
  @Select(
      """
          SELECT version_id AS versionId, base_revision AS baseRevision, lifecycle, snapshot
          FROM glossary_term_version_info
          WHERE term_id = #{termId}
          ORDER BY version_id DESC
          """)
  List<Version> listVersionsByTermId(@Param("termId") long termId);

  /** Records the decision without replacing the snapshot. */
  @Update(
      """
          UPDATE glossary_term_version_info
          SET lifecycle = #{state}
          WHERE term_id = #{termId}
            AND version_id = #{versionId}
          """)
  void updateVersionLifecycle(
      @Param("termId") long termId,
      @Param("versionId") long versionId,
      @Param("state") String state);

  /** Removes one collected identity's snapshots. */
  @Delete("DELETE FROM glossary_term_version_info WHERE term_id = #{termId}")
  void deleteVersionsByTermId(long termId);

  /** Removes the pointers for a collected identity. */
  @Delete("DELETE FROM glossary_term_version_head WHERE term_id = #{termId}")
  void deleteVersionHeadByTermId(long termId);

  /**
   * Removes snapshots including those belonging to unpublished identities during metalake deletion.
   */
  @Delete(
      """
          DELETE FROM glossary_term_version_info
          WHERE term_id IN (
              SELECT term_id FROM glossary_term_version_head WHERE metalake_id = #{metalakeId}
          )
          """)
  void deleteVersionsByMetalakeId(long metalakeId);

  /** Releases all pointers and candidate name reservations during metalake deletion. */
  @Delete("DELETE FROM glossary_term_version_head WHERE metalake_id = #{metalakeId}")
  void deleteVersionHeadsByMetalakeId(long metalakeId);
}
