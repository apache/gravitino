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

/** Glossary term version SQL shared by MySQL, H2 and PostgreSQL. */
public class GlossaryTermVersionBaseSQLProvider {
  /** Reads pointers scoped by both identity and metalake. */
  public String getVersionHeadByMetalakeAndTermId() {
    return """
          SELECT term_id AS termId,
                 published_version AS publishedVersion,
                 candidate_version AS candidateVersion,
                 last_version AS lastVersion,
                 reserved_name AS reservedName
          FROM glossary_term_version_head
          WHERE metalake_id = #{metalakeId}
            AND term_id = #{termId}
          """;
  }

  /** Finds the identity reserved by an unpublished create. */
  public String findTermIdByReservedName() {
    return """
          SELECT term_id
          FROM glossary_term_version_head
          WHERE metalake_id = #{metalakeId}
            AND reserved_name = #{normalizedName}
          """;
  }

  /** Initializes pointers in the caller's transaction. */
  public String insertVersionHead() {
    return """
          INSERT INTO glossary_term_version_head(term_id, metalake_id, last_version, reserved_name)
          VALUES (#{termId}, #{metalakeId}, 0, #{normalizedName})
          """;
  }

  /** Advances the pointers after the snapshot or decision is persisted. */
  public String updateVersionHead() {
    return """
          UPDATE glossary_term_version_head
          SET last_version = #{lastVersion},
              published_version = #{publishedVersion},
              candidate_version = #{candidateVersion},
              reserved_name = #{reservedName}
          WHERE term_id = #{termId}
          """;
  }

  /** Appends the proposed or published content; subsequent decisions only change lifecycle. */
  public String insertTermVersion() {
    return """
          INSERT INTO glossary_term_version_info(term_id, version_id, base_revision, lifecycle, snapshot)
          VALUES (#{termId}, #{version.versionId}, #{version.baseRevision}, #{version.lifecycle}, #{version.snapshot})
          """;
  }

  /** Reads one exact version. */
  public String getVersionByTermIdAndVersionId() {
    return """
          SELECT version_id AS versionId, base_revision AS baseRevision, lifecycle, snapshot
          FROM glossary_term_version_info
          WHERE term_id = #{termId}
            AND version_id = #{versionId}
          """;
  }

  /** Lists snapshots and their publication states, newest first. */
  public String listVersionsByTermId() {
    return """
          SELECT version_id AS versionId, base_revision AS baseRevision, lifecycle, snapshot
          FROM glossary_term_version_info
          WHERE term_id = #{termId}
          ORDER BY version_id DESC
          """;
  }

  /** Records the decision without replacing the snapshot. */
  public String updateVersionLifecycle() {
    return """
          UPDATE glossary_term_version_info
          SET lifecycle = #{state}
          WHERE term_id = #{termId}
            AND version_id = #{versionId}
          """;
  }

  /** Removes one collected identity's snapshots. */
  public String deleteVersionsByTermId() {
    return "DELETE FROM glossary_term_version_info WHERE term_id = #{termId}";
  }

  /** Removes the pointers for a collected identity. */
  public String deleteVersionHeadByTermId() {
    return "DELETE FROM glossary_term_version_head WHERE term_id = #{termId}";
  }

  /**
   * Removes snapshots including those belonging to unpublished identities during metalake deletion.
   */
  public String deleteVersionsByMetalakeId() {
    return """
          DELETE FROM glossary_term_version_info
          WHERE term_id IN (
              SELECT term_id FROM glossary_term_version_head WHERE metalake_id = #{metalakeId}
          )
          """;
  }

  /** Releases all pointers and candidate name reservations during metalake deletion. */
  public String deleteVersionHeadsByMetalakeId() {
    return "DELETE FROM glossary_term_version_head WHERE metalake_id = #{metalakeId}";
  }
}
