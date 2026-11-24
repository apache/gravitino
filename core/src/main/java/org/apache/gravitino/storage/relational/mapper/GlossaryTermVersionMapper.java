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
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.UpdateProvider;

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
  @SelectProvider(
      type = GlossaryTermVersionSQLProviderFactory.class,
      method = "getVersionHeadByMetalakeAndTermId")
  Head getVersionHeadByMetalakeAndTermId(
      @Param("metalakeId") long metalakeId, @Param("termId") long termId);

  /** Finds the identity reserved by an unpublished create. */
  @Nullable
  @SelectProvider(
      type = GlossaryTermVersionSQLProviderFactory.class,
      method = "findTermIdByReservedName")
  Long findTermIdByReservedName(
      @Param("metalakeId") long metalakeId, @Param("normalizedName") String normalizedName);

  /** Initializes pointers in the caller's transaction. */
  @InsertProvider(type = GlossaryTermVersionSQLProviderFactory.class, method = "insertVersionHead")
  void insertVersionHead(
      @Param("metalakeId") long metalakeId,
      @Param("termId") long termId,
      @Param("normalizedName") @Nullable String normalizedName);

  /** Advances the pointers after the snapshot or decision is persisted. */
  @UpdateProvider(type = GlossaryTermVersionSQLProviderFactory.class, method = "updateVersionHead")
  void updateVersionHead(Head head);

  /** Appends the proposed or published content; subsequent decisions only change lifecycle. */
  @InsertProvider(type = GlossaryTermVersionSQLProviderFactory.class, method = "insertTermVersion")
  void insertTermVersion(@Param("termId") long termId, @Param("version") Version version);

  /** Reads one exact version. */
  @Nullable
  @SelectProvider(
      type = GlossaryTermVersionSQLProviderFactory.class,
      method = "getVersionByTermIdAndVersionId")
  Version getVersionByTermIdAndVersionId(
      @Param("termId") long termId, @Param("versionId") long versionId);

  /** Lists snapshots and their publication states, newest first. */
  @Options(useCache = false, flushCache = Options.FlushCachePolicy.TRUE)
  @SelectProvider(
      type = GlossaryTermVersionSQLProviderFactory.class,
      method = "listVersionsByTermId")
  List<Version> listVersionsByTermId(
      @Param("termId") long termId,
      @Param("maxVersionId") long maxVersionId,
      @Param("limit") int limit);

  /** Lists business version IDs, newest first. */
  @SelectProvider(
      type = GlossaryTermVersionSQLProviderFactory.class,
      method = "listVersionIdsByTermId")
  List<Long> listVersionIdsByTermId(
      @Param("termId") long termId,
      @Param("maxVersionId") long maxVersionId,
      @Param("limit") int limit);

  /** Records the decision without replacing the snapshot. */
  @UpdateProvider(
      type = GlossaryTermVersionSQLProviderFactory.class,
      method = "updateVersionLifecycle")
  void updateVersionLifecycle(
      @Param("termId") long termId,
      @Param("versionId") long versionId,
      @Param("state") String state);

  /** Removes one collected identity's snapshots. */
  @DeleteProvider(
      type = GlossaryTermVersionSQLProviderFactory.class,
      method = "deleteVersionsByTermId")
  void deleteVersionsByTermId(long termId);

  /** Removes the pointers for a collected identity. */
  @DeleteProvider(
      type = GlossaryTermVersionSQLProviderFactory.class,
      method = "deleteVersionHeadByTermId")
  void deleteVersionHeadByTermId(long termId);

  /**
   * Removes snapshots including those belonging to unpublished identities during metalake deletion.
   */
  @DeleteProvider(
      type = GlossaryTermVersionSQLProviderFactory.class,
      method = "deleteVersionsByMetalakeId")
  void deleteVersionsByMetalakeId(long metalakeId);

  /** Releases all pointers and candidate name reservations during metalake deletion. */
  @DeleteProvider(
      type = GlossaryTermVersionSQLProviderFactory.class,
      method = "deleteVersionHeadsByMetalakeId")
  void deleteVersionHeadsByMetalakeId(long metalakeId);
}
