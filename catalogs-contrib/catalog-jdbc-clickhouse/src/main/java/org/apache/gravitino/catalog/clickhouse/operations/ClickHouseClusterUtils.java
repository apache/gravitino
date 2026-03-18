/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.gravitino.catalog.clickhouse.operations;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;

/**
 * Utilities for embedding and extracting ClickHouse cluster metadata in object COMMENT fields.
 *
 * <p><b>Why COMMENT?</b>
 *
 * <p>ClickHouse does not persist {@code ON CLUSTER} information in any queryable system table for
 * non-Replicated objects:
 *
 * <ul>
 *   <li>{@code SHOW CREATE DATABASE} omits the {@code ON CLUSTER} clause.
 *   <li>{@code SHOW CREATE TABLE} omits the {@code ON CLUSTER} clause (each node stores the local
 *       DDL without the distribution directive).
 *   <li>{@code system.databases.cluster} is only populated for {@code Replicated}-engine databases.
 * </ul>
 *
 * <p>Gravitino therefore embeds the cluster name inside the object's COMMENT field at creation time
 * using a non-printable SOH separator ({@code \u0001}). The metadata is invisible to end users
 * because Gravitino strips it before surfacing the comment.
 *
 * <p><b>Stored format:</b> {@code userComment\u0001ch.cluster=clusterName}
 *
 * <p><b>Limitation:</b> This mechanism only works for databases and tables created through
 * Gravitino. If a database or table was created directly in ClickHouse (bypassing Gravitino),
 * Gravitino has no way to determine whether it was created {@code ON CLUSTER} or which cluster name
 * was used. In that case {@link #extractClusterFromComment} returns {@code null} and the {@code
 * on-cluster} / {@code cluster-name} properties reported by Gravitino will be absent or inaccurate.
 */
public final class ClickHouseClusterUtils {

  /**
   * Separator character between the user comment and the cluster metadata token. SOH (U+0001) is a
   * non-printable control character that will not appear in normal user-supplied comments.
   */
  @VisibleForTesting public static final char CLUSTER_META_SEP = '\u0001';

  /** Full prefix of the cluster metadata token appended after {@link #CLUSTER_META_SEP}. */
  @VisibleForTesting
  public static final String CLUSTER_META_PREFIX = CLUSTER_META_SEP + "ch.cluster=";

  private ClickHouseClusterUtils() {}

  /**
   * Appends cluster metadata to {@code comment}, producing the string that will be stored in
   * ClickHouse's {@code COMMENT} field.
   *
   * @param comment The user-visible comment (may be {@code null} or the Gravitino-encoded comment
   *     string).
   * @param clusterName The cluster name to embed.
   * @return The combined string: {@code comment\u0001ch.cluster=clusterName}.
   */
  public static String embedClusterInComment(String comment, String clusterName) {
    return StringUtils.defaultString(comment) + CLUSTER_META_PREFIX + clusterName;
  }

  /**
   * Extracts the cluster name embedded by {@link #embedClusterInComment}, or {@code null} if none
   * is present (e.g., the object was not created {@code ON CLUSTER} through Gravitino, or was
   * created by a third-party tool).
   *
   * @param storedComment The raw comment as stored in ClickHouse.
   * @return The cluster name, or {@code null}.
   */
  public static String extractClusterFromComment(String storedComment) {
    if (storedComment == null) {
      return null;
    }
    int idx = storedComment.indexOf(CLUSTER_META_PREFIX);
    if (idx < 0) {
      return null;
    }
    return storedComment.substring(idx + CLUSTER_META_PREFIX.length());
  }

  /**
   * Returns the user-visible portion of the stored comment, stripping any embedded cluster metadata
   * suffix. Returns {@code null} if {@code storedComment} is {@code null}.
   *
   * @param storedComment The raw comment as stored in ClickHouse.
   * @return The comment without cluster metadata.
   */
  public static String stripClusterMetadata(String storedComment) {
    if (storedComment == null) {
      return null;
    }
    int idx = storedComment.indexOf(CLUSTER_META_SEP);
    return idx < 0 ? storedComment : storedComment.substring(0, idx);
  }
}
