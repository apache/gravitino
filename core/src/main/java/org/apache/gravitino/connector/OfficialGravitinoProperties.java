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
package org.apache.gravitino.connector;

import com.google.common.collect.ImmutableSet;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Registry of property keys that Gravitino itself defines across base, credential, cloud-storage,
 * and connector metadata.
 *
 * <p>Name-based (fuzzy) masking and secret recovery treat only keys <em>not</em> in this registry
 * as unknown. Official keys follow {@link #isHidden(String)} / {@link #isReserved(String)} even
 * when the current catalog's {@link PropertiesMetadata} does not declare them (for example a
 * runtime-copied {@code s3-access-key-id} on Glue).
 *
 * <p>Keep this list in sync when adding catalog property entries. Connectors may depend on these
 * definitions; {@code core} must not depend on catalog modules.
 */
public final class OfficialGravitinoProperties {

  /** Fileset multi-location property prefix ({@code location-<name>}). */
  public static final String LOCATION_PROPERTY_PREFIX = "location-";

  private static final Set<String> DEFINED_KEYS =
      ImmutableSet.<String>builder()
          .add(
              "EXTERNAL",
              "adls-token-expire-in-secs",
              "authentication.impersonation-enable",
              "authentication.kerberos.keytab-uri",
              "authentication.kerberos.principal")
          .add(
              "authentication.type",
              "authorization-provider",
              "aws-access-key-id",
              "aws-glue-catalog-id",
              "aws-glue-endpoint")
          .add(
              "aws-secret-access-key",
              "azure-client-id",
              "azure-client-secret",
              "azure-storage-account-key",
              "azure-storage-account-name")
          .add(
              "azure-tenant-id",
              "bloom_filter_columns",
              "bucket",
              "bucket-key",
              "cherry-pick-snapshot-id")
          .add(
              "cloud.name",
              "cloud.region-code",
              "cluster-name",
              "cluster-remote-database",
              "cluster-remote-table")
          .add("cluster-sharding-key", "comment", "compression", "cos-access-key-id", "cos-app-id")
          .add(
              "cos-endpoint",
              "cos-external-id",
              "cos-region",
              "cos-role-arn",
              "cos-secret-access-key")
          .add(
              "cos-token-expire-in-secs",
              "creator",
              "credential-cache-expire-ratio",
              "credential-cache-max-size",
              "credential-providers")
          .add(
              "current-snapshot-id",
              "data-access",
              "default-filesystem-provider",
              "default-location-name",
              "default-table-format")
          .add(
              "default-uri-name",
              "default.catalog",
              "disable-filesystem-ops",
              "dlf-access-key-id",
              "dlf-access-key-secret")
          .add(
              "dlf-security-token",
              "dlf-token-loader",
              "dlf-token-path",
              "enable_unique_key_merge_on_write",
              "engine_parameters")
          .add(
              "external",
              "file",
              "filesystem-providers",
              "gcs-service-account-file",
              "gravitino.identifier")
          .add(
              "gravitino.view.default-catalog",
              "gravitino.view.default-schema",
              "identifier-fields",
              "impersonation-enable",
              "in-use")
          .add("input-format", "io-impl", "jdbc-driver", "jdbc-password", "jdbc-user")
          .add(
              "kerberos.keytab-uri",
              "kerberos.principal",
              "lance.schema-refresh-mode",
              "light_schema_change",
              "list-all-tables")
          .add(
              "location",
              "location-unknown",
              "merge-engine",
              "metadata_location",
              "metalake-in-use")
          .add("numFiles", "on-cluster", "ops-impl", "oss-access-key-id", "oss-endpoint")
          .add(
              "oss-external-id",
              "oss-region",
              "oss-role-arn",
              "oss-secret-access-key",
              "oss-token-expire-in-secs")
          .add("output-format", "owner", "package", "partition", "partition-key")
          .add(
              "placeholder-catalog",
              "placeholder-fileset",
              "placeholder-schema",
              "presto_view",
              "primary-key")
          .add(
              "provider",
              "replication_allocation",
              "rowkind.field",
              "s3-access-key-id",
              "s3-creds-provider")
          .add("s3-endpoint", "s3-external-id", "s3-path-style-access", "s3-region", "s3-role-arn")
          .add(
              "s3-secret-access-key",
              "s3-token-expire-in-secs",
              "s3-token-service-endpoint",
              "sequence.field",
              "serde-lib")
          .add("serde-name", "sort-order", "storage_policy", "table-format", "table-format-filter")
          .add(
              "table-metadata-cache-impl",
              "token",
              "token-provider",
              "totalSize",
              "transient_lastDdlTime")
          .add("uri", "warehouse")
          .build();

  private static final Set<String> HIDDEN_KEYS =
      ImmutableSet.<String>builder()
          .add(
              "EXTERNAL",
              "aws-secret-access-key",
              "azure-client-secret",
              "azure-storage-account-key",
              "comment")
          .add(
              "cos-secret-access-key",
              "dlf-access-key-secret",
              "dlf-security-token",
              "gravitino.identifier",
              "gravitino.view.default-catalog")
          .add(
              "gravitino.view.default-schema",
              "jdbc-password",
              "metalake-in-use",
              "oss-secret-access-key",
              "placeholder-catalog")
          .add(
              "placeholder-fileset",
              "placeholder-schema",
              "presto_view",
              "s3-secret-access-key",
              "token")
          .build();

  private static final Set<String> RESERVED_KEYS =
      ImmutableSet.<String>builder()
          .add("EXTERNAL", "bucket", "bucket-key", "cherry-pick-snapshot-id", "comment")
          .add(
              "creator",
              "current-snapshot-id",
              "file",
              "gravitino.identifier",
              "gravitino.view.default-catalog")
          .add(
              "gravitino.view.default-schema",
              "identifier-fields",
              "in-use",
              "input-format",
              "metalake-in-use")
          .add("numFiles", "output-format", "owner", "partition", "placeholder-catalog")
          .add(
              "placeholder-fileset",
              "placeholder-schema",
              "presto_view",
              "primary-key",
              "sort-order")
          .add("totalSize", "transient_lastDdlTime")
          .build();

  private OfficialGravitinoProperties() {}

  /**
   * Returns whether {@code key} is a Gravitino-defined property name (exact or known prefix).
   *
   * @param key property key
   * @return true when Gravitino defines the key
   */
  public static boolean isDefined(@Nullable String key) {
    if (key == null || key.isEmpty()) {
      return false;
    }
    return DEFINED_KEYS.contains(key) || key.startsWith(LOCATION_PROPERTY_PREFIX);
  }

  /**
   * Returns whether an official property is hidden when the catalog metadata does not declare it.
   *
   * @param key property key
   * @return true when the official definition marks the key hidden
   */
  public static boolean isHidden(@Nullable String key) {
    return key != null && HIDDEN_KEYS.contains(key);
  }

  /**
   * Returns whether an official property is reserved when the catalog metadata does not declare it.
   *
   * @param key property key
   * @return true when the official definition marks the key reserved
   */
  public static boolean isReserved(@Nullable String key) {
    return key != null && RESERVED_KEYS.contains(key);
  }

  /** Returns the immutable set of exact official property keys (excludes prefix matches). */
  public static Set<String> definedKeys() {
    return DEFINED_KEYS;
  }
}
