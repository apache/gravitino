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

import static org.apache.gravitino.Catalog.AUTHORIZATION_PROVIDER;
import static org.apache.gravitino.Catalog.CLOUD_NAME;
import static org.apache.gravitino.Catalog.CLOUD_REGION_CODE;
import static org.apache.gravitino.Catalog.PROPERTY_IN_USE;
import static org.apache.gravitino.Catalog.PROPERTY_PACKAGE;
import static org.apache.gravitino.StringIdentifier.ID_KEY;
import static org.apache.gravitino.connector.BaseCatalog.CATALOG_OPERATION_IMPL;
import static org.apache.gravitino.connector.BaseCatalogPropertiesMetadata.PROPERTY_METALAKE_IN_USE;
import static org.apache.gravitino.file.Fileset.LOCATION_NAME_UNKNOWN;
import static org.apache.gravitino.file.Fileset.PROPERTY_MULTIPLE_LOCATIONS_PREFIX;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import javax.annotation.Nullable;
import org.apache.gravitino.cloud.storage.AzurePropertiesMetadata;
import org.apache.gravitino.cloud.storage.COSPropertiesMetadata;
import org.apache.gravitino.cloud.storage.GCSPropertiesMetadata;
import org.apache.gravitino.cloud.storage.OSSPropertiesMetadata;
import org.apache.gravitino.cloud.storage.S3PropertiesMetadata;
import org.apache.gravitino.credential.config.CredentialConfig;

/**
 * Registry of property keys that Gravitino defines in base, credential, cloud-storage, and
 * connector metadata.
 *
 * <p>Name-based (fuzzy) masking and secret recovery treat only keys that are <em>not</em>
 * registered here as unknown. For registered keys omitted from the current catalog {@link
 * PropertiesMetadata}, {@link #isHidden(String)} / {@link #isReserved(String)} still apply (for
 * example a Glue runtime copy of {@code s3-access-key-id}).
 *
 * <p>{@link BasePropertiesMetadata} requires every connector {@code specificPropertyEntries()} key
 * to appear in shared cloud/credential metadata, base catalog keys, or this registry. User-supplied
 * entity property maps are not restricted by that check.
 *
 * <p>When adding a connector-defined {@link PropertyEntry}, update {@link #CONNECTOR_KEYS} (and
 * {@link #CONNECTOR_HIDDEN_KEYS} / {@link #CONNECTOR_RESERVED_KEYS} when applicable). Prefer shared
 * {@code *PropertiesMetadata} for cloud credentials instead of duplicating them here.
 */
public final class RegisteredPropertyKeys {

  /** Fileset multi-location property prefix ({@code location-<name>}). */
  public static final String LOCATION_PROPERTY_PREFIX = PROPERTY_MULTIPLE_LOCATIONS_PREFIX;

  /**
   * Credential-vending and shared cloud-storage {@link PropertyEntry} definitions from core
   * metadata modules. Connectors that only re-export these entries do not need to list them again
   * in {@link #CONNECTOR_KEYS}.
   */
  private static final Map<String, PropertyEntry<?>> SHARED_CLOUD_AND_CREDENTIAL_ENTRIES =
      ImmutableMap.<String, PropertyEntry<?>>builder()
          .putAll(CredentialConfig.CREDENTIAL_PROPERTY_ENTRIES)
          .putAll(S3PropertiesMetadata.PROPERTY_ENTRIES)
          .putAll(OSSPropertiesMetadata.PROPERTY_ENTRIES)
          .putAll(AzurePropertiesMetadata.PROPERTY_ENTRIES)
          .putAll(GCSPropertiesMetadata.PROPERTY_ENTRIES)
          .putAll(COSPropertiesMetadata.PROPERTY_ENTRIES)
          .build();

  private static final Set<String> SHARED_CLOUD_AND_CREDENTIAL_KEYS =
      SHARED_CLOUD_AND_CREDENTIAL_ENTRIES.keySet();

  /**
   * Catalog/entity keys defined by connectors (and shared catalog base fields) that are not covered
   * by {@link #SHARED_CLOUD_AND_CREDENTIAL_ENTRIES}. Grouped by area for maintenance.
   */
  private static final Set<String> CONNECTOR_KEYS =
      ImmutableSet.<String>builder()
          // Shared catalog / metalake base
          .add(ID_KEY)
          .add(PROPERTY_PACKAGE)
          .add(CATALOG_OPERATION_IMPL)
          .add(AUTHORIZATION_PROVIDER)
          .add(CLOUD_NAME)
          .add(CLOUD_REGION_CODE)
          .add(PROPERTY_IN_USE)
          .add(PROPERTY_METALAKE_IN_USE)
          .add(PROPERTY_MULTIPLE_LOCATIONS_PREFIX + LOCATION_NAME_UNKNOWN)
          // Glue
          .add(
              "aws-access-key-id",
              "aws-glue-catalog-id",
              "aws-glue-endpoint",
              "aws-region",
              "aws-secret-access-key")
          .add("default-table-format", "table-format", "table-format-filter")
          .add("format", "input-format", "output-format", "serde-lib", "metadata_location")
          // Hive / Hudi client + auth
          .add(
              "client.pool-size",
              "client.pool-cache.eviction-interval-ms",
              "default.catalog",
              "impersonation-enable",
              "list-all-tables")
          .add(
              "kerberos.keytab-uri",
              "kerberos.principal",
              "kerberos.check-interval-sec",
              "kerberos.keytab-fetch-timeout-sec")
          .add(
              "authentication.type",
              "authentication.impersonation-enable",
              "authentication.kerberos.keytab-uri",
              "authentication.kerberos.principal")
          // Fileset
          .add(
              "location",
              "warehouse",
              "default-location-name",
              "default-filesystem-provider",
              "disable-filesystem-ops",
              "filesystem-providers")
          .add("placeholder-catalog", "placeholder-fileset", "placeholder-schema")
          // Hive / Hudi / Iceberg / Paimon table & schema
          .add("comment", "EXTERNAL", "external", "numFiles", "totalSize")
          .add("transient_lastDdlTime", "presto_view", "serde-name", "owner", "creator")
          .add(
              "current-snapshot-id",
              "cherry-pick-snapshot-id",
              "identifier-fields",
              "sort-order",
              "provider")
          .add("io-impl", "data-access", "table-metadata-cache-impl", "jdbc-user", "jdbc-password")
          .add("jdbc-driver", "uri")
          .add(
              "token",
              "token-provider",
              "dlf-access-key-id",
              "dlf-access-key-secret",
              "dlf-security-token")
          .add("dlf-token-loader", "dlf-token-path")
          .add(
              "bucket",
              "bucket-key",
              "partition",
              "primary-key",
              "merge-engine",
              "rowkind.field",
              "sequence.field")
          .add("gravitino.view.default-catalog", "gravitino.view.default-schema")
          // Lance
          .add(
              "lance",
              "lance.creation-mode",
              "lance.declared",
              "lance.register",
              "lance.schema-refresh-mode",
              "lance.version")
          // Doris
          .add(
              "bloom_filter_columns",
              "compression",
              "enable_unique_key_merge_on_write",
              "light_schema_change",
              "replication_allocation",
              "storage_policy")
          .add(
              "PartitionName",
              "PartitionId",
              "PartitionKey",
              "Range",
              "VisibleVersion",
              "VisibleVersionTime",
              "State",
              "DataSize",
              "IsInMemory",
              "file")
          // ClickHouse
          .add(
              "cluster-name",
              "cluster-remote-database",
              "cluster-remote-table",
              "cluster-sharding-key",
              "engine_parameters",
              "graphite.config",
              "on-cluster",
              "partition-key")
          // Model
          .add("default-uri-name")
          .build();

  private static final Set<String> REGISTERED_KEYS =
      ImmutableSet.<String>builder()
          .addAll(SHARED_CLOUD_AND_CREDENTIAL_KEYS)
          .addAll(CONNECTOR_KEYS)
          .build();

  /**
   * Connector-defined keys that are hidden when the current catalog metadata does not declare them.
   * Shared cloud/credential hidden flags are derived from {@link
   * #SHARED_CLOUD_AND_CREDENTIAL_ENTRIES}.
   */
  private static final Set<String> CONNECTOR_HIDDEN_KEYS =
      ImmutableSet.of(
          ID_KEY,
          PROPERTY_METALAKE_IN_USE,
          PROPERTY_MULTIPLE_LOCATIONS_PREFIX + LOCATION_NAME_UNKNOWN,
          "EXTERNAL",
          "aws-secret-access-key",
          "comment",
          "dlf-access-key-secret",
          "dlf-security-token",
          "gravitino.view.default-catalog",
          "gravitino.view.default-schema",
          "jdbc-password",
          "placeholder-catalog",
          "placeholder-fileset",
          "placeholder-schema",
          "presto_view",
          "token");

  /**
   * Connector-defined keys that are reserved when the current catalog metadata does not declare
   * them. Shared cloud/credential reserved flags are derived from {@link
   * #SHARED_CLOUD_AND_CREDENTIAL_ENTRIES}.
   */
  private static final Set<String> CONNECTOR_RESERVED_KEYS =
      ImmutableSet.of(
          ID_KEY,
          PROPERTY_IN_USE,
          PROPERTY_METALAKE_IN_USE,
          PROPERTY_MULTIPLE_LOCATIONS_PREFIX + LOCATION_NAME_UNKNOWN,
          "DataSize",
          "EXTERNAL",
          "IsInMemory",
          "PartitionId",
          "PartitionKey",
          "PartitionName",
          "Range",
          "State",
          "VisibleVersion",
          "VisibleVersionTime",
          "bucket",
          "bucket-key",
          "cherry-pick-snapshot-id",
          "comment",
          "creator",
          "current-snapshot-id",
          "file",
          "gravitino.view.default-catalog",
          "gravitino.view.default-schema",
          "identifier-fields",
          "input-format",
          "numFiles",
          "output-format",
          "owner",
          "partition",
          "placeholder-catalog",
          "placeholder-fileset",
          "placeholder-schema",
          "presto_view",
          "primary-key",
          "sort-order",
          "totalSize",
          "transient_lastDdlTime");

  private static final Set<String> HIDDEN_KEYS =
      ImmutableSet.<String>builder()
          .addAll(keysMatching(SHARED_CLOUD_AND_CREDENTIAL_ENTRIES, PropertyEntry::isHidden))
          .addAll(CONNECTOR_HIDDEN_KEYS)
          .build();

  private static final Set<String> RESERVED_KEYS =
      ImmutableSet.<String>builder()
          .addAll(keysMatching(SHARED_CLOUD_AND_CREDENTIAL_ENTRIES, PropertyEntry::isReserved))
          .addAll(CONNECTOR_RESERVED_KEYS)
          .build();

  private RegisteredPropertyKeys() {}

  /**
   * Returns whether {@code key} is a registered Gravitino property name (exact match or fileset
   * location prefix).
   *
   * @param key property key
   * @return true when the key is registered
   */
  public static boolean isRegistered(@Nullable String key) {
    if (key == null || key.isEmpty()) {
      return false;
    }
    return REGISTERED_KEYS.contains(key) || key.startsWith(LOCATION_PROPERTY_PREFIX);
  }

  /**
   * Returns whether {@code key} is a shared credential-vending or cloud-storage property.
   *
   * @param key property key
   * @return true when the key comes from credential or shared cloud metadata
   */
  public static boolean isSharedCloudOrCredentialKey(@Nullable String key) {
    return key != null && SHARED_CLOUD_AND_CREDENTIAL_KEYS.contains(key);
  }

  /**
   * Returns whether a registered property is hidden when the catalog metadata does not declare it.
   *
   * @param key property key
   * @return true when the registered definition marks the key hidden
   */
  public static boolean isHidden(@Nullable String key) {
    return key != null && HIDDEN_KEYS.contains(key);
  }

  /**
   * Returns whether a registered property is reserved when the catalog metadata does not declare
   * it.
   *
   * @param key property key
   * @return true when the registered definition marks the key reserved
   */
  public static boolean isReserved(@Nullable String key) {
    return key != null && RESERVED_KEYS.contains(key);
  }

  /** Returns the immutable set of exact registered property keys (excludes prefix matches). */
  public static Set<String> registeredKeys() {
    return REGISTERED_KEYS;
  }

  private static Set<String> keysMatching(
      Map<String, PropertyEntry<?>> entries, Predicate<PropertyEntry<?>> match) {
    return entries.values().stream()
        .filter(match)
        .map(PropertyEntry::getName)
        .collect(ImmutableSet.toImmutableSet());
  }
}
