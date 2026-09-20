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
import static org.apache.gravitino.file.Fileset.PROPERTY_LOCATION_PLACEHOLDER_PREFIX;
import static org.apache.gravitino.file.Fileset.PROPERTY_MULTIPLE_LOCATIONS_PREFIX;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Map;
import java.util.Set;
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
 * <p>Like {@link CredentialConfig#CREDENTIAL_PROPERTY_ENTRIES}, this class exposes a {@link
 * #PROPERTY_ENTRIES} map. Name-based (fuzzy) masking and secret recovery treat only keys that are
 * <em>not</em> in this map (and not matching {@link #LOCATION_PROPERTY_PREFIX}) as unknown. For
 * registered keys omitted from the current catalog {@link PropertiesMetadata}, {@link
 * PropertyEntry#isHidden()} / {@link PropertyEntry#isReserved()} from this map still apply.
 *
 * <p>Shared credential and cloud-storage entries are reused from existing metadata modules.
 * Connector-specific keys that may appear outside their owning catalog metadata (for example Glue
 * copying {@code s3-access-key-id}) are declared in {@link #CONNECTOR_PROPERTY_ENTRIES}. Prefer
 * shared {@code *PropertiesMetadata} for cloud credentials instead of duplicating them there.
 *
 * <p>{@link BasePropertiesMetadata} requires every connector {@code specificPropertyEntries()} key
 * to appear in shared cloud/credential metadata, base catalog keys, or this registry (including
 * test catalogs). Prefer shared {@code *PropertiesMetadata} for cloud credentials instead of
 * duplicating them in {@link #CONNECTOR_PROPERTY_ENTRIES}.
 */
public final class RegisteredPropertyKeys {

  /** Fileset multi-location property prefix ({@code location-<name>}). */
  public static final String LOCATION_PROPERTY_PREFIX = PROPERTY_MULTIPLE_LOCATIONS_PREFIX;

  /**
   * Credential-vending and shared cloud-storage entries from core metadata modules. Connectors that
   * only re-export these do not need to list them again in {@link #CONNECTOR_PROPERTY_ENTRIES}.
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

  /**
   * Connector and shared catalog/entity keys not covered by {@link
   * #SHARED_CLOUD_AND_CREDENTIAL_ENTRIES}. Descriptions are abbreviated; authoritative definitions
   * remain in each connector's {@code *PropertiesMetadata}.
   */
  private static final Map<String, PropertyEntry<?>> CONNECTOR_PROPERTY_ENTRIES =
      ImmutableMap.<String, PropertyEntry<?>>builder()
          // Shared catalog / metalake base
          .put(ID_KEY, reserved(ID_KEY, true))
          .put(PROPERTY_PACKAGE, optional(PROPERTY_PACKAGE))
          .put(CATALOG_OPERATION_IMPL, optional(CATALOG_OPERATION_IMPL))
          .put(AUTHORIZATION_PROVIDER, optional(AUTHORIZATION_PROVIDER))
          .put(CLOUD_NAME, optional(CLOUD_NAME))
          .put(CLOUD_REGION_CODE, optional(CLOUD_REGION_CODE))
          .put(PROPERTY_IN_USE, reserved(PROPERTY_IN_USE, false))
          .put(PROPERTY_METALAKE_IN_USE, reserved(PROPERTY_METALAKE_IN_USE, true))
          .put(
              PROPERTY_MULTIPLE_LOCATIONS_PREFIX + LOCATION_NAME_UNKNOWN,
              reserved(PROPERTY_MULTIPLE_LOCATIONS_PREFIX + LOCATION_NAME_UNKNOWN, true))
          .put(
              PROPERTY_MULTIPLE_LOCATIONS_PREFIX,
              optionalPrefix(PROPERTY_MULTIPLE_LOCATIONS_PREFIX))
          .put(
              PROPERTY_LOCATION_PLACEHOLDER_PREFIX,
              optionalPrefix(PROPERTY_LOCATION_PLACEHOLDER_PREFIX))
          // Hive / Kafka catalog connection
          .put("metastore.uris", optional("metastore.uris"))
          .put("bootstrap.servers", optional("bootstrap.servers"))
          // Glue
          .put("aws-access-key-id", optional("aws-access-key-id"))
          .put("aws-glue-catalog-id", optional("aws-glue-catalog-id"))
          .put("aws-glue-endpoint", optional("aws-glue-endpoint"))
          .put("aws-region", optional("aws-region"))
          .put("aws-secret-access-key", optionalHidden("aws-secret-access-key"))
          .put("default-table-format", optional("default-table-format"))
          .put("table-format", optional("table-format"))
          .put("table-format-filter", optional("table-format-filter"))
          .put("format", optional("format"))
          .put("input-format", reserved("input-format", false))
          .put("output-format", reserved("output-format", false))
          .put("serde-lib", optional("serde-lib"))
          .put("metadata_location", optional("metadata_location"))
          // Hive / Hudi client + auth
          .put("client.pool-size", optional("client.pool-size"))
          .put(
              "client.pool-cache.eviction-interval-ms",
              optional("client.pool-cache.eviction-interval-ms"))
          .put("default.catalog", optional("default.catalog"))
          .put("impersonation-enable", optional("impersonation-enable"))
          .put("list-all-tables", optional("list-all-tables"))
          .put("kerberos.keytab-uri", optional("kerberos.keytab-uri"))
          .put("kerberos.principal", optional("kerberos.principal"))
          .put("kerberos.check-interval-sec", optional("kerberos.check-interval-sec"))
          .put("kerberos.keytab-fetch-timeout-sec", optional("kerberos.keytab-fetch-timeout-sec"))
          .put("authentication.type", optional("authentication.type"))
          .put(
              "authentication.impersonation-enable",
              optional("authentication.impersonation-enable"))
          .put("authentication.kerberos.keytab-uri", optional("authentication.kerberos.keytab-uri"))
          .put("authentication.kerberos.principal", optional("authentication.kerberos.principal"))
          // Fileset
          .put("location", optional("location"))
          .put("warehouse", optional("warehouse"))
          .put("default-location-name", optional("default-location-name"))
          .put("default-filesystem-provider", optional("default-filesystem-provider"))
          .put("disable-filesystem-ops", optional("disable-filesystem-ops"))
          .put("filesystem-providers", optional("filesystem-providers"))
          .put("placeholder-catalog", reserved("placeholder-catalog", true))
          .put("placeholder-fileset", reserved("placeholder-fileset", true))
          .put("placeholder-schema", reserved("placeholder-schema", true))
          // Hive / Hudi / Iceberg / Paimon table & schema
          .put("comment", reserved("comment", true))
          .put("EXTERNAL", reserved("EXTERNAL", true))
          .put("external", optional("external"))
          .put("numFiles", reserved("numFiles", false))
          .put("totalSize", reserved("totalSize", false))
          .put("transient_lastDdlTime", reserved("transient_lastDdlTime", false))
          .put("presto_view", reserved("presto_view", true))
          .put("serde-name", optional("serde-name"))
          .put("owner", reserved("owner", false))
          .put("creator", reserved("creator", false))
          .put("current-snapshot-id", reserved("current-snapshot-id", false))
          .put("cherry-pick-snapshot-id", reserved("cherry-pick-snapshot-id", false))
          .put("identifier-fields", reserved("identifier-fields", false))
          .put("sort-order", reserved("sort-order", false))
          .put("provider", optional("provider"))
          .put("io-impl", optional("io-impl"))
          .put("data-access", optional("data-access"))
          .put("table-metadata-cache-impl", optional("table-metadata-cache-impl"))
          .put("jdbc-user", optional("jdbc-user"))
          .put("jdbc-password", optionalHidden("jdbc-password"))
          .put("jdbc-driver", optional("jdbc-driver"))
          .put("uri", optional("uri"))
          .put("token", optionalHidden("token"))
          .put("token-provider", optional("token-provider"))
          .put("dlf-access-key-id", optional("dlf-access-key-id"))
          .put("dlf-access-key-secret", optionalHidden("dlf-access-key-secret"))
          .put("dlf-security-token", optionalHidden("dlf-security-token"))
          .put("dlf-token-loader", optional("dlf-token-loader"))
          .put("dlf-token-path", optional("dlf-token-path"))
          .put("bucket", reserved("bucket", false))
          .put("bucket-key", reserved("bucket-key", false))
          .put("partition", reserved("partition", false))
          .put("primary-key", reserved("primary-key", false))
          .put("merge-engine", optional("merge-engine"))
          .put("rowkind.field", optional("rowkind.field"))
          .put("sequence.field", optional("sequence.field"))
          .put("gravitino.view.default-catalog", reserved("gravitino.view.default-catalog", true))
          .put("gravitino.view.default-schema", reserved("gravitino.view.default-schema", true))
          // Lance
          .put("lance", optional("lance"))
          .put("lance.creation-mode", optional("lance.creation-mode"))
          .put("lance.declared", optional("lance.declared"))
          .put("lance.register", optional("lance.register"))
          .put("lance.schema-refresh-mode", optional("lance.schema-refresh-mode"))
          .put("lance.version", optional("lance.version"))
          // Doris
          .put("bloom_filter_columns", optional("bloom_filter_columns"))
          .put("compression", optional("compression"))
          .put("enable_unique_key_merge_on_write", optional("enable_unique_key_merge_on_write"))
          .put("light_schema_change", optional("light_schema_change"))
          .put("replication_allocation", optional("replication_allocation"))
          .put("storage_policy", optional("storage_policy"))
          .put("PartitionName", reserved("PartitionName", false))
          .put("PartitionId", reserved("PartitionId", false))
          .put("PartitionKey", reserved("PartitionKey", false))
          .put("Range", reserved("Range", false))
          .put("VisibleVersion", reserved("VisibleVersion", false))
          .put("VisibleVersionTime", reserved("VisibleVersionTime", false))
          .put("State", reserved("State", false))
          .put("DataSize", reserved("DataSize", false))
          .put("IsInMemory", reserved("IsInMemory", false))
          .put("file", reserved("file", false))
          // ClickHouse
          .put("cluster-name", optional("cluster-name"))
          .put("cluster-remote-database", optional("cluster-remote-database"))
          .put("cluster-remote-table", optional("cluster-remote-table"))
          .put("cluster-sharding-key", optional("cluster-sharding-key"))
          .put("engine_parameters", optional("engine_parameters"))
          .put("graphite.config", optional("graphite.config"))
          .put("on-cluster", optional("on-cluster"))
          .put("partition-key", optional("partition-key"))
          // Model
          .put("default-uri-name", optional("default-uri-name"))
          // Core unit-test catalog (TestCatalog) — same strict registration as production
          .put("key1", optional("key1"))
          .put("key2", optional("key2"))
          .put("key3", optional("key3"))
          .put("key4", optional("key4"))
          .put("reserved_key", reserved("reserved_key", false))
          .put("hidden_key", optionalHidden("hidden_key"))
          .put("fail-create", optional("fail-create"))
          .put("key5-", optionalPrefix("key5-"))
          .put("key6-", optionalPrefix("key6-"))
          .build();

  /**
   * All registered Gravitino property entries used for cross-catalog fuzzy-mask and secret-recovery
   * fallbacks, and for {@link BasePropertiesMetadata} connector-key registration checks. Shared
   * cloud/credential maps are included as-is; connector keys are summaries for masking / check
   * semantics.
   */
  public static final Map<String, PropertyEntry<?>> PROPERTY_ENTRIES =
      ImmutableMap.<String, PropertyEntry<?>>builder()
          .putAll(SHARED_CLOUD_AND_CREDENTIAL_ENTRIES)
          .putAll(CONNECTOR_PROPERTY_ENTRIES)
          .build();

  private static final Set<String> REGISTERED_PREFIXES =
      PROPERTY_ENTRIES.values().stream()
          .filter(PropertyEntry::isPrefix)
          .map(PropertyEntry::getName)
          .collect(ImmutableSet.toImmutableSet());

  private RegisteredPropertyKeys() {}

  /**
   * Returns whether {@code key} is a registered Gravitino property name (exact match or a
   * registered property prefix).
   *
   * @param key property key
   * @return true when the key is registered
   */
  public static boolean isRegistered(@Nullable String key) {
    if (key == null || key.isEmpty()) {
      return false;
    }
    if (PROPERTY_ENTRIES.containsKey(key)) {
      return true;
    }
    for (String prefix : REGISTERED_PREFIXES) {
      if (key.startsWith(prefix)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns whether {@code key} is a shared credential-vending or cloud-storage property.
   *
   * @param key property key
   * @return true when the key comes from credential or shared cloud metadata
   */
  public static boolean isSharedCloudOrCredentialKey(@Nullable String key) {
    return key != null && SHARED_CLOUD_AND_CREDENTIAL_ENTRIES.containsKey(key);
  }

  /**
   * Returns whether a registered property is hidden when the catalog metadata does not declare it.
   *
   * @param key property key
   * @return true when the registered definition marks the key hidden
   */
  public static boolean isHidden(@Nullable String key) {
    if (key == null) {
      return false;
    }
    PropertyEntry<?> entry = PROPERTY_ENTRIES.get(key);
    return entry != null && entry.isHidden();
  }

  /**
   * Returns whether a registered property is reserved when the catalog metadata does not declare
   * it.
   *
   * @param key property key
   * @return true when the registered definition marks the key reserved
   */
  public static boolean isReserved(@Nullable String key) {
    if (key == null) {
      return false;
    }
    PropertyEntry<?> entry = PROPERTY_ENTRIES.get(key);
    return entry != null && entry.isReserved();
  }

  /** Returns the immutable map of exact registered property entries (excludes prefix matches). */
  public static Map<String, PropertyEntry<?>> propertyEntries() {
    return PROPERTY_ENTRIES;
  }

  private static PropertyEntry<String> optional(String name) {
    return PropertyEntry.stringOptionalPropertyEntry(
        name, name, false /* immutable */, null /* defaultValue */, false /* hidden */);
  }

  private static PropertyEntry<String> optionalHidden(String name) {
    return PropertyEntry.stringOptionalPropertyEntry(
        name, name, false /* immutable */, null /* defaultValue */, true /* hidden */);
  }

  private static PropertyEntry<String> reserved(String name, boolean hidden) {
    return PropertyEntry.stringReservedPropertyEntry(name, name, hidden);
  }

  private static PropertyEntry<String> optionalPrefix(String name) {
    return PropertyEntry.stringImmutablePropertyPrefixEntry(
        name,
        name,
        false /* required */,
        null /* defaultValue */,
        false /* hidden */,
        false /* reserved */);
  }
}
