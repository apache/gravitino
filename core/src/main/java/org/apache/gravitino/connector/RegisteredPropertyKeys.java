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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
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
 * <em>not</em> in this map (and do not match a registered property prefix) as unknown. For
 * registered keys omitted from the current catalog {@link PropertiesMetadata}, the {@link
 * PropertyEntry} hidden / reserved flags from this map still apply.
 *
 * <p>Shared credential and cloud-storage entries are reused from existing metadata modules.
 * Connector-specific keys that may appear outside their owning catalog metadata (for example Glue
 * copying {@code s3-access-key-id}) are declared in {@link #CONNECTOR_PROPERTY_ENTRIES}. Prefer
 * shared {@code *PropertiesMetadata} for cloud credentials instead of duplicating them there.
 *
 * <p>{@link BasePropertiesMetadata} requires every production connector {@code
 * specificPropertyEntries()} key to appear in shared cloud/credential metadata, base catalog keys,
 * or this registry. Do not register test-only keys here — test catalogs should override the check
 * in their own {@code PropertiesMetadata}. Prefer shared {@code *PropertiesMetadata} for cloud
 * credentials instead of duplicating them in {@link #CONNECTOR_PROPERTY_ENTRIES}.
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
   * #SHARED_CLOUD_AND_CREDENTIAL_ENTRIES}. Descriptions reuse the key name; authoritative
   * definitions remain in each connector's {@code *PropertiesMetadata}.
   */
  private static final Map<String, PropertyEntry<?>> CONNECTOR_PROPERTY_ENTRIES =
      Maps.uniqueIndex(
          ImmutableList.of(
              // Shared catalog / metalake base
              reservedHidden(ID_KEY),
              optional(PROPERTY_PACKAGE),
              optional(CATALOG_OPERATION_IMPL),
              optional(AUTHORIZATION_PROVIDER),
              optional(CLOUD_NAME),
              optional(CLOUD_REGION_CODE),
              reserved(PROPERTY_IN_USE),
              reservedHidden(PROPERTY_METALAKE_IN_USE),
              reservedHidden(PROPERTY_MULTIPLE_LOCATIONS_PREFIX + LOCATION_NAME_UNKNOWN),
              prefix(PROPERTY_MULTIPLE_LOCATIONS_PREFIX),
              prefix(PROPERTY_LOCATION_PLACEHOLDER_PREFIX),
              // Hive / Kafka catalog connection
              optional("metastore.uris"),
              optional("bootstrap.servers"),
              // Glue
              optional("aws-access-key-id"),
              optional("aws-glue-catalog-id"),
              optional("aws-glue-endpoint"),
              optional("aws-region"),
              optionalHidden("aws-secret-access-key"),
              optional("default-table-format"),
              optional("table-format"),
              optional("table-format-filter"),
              optional("format"),
              reserved("input-format"),
              reserved("output-format"),
              optional("serde-lib"),
              optional("metadata_location"),
              // Hive / Hudi client + auth
              optional("client.pool-size"),
              optional("client.pool-cache.eviction-interval-ms"),
              optional("default.catalog"),
              optional("impersonation-enable"),
              optional("list-all-tables"),
              optional("kerberos.keytab-uri"),
              optional("kerberos.principal"),
              optional("kerberos.check-interval-sec"),
              optional("kerberos.keytab-fetch-timeout-sec"),
              optional("authentication.type"),
              optional("authentication.impersonation-enable"),
              optional("authentication.kerberos.keytab-uri"),
              optional("authentication.kerberos.principal"),
              // Fileset
              optional("location"),
              optional("warehouse"),
              optional("default-location-name"),
              optional("default-filesystem-provider"),
              optional("disable-filesystem-ops"),
              optional("filesystem-providers"),
              reservedHidden("placeholder-catalog"),
              reservedHidden("placeholder-fileset"),
              reservedHidden("placeholder-schema"),
              // Hive / Hudi / Iceberg / Paimon table & schema
              reservedHidden("comment"),
              reservedHidden("EXTERNAL"),
              optional("external"),
              reserved("numFiles"),
              reserved("totalSize"),
              reserved("transient_lastDdlTime"),
              reservedHidden("presto_view"),
              optional("serde-name"),
              reserved("owner"),
              reserved("creator"),
              reserved("current-snapshot-id"),
              reserved("cherry-pick-snapshot-id"),
              reserved("identifier-fields"),
              reserved("sort-order"),
              optional("provider"),
              optional("io-impl"),
              optional("data-access"),
              optional("table-metadata-cache-impl"),
              optional("jdbc-user"),
              optionalHidden("jdbc-password"),
              optional("jdbc-driver"),
              optional("uri"),
              optionalHidden("token"),
              optional("token-provider"),
              optional("dlf-access-key-id"),
              optionalHidden("dlf-access-key-secret"),
              optionalHidden("dlf-security-token"),
              optional("dlf-token-loader"),
              optional("dlf-token-path"),
              reserved("bucket"),
              reserved("bucket-key"),
              reserved("partition"),
              reserved("primary-key"),
              optional("merge-engine"),
              optional("rowkind.field"),
              optional("sequence.field"),
              reservedHidden("gravitino.view.default-catalog"),
              reservedHidden("gravitino.view.default-schema"),
              // Lance
              optional("lance"),
              optional("lance.creation-mode"),
              optional("lance.declared"),
              optional("lance.register"),
              optional("lance.schema-refresh-mode"),
              optional("lance.version"),
              // Doris
              optional("bloom_filter_columns"),
              optional("compression"),
              optional("enable_unique_key_merge_on_write"),
              optional("light_schema_change"),
              optional("replication_allocation"),
              optional("storage_policy"),
              reserved("PartitionName"),
              reserved("PartitionId"),
              reserved("PartitionKey"),
              reserved("Range"),
              reserved("VisibleVersion"),
              reserved("VisibleVersionTime"),
              reserved("State"),
              reserved("DataSize"),
              reserved("IsInMemory"),
              reserved("file"),
              // ClickHouse
              optional("cluster-name"),
              optional("cluster-remote-database"),
              optional("cluster-remote-table"),
              optional("cluster-sharding-key"),
              optional("engine_parameters"),
              optional("graphite.config"),
              optional("on-cluster"),
              optional("partition-key"),
              // Hive / Iceberg / Paimon / Hudi / Kafka / Doris / MySQL / Fileset
              optional("catalog-backend"),
              optional("table-type"),
              optional("format-version"),
              optional("replication_num"),
              optional("engine"),
              optional("partition-count"),
              optional("replication-factor"),
              optional("auto-increment-offset"),
              optional("fileset-cache-max-size"),
              optional("fileset-cache-eviction-interval-ms"),
              optional("filesystem-conn-timeout-secs"),
              optional("table-metadata-cache-capacity"),
              optional("table-metadata-cache-expire-minutes"),
              optional("rest-client-connection-timeout-ms"),
              optional("rest-client-socket-timeout-ms"),
              optional("authentication.kerberos.check-interval-sec"),
              optional("authentication.kerberos.keytab-fetch-timeout-sec"),
              // Model
              optional("default-uri-name")),
          PropertyEntry::getName);

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

  /**
   * Optional mutable string property used only for registry membership / masking flags. Description
   * reuses {@code name}; connectors keep the authoritative entry.
   */
  private static PropertyEntry<String> optional(String name) {
    return PropertyEntry.stringOptionalPropertyEntry(name, name, false, null, false);
  }

  /** Like {@link #optional(String)} but marked hidden. */
  private static PropertyEntry<String> optionalHidden(String name) {
    return PropertyEntry.stringOptionalPropertyEntry(name, name, false, null, true);
  }

  /** Reserved visible string property for registry membership / masking flags. */
  private static PropertyEntry<String> reserved(String name) {
    return PropertyEntry.stringReservedPropertyEntry(name, name, false);
  }

  /** Reserved hidden string property for registry membership / masking flags. */
  private static PropertyEntry<String> reservedHidden(String name) {
    return PropertyEntry.stringReservedPropertyEntry(name, name, true);
  }

  /** Immutable property-prefix entry for registry membership / prefix matching. */
  private static PropertyEntry<String> prefix(String name) {
    return PropertyEntry.stringImmutablePropertyPrefixEntry(name, name, false, null, false, false);
  }
}
