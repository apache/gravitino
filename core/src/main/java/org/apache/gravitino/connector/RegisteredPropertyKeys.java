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
import static org.apache.gravitino.connector.PropertyEntry.stringImmutablePropertyPrefixEntry;
import static org.apache.gravitino.connector.PropertyEntry.stringOptionalPropertyEntry;
import static org.apache.gravitino.connector.PropertyEntry.stringReservedPropertyEntry;
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
          .put(ID_KEY, stringReservedPropertyEntry(ID_KEY, ID_KEY, true /* hidden */))
          .put(
              PROPERTY_PACKAGE,
              stringOptionalPropertyEntry(
                  PROPERTY_PACKAGE,
                  PROPERTY_PACKAGE,
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              CATALOG_OPERATION_IMPL,
              stringOptionalPropertyEntry(
                  CATALOG_OPERATION_IMPL,
                  CATALOG_OPERATION_IMPL,
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              AUTHORIZATION_PROVIDER,
              stringOptionalPropertyEntry(
                  AUTHORIZATION_PROVIDER,
                  AUTHORIZATION_PROVIDER,
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              CLOUD_NAME,
              stringOptionalPropertyEntry(
                  CLOUD_NAME,
                  CLOUD_NAME,
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              CLOUD_REGION_CODE,
              stringOptionalPropertyEntry(
                  CLOUD_REGION_CODE,
                  CLOUD_REGION_CODE,
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              PROPERTY_IN_USE,
              stringReservedPropertyEntry(PROPERTY_IN_USE, PROPERTY_IN_USE, false /* hidden */))
          .put(
              PROPERTY_METALAKE_IN_USE,
              stringReservedPropertyEntry(
                  PROPERTY_METALAKE_IN_USE, PROPERTY_METALAKE_IN_USE, true /* hidden */))
          .put(
              PROPERTY_MULTIPLE_LOCATIONS_PREFIX + LOCATION_NAME_UNKNOWN,
              stringReservedPropertyEntry(
                  PROPERTY_MULTIPLE_LOCATIONS_PREFIX + LOCATION_NAME_UNKNOWN,
                  PROPERTY_MULTIPLE_LOCATIONS_PREFIX + LOCATION_NAME_UNKNOWN,
                  true /* hidden */))
          .put(
              PROPERTY_MULTIPLE_LOCATIONS_PREFIX,
              stringImmutablePropertyPrefixEntry(
                  PROPERTY_MULTIPLE_LOCATIONS_PREFIX,
                  PROPERTY_MULTIPLE_LOCATIONS_PREFIX,
                  false /* required */,
                  null /* defaultValue */,
                  false /* hidden */,
                  false /* reserved */))
          .put(
              PROPERTY_LOCATION_PLACEHOLDER_PREFIX,
              stringImmutablePropertyPrefixEntry(
                  PROPERTY_LOCATION_PLACEHOLDER_PREFIX,
                  PROPERTY_LOCATION_PLACEHOLDER_PREFIX,
                  false /* required */,
                  null /* defaultValue */,
                  false /* hidden */,
                  false /* reserved */))
          // Hive / Kafka catalog connection
          .put(
              "metastore.uris",
              stringOptionalPropertyEntry(
                  "metastore.uris",
                  "metastore.uris",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "bootstrap.servers",
              stringOptionalPropertyEntry(
                  "bootstrap.servers",
                  "bootstrap.servers",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          // Glue
          .put(
              "aws-access-key-id",
              stringOptionalPropertyEntry(
                  "aws-access-key-id",
                  "aws-access-key-id",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "aws-glue-catalog-id",
              stringOptionalPropertyEntry(
                  "aws-glue-catalog-id",
                  "aws-glue-catalog-id",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "aws-glue-endpoint",
              stringOptionalPropertyEntry(
                  "aws-glue-endpoint",
                  "aws-glue-endpoint",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "aws-region",
              stringOptionalPropertyEntry(
                  "aws-region",
                  "aws-region",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "aws-secret-access-key",
              stringOptionalPropertyEntry(
                  "aws-secret-access-key",
                  "aws-secret-access-key",
                  false /* immutable */,
                  null /* defaultValue */,
                  true /* hidden */))
          .put(
              "default-table-format",
              stringOptionalPropertyEntry(
                  "default-table-format",
                  "default-table-format",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "table-format",
              stringOptionalPropertyEntry(
                  "table-format",
                  "table-format",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "table-format-filter",
              stringOptionalPropertyEntry(
                  "table-format-filter",
                  "table-format-filter",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "format",
              stringOptionalPropertyEntry(
                  "format",
                  "format",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "input-format",
              stringReservedPropertyEntry("input-format", "input-format", false /* hidden */))
          .put(
              "output-format",
              stringReservedPropertyEntry("output-format", "output-format", false /* hidden */))
          .put(
              "serde-lib",
              stringOptionalPropertyEntry(
                  "serde-lib",
                  "serde-lib",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "metadata_location",
              stringOptionalPropertyEntry(
                  "metadata_location",
                  "metadata_location",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          // Hive / Hudi client + auth
          .put(
              "client.pool-size",
              stringOptionalPropertyEntry(
                  "client.pool-size",
                  "client.pool-size",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "client.pool-cache.eviction-interval-ms",
              stringOptionalPropertyEntry(
                  "client.pool-cache.eviction-interval-ms",
                  "client.pool-cache.eviction-interval-ms",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "default.catalog",
              stringOptionalPropertyEntry(
                  "default.catalog",
                  "default.catalog",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "impersonation-enable",
              stringOptionalPropertyEntry(
                  "impersonation-enable",
                  "impersonation-enable",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "list-all-tables",
              stringOptionalPropertyEntry(
                  "list-all-tables",
                  "list-all-tables",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "kerberos.keytab-uri",
              stringOptionalPropertyEntry(
                  "kerberos.keytab-uri",
                  "kerberos.keytab-uri",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "kerberos.principal",
              stringOptionalPropertyEntry(
                  "kerberos.principal",
                  "kerberos.principal",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "kerberos.check-interval-sec",
              stringOptionalPropertyEntry(
                  "kerberos.check-interval-sec",
                  "kerberos.check-interval-sec",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "kerberos.keytab-fetch-timeout-sec",
              stringOptionalPropertyEntry(
                  "kerberos.keytab-fetch-timeout-sec",
                  "kerberos.keytab-fetch-timeout-sec",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "authentication.type",
              stringOptionalPropertyEntry(
                  "authentication.type",
                  "authentication.type",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "authentication.impersonation-enable",
              stringOptionalPropertyEntry(
                  "authentication.impersonation-enable",
                  "authentication.impersonation-enable",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "authentication.kerberos.keytab-uri",
              stringOptionalPropertyEntry(
                  "authentication.kerberos.keytab-uri",
                  "authentication.kerberos.keytab-uri",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "authentication.kerberos.principal",
              stringOptionalPropertyEntry(
                  "authentication.kerberos.principal",
                  "authentication.kerberos.principal",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          // Fileset
          .put(
              "location",
              stringOptionalPropertyEntry(
                  "location",
                  "location",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "warehouse",
              stringOptionalPropertyEntry(
                  "warehouse",
                  "warehouse",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "default-location-name",
              stringOptionalPropertyEntry(
                  "default-location-name",
                  "default-location-name",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "default-filesystem-provider",
              stringOptionalPropertyEntry(
                  "default-filesystem-provider",
                  "default-filesystem-provider",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "disable-filesystem-ops",
              stringOptionalPropertyEntry(
                  "disable-filesystem-ops",
                  "disable-filesystem-ops",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "filesystem-providers",
              stringOptionalPropertyEntry(
                  "filesystem-providers",
                  "filesystem-providers",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "placeholder-catalog",
              stringReservedPropertyEntry(
                  "placeholder-catalog", "placeholder-catalog", true /* hidden */))
          .put(
              "placeholder-fileset",
              stringReservedPropertyEntry(
                  "placeholder-fileset", "placeholder-fileset", true /* hidden */))
          .put(
              "placeholder-schema",
              stringReservedPropertyEntry(
                  "placeholder-schema", "placeholder-schema", true /* hidden */))
          // Hive / Hudi / Iceberg / Paimon table & schema
          .put("comment", stringReservedPropertyEntry("comment", "comment", true /* hidden */))
          .put("EXTERNAL", stringReservedPropertyEntry("EXTERNAL", "EXTERNAL", true /* hidden */))
          .put(
              "external",
              stringOptionalPropertyEntry(
                  "external",
                  "external",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put("numFiles", stringReservedPropertyEntry("numFiles", "numFiles", false /* hidden */))
          .put(
              "totalSize",
              stringReservedPropertyEntry("totalSize", "totalSize", false /* hidden */))
          .put(
              "transient_lastDdlTime",
              stringReservedPropertyEntry(
                  "transient_lastDdlTime", "transient_lastDdlTime", false /* hidden */))
          .put(
              "presto_view",
              stringReservedPropertyEntry("presto_view", "presto_view", true /* hidden */))
          .put(
              "serde-name",
              stringOptionalPropertyEntry(
                  "serde-name",
                  "serde-name",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put("owner", stringReservedPropertyEntry("owner", "owner", false /* hidden */))
          .put("creator", stringReservedPropertyEntry("creator", "creator", false /* hidden */))
          .put(
              "current-snapshot-id",
              stringReservedPropertyEntry(
                  "current-snapshot-id", "current-snapshot-id", false /* hidden */))
          .put(
              "cherry-pick-snapshot-id",
              stringReservedPropertyEntry(
                  "cherry-pick-snapshot-id", "cherry-pick-snapshot-id", false /* hidden */))
          .put(
              "identifier-fields",
              stringReservedPropertyEntry(
                  "identifier-fields", "identifier-fields", false /* hidden */))
          .put(
              "sort-order",
              stringReservedPropertyEntry("sort-order", "sort-order", false /* hidden */))
          .put(
              "provider",
              stringOptionalPropertyEntry(
                  "provider",
                  "provider",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "io-impl",
              stringOptionalPropertyEntry(
                  "io-impl",
                  "io-impl",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "data-access",
              stringOptionalPropertyEntry(
                  "data-access",
                  "data-access",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "table-metadata-cache-impl",
              stringOptionalPropertyEntry(
                  "table-metadata-cache-impl",
                  "table-metadata-cache-impl",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "jdbc-user",
              stringOptionalPropertyEntry(
                  "jdbc-user",
                  "jdbc-user",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "jdbc-password",
              stringOptionalPropertyEntry(
                  "jdbc-password",
                  "jdbc-password",
                  false /* immutable */,
                  null /* defaultValue */,
                  true /* hidden */))
          .put(
              "jdbc-driver",
              stringOptionalPropertyEntry(
                  "jdbc-driver",
                  "jdbc-driver",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "uri",
              stringOptionalPropertyEntry(
                  "uri", "uri", false /* immutable */, null /* defaultValue */, false /* hidden */))
          .put(
              "token",
              stringOptionalPropertyEntry(
                  "token",
                  "token",
                  false /* immutable */,
                  null /* defaultValue */,
                  true /* hidden */))
          .put(
              "token-provider",
              stringOptionalPropertyEntry(
                  "token-provider",
                  "token-provider",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "dlf-access-key-id",
              stringOptionalPropertyEntry(
                  "dlf-access-key-id",
                  "dlf-access-key-id",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "dlf-access-key-secret",
              stringOptionalPropertyEntry(
                  "dlf-access-key-secret",
                  "dlf-access-key-secret",
                  false /* immutable */,
                  null /* defaultValue */,
                  true /* hidden */))
          .put(
              "dlf-security-token",
              stringOptionalPropertyEntry(
                  "dlf-security-token",
                  "dlf-security-token",
                  false /* immutable */,
                  null /* defaultValue */,
                  true /* hidden */))
          .put(
              "dlf-token-loader",
              stringOptionalPropertyEntry(
                  "dlf-token-loader",
                  "dlf-token-loader",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "dlf-token-path",
              stringOptionalPropertyEntry(
                  "dlf-token-path",
                  "dlf-token-path",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put("bucket", stringReservedPropertyEntry("bucket", "bucket", false /* hidden */))
          .put(
              "bucket-key",
              stringReservedPropertyEntry("bucket-key", "bucket-key", false /* hidden */))
          .put(
              "partition",
              stringReservedPropertyEntry("partition", "partition", false /* hidden */))
          .put(
              "primary-key",
              stringReservedPropertyEntry("primary-key", "primary-key", false /* hidden */))
          .put(
              "merge-engine",
              stringOptionalPropertyEntry(
                  "merge-engine",
                  "merge-engine",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "rowkind.field",
              stringOptionalPropertyEntry(
                  "rowkind.field",
                  "rowkind.field",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "sequence.field",
              stringOptionalPropertyEntry(
                  "sequence.field",
                  "sequence.field",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "gravitino.view.default-catalog",
              stringReservedPropertyEntry(
                  "gravitino.view.default-catalog",
                  "gravitino.view.default-catalog",
                  true /* hidden */))
          .put(
              "gravitino.view.default-schema",
              stringReservedPropertyEntry(
                  "gravitino.view.default-schema",
                  "gravitino.view.default-schema",
                  true /* hidden */))
          // Lance
          .put(
              "lance",
              stringOptionalPropertyEntry(
                  "lance",
                  "lance",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "lance.creation-mode",
              stringOptionalPropertyEntry(
                  "lance.creation-mode",
                  "lance.creation-mode",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "lance.declared",
              stringOptionalPropertyEntry(
                  "lance.declared",
                  "lance.declared",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "lance.register",
              stringOptionalPropertyEntry(
                  "lance.register",
                  "lance.register",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "lance.schema-refresh-mode",
              stringOptionalPropertyEntry(
                  "lance.schema-refresh-mode",
                  "lance.schema-refresh-mode",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "lance.version",
              stringOptionalPropertyEntry(
                  "lance.version",
                  "lance.version",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          // Doris
          .put(
              "bloom_filter_columns",
              stringOptionalPropertyEntry(
                  "bloom_filter_columns",
                  "bloom_filter_columns",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "compression",
              stringOptionalPropertyEntry(
                  "compression",
                  "compression",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "enable_unique_key_merge_on_write",
              stringOptionalPropertyEntry(
                  "enable_unique_key_merge_on_write",
                  "enable_unique_key_merge_on_write",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "light_schema_change",
              stringOptionalPropertyEntry(
                  "light_schema_change",
                  "light_schema_change",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "replication_allocation",
              stringOptionalPropertyEntry(
                  "replication_allocation",
                  "replication_allocation",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "storage_policy",
              stringOptionalPropertyEntry(
                  "storage_policy",
                  "storage_policy",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "PartitionName",
              stringReservedPropertyEntry("PartitionName", "PartitionName", false /* hidden */))
          .put(
              "PartitionId",
              stringReservedPropertyEntry("PartitionId", "PartitionId", false /* hidden */))
          .put(
              "PartitionKey",
              stringReservedPropertyEntry("PartitionKey", "PartitionKey", false /* hidden */))
          .put("Range", stringReservedPropertyEntry("Range", "Range", false /* hidden */))
          .put(
              "VisibleVersion",
              stringReservedPropertyEntry("VisibleVersion", "VisibleVersion", false /* hidden */))
          .put(
              "VisibleVersionTime",
              stringReservedPropertyEntry(
                  "VisibleVersionTime", "VisibleVersionTime", false /* hidden */))
          .put("State", stringReservedPropertyEntry("State", "State", false /* hidden */))
          .put("DataSize", stringReservedPropertyEntry("DataSize", "DataSize", false /* hidden */))
          .put(
              "IsInMemory",
              stringReservedPropertyEntry("IsInMemory", "IsInMemory", false /* hidden */))
          .put("file", stringReservedPropertyEntry("file", "file", false /* hidden */))
          // ClickHouse
          .put(
              "cluster-name",
              stringOptionalPropertyEntry(
                  "cluster-name",
                  "cluster-name",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "cluster-remote-database",
              stringOptionalPropertyEntry(
                  "cluster-remote-database",
                  "cluster-remote-database",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "cluster-remote-table",
              stringOptionalPropertyEntry(
                  "cluster-remote-table",
                  "cluster-remote-table",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "cluster-sharding-key",
              stringOptionalPropertyEntry(
                  "cluster-sharding-key",
                  "cluster-sharding-key",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "engine_parameters",
              stringOptionalPropertyEntry(
                  "engine_parameters",
                  "engine_parameters",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "graphite.config",
              stringOptionalPropertyEntry(
                  "graphite.config",
                  "graphite.config",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "on-cluster",
              stringOptionalPropertyEntry(
                  "on-cluster",
                  "on-cluster",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "partition-key",
              stringOptionalPropertyEntry(
                  "partition-key",
                  "partition-key",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          // Model
          .put(
              "default-uri-name",
              stringOptionalPropertyEntry(
                  "default-uri-name",
                  "default-uri-name",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          // Core unit-test catalog (TestCatalog) — same strict registration as production
          .put(
              "key1",
              stringOptionalPropertyEntry(
                  "key1",
                  "key1",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "key2",
              stringOptionalPropertyEntry(
                  "key2",
                  "key2",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "key3",
              stringOptionalPropertyEntry(
                  "key3",
                  "key3",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "key4",
              stringOptionalPropertyEntry(
                  "key4",
                  "key4",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "reserved_key",
              stringReservedPropertyEntry("reserved_key", "reserved_key", false /* hidden */))
          .put(
              "hidden_key",
              stringOptionalPropertyEntry(
                  "hidden_key",
                  "hidden_key",
                  false /* immutable */,
                  null /* defaultValue */,
                  true /* hidden */))
          .put(
              "fail-create",
              stringOptionalPropertyEntry(
                  "fail-create",
                  "fail-create",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "key5-",
              stringImmutablePropertyPrefixEntry(
                  "key5-",
                  "key5-",
                  false /* required */,
                  null /* defaultValue */,
                  false /* hidden */,
                  false /* reserved */))
          .put(
              "key6-",
              stringImmutablePropertyPrefixEntry(
                  "key6-",
                  "key6-",
                  false /* required */,
                  null /* defaultValue */,
                  false /* hidden */,
                  false /* reserved */))
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
}
