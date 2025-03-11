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

package org.apache.gravitino.trino.connector.catalog.iceberg;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.trino.spi.TrinoException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.commons.collections4.bidimap.TreeBidiMap;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergPropertiesUtils;
import org.apache.gravitino.catalog.property.PropertyConverter;
import org.apache.gravitino.trino.connector.GravitinoErrorCode;

public class IcebergCatalogPropertyConverter extends PropertyConverter {

  private static final TreeBidiMap<String, String> TRINO_ICEBERG_TO_GRAVITINO_ICEBERG =
      new TreeBidiMap<>(
          new ImmutableMap.Builder<String, String>()
              // General configuration
              .put("iceberg.catalog.type", TRINO_PROPERTIES_PREFIX + "iceberg.catalog.type")
              .put("iceberg.file-format", TRINO_PROPERTIES_PREFIX + "iceberg.file-format")
              .put(
                  "iceberg.compression-codec",
                  TRINO_PROPERTIES_PREFIX + "iceberg.compression-codec")
              .put(
                  "iceberg.use-file-size-from-metadata",
                  TRINO_PROPERTIES_PREFIX + "iceberg.use-file-size-from-metadata")
              .put(
                  "iceberg.max-partitions-per-writer",
                  TRINO_PROPERTIES_PREFIX + "iceberg.max-partitions-per-writer")
              .put(
                  "iceberg.target-max-file-size",
                  TRINO_PROPERTIES_PREFIX + "iceberg.target-max-file-size")
              .put(
                  "iceberg.unique-table-location",
                  TRINO_PROPERTIES_PREFIX + "iceberg.unique-table-location")
              .put(
                  "iceberg.dynamic-filtering.wait-timeout",
                  TRINO_PROPERTIES_PREFIX + "iceberg.dynamic-filtering.wait-timeout")
              .put(
                  "iceberg.delete-schema-locations-fallback",
                  TRINO_PROPERTIES_PREFIX + "iceberg.delete-schema-locations-fallback")
              .put(
                  "iceberg.minimum-assigned-split-weight",
                  TRINO_PROPERTIES_PREFIX + "iceberg.minimum-assigned-split-weight")
              .put(
                  "iceberg.table-statistics-enabled",
                  TRINO_PROPERTIES_PREFIX + "iceberg.table-statistics-enabled")
              .put(
                  "iceberg.extended-statistics.enabled",
                  TRINO_PROPERTIES_PREFIX + "iceberg.extended-statistics.enabled")
              .put(
                  "iceberg.extended-statistics.collect-on-write",
                  TRINO_PROPERTIES_PREFIX + "iceberg.extended-statistics.collect-on-write")
              .put(
                  "iceberg.projection-pushdown-enabled",
                  TRINO_PROPERTIES_PREFIX + "iceberg.projection-pushdown-enabled")
              .put(
                  "iceberg.hive-catalog-name",
                  TRINO_PROPERTIES_PREFIX + "iceberg.hive-catalog-name")
              .put(
                  "iceberg.materialized-views.storage-schema",
                  TRINO_PROPERTIES_PREFIX + "iceberg.materialized-views.storage-schema")
              .put(
                  "iceberg.materialized-views.hide-storage-table",
                  TRINO_PROPERTIES_PREFIX + "iceberg.materialized-views.hide-storage-table")
              .put(
                  "iceberg.register-table-procedure.enabled",
                  TRINO_PROPERTIES_PREFIX + "iceberg.register-table-procedure.enabled")
              .put(
                  "iceberg.query-partition-filter-required",
                  TRINO_PROPERTIES_PREFIX + "iceberg.query-partition-filter-required")

              // Hive
              .put("hive.config.resources", TRINO_PROPERTIES_PREFIX + "hive.config.resources")
              .put(
                  "hive.recursive-directories",
                  TRINO_PROPERTIES_PREFIX + "hive.recursive-directories")
              .put(
                  "hive.ignore-absent-partitions",
                  TRINO_PROPERTIES_PREFIX + "hive.ignore-absent-partitions")
              .put("hive.storage-format", TRINO_PROPERTIES_PREFIX + "hive.storage-format")
              .put("hive.compression-codec", TRINO_PROPERTIES_PREFIX + "hive.compression-codec")
              .put(
                  "hive.force-local-scheduling",
                  TRINO_PROPERTIES_PREFIX + "hive.force-local-scheduling")
              .put(
                  "hive.respect-table-format",
                  TRINO_PROPERTIES_PREFIX + "hive.respect-table-format")
              .put(
                  "hive.immutable-partitions",
                  TRINO_PROPERTIES_PREFIX + "hive.immutable-partitions")
              .put(
                  "hive.insert-existing-partitions-behavior",
                  TRINO_PROPERTIES_PREFIX + "hive.insert-existing-partitions-behavior")
              .put(
                  "hive.target-max-file-size",
                  TRINO_PROPERTIES_PREFIX + "hive.target-max-file-size")
              .put(
                  "hive.create-empty-bucket-files",
                  TRINO_PROPERTIES_PREFIX + "hive.create-empty-bucket-files")
              .put("hive.validate-bucketing", TRINO_PROPERTIES_PREFIX + "hive.validate-bucketing")
              .put(
                  "hive.partition-statistics-sample-size",
                  TRINO_PROPERTIES_PREFIX + "hive.partition-statistics-sample-size")
              .put(
                  "hive.max-partitions-per-writers",
                  TRINO_PROPERTIES_PREFIX + "hive.max-partitions-per-writers")
              .put(
                  "hive.max-partitions-for-eager-load",
                  TRINO_PROPERTIES_PREFIX + "hive.max-partitions-for-eager-load")
              .put(
                  "hive.max-partitions-per-scan",
                  TRINO_PROPERTIES_PREFIX + "hive.max-partitions-per-scan")
              .put("hive.dfs.replication", TRINO_PROPERTIES_PREFIX + "hive.dfs.replication")
              .put("hive.security", TRINO_PROPERTIES_PREFIX + "hive.security")
              .put("security.config-file", TRINO_PROPERTIES_PREFIX + "security.config-file")
              .put(
                  "hive.non-managed-table-writes-enabled",
                  TRINO_PROPERTIES_PREFIX + "hive.non-managed-table-writes-enabled")
              .put(
                  "hive.non-managed-table-creates-enabled",
                  TRINO_PROPERTIES_PREFIX + "hive.non-managed-table-creates-enabled")
              .put(
                  "hive.collect-column-statistics-on-write",
                  TRINO_PROPERTIES_PREFIX + "hive.collect-column-statistics-on-write")
              .put(
                  "hive.file-status-cache-tables",
                  TRINO_PROPERTIES_PREFIX + "hive.file-status-cache-tables")
              .put(
                  "hive.file-status-cache.max-retained-size",
                  TRINO_PROPERTIES_PREFIX + "hive.file-status-cache.max-retained-size")
              .put(
                  "hive.file-status-cache-expire-time",
                  TRINO_PROPERTIES_PREFIX + "hive.file-status-cache-expire-time")
              .put(
                  "hive.per-transaction-file-status-cache.max-retained-size",
                  TRINO_PROPERTIES_PREFIX
                      + "hive.per-transaction-file-status-cache.max-retained-size")
              .put("hive.timestamp-precision", TRINO_PROPERTIES_PREFIX + "hive.timestamp-precision")
              .put(
                  "hive.temporary-staging-directory-enabled",
                  TRINO_PROPERTIES_PREFIX + "hive.temporary-staging-directory-enabled")
              .put(
                  "hive.temporary-staging-directory-path",
                  TRINO_PROPERTIES_PREFIX + "hive.temporary-staging-directory-path")
              .put("hive.hive-views.enabled", TRINO_PROPERTIES_PREFIX + "hive.hive-views.enabled")
              .put(
                  "hive.hive-views.legacy-translation",
                  TRINO_PROPERTIES_PREFIX + "hive.hive-views.legacy-translation")
              .put(
                  "hive.parallel-partitioned-bucketed-writes",
                  TRINO_PROPERTIES_PREFIX + "hive.parallel-partitioned-bucketed-writes")
              .put(
                  "hive.fs.new-directory-permissions",
                  TRINO_PROPERTIES_PREFIX + "hive.fs.new-directory-permissions")
              .put("hive.fs.cache.max-size", TRINO_PROPERTIES_PREFIX + "hive.fs.cache.max-size")
              .put(
                  "hive.query-partition-filter-required",
                  TRINO_PROPERTIES_PREFIX + "hive.query-partition-filter-required")
              .put(
                  "hive.table-statistics-enabled",
                  TRINO_PROPERTIES_PREFIX + "hive.table-statistics-enabled")
              .put("hive.auto-purge", TRINO_PROPERTIES_PREFIX + "hive.auto-purge")
              .put(
                  "hive.partition-projection-enabled",
                  TRINO_PROPERTIES_PREFIX + "hive.partition-projection-enabled")
              .put(
                  "hive.max-partition-drops-per-query",
                  TRINO_PROPERTIES_PREFIX + "hive.max-partition-drops-per-query")
              .put(
                  "hive.single-statement-writes",
                  TRINO_PROPERTIES_PREFIX + "hive.single-statement-writes")

              // Hive performance
              .put(
                  "hive.max-outstanding-splits",
                  TRINO_PROPERTIES_PREFIX + "hive.max-outstanding-splits")
              .put(
                  "hive.max-outstanding-splits-size",
                  TRINO_PROPERTIES_PREFIX + "hive.max-outstanding-splits-size")
              .put(
                  "hive.max-splits-per-second",
                  TRINO_PROPERTIES_PREFIX + "hive.max-splits-per-second")
              .put("hive.max-initial-splits", TRINO_PROPERTIES_PREFIX + "hive.max-initial-splits")
              .put(
                  "hive.max-initial-split-size",
                  TRINO_PROPERTIES_PREFIX + "hive.max-initial-split-size")
              .put("hive.max-split-size", TRINO_PROPERTIES_PREFIX + "hive.max-split-size")

              // S3
              .put("hive.s3.aws-access-key", TRINO_PROPERTIES_PREFIX + "hive.s3.aws-access-key")
              .put("hive.s3.aws-secret-key", TRINO_PROPERTIES_PREFIX + "hive.s3.aws-secret-key")
              .put("hive.s3.iam-role", TRINO_PROPERTIES_PREFIX + "hive.s3.iam-role")
              .put("hive.s3.external-id", TRINO_PROPERTIES_PREFIX + "hive.s3.external-id")
              .put("hive.s3.endpoint", TRINO_PROPERTIES_PREFIX + "hive.s3.endpoint")
              .put("hive.s3.region", TRINO_PROPERTIES_PREFIX + "hive.s3.region")
              .put("hive.s3.storage-class", TRINO_PROPERTIES_PREFIX + "hive.s3.storage-class")
              .put("hive.s3.signer-type", TRINO_PROPERTIES_PREFIX + "hive.s3.signer-type")
              .put("hive.s3.signer-class", TRINO_PROPERTIES_PREFIX + "hive.s3.signer-class")
              .put(
                  "hive.s3.path-style-access",
                  TRINO_PROPERTIES_PREFIX + "hive.s3.path-style-access")
              .put(
                  "hive.s3.staging-directory",
                  TRINO_PROPERTIES_PREFIX + "hive.s3.staging-directory")
              .put(
                  "hive.s3.pin-client-to-current-region",
                  TRINO_PROPERTIES_PREFIX + "hive.s3.pin-client-to-current-region")
              .put("hive.s3.ssl.enabled", TRINO_PROPERTIES_PREFIX + "hive.s3.ssl.enabled")
              .put("hive.s3.sse.enabled", TRINO_PROPERTIES_PREFIX + "hive.s3.sse.enabled")
              .put("hive.s3.sse.type", TRINO_PROPERTIES_PREFIX + "hive.s3.sse.type")
              .put("hive.s3.sse.kms-key-id", TRINO_PROPERTIES_PREFIX + "hive.s3.sse.kms-key-id")
              .put("hive.s3.kms-key-id", TRINO_PROPERTIES_PREFIX + "hive.s3.kms-key-id")
              .put(
                  "hive.s3.encryption-materials-provider",
                  TRINO_PROPERTIES_PREFIX + "hive.s3.encryption-materials-provider")
              .put("hive.s3.upload-acl-type", TRINO_PROPERTIES_PREFIX + "hive.s3.upload-acl-type")
              .put(
                  "hive.s3.skip-glacier-objects",
                  TRINO_PROPERTIES_PREFIX + "hive.s3.skip-glacier-objects")
              .put(
                  "hive.s3.streaming.enabled",
                  TRINO_PROPERTIES_PREFIX + "hive.s3.streaming.enabled")
              .put(
                  "hive.s3.streaming.part-size",
                  TRINO_PROPERTIES_PREFIX + "hive.s3.streaming.part-size")
              .put("hive.s3.proxy.host", TRINO_PROPERTIES_PREFIX + "hive.s3.proxy.host")
              .put("hive.s3.proxy.port", TRINO_PROPERTIES_PREFIX + "hive.s3.proxy.port")
              .put("hive.s3.proxy.protocol", TRINO_PROPERTIES_PREFIX + "hive.s3.proxy.protocol")
              .put(
                  "hive.s3.proxy.non-proxy-hosts",
                  TRINO_PROPERTIES_PREFIX + "hive.s3.proxy.non-proxy-hosts")
              .put("hive.s3.proxy.username", TRINO_PROPERTIES_PREFIX + "hive.s3.proxy.username")
              .put("hive.s3.proxy.password", TRINO_PROPERTIES_PREFIX + "hive.s3.proxy.password")
              .put(
                  "hive.s3.proxy.preemptive-basic-auth",
                  TRINO_PROPERTIES_PREFIX + "hive.s3.proxy.preemptive-basic-auth")
              .put("hive.s3.sts.endpoint", TRINO_PROPERTIES_PREFIX + "hive.s3.sts.endpoint")
              .put("hive.s3.sts.region", TRINO_PROPERTIES_PREFIX + "hive.s3.sts.region")

              // Hive metastore Thrift service authentication
              .put(
                  "hive.metastore.authentication.type",
                  TRINO_PROPERTIES_PREFIX + "hive.metastore.authentication.type")
              .put(
                  "hive.metastore.thrift.impersonation.enabled",
                  TRINO_PROPERTIES_PREFIX + "hive.metastore.thrift.impersonation.enabled")
              .put(
                  "hive.metastore.service.principal",
                  TRINO_PROPERTIES_PREFIX + "hive.metastore.service.principal")
              .put(
                  "hive.metastore.client.principal",
                  TRINO_PROPERTIES_PREFIX + "hive.metastore.client.principal")
              .put(
                  "hive.metastore.client.keytab",
                  TRINO_PROPERTIES_PREFIX + "hive.metastore.client.keytab")

              // HDFS authentication
              .put(
                  "hive.hdfs.authentication.type",
                  TRINO_PROPERTIES_PREFIX + "hive.hdfs.authentication.type")
              .put(
                  "hive.hdfs.impersonation.enabled",
                  TRINO_PROPERTIES_PREFIX + "hive.hdfs.impersonation.enabled")
              .put(
                  "hive.hdfs.trino.principal",
                  TRINO_PROPERTIES_PREFIX + "hive.hdfs.trino.principal")
              .put("hive.hdfs.trino.keytab", TRINO_PROPERTIES_PREFIX + "hive.hdfs.trino.keytab")
              .put(
                  "hive.hdfs.wire-encryption.enabled",
                  TRINO_PROPERTIES_PREFIX + "hive.hdfs.wire-encryption.enabled")
              .build());

  private static final Set<String> JDBC_BACKEND_REQUIRED_PROPERTIES =
      Set.of("jdbc-driver", "uri", "jdbc-user", "jdbc-password");

  private static final Set<String> HIVE_BACKEND_REQUIRED_PROPERTIES = Set.of("uri");

  @Override
  public TreeBidiMap<String, String> engineToGravitinoMapping() {
    return TRINO_ICEBERG_TO_GRAVITINO_ICEBERG;
  }

  @Override
  public Map<String, String> gravitinoToEngineProperties(Map<String, String> properties) {
    Map<String, String> stringStringMap;
    String backend = properties.get("catalog-backend");
    switch (backend) {
      case "hive":
        stringStringMap = buildHiveBackendProperties(properties);
        break;
      case "jdbc":
        stringStringMap = buildJDBCBackendProperties(properties);
        break;
      default:
        throw new UnsupportedOperationException("Unsupported backend type: " + backend);
    }
    stringStringMap.putAll(super.gravitinoToEngineProperties(properties));
    return stringStringMap;
  }

  private Map<String, String> buildHiveBackendProperties(Map<String, String> properties) {
    Set<String> missingProperty =
        Sets.difference(HIVE_BACKEND_REQUIRED_PROPERTIES, properties.keySet());
    if (!missingProperty.isEmpty()) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_MISSING_REQUIRED_PROPERTY,
          "Missing required property for Hive backend: " + missingProperty);
    }

    Map<String, String> hiveProperties = new HashMap<>();
    hiveProperties.put("iceberg.catalog.type", "hive_metastore");
    hiveProperties.put("hive.metastore.uri", properties.get("uri"));
    return hiveProperties;
  }

  private Map<String, String> buildJDBCBackendProperties(Map<String, String> properties) {
    Set<String> missingProperty =
        Sets.difference(JDBC_BACKEND_REQUIRED_PROPERTIES, properties.keySet());
    if (!missingProperty.isEmpty()) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_MISSING_REQUIRED_PROPERTY,
          "Missing required property for JDBC backend: " + missingProperty);
    }

    Map<String, String> jdbcProperties = new HashMap<>();
    jdbcProperties.put("iceberg.catalog.type", "jdbc");
    jdbcProperties.put(
        "iceberg.jdbc-catalog.driver-class",
        properties.get(IcebergConstants.GRAVITINO_JDBC_DRIVER));
    jdbcProperties.put("iceberg.jdbc-catalog.connection-url", properties.get(IcebergConstants.URI));
    jdbcProperties.put(
        "iceberg.jdbc-catalog.connection-user",
        properties.get(IcebergConstants.GRAVITINO_JDBC_USER));
    jdbcProperties.put(
        "iceberg.jdbc-catalog.connection-password",
        properties.get(IcebergConstants.GRAVITINO_JDBC_PASSWORD));
    jdbcProperties.put(
        "iceberg.jdbc-catalog.default-warehouse-dir", properties.get(IcebergConstants.WAREHOUSE));

    jdbcProperties.put(
        "iceberg.jdbc-catalog.catalog-name",
        IcebergPropertiesUtils.getCatalogBackendName(properties));

    return jdbcProperties;
  }
}
