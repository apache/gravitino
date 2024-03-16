/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector.catalog.iceberg;

import com.datastrato.gravitino.catalog.property.PropertyConverter;
import com.datastrato.gravitino.connector.PropertyEntry;
import com.datastrato.gravitino.trino.connector.GravitinoErrorCode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.trino.spi.TrinoException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.commons.collections4.bidimap.TreeBidiMap;

public class IcebergCatalogPropertyConverter extends PropertyConverter {

  private static final TreeBidiMap<String, String> TRINO_ICEBERG_TO_GRAVITINO_ICEBERG =
      new TreeBidiMap<>(
          new ImmutableMap.Builder<String, String>()
              // General configuration
              .put("iceberg.catalog.type", "iceberg.catalog.type")
              .put("iceberg.file-format", "iceberg.file-format")
              .put("iceberg.compression-codec", "iceberg.compression-codec")
              .put("iceberg.use-file-size-from-metadata", "iceberg.use-file-size-from-metadata")
              .put("iceberg.max-partitions-per-writer", "iceberg.max-partitions-per-writer")
              .put("iceberg.target-max-file-size", "iceberg.target-max-file-size")
              .put("iceberg.unique-table-location", "iceberg.unique-table-location")
              .put(
                  "iceberg.dynamic-filtering.wait-timeout",
                  "iceberg.dynamic-filtering.wait-timeout")
              .put(
                  "iceberg.delete-schema-locations-fallback",
                  "iceberg.delete-schema-locations-fallback")
              .put("iceberg.minimum-assigned-split-weight", "iceberg.minimum-assigned-split-weight")
              .put("iceberg.table-statistics-enabled", "iceberg.table-statistics-enabled")
              .put("iceberg.extended-statistics.enabled", "iceberg.extended-statistics.enabled")
              .put(
                  "iceberg.extended-statistics.collect-on-write",
                  "iceberg.extended-statistics.collect-on-write")
              .put("iceberg.projection-pushdown-enabled", "iceberg.projection-pushdown-enabled")
              .put("iceberg.hive-catalog-name", "iceberg.hive-catalog-name")
              .put(
                  "iceberg.materialized-views.storage-schema",
                  "iceberg.materialized-views.storage-schema")
              .put(
                  "iceberg.materialized-views.hide-storage-table",
                  "iceberg.materialized-views.hide-storage-table")
              .put(
                  "iceberg.register-table-procedure.enabled",
                  "iceberg.register-table-procedure.enabled")
              .put(
                  "iceberg.query-partition-filter-required",
                  "iceberg.query-partition-filter-required")

              // Hive
              .put("hive.config.resources", "hive.config.resources")
              .put("hive.recursive-directories", "hive.recursive-directories")
              .put("hive.ignore-absent-partitions", "hive.ignore-absent-partitions")
              .put("hive.storage-format", "hive.storage-format")
              .put("hive.compression-codec", "hive.compression-codec")
              .put("hive.force-local-scheduling", "hive.force-local-scheduling")
              .put("hive.respect-table-format", "hive.respect-table-format")
              .put("hive.immutable-partitions", "hive.immutable-partitions")
              .put(
                  "hive.insert-existing-partitions-behavior",
                  "hive.insert-existing-partitions-behavior")
              .put("hive.target-max-file-size", "hive.target-max-file-size")
              .put("hive.create-empty-bucket-files", "hive.create-empty-bucket-files")
              .put("hive.validate-bucketing", "hive.validate-bucketing")
              .put("hive.partition-statistics-sample-size", "hive.partition-statistics-sample-size")
              .put("hive.max-partitions-per-writers", "hive.max-partitions-per-writers")
              .put("hive.max-partitions-for-eager-load", "hive.max-partitions-for-eager-load")
              .put("hive.max-partitions-per-scan", "hive.max-partitions-per-scan")
              .put("hive.dfs.replication", "hive.dfs.replication")
              .put("hive.security", "hive.security")
              .put("security.config-file", "security.config-file")
              .put("hive.non-managed-table-writes-enabled", "hive.non-managed-table-writes-enabled")
              .put(
                  "hive.non-managed-table-creates-enabled",
                  "hive.non-managed-table-creates-enabled")
              .put(
                  "hive.collect-column-statistics-on-write",
                  "hive.collect-column-statistics-on-write")
              .put("hive.file-status-cache-tables", "hive.file-status-cache-tables")
              .put(
                  "hive.file-status-cache.max-retained-size",
                  "hive.file-status-cache.max-retained-size")
              .put("hive.file-status-cache-expire-time", "hive.file-status-cache-expire-time")
              .put(
                  "hive.per-transaction-file-status-cache.max-retained-size",
                  "hive.per-transaction-file-status-cache.max-retained-size")
              .put("hive.timestamp-precision", "hive.timestamp-precision")
              .put(
                  "hive.temporary-staging-directory-enabled",
                  "hive.temporary-staging-directory-enabled")
              .put("hive.temporary-staging-directory-path", "hive.temporary-staging-directory-path")
              .put("hive.hive-views.enabled", "hive.hive-views.enabled")
              .put("hive.hive-views.legacy-translation", "hive.hive-views.legacy-translation")
              .put(
                  "hive.parallel-partitioned-bucketed-writes",
                  "hive.parallel-partitioned-bucketed-writes")
              .put("hive.fs.new-directory-permissions", "hive.fs.new-directory-permissions")
              .put("hive.fs.cache.max-size", "hive.fs.cache.max-size")
              .put("hive.query-partition-filter-required", "hive.query-partition-filter-required")
              .put("hive.table-statistics-enabled", "hive.table-statistics-enabled")
              .put("hive.auto-purge", "hive.auto-purge")
              .put("hive.partition-projection-enabled", "hive.partition-projection-enabled")
              .put("hive.max-partition-drops-per-query", "hive.max-partition-drops-per-query")
              .put("hive.single-statement-writes", "hive.single-statement-writes")

              // Hive performance
              .put("hive.max-outstanding-splits", "hive.max-outstanding-splits")
              .put("hive.max-outstanding-splits-size", "hive.max-outstanding-splits-size")
              .put("hive.max-splits-per-second", "hive.max-splits-per-second")
              .put("hive.max-initial-splits", "hive.max-initial-splits")
              .put("hive.max-initial-split-size", "hive.max-initial-split-size")
              .put("hive.max-split-size", "hive.max-split-size")

              // S3
              .put("hive.s3.aws-access-key", "hive.s3.aws-access-key")
              .put("hive.s3.aws-secret-key", "hive.s3.aws-secret-key")
              .put("hive.s3.iam-role", "hive.s3.iam-role")
              .put("hive.s3.external-id", "hive.s3.external-id")
              .put("hive.s3.endpoint", "hive.s3.endpoint")
              .put("hive.s3.region", "hive.s3.region")
              .put("hive.s3.storage-class", "hive.s3.storage-class")
              .put("hive.s3.signer-type", "hive.s3.signer-type")
              .put("hive.s3.signer-class", "hive.s3.signer-class")
              .put("hive.s3.path-style-access", "hive.s3.path-style-access")
              .put("hive.s3.staging-directory", "hive.s3.staging-directory")
              .put("hive.s3.pin-client-to-current-region", "hive.s3.pin-client-to-current-region")
              .put("hive.s3.ssl.enabled", "hive.s3.ssl.enabled")
              .put("hive.s3.sse.enabled", "hive.s3.sse.enabled")
              .put("hive.s3.sse.type", "hive.s3.sse.type")
              .put("hive.s3.sse.kms-key-id", "hive.s3.sse.kms-key-id")
              .put("hive.s3.kms-key-id", "hive.s3.kms-key-id")
              .put("hive.s3.encryption-materials-provider", "hive.s3.encryption-materials-provider")
              .put("hive.s3.upload-acl-type", "hive.s3.upload-acl-type")
              .put("hive.s3.skip-glacier-objects", "hive.s3.skip-glacier-objects")
              .put("hive.s3.streaming.enabled", "hive.s3.streaming.enabled")
              .put("hive.s3.streaming.part-size", "hive.s3.streaming.part-size")
              .put("hive.s3.proxy.host", "hive.s3.proxy.host")
              .put("hive.s3.proxy.port", "hive.s3.proxy.port")
              .put("hive.s3.proxy.protocol", "hive.s3.proxy.protocol")
              .put("hive.s3.proxy.non-proxy-hosts", "hive.s3.proxy.non-proxy-hosts")
              .put("hive.s3.proxy.username", "hive.s3.proxy.username")
              .put("hive.s3.proxy.password", "hive.s3.proxy.password")
              .put("hive.s3.proxy.preemptive-basic-auth", "hive.s3.proxy.preemptive-basic-auth")
              .put("hive.s3.sts.endpoint", "hive.s3.sts.endpoint")
              .put("hive.s3.sts.region", "hive.s3.sts.region")

              // Hive metastore Thrift service authentication
              .put("hive.metastore.authentication.type", "hive.metastore.authentication.type")
              .put(
                  "hive.metastore.thrift.impersonation.enabled",
                  "hive.metastore.thrift.impersonation.enabled")
              .put("hive.metastore.service.principal", "hive.metastore.service.principal")
              .put("hive.metastore.client.principal", "hive.metastore.client.principal")
              .put("hive.metastore.client.keytab", "hive.metastore.client.keytab")

              // HDFS authentication
              .put("hive.hdfs.authentication.type", "hive.hdfs.authentication.type")
              .put("hive.hdfs.impersonation.enabled", "hive.hdfs.impersonation.enabled")
              .put("hive.hdfs.trino.principal", "hive.hdfs.trino.principal")
              .put("hive.hdfs.trino.keytab", "hive.hdfs.trino.keytab")
              .put("hive.hdfs.wire-encryption.enabled", "hive.hdfs.wire-encryption.enabled")
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
    String backend = properties.get("catalog-backend");
    switch (backend) {
      case "hive":
        return buildHiveBackendProperties(properties);
      case "jdbc":
        return buildJDBCBackendProperties(properties);
      default:
        throw new UnsupportedOperationException("Unsupported backend type: " + backend);
    }
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
    jdbcProperties.put("iceberg.jdbc-catalog.driver-class", properties.get("jdbc-driver"));
    jdbcProperties.put("iceberg.jdbc-catalog.connection-url", properties.get("uri"));
    jdbcProperties.put("iceberg.jdbc-catalog.connection-user", properties.get("jdbc-user"));
    jdbcProperties.put("iceberg.jdbc-catalog.connection-password", properties.get("jdbc-password"));
    jdbcProperties.put("iceberg.jdbc-catalog.default-warehouse-dir", properties.get("warehouse"));

    // TODO (FANG) make the catalog name equal to the catalog name in Gravitino
    jdbcProperties.put("iceberg.jdbc-catalog.catalog-name", "jdbc");

    return jdbcProperties;
  }

  @Override
  public Map<String, PropertyEntry<?>> gravitinoPropertyMeta() {
    return ImmutableMap.of();
  }
}
