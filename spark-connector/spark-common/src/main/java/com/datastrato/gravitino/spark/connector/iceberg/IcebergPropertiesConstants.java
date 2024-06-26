/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector.iceberg;

import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergCatalogPropertiesMetadata;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergTablePropertiesMetadata;
import com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;

public class IcebergPropertiesConstants {
  @VisibleForTesting
  public static final String GRAVITINO_ICEBERG_CATALOG_BACKEND =
      IcebergCatalogPropertiesMetadata.CATALOG_BACKEND;

  static final String ICEBERG_CATALOG_TYPE = CatalogUtil.ICEBERG_CATALOG_TYPE;

  @VisibleForTesting
  public static final String GRAVITINO_ICEBERG_CATALOG_WAREHOUSE =
      IcebergCatalogPropertiesMetadata.WAREHOUSE;

  static final String ICEBERG_CATALOG_WAREHOUSE = CatalogProperties.WAREHOUSE_LOCATION;

  @VisibleForTesting
  public static final String GRAVITINO_ICEBERG_CATALOG_URI = IcebergCatalogPropertiesMetadata.URI;

  static final String ICEBERG_CATALOG_URI = CatalogProperties.URI;

  static final String GRAVITINO_ICEBERG_CATALOG_JDBC_USER =
      IcebergCatalogPropertiesMetadata.GRAVITINO_JDBC_USER;
  static final String ICEBERG_CATALOG_JDBC_USER =
      IcebergCatalogPropertiesMetadata.ICEBERG_JDBC_USER;

  static final String GRAVITINO_ICEBERG_CATALOG_JDBC_PASSWORD =
      IcebergCatalogPropertiesMetadata.GRAVITINO_JDBC_PASSWORD;
  static final String ICEBERG_CATALOG_JDBC_PASSWORD =
      IcebergCatalogPropertiesMetadata.ICEBERG_JDBC_PASSWORD;

  @VisibleForTesting
  public static final String ICEBERG_CATALOG_BACKEND_HIVE = CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE;

  static final String GRAVITINO_ICEBERG_CATALOG_BACKEND_HIVE = "hive";

  static final String GRAVITINO_ICEBERG_CATALOG_BACKEND_JDBC = "jdbc";
  static final String ICEBERG_CATALOG_BACKEND_JDBC = "jdbc";

  @VisibleForTesting
  public static final String ICEBERG_CATALOG_BACKEND_REST = CatalogUtil.ICEBERG_CATALOG_TYPE_REST;

  static final String GRAVITINO_ICEBERG_CATALOG_BACKEND_REST = "rest";

  @VisibleForTesting
  public static final String ICEBERG_LOCATION = IcebergTablePropertiesMetadata.LOCATION;

  @VisibleForTesting
  public static final String ICEBERG_CURRENT_SNAPSHOT_ID =
      IcebergTablePropertiesMetadata.CURRENT_SNAPSHOT_ID;

  @VisibleForTesting
  public static final String ICEBERG_SORT_ORDER = IcebergTablePropertiesMetadata.SORT_ORDER;

  @VisibleForTesting
  public static final String ICEBERG_IDENTIFIER_FIELDS =
      IcebergTablePropertiesMetadata.IDENTIFIER_FIELDS;

  @VisibleForTesting
  public static final String ICEBERG_PROVIDER = IcebergTablePropertiesMetadata.PROVIDER;

  @VisibleForTesting
  public static final String ICEBERG_FILE_FORMAT = IcebergTablePropertiesMetadata.FORMAT;

  @VisibleForTesting
  public static final String ICEBERG_FORMAT_VERSION = IcebergTablePropertiesMetadata.FORMAT_VERSION;

  static final String ICEBERG_CATALOG_CACHE_ENABLED = CatalogProperties.CACHE_ENABLED;

  static final String GRAVITINO_ICEBERG_CATALOG_BACKEND_NAME =
      IcebergCatalogPropertiesMetadata.CATALOG_BACKEND_NAME;

  private IcebergPropertiesConstants() {}
}
