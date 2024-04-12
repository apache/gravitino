/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector.iceberg;

import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergCatalogPropertiesMetadata;
import com.google.common.annotations.VisibleForTesting;

public class IcebergPropertiesConstants {

  @VisibleForTesting
  public static final String GRAVITINO_ICEBERG_CATALOG_BACKEND =
      IcebergCatalogPropertiesMetadata.CATALOG_BACKEND_NAME;

  @VisibleForTesting
  public static final String GRAVITINO_ICEBERG_CATALOG_WAREHOUSE =
      IcebergCatalogPropertiesMetadata.WAREHOUSE;

  @VisibleForTesting
  public static final String GRAVITINO_ICEBERG_CATALOG_URI = IcebergCatalogPropertiesMetadata.URI;

  public static final String GRAVITINO_JDBC_USER =
      IcebergCatalogPropertiesMetadata.GRAVITINO_JDBC_USER;
  public static final String GRAVITINO_ICEBERG_JDBC_USER =
      IcebergCatalogPropertiesMetadata.ICEBERG_JDBC_USER;
  public static final String GRAVITINO_JDBC_PASSWORD =
      IcebergCatalogPropertiesMetadata.GRAVITINO_JDBC_PASSWORD;
  public static final String GRAVITINO_ICEBERG_JDBC_PASSWORD =
      IcebergCatalogPropertiesMetadata.ICEBERG_JDBC_PASSWORD;
  public static final String GRAVITINO_ICEBERG_JDBC_DRIVER =
      IcebergCatalogPropertiesMetadata.GRAVITINO_JDBC_DRIVER;

  public static final String GRAVITINO_ICEBERG_CATALOG_TYPE = "type";
  public static final String GRAVITINO_ICEBERG_CATALOG_BACKEND_HIVE = "hive";
  public static final String GRAVITINO_ICEBERG_CATALOG_BACKEND_JDBC = "jdbc";

  private IcebergPropertiesConstants() {}
}
