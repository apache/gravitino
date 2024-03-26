/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector;

public class GravitinoSparkConfig {

  private static final String GRAVITINO_PREFIX = "spark.sql.gravitino.";
  public static final String GRAVITINO_URI = GRAVITINO_PREFIX + "uri";
  public static final String GRAVITINO_METALAKE = GRAVITINO_PREFIX + "metalake";
  public static final String GRAVITINO_HIVE_METASTORE_URI = "metastore.uris";
  public static final String SPARK_HIVE_METASTORE_URI = "hive.metastore.uris";

  public static final String LAKEHOUSE_ICEBERG_CATALOG_BACKEND = "catalog-backend";
  public static final String LAKEHOUSE_ICEBERG_CATALOG_TYPE = "type";
  public static final String LAKEHOUSE_ICEBERG_CATALOG_WAREHOUSE = "warehouse";
  public static final String LAKEHOUSE_ICEBERG_CATALOG_URI = "uri";
  public static final String GRAVITINO_JDBC_USER = "jdbc-user";
  public static final String LAKEHOUSE_ICEBERG_CATALOG_JDBC_USER = "jdbc.user";
  public static final String GRAVITINO_JDBC_PASSWORD = "jdbc-password";
  public static final String LAKEHOUSE_ICEBERG_CATALOG_JDBC_PASSWORD = "jdbc.password";
  public static final String LAKEHOUSE_ICEBERG_CATALOG_JDBC_INITIALIZE = "jdbc-initialize";
  public static final String LAKEHOUSE_ICEBERG_CATALOG_JDBC_DRIVER = "jdbc-driver";

  public static final String LAKEHOUSE_ICEBERG_CATALOG_BACKEND_HIVE = "hive";
  public static final String LAKEHOUSE_ICEBERG_CATALOG_BACKEND_JDBC = "jdbc";

  private GravitinoSparkConfig() {}
}
