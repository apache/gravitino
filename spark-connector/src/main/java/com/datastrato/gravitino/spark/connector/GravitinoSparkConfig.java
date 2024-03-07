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

  private GravitinoSparkConfig() {}
}
