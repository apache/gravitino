/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector.hive;

import com.datastrato.gravitino.catalog.hive.HiveTablePropertiesMetadata;

public class HivePropertyConstants {
  public static final String GRAVITINO_HIVE_FORMAT = HiveTablePropertiesMetadata.FORMAT;
  public static final String GRAVITINO_HIVE_INPUT_FORMAT = HiveTablePropertiesMetadata.INPUT_FORMAT;
  public static final String GRAVITINO_HIVE_OUTPUT_FORMAT =
      HiveTablePropertiesMetadata.OUTPUT_FORMAT;
  public static final String GRAVITINO_HIVE_SERDE_LIB = HiveTablePropertiesMetadata.SERDE_LIB;
  public static final String GRAVITINO_HIVE_SERDE_PARAMETER_PREFIX =
      HiveTablePropertiesMetadata.SERDE_PARAMETER_PREFIX;

  public static final String SPARK_HIVE_STORED_AS = "hive.stored-as";
  public static final String SPARK_HIVE_INPUT_FORMAT = "input-format";
  public static final String SPARK_HIVE_OUTPUT_FORMAT = "output-format";
  public static final String SPARK_HIVE_SERDE_LIB = "serde-lib";

  public static final String TEXT_INPUT_FORMAT_CLASS =
      HiveTablePropertiesMetadata.TEXT_INPUT_FORMAT_CLASS;
  public static final String IGNORE_KEY_OUTPUT_FORMAT_CLASS =
      HiveTablePropertiesMetadata.IGNORE_KEY_OUTPUT_FORMAT_CLASS;
  public static final String LAZY_SIMPLE_SERDE_CLASS =
      HiveTablePropertiesMetadata.LAZY_SIMPLE_SERDE_CLASS;

  public static final String PARQUET_INPUT_FORMAT_CLASS =
      HiveTablePropertiesMetadata.PARQUET_INPUT_FORMAT_CLASS;
  public static final String PARQUET_OUTPUT_FORMAT_CLASS =
      HiveTablePropertiesMetadata.PARQUET_OUTPUT_FORMAT_CLASS;
  public static final String PARQUET_SERDE_CLASS = HiveTablePropertiesMetadata.PARQUET_SERDE_CLASS;

  private HivePropertyConstants() {}
}
