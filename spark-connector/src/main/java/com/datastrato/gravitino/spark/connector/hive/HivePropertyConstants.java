/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector.hive;

public class HivePropertyConstants {
  public static final String GRAVITINO_HIVE_FORMAT = "format";
  public static final String GRAVITINO_HIVE_LOCATION = "location";
  public static final String GRAVITINO_HIVE_TABLE_TYPE = "table-type";
  public static final String GRAVITINO_HIVE_INPUT_FORMAT = "input-format";
  public static final String GRAVITINO_HIVE_OUTPUT_FORMAT = "output-format";
  public static final String GRAVITINO_HIVE_SERDE_LIB = "serde-lib";

  public static final String SPARK_PROVIDER = "provider";
  public static final String SPARK_OPTION_PREFIX = "option.";
  public static final String SPARK_HIVE_STORED_AS = "hive.stored-as";
  public static final String SPARK_HIVE_INPUT_FORMAT = "input-format";
  public static final String SPARK_HIVE_OUTPUT_FORMAT = "output-format";
  public static final String SPARK_HIVE_SERDE_LIB = "serde-lib";

  public static final String TEXT_INPUT_FORMAT_CLASS = "org.apache.hadoop.mapred.TextInputFormat";
  public static final String IGNORE_KEY_OUTPUT_FORMAT_CLASS =
      "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat";
  public static final String LAZY_SIMPLE_SERDE_CLASS =
      "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";

  public static final String PARQUET_INPUT_FORMAT_CLASS =
      "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
  public static final String PARQUET_OUTPUT_FORMAT_CLASS =
      "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat";
  public static final String PARQUET_SERDE_CLASS =
      "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe";

  private HivePropertyConstants() {}
}
