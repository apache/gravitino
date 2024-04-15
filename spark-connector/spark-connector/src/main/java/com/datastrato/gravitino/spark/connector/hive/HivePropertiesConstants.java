/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector.hive;

import static com.datastrato.gravitino.catalog.hive.HiveTablePropertiesMetadata.TableType.EXTERNAL_TABLE;

import com.datastrato.gravitino.catalog.hive.HiveTablePropertiesMetadata;
import com.datastrato.gravitino.catalog.hive.HiveTablePropertiesMetadata.StorageFormat;
import com.google.common.annotations.VisibleForTesting;
import org.apache.spark.sql.connector.catalog.TableCatalog;

public class HivePropertiesConstants {
  public static final String GRAVITINO_HIVE_FORMAT = HiveTablePropertiesMetadata.FORMAT;
  public static final String GRAVITINO_HIVE_INPUT_FORMAT = HiveTablePropertiesMetadata.INPUT_FORMAT;
  public static final String GRAVITINO_HIVE_OUTPUT_FORMAT =
      HiveTablePropertiesMetadata.OUTPUT_FORMAT;
  public static final String GRAVITINO_HIVE_SERDE_LIB = HiveTablePropertiesMetadata.SERDE_LIB;
  public static final String GRAVITINO_HIVE_SERDE_PARAMETER_PREFIX =
      HiveTablePropertiesMetadata.SERDE_PARAMETER_PREFIX;

  public static final String GRAVITINO_HIVE_FORMAT_PARQUET = StorageFormat.PARQUET.toString();
  public static final String GRAVITINO_HIVE_FORMAT_SEQUENCEFILE =
      StorageFormat.SEQUENCEFILE.toString();
  public static final String GRAVITINO_HIVE_FORMAT_ORC = StorageFormat.ORC.toString();
  public static final String GRAVITINO_HIVE_FORMAT_RCFILE = StorageFormat.RCFILE.toString();
  public static final String GRAVITINO_HIVE_FORMAT_TEXTFILE = StorageFormat.TEXTFILE.toString();
  public static final String GRAVITINO_HIVE_FORMAT_AVRO = StorageFormat.AVRO.toString();
  public static final String GRAVITINO_HIVE_FORMAT_JSON = StorageFormat.JSON.toString();
  public static final String GRAVITINO_HIVE_FORMAT_CSV = StorageFormat.CSV.toString();
  public static final String GRAVITINO_HIVE_EXTERNAL_TABLE = EXTERNAL_TABLE.name();
  public static final String GRAVITINO_HIVE_TABLE_TYPE = HiveTablePropertiesMetadata.TABLE_TYPE;
  public static final String GRAVITINO_HIVE_TABLE_LOCATION = HiveTablePropertiesMetadata.LOCATION;

  public static final String SPARK_HIVE_STORED_AS = "hive.stored-as";
  public static final String SPARK_HIVE_INPUT_FORMAT = "input-format";
  public static final String SPARK_HIVE_OUTPUT_FORMAT = "output-format";
  public static final String SPARK_HIVE_SERDE_LIB = "serde-lib";
  public static final String SPARK_HIVE_EXTERNAL = "external";
  public static final String SPARK_HIVE_LOCATION = TableCatalog.PROP_LOCATION;

  @VisibleForTesting
  public static final String TEXT_INPUT_FORMAT_CLASS =
      HiveTablePropertiesMetadata.TEXT_INPUT_FORMAT_CLASS;

  @VisibleForTesting
  public static final String IGNORE_KEY_OUTPUT_FORMAT_CLASS =
      HiveTablePropertiesMetadata.IGNORE_KEY_OUTPUT_FORMAT_CLASS;

  @VisibleForTesting
  public static final String LAZY_SIMPLE_SERDE_CLASS =
      HiveTablePropertiesMetadata.LAZY_SIMPLE_SERDE_CLASS;

  @VisibleForTesting
  public static final String PARQUET_INPUT_FORMAT_CLASS =
      HiveTablePropertiesMetadata.PARQUET_INPUT_FORMAT_CLASS;

  @VisibleForTesting
  public static final String PARQUET_OUTPUT_FORMAT_CLASS =
      HiveTablePropertiesMetadata.PARQUET_OUTPUT_FORMAT_CLASS;

  @VisibleForTesting
  public static final String PARQUET_SERDE_CLASS = HiveTablePropertiesMetadata.PARQUET_SERDE_CLASS;

  private HivePropertiesConstants() {}
}
