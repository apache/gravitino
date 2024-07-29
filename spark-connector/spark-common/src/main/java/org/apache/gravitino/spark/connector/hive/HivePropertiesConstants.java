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

package org.apache.gravitino.spark.connector.hive;

import com.google.common.annotations.VisibleForTesting;
import org.apache.gravitino.catalog.hive.HiveConstants;
import org.apache.gravitino.catalog.hive.HiveStorageConstants;
import org.apache.gravitino.catalog.hive.StorageFormat;
import org.apache.gravitino.catalog.hive.TableType;
import org.apache.spark.sql.connector.catalog.TableCatalog;

public class HivePropertiesConstants {
  public static final String GRAVITINO_HIVE_FORMAT = HiveConstants.FORMAT;
  public static final String GRAVITINO_HIVE_INPUT_FORMAT = HiveConstants.INPUT_FORMAT;
  public static final String GRAVITINO_HIVE_OUTPUT_FORMAT = HiveConstants.OUTPUT_FORMAT;
  public static final String GRAVITINO_HIVE_SERDE_LIB = HiveConstants.SERDE_LIB;
  public static final String GRAVITINO_HIVE_SERDE_PARAMETER_PREFIX =
      HiveConstants.SERDE_PARAMETER_PREFIX;

  public static final String GRAVITINO_HIVE_FORMAT_PARQUET = StorageFormat.PARQUET.toString();
  public static final String GRAVITINO_HIVE_FORMAT_SEQUENCEFILE =
      StorageFormat.SEQUENCEFILE.toString();
  public static final String GRAVITINO_HIVE_FORMAT_ORC = StorageFormat.ORC.toString();
  public static final String GRAVITINO_HIVE_FORMAT_RCFILE = StorageFormat.RCFILE.toString();
  public static final String GRAVITINO_HIVE_FORMAT_TEXTFILE = StorageFormat.TEXTFILE.toString();
  public static final String GRAVITINO_HIVE_FORMAT_AVRO = StorageFormat.AVRO.toString();
  public static final String GRAVITINO_HIVE_FORMAT_JSON = StorageFormat.JSON.toString();
  public static final String GRAVITINO_HIVE_FORMAT_CSV = StorageFormat.CSV.toString();
  public static final String GRAVITINO_HIVE_EXTERNAL_TABLE = TableType.EXTERNAL_TABLE.name();
  public static final String GRAVITINO_HIVE_TABLE_TYPE = HiveConstants.TABLE_TYPE;
  public static final String GRAVITINO_HIVE_TABLE_LOCATION = HiveConstants.LOCATION;

  public static final String SPARK_HIVE_STORED_AS = "hive.stored-as";
  public static final String SPARK_HIVE_INPUT_FORMAT = HiveConstants.INPUT_FORMAT;
  public static final String SPARK_HIVE_OUTPUT_FORMAT = HiveConstants.OUTPUT_FORMAT;
  public static final String SPARK_HIVE_SERDE_LIB = HiveConstants.SERDE_LIB;
  public static final String SPARK_HIVE_EXTERNAL = TableCatalog.PROP_EXTERNAL;
  public static final String SPARK_HIVE_LOCATION = TableCatalog.PROP_LOCATION;

  @VisibleForTesting
  public static final String TEXT_INPUT_FORMAT_CLASS = HiveStorageConstants.TEXT_INPUT_FORMAT_CLASS;

  @VisibleForTesting
  public static final String IGNORE_KEY_OUTPUT_FORMAT_CLASS =
      HiveStorageConstants.IGNORE_KEY_OUTPUT_FORMAT_CLASS;

  @VisibleForTesting
  public static final String LAZY_SIMPLE_SERDE_CLASS = HiveStorageConstants.LAZY_SIMPLE_SERDE_CLASS;

  @VisibleForTesting
  public static final String PARQUET_INPUT_FORMAT_CLASS =
      HiveStorageConstants.PARQUET_INPUT_FORMAT_CLASS;

  @VisibleForTesting
  public static final String PARQUET_OUTPUT_FORMAT_CLASS =
      HiveStorageConstants.PARQUET_OUTPUT_FORMAT_CLASS;

  @VisibleForTesting
  public static final String PARQUET_SERDE_CLASS = HiveStorageConstants.PARQUET_SERDE_CLASS;

  private HivePropertiesConstants() {}
}
