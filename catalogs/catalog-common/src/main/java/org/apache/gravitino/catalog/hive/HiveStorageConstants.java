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
package org.apache.gravitino.catalog.hive;

public class HiveStorageConstants {
  static final String SEQUENCEFILE_INPUT_FORMAT_CLASS =
      "org.apache.hadoop.mapred.SequenceFileInputFormat";
  static final String SEQUENCEFILE_OUTPUT_FORMAT_CLASS =
      "org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat";

  public static final String ORC_INPUT_FORMAT_CLASS =
      "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat";
  public static final String ORC_OUTPUT_FORMAT_CLASS =
      "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat";
  public static final String ORC_SERDE_CLASS = "org.apache.hadoop.hive.ql.io.orc.OrcSerde";

  public static final String PARQUET_INPUT_FORMAT_CLASS =
      "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
  public static final String PARQUET_OUTPUT_FORMAT_CLASS =
      "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat";
  public static final String PARQUET_SERDE_CLASS =
      "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe";

  static final String COLUMNAR_SERDE_CLASS = "org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe";
  static final String RCFILE_INPUT_FORMAT_CLASS = "org.apache.hadoop.hive.ql.io.RCFileInputFormat";
  static final String RCFILE_OUTPUT_FORMAT_CLASS =
      "org.apache.hadoop.hive.ql.io.RCFileOutputFormat";

  static final String AVRO_INPUT_FORMAT_CLASS =
      "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat";
  static final String AVRO_OUTPUT_FORMAT_CLASS =
      "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat";
  static final String AVRO_SERDE_CLASS = "org.apache.hadoop.hive.serde2.avro.AvroSerDe";

  public static final String TEXT_INPUT_FORMAT_CLASS = "org.apache.hadoop.mapred.TextInputFormat";

  public static final String IGNORE_KEY_OUTPUT_FORMAT_CLASS =
      "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat";
  public static final String LAZY_SIMPLE_SERDE_CLASS =
      "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";

  static final String JSON_SERDE_CLASS = "org.apache.hive.hcatalog.data.JsonSerDe";

  public static final String OPENCSV_SERDE_CLASS = "org.apache.hadoop.hive.serde2.OpenCSVSerde";

  static final String REGEX_SERDE_CLASS = "org.apache.hadoop.hive.serde2.RegexSerDe";
}
