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

public enum StorageFormat {
  SEQUENCEFILE(
      HiveStorageConstants.SEQUENCEFILE_INPUT_FORMAT_CLASS,
      HiveStorageConstants.SEQUENCEFILE_OUTPUT_FORMAT_CLASS,
      HiveStorageConstants.LAZY_SIMPLE_SERDE_CLASS),
  TEXTFILE(
      HiveStorageConstants.TEXT_INPUT_FORMAT_CLASS,
      HiveStorageConstants.IGNORE_KEY_OUTPUT_FORMAT_CLASS,
      HiveStorageConstants.LAZY_SIMPLE_SERDE_CLASS),
  RCFILE(
      HiveStorageConstants.RCFILE_INPUT_FORMAT_CLASS,
      HiveStorageConstants.RCFILE_OUTPUT_FORMAT_CLASS,
      HiveStorageConstants.COLUMNAR_SERDE_CLASS),
  ORC(
      HiveStorageConstants.ORC_INPUT_FORMAT_CLASS,
      HiveStorageConstants.ORC_OUTPUT_FORMAT_CLASS,
      HiveStorageConstants.ORC_SERDE_CLASS),
  PARQUET(
      HiveStorageConstants.PARQUET_INPUT_FORMAT_CLASS,
      HiveStorageConstants.PARQUET_OUTPUT_FORMAT_CLASS,
      HiveStorageConstants.PARQUET_SERDE_CLASS),
  AVRO(
      HiveStorageConstants.AVRO_INPUT_FORMAT_CLASS,
      HiveStorageConstants.AVRO_OUTPUT_FORMAT_CLASS,
      HiveStorageConstants.AVRO_SERDE_CLASS),
  JSON(
      HiveStorageConstants.TEXT_INPUT_FORMAT_CLASS,
      HiveStorageConstants.IGNORE_KEY_OUTPUT_FORMAT_CLASS,
      HiveStorageConstants.JSON_SERDE_CLASS),
  CSV(
      HiveStorageConstants.TEXT_INPUT_FORMAT_CLASS,
      HiveStorageConstants.IGNORE_KEY_OUTPUT_FORMAT_CLASS,
      HiveStorageConstants.OPENCSV_SERDE_CLASS),
  REGEX(
      HiveStorageConstants.TEXT_INPUT_FORMAT_CLASS,
      HiveStorageConstants.IGNORE_KEY_OUTPUT_FORMAT_CLASS,
      HiveStorageConstants.REGEX_SERDE_CLASS);

  private final String inputFormat;
  private final String outputFormat;
  private final String serde;

  StorageFormat(String inputFormat, String outputFormat, String serde) {
    if (inputFormat == null) throw new RuntimeException("inputFormat must not be null");
    if (outputFormat == null) throw new RuntimeException("outputFormat must not be null");
    if (serde == null) throw new RuntimeException("serde must not be null");
    this.inputFormat = inputFormat;
    this.outputFormat = outputFormat;
    this.serde = serde;
  }

  public String getInputFormat() {
    return inputFormat;
  }

  public String getOutputFormat() {
    return outputFormat;
  }

  public String getSerde() {
    return serde;
  }
}
