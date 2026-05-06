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
package org.apache.gravitino.catalog.glue;

import java.time.Instant;
import java.util.Map;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.Order;
import software.amazon.awssdk.services.glue.model.SerDeInfo;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;

/**
 * Runs {@link GlueTableTestBase} scenarios using AWS SDK builders — no network or credentials
 * required.
 */
class TestGlueTableConversion extends GlueTableTestBase {

  private static final String INPUT_FMT = "org.apache.hadoop.mapred.TextInputFormat";
  private static final String OUTPUT_FMT =
      "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat";
  private static final String SERDE = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";
  private static final String LOCATION = "s3://my-bucket/warehouse/";

  @Override
  protected Table provideHiveTable(String schemaName, String tableName) {
    return Table.builder()
        .name(tableName)
        .description("a hive table")
        .tableType("EXTERNAL_TABLE")
        .storageDescriptor(
            StorageDescriptor.builder()
                .columns(
                    Column.builder().name("id").type("bigint").comment("primary key").build(),
                    Column.builder().name("name").type("string").build())
                .location(LOCATION + tableName)
                .inputFormat(INPUT_FMT)
                .outputFormat(OUTPUT_FMT)
                .serdeInfo(SerDeInfo.builder().serializationLibrary(SERDE).build())
                .bucketColumns("id")
                .numberOfBuckets(4)
                .sortColumns(Order.builder().column("name").sortOrder(1).build())
                .build())
        .partitionKeys(Column.builder().name("dt").type("date").build())
        .parameters(Map.of("created_by", "test"))
        .createTime(Instant.now())
        .updateTime(Instant.now())
        .build();
  }

  @Override
  protected Table provideIcebergTable(String schemaName, String tableName) {
    return Table.builder()
        .name(tableName)
        .tableType("EXTERNAL_TABLE")
        .storageDescriptor(StorageDescriptor.builder().build())
        .parameters(
            Map.of(
                GlueConstants.TABLE_FORMAT, "ICEBERG",
                GlueConstants.METADATA_LOCATION, "s3://bucket/path/metadata/v1.metadata.json"))
        .createTime(Instant.now())
        .updateTime(Instant.now())
        .build();
  }

  @Override
  protected Table provideMinimalTable(String schemaName, String tableName) {
    return Table.builder()
        .name(tableName)
        .createTime(Instant.now())
        .updateTime(Instant.now())
        .build();
  }
}
