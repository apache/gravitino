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

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.DeleteDatabaseRequest;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GlueException;
import software.amazon.awssdk.services.glue.model.Order;
import software.amazon.awssdk.services.glue.model.SerDeInfo;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;

/**
 * Runs {@link GlueTableTestBase} scenarios against a real AWS Glue endpoint.
 *
 * <p>This test is <b>skipped by default</b> and only runs when {@code AWS_ACCESS_KEY_ID} is set. To
 * run it, set the following environment variables:
 *
 * <ul>
 *   <li>{@code AWS_ACCESS_KEY_ID}
 *   <li>{@code AWS_SECRET_ACCESS_KEY}
 *   <li>{@code AWS_DEFAULT_REGION} (e.g. {@code us-east-1})
 *   <li>{@code GLUE_CATALOG_ID} (12-digit AWS account ID; optional)
 * </ul>
 *
 * <p>Each test creates a real Glue table in a pre-created database, retrieves it via the API,
 * converts it to a {@link GlueTable}, and asserts the field mapping. The table (and schema) is
 * deleted in {@link #cleanup} regardless of test outcome.
 */
@EnabledIfEnvironmentVariable(named = "AWS_ACCESS_KEY_ID", matches = ".+")
class TestAwsGlueTableConversion extends GlueTableTestBase {

  private static GlueClient glueClient;
  private static String catalogId;
  private static String testSchemaName;

  private static final String INPUT_FMT = "org.apache.hadoop.mapred.TextInputFormat";
  private static final String OUTPUT_FMT =
      "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat";
  private static final String SERDE = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";
  private static final String LOCATION = "s3://my-bucket/warehouse/";

  @BeforeAll
  static void initClient() {
    Map<String, String> config = new HashMap<>();
    config.put(
        GlueConstants.AWS_REGION, System.getenv().getOrDefault("AWS_DEFAULT_REGION", "us-east-1"));
    String accessKey = System.getenv("AWS_ACCESS_KEY_ID");
    String secretKey = System.getenv("AWS_SECRET_ACCESS_KEY");
    if (accessKey != null && secretKey != null) {
      config.put(GlueConstants.AWS_ACCESS_KEY_ID, accessKey);
      config.put(GlueConstants.AWS_SECRET_ACCESS_KEY, secretKey);
    }
    glueClient = GlueClientProvider.buildClient(config);
    catalogId = System.getenv("GLUE_CATALOG_ID");

    // Create a dedicated test schema once per test class.
    testSchemaName = "aws_glue_table_it_" + System.currentTimeMillis();
    CreateDatabaseRequest.Builder dbReq =
        CreateDatabaseRequest.builder()
            .databaseInput(
                software.amazon.awssdk.services.glue.model.DatabaseInput.builder()
                    .name(testSchemaName)
                    .description("schema for TestAwsGlueTable")
                    .build());
    if (catalogId != null) {
      dbReq.catalogId(catalogId);
    }
    glueClient.createDatabase(dbReq.build());
  }

  @Override
  protected Table provideHiveTable(String schemaName, String tableName) {
    TableInput input =
        TableInput.builder()
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
            .parameters(Map.of("created_by", "aws_glue_table_it"))
            .build();

    CreateTableRequest.Builder req =
        CreateTableRequest.builder().databaseName(testSchemaName).tableInput(input);
    if (catalogId != null) {
      req.catalogId(catalogId);
    }
    glueClient.createTable(req.build());

    return retrieveTable(tableName);
  }

  @Override
  protected Table provideIcebergTable(String schemaName, String tableName) {
    TableInput input =
        TableInput.builder()
            .name(tableName)
            .tableType("EXTERNAL_TABLE")
            .storageDescriptor(StorageDescriptor.builder().build())
            .parameters(
                Map.of(
                    GlueConstants.TABLE_FORMAT, "ICEBERG",
                    GlueConstants.METADATA_LOCATION, "s3://bucket/path/metadata/v1.metadata.json"))
            .build();

    CreateTableRequest.Builder req =
        CreateTableRequest.builder().databaseName(testSchemaName).tableInput(input);
    if (catalogId != null) {
      req.catalogId(catalogId);
    }
    glueClient.createTable(req.build());

    return retrieveTable(tableName);
  }

  @Override
  protected Table provideMinimalTable(String schemaName, String tableName) {
    TableInput input = TableInput.builder().name(tableName).build();

    CreateTableRequest.Builder req =
        CreateTableRequest.builder().databaseName(testSchemaName).tableInput(input);
    if (catalogId != null) {
      req.catalogId(catalogId);
    }
    glueClient.createTable(req.build());

    return retrieveTable(tableName);
  }

  private Table retrieveTable(String tableName) {
    GetTableRequest.Builder getReq =
        GetTableRequest.builder().databaseName(testSchemaName).name(tableName);
    if (catalogId != null) {
      getReq.catalogId(catalogId);
    }
    return glueClient.getTable(getReq.build()).table();
  }

  @Override
  protected void cleanup(String schemaName, String tableName) {
    try {
      DeleteTableRequest.Builder req =
          DeleteTableRequest.builder().databaseName(testSchemaName).name(tableName);
      if (catalogId != null) {
        req.catalogId(catalogId);
      }
      glueClient.deleteTable(req.build());
    } catch (GlueException ignored) {
      // Best-effort cleanup - ignore any AWS errors
    }
  }

  @AfterAll
  static void cleanupSchema() {
    try {
      DeleteDatabaseRequest.Builder dbReq = DeleteDatabaseRequest.builder().name(testSchemaName);
      if (catalogId != null) {
        dbReq.catalogId(catalogId);
      }
      glueClient.deleteDatabase(dbReq.build());
    } catch (GlueException ignored) {
      // Best-effort cleanup - ignore any AWS errors
    }
  }

  @AfterAll
  static void closeClient() {
    if (glueClient != null) {
      glueClient.close();
    }
  }
}
