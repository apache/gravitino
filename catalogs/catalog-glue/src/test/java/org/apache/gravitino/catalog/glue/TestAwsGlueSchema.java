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
import software.amazon.awssdk.services.glue.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.DatabaseInput;
import software.amazon.awssdk.services.glue.model.DeleteDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GlueException;

/**
 * Runs {@link AbstractGlueSchemaTest} scenarios against a real AWS Glue endpoint.
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
 * <p>Each test creates a real Glue database, retrieves it via the API (getting a real serialized
 * response), converts it to a {@link GlueSchema}, and asserts the field mapping. The database is
 * deleted in {@link #cleanup} regardless of test outcome.
 */
@EnabledIfEnvironmentVariable(named = "AWS_ACCESS_KEY_ID", matches = ".+")
class TestAwsGlueSchema extends AbstractGlueSchemaTest {

  private static GlueClient glueClient;
  private static String catalogId;

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
  }

  @Override
  protected Database provideDatabase(String name, String description, Map<String, String> params) {
    CreateDatabaseRequest.Builder req =
        CreateDatabaseRequest.builder()
            .databaseInput(
                DatabaseInput.builder()
                    .name(name)
                    .description(description)
                    .parameters(params)
                    .build());
    if (catalogId != null) {
      req.catalogId(catalogId);
    }
    glueClient.createDatabase(req.build());

    GetDatabaseRequest.Builder getReq = GetDatabaseRequest.builder().name(name);
    if (catalogId != null) {
      getReq.catalogId(catalogId);
    }
    return glueClient.getDatabase(getReq.build()).database();
  }

  @Override
  protected void cleanup(String name) {
    try {
      DeleteDatabaseRequest.Builder req = DeleteDatabaseRequest.builder().name(name);
      if (catalogId != null) {
        req.catalogId(catalogId);
      }
      glueClient.deleteDatabase(req.build());
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
