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
package org.apache.gravitino.catalog.glue.integration.test;

import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.catalog.glue.GlueConstants;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Runs {@link AbstractGlueCatalogIT} scenarios against a real AWS Glue endpoint.
 *
 * <p>This test is <b>skipped by default</b> and only runs when {@code AWS_ACCESS_KEY_ID} is set.
 * Required environment variables:
 *
 * <ul>
 *   <li>{@code AWS_ACCESS_KEY_ID}
 *   <li>{@code AWS_SECRET_ACCESS_KEY}
 *   <li>{@code AWS_DEFAULT_REGION} (e.g. {@code us-east-1})
 *   <li>{@code GLUE_CATALOG_ID} (12-digit AWS account ID; optional)
 * </ul>
 */
@EnabledIfEnvironmentVariable(named = "AWS_ACCESS_KEY_ID", matches = ".+")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AwsGlueCatalogIT extends AbstractGlueCatalogIT {

  @Override
  protected Map<String, String> catalogConfig() {
    Map<String, String> config = new HashMap<>();
    config.put(
        GlueConstants.AWS_REGION, System.getenv().getOrDefault("AWS_DEFAULT_REGION", "us-east-1"));
    config.put(GlueConstants.AWS_ACCESS_KEY_ID, System.getenv("AWS_ACCESS_KEY_ID"));
    config.put(GlueConstants.AWS_SECRET_ACCESS_KEY, System.getenv("AWS_SECRET_ACCESS_KEY"));
    String catalogId = System.getenv("GLUE_CATALOG_ID");
    if (catalogId != null) {
      config.put(GlueConstants.AWS_GLUE_CATALOG_ID, catalogId);
    }
    return config;
  }
}
